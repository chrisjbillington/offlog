import sys
import os
from pathlib import Path
import socket
import select
import traceback
import signal
import weakref

from . import DEFAULT_SOCK_PATH, Logger

BUFSIZE = 4096
PATH_MAX = os.pathconf('/', 'PC_PATH_MAX')

ERR_PATH_TOO_LONG = b"error: given path longer than PATH_MAX"
ERR_RELPATH = b"error: not an absolute filepath"
OK = b'\0'

logger = None


def _format_exc():
    """Format just the last line of the current exception, not the whole traceback"""
    exc_type, exc_value, _ = sys.exc_info()
    return traceback.format_exception_only(exc_type, exc_value)[0].strip()


class FileHandler:
    _instances = weakref.WeakValueDictionary()

    @classmethod
    def instance(cls, filepath):
        instance = cls._instances.get(filepath)
        if instance is None:
            instance = cls(filepath)
            cls._instances[filepath] = instance
        return instance

    def __init__(self, filepath):
        self.filepath = filepath
        self.clients = set()
        self.file = open(self.filepath, 'ab')

    def new_client(self, client_id):
        self.clients.add(client_id)
        logger.info(
            "New client %d (total: %d) for %s",
            client_id,
            len(self.clients),
            self.filepath,
        )

    def write(self, msg):
        if self.file is not None:
            try:
                self.file.write(msg)
                self.file.flush()
            except (OSError, IOError):
                logger.warning(
                    "Failed to write to %s:\n%s",
                    self.filepath,
                    _format_exc(),
                )
                # Don't keep trying to write
                self.file.close()
                self.file = None

    def client_done(self, client_id):
        if client_id in self.clients:
            self.clients.remove(client_id)
            logger.info(
                "Client %d done (remaining: %d) with %s",
                client_id,
                len(self.clients),
                self.filepath,
            )
            if not self.clients:
                self.close()

    def close(self):
        del self._instances[self.filepath]
        if self.file is not None:
            self.file.close()
            self.file = None
        logger.info("Closed %s", self.filepath)


class Server:
    def __init__(
        self,
        sock_path=DEFAULT_SOCK_PATH,
        log_path=None,
    ):
        # Create a logger for the server itself
        global logger
        logger = Logger(name='ulog', filepath=log_path, local_file=True)

        self.sock_path = Path(sock_path)
        self.sock_path.unlink(missing_ok=True)

        self.listen_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listen_sock.setblocking(0)
        self.listen_sock.bind(str(self.sock_path))

        # A pipe we can use to signal shutdown from a signal handler
        self.selfpipe_reader, self.selfpipe_writer = os.pipe()

        self.next_client_id = 0
        # mapping of fds to client socks:
        self.client_socks = {}
        # Mapping of client socks to client ids
        self.client_ids = {}
        # Mapping of client socks to handlers
        self.handlers = {}

        # Storage for partially received data and size counts
        self.recv_bufs = {}
        self.recv_bufs_nbytes = {}

        self.poller = select.poll()
        self.poller.register(self.listen_sock, select.POLLIN)
        self.poller.register(self.selfpipe_reader, select.POLLIN)

        logger.info("This is ulog server")
        logger.info("Listening on socket %s", self.sock_path)

    def handle_client_connect(self):
        client_sock, _ = self.listen_sock.accept()
        client_sock.setblocking(0)
        self.client_ids[client_sock] = self.next_client_id
        self.client_socks[client_sock.fileno()] = client_sock
        self.poller.register(client_sock, select.POLLIN)
        logger.info("Client %d connected", self.next_client_id)
        self.next_client_id += 1

    def handle_client_disconnect(self, client_sock, initiated_by_us=False):
        client_id = self.client_ids[client_sock]
        if initiated_by_us:
            logger.info("Closing connection with client %d", client_id)
        else:
            logger.info("Client %d disconnected", client_id)
        if client_sock in self.handlers:
            self.handlers[client_sock].client_done(client_id)
            del self.handlers[client_sock]
        if client_sock in self.recv_bufs:
            del self.recv_bufs[client_sock]
            del self.recv_bufs_nbytes[client_sock]
        del self.client_ids[client_sock]
        del self.client_socks[client_sock.fileno()]
        self.poller.unregister(client_sock)
        client_sock.close()

    def process_set_filepath(self, client_id, sock, data):
        msg, null, extradata = data.partition(b'\0')

        if not null:
            # We haven't received the full message. Store for later
            if sock not in self.recv_bufs:
                self.recv_bufs[sock] = []
                self.recv_bufs_nbytes[sock] = 0
            self.recv_bufs[sock].append(msg)
            self.recv_bufs_nbytes[sock] += len(msg)
            if self.recv_bufs_nbytes[sock] > PATH_MAX:
                logger.warning('Client %d error, path too long', client_id)
                return ERR_PATH_TOO_LONG
            return

        # Include any previously read data:
        if sock in self.recv_bufs:
            msg = b''.join(self.recv_bufs[sock]) + msg
            del self.recv_bufs[sock]
            del self.recv_bufs_nbytes[sock]

        filepath = os.fsdecode(msg)
        if len(filepath) > PATH_MAX:
            logger.warning('Client %d error, path too long', client_id)
            return ERR_PATH_TOO_LONG
        if not filepath.startswith('/'):
            logger.warning('Client %d error, not absolute path', client_id)
            return ERR_RELPATH
        try:
            handler = FileHandler.instance(filepath)
        except (OSError, IOError):
            emsg = _format_exc()
            logger.warning(
                'Client %d access denied for %s: \n    %s', client_id, filepath, emsg
            )
            return emsg.encode('utf8')

        self.handlers[sock] = handler
        handler.new_client(client_id)
        if extradata:
            # Client sent through some data to be written without waiting for a
            # response. That's fine, write the data:
            handler.write(extradata)

        logger.info('Client %d access confirmed for %s', client_id, filepath)
        return OK

    def handle_client_data(self, sock):
        client_id = self.client_ids[sock]

        # Read data from the client
        try:
            data = sock.recv(BUFSIZE)
        except ConnectionResetError:
            self.handle_client_disconnect(sock)
            return
        if not data:
            # Client disconnected
            self.handle_client_disconnect(sock)
            return

        # If it already has an open file, write the data:
        handler = self.handlers.get(sock)
        if handler is not None:
            handler.write(data)
        else:
            # Client hasn't set their filepath yet
            response = self.process_set_filepath(client_id, sock, data)
            if response:
                try:
                    sock.send(response)
                    if response != OK:
                        self.handle_client_disconnect(sock, initiated_by_us=True)
                except BlockingIOError:
                    logger.warning('Client %d sock not ready', client_id)
                    self.handle_client_disconnect(sock, initiated_by_us=True)
                except BrokenPipeError:
                    self.handle_client_disconnect(sock)

    def run(self):
        self.listen_sock.listen()
        while True:
            events = self.poller.poll()
            for fd, _ in events:
                if fd == self.listen_sock.fileno():
                    self.handle_client_connect()
                elif fd == self.selfpipe_reader:
                    # Shutting down. We will process remaining data before exiting the
                    # mainloop.
                    logger.info("Interrupted, shutting down")
                    self.shutdown()
                else:
                    # Data from a client:
                    self.handle_client_data(self.client_socks[fd])

            if self.listen_sock.fileno() == -1 and not self.client_socks:
                # Finished shutting down
                logger.info("Exit")
                break

    def shutdown(self):
        os.close(self.selfpipe_reader)
        os.close(self.selfpipe_writer)
        self.poller.unregister(self.selfpipe_reader)

        # Stop accepting new connections
        self.poller.unregister(self.listen_sock)
        self.listen_sock.close()
        self.sock_path.unlink(missing_ok=True)

        # Shutdown sockets for receiving new data. Mainloop will keep running to process
        # remaining data, then exit once there are no more clients with unread data
        # left.
        for client_sock in self.client_socks.values():
            client_sock.shutdown(socket.SHUT_RD)

    def connect_shutdown_handler(self):
        """Handle SIGINT and SIGTERM to shutdown gracefully"""
        shutdown = lambda *_: os.write(self.selfpipe_writer, b'\0')
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)
