import sys
import os
from pathlib import Path
import socket
import select
import traceback
import signal
import weakref
import io

from . import DEFAULT_SOCK_PATH, Logger

BUFSIZE = 4096
PATH_MAX = os.pathconf('/', 'PC_PATH_MAX')

# Possible messages to the client. Other messages are possible, if we catch an exception
# opening or writing to a file and return it to the client.
ERR_PATH_TOO_LONG = b"ValueError: path longer than PATH_MAX"
ERR_NABSPATH = b"ValueError: not an absolute path"
ERR_SHUTDOWN = b"OSError: ulog server exited"
FILE_OK = b"OK"

# Possible states a client session might be in after receiving data from it:
CONNECTION_ACTIVE = 0
CLIENT_CLOSED_CONNECTION = 1
SERVER_TO_CLOSE_CONNECTION = 2

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
        self.file.write(msg)
        self.file.flush()

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


class Session:
    """Class representing a connected client"""

    _next_client_id = 0

    def __init__(self, sock):
        self.id = self.__class__._next_client_id
        self.__class__._next_client_id += 1
        self.sock = sock
        self.sock.setblocking(0)
        self.filehandler = None
        self._recv_buf = io.BytesIO()

    def close(self):
        if self.filehandler is not None:
            self.filehandler.client_done(self.id)
            self.filehandler = None
        self.sock.close()

    def do_send(self, msg):
        # Send null-terminated message to the client without blocking, return None on
        # sucess or disconnect reason on failure.
        try:
            self.sock.sendall(msg + b'\0')
        except BlockingIOError:
            logger.warning('Client %d sock not writeable', self.id)
            return SERVER_TO_CLOSE_CONNECTION
        except BrokenPipeError:
            return CLIENT_CLOSED_CONNECTION

    def do_recv(self):
        # Read data from the client
        try:
            data = self.sock.recv(BUFSIZE)
        except ConnectionResetError:
            return CLIENT_CLOSED_CONNECTION
        if not data:
            return CLIENT_CLOSED_CONNECTION

        # If we already have an open file, write the data:
        if self.filehandler is not None:
            try:
                self.filehandler.write(data)
            except OSError:
                emsg = _format_exc()
                logger.warning(
                    "Failed to write to %s:\n%s", self.filehandler.filepath, emsg
                )
                return self.do_send(emsg.encode('utf8')) or SERVER_TO_CLOSE_CONNECTION
            return CONNECTION_ACTIVE

        # Otherwise we're reading a null-terminated filepath from the client:
        msg, null, extradata = data.partition(b'\0')

        # Add to any previously-received data:
        self._recv_buf.write(msg)

        # Check if we've received too much data:
        if self._recv_buf.getbuffer().nbytes > PATH_MAX:
            logger.warning('Client %d error, path too long', self.id)
            return self.do_send(ERR_PATH_TOO_LONG) or SERVER_TO_CLOSE_CONNECTION

        # If not the full filepath, wait for more data:
        if not null:
            return CONNECTION_ACTIVE

        # We are done reading a filepath
        path = os.fsdecode(self._recv_buf.getvalue())
        self._recv_buf.seek(0)
        self._recv_buf.truncate()

        # Check it's an absolute path:
        if not path.startswith('/'):
            logger.warning('Client %d error, not an absolute path: %s', self.id, path)
            return self.do_send(ERR_NABSPATH) or SERVER_TO_CLOSE_CONNECTION

        # Try opening the file:
        try:
            self.filehandler = FileHandler.instance(path)
        except OSError:
            emsg = _format_exc()
            logger.warning('Client %d access denied for %s:\n%s', self.id, path, emsg)
            return self.do_send(emsg.encode('utf8')) or SERVER_TO_CLOSE_CONNECTION

        logger.info('Client %d access confirmed for %s', self.id, path)
        self.filehandler.new_client(self.id)
        if extradata:
            # Client sent through some data to be written without waiting for a
            # response. That's fine, write the data:
            self.filehandler.write(extradata)

        return self.do_send(FILE_OK) or CONNECTION_ACTIVE


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

        self.poller = select.poll()
        self.poller.register(self.listen_sock, select.POLLIN)
        self.poller.register(self.selfpipe_reader, select.POLLIN)

        # Mapping of socket file descriptors to Session objects for connected clients
        self.clients = {}

        logger.info("This is ulog server")
        logger.info("Listening on socket %s", self.sock_path)

    def handle_client_connect(self):
        client_sock, _ = self.listen_sock.accept()
        client = Session(client_sock)
        self.clients[client_sock.fileno()] = client
        self.poller.register(client.sock, select.POLLIN)
        logger.info("Client %d connected", client.id)

    def handle_client_disconnect(self, client, reason):
        if reason == SERVER_TO_CLOSE_CONNECTION:
            logger.info("Closing connection with client %d", client.id)
        elif reason == CLIENT_CLOSED_CONNECTION:
            logger.info("Client %d disconnected", client.id)
        else:
            raise ValueError(reason)
        del self.clients[client.sock.fileno()]
        self.poller.unregister(client.sock)
        client.close()

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
                    client = self.clients[fd]
                    status = client.do_recv()
                    if status != CONNECTION_ACTIVE:
                        self.handle_client_disconnect(client, status)
            if self.listen_sock.fileno() == -1 and not self.clients:
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
        for client in self.clients.values():
            client.do_send(ERR_SHUTDOWN)  # Ignore errors here if we can't send
            client.sock.shutdown(socket.SHUT_RD)

    def connect_shutdown_handler(self):
        """Handle SIGINT and SIGTERM to shutdown gracefully"""
        shutdown = lambda *_: os.write(self.selfpipe_writer, b'\0')
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)
