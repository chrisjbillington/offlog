import sys
import os
from pathlib import Path
import socket
import select
import traceback
from time import monotonic
from bisect import insort
import signal
from select import PIPE_BUF

from . import DEFAULT_CONTROL_SOCK_PATH, DEFAULT_DATA_SOCK_PATH, Logger

logger = None

# If no client writes to a file in this time, we close it:
FILE_CLOSE_TIMEOUT = 5

ERR_INVALID_COMMAND = b'error: invalid command'
ERR_EMBEDDED_NULLS = b'error: embedded null character in filepath'


def _format_exc():
    """Format just the last line of the current exception, not the whole traceback"""
    exc_type, exc_value, _ = sys.exc_info()
    return traceback.format_exception_only(exc_type, exc_value)[0].strip()


class Task(object):
    def __init__(self, due_in, func, *args, **kwargs):
        """Wrapper for a function call to be executed after a specified time interval.
        due_in is how long in the future, in seconds, the function should be called,
        func is the function to call. All subsequent arguments and keyword arguments
        will be passed to the function."""
        self.due_at = monotonic() + due_in
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.called = False

    def due_in(self):
        """The time interval in seconds until the task is due"""
        return self.due_at - monotonic()

    def __call__(self):
        if self.called:
            raise RuntimeError('Task has already been called')
        self.called = True
        return self.func(*self.args, **self.kwargs)

    def __gt__(self, other):
        # Tasks due sooner are 'greater than' tasks due later. This is necessary for
        # insort() and pop() as used with TaskQueue.
        return self.due_at < other.due_at


class TaskQueue(list):
    """A list of pending tasks due at certain times. Tasks are stored with the soonest
    due at the end of the list, to be removed with pop()"""

    def add(self, task):
        """Insert the task into the queue, maintaining sort order"""
        insort(self, task)

    def next(self):
        """Return the next due task, without removing it from the queue"""
        return self[-1]

    def cancel(self, task):
        self.remove(task)


class FileHandler:
    instances = {}

    def __init__(self, filepath):
        self.filepath = filepath
        self.clients = set()
        self.file = None

    @classmethod
    def instance(cls, filepath):
        if filepath not in cls.instances:
            cls.instances[filepath] = cls(filepath)
        return cls.instances[filepath]

    def write(self, client_id, msg):
        if client_id not in self.clients:
            self.clients.add(client_id)
            logger.info(
                "New client %d (total: %d) for %s",
                client_id,
                len(self.clients),
                self.filepath,
            )
        if self.file is None:
            try:
                self.file = open(self.filepath, 'ab')
            except (OSError, IOError):
                logger.warning(
                    "Failed to open %s:\n    %s", self.filepath, _format_exc()
                )
                return
        if self.file is not None:
            try:
                self.file.write(msg)
                self.file.flush()
            except (OSError, IOError):
                # Nothing to be done.
                return

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
        del self.instances[self.filepath]
        if self.file is not None:
            self.file.close()
            self.file = None
        logger.info("Closed %s", self.filepath)

    @classmethod
    def close_all(cls):
        for instance in list(cls.instances.values()):
            instance.close()


class _DummyClient:
    # Object implementing the methods of Client that ulog.Logger uses, so that we can
    # use ulog.Logger to do logging for e.g. the server itself, without it actually
    # going via the socket machinery.
    def __init__(self):
        self.filepath = None
        self.file = None

    def _open(self, filepath):
        # Open the file, if it's not already open
        filepath = os.path.abspath(filepath)
        if filepath != self.filepath:
            self.filepath = filepath
            if self.file is not None:
                self.file.close()
            self.file = open(self.filepath, 'a', encoding='utf8')

    def check_access(self, filepath):
        self._open(filepath)

    def write(self, filepath, msg):
        self._open(filepath)
        self.file.write(msg)
        self.file.flush()

    def close(self):
        self.file.close()
        self.file = None
        self.filepath = None


class Server:
    def __init__(
        self,
        control_sock_path=DEFAULT_CONTROL_SOCK_PATH,
        data_sock_path=DEFAULT_DATA_SOCK_PATH,
        log_dir=None,
    ):
        # Create a logger for the server itself
        global logger
        if log_dir is not None:
            log_path = os.path.join(log_dir, 'ulog.log')
        else:
            log_path = None
        logger = Logger(name='ulog', filepath=log_path, ulog_client=_DummyClient())

        self.control_sock_path = Path(control_sock_path)
        self.data_sock_path = Path(data_sock_path)

        self.control_sock_path.unlink(missing_ok=True)
        self.data_sock_path.unlink(missing_ok=True)

        self.control_sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.data_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.data_sock.setblocking(0)

        self.control_sock.bind(str(self.control_sock_path))
        self.data_sock.bind(str(self.data_sock_path))

        # A pipe we can use to signal shutdown from a signal handler
        self.selfpipe_reader, self.selfpipe_writer = os.pipe()

        self.next_client_id = 0
        # Mapping of sock fds to sockets
        self.client_socks = {}
        # Mapping of sock fds to client ids
        self.client_ids = {}

        self.poller = select.poll()
        self.poller.register(self.control_sock, select.POLLIN)
        self.poller.register(self.data_sock, select.POLLIN)
        self.poller.register(self.selfpipe_reader, select.POLLIN)

        self.tasks = TaskQueue()
        self.timeout_tasks = {}

        logger.info("This is ulog server")
        logger.info("Listening on control socket %s", self.control_sock_path)
        logger.info("Listening on data socket %s", self.data_sock_path)

    def accept_new_client(self):
        client_sock, _ = self.control_sock.accept()
        client_sock.setblocking(0)
        self.client_ids[client_sock.fileno()] = self.next_client_id
        self.client_socks[client_sock.fileno()] = client_sock
        self.poller.register(client_sock, select.POLLIN)
        logger.info("Client %d connected", self.next_client_id)
        self.next_client_id += 1

    def cmd_hello(self, client_id):
        logger.info("Client %d said hello", client_id)
        return b'welcome\0%d' % client_id

    def cmd_check_access(self, client_id, filepath):
        if b'\0' in filepath:
            return ERR_EMBEDDED_NULLS
        filepath = os.path.abspath(os.fsdecode(filepath))
        try:
            # Check we can open the file in append mode:
            with open(filepath, 'ab') as _:
                pass
            # TODO: handle rotation? If so, check we can create files in given directory
        except (OSError, IOError):
            emsg = _format_exc()
            logger.warning(
                'Client %d access denied for %s: \n    %s', client_id, filepath, emsg
            )
            return emsg.encode('utf8')
        logger.info('Client %d access confirmed for %s', client_id, filepath)
        return b'ok'

    def cmd_done(self, client_id, filepath):
        if b'\0' in filepath:
            return ERR_EMBEDDED_NULLS
        filepath = os.path.abspath(os.fsdecode(filepath))
        self.cancel_timeout(client_id, filepath)
        handler = FileHandler.instance(filepath)
        handler.client_done(client_id)
        return b'ok'

    def handle_control_request(self, sock):
        try:
            msg = sock.recv(PIPE_BUF)
        except ConnectionResetError:
            self.handle_client_disconnect(sock)
            return

        if not msg:
            # Client disconnected
            return self.handle_client_disconnect(sock)

        command, *arg = msg.split(b'\0', 1)
        if arg:
            arg = arg[0]

        client_id = self.client_ids[sock.fileno()]

        if command == b'hello' and not arg:
            response = self.cmd_hello(client_id)
        elif command == b'check_access':
            response = self.cmd_check_access(client_id, arg)
        elif command == b'done':
            response = self.cmd_done(client_id, arg)
        else:
            response = ERR_INVALID_COMMAND

        try:
            sock.send(response)
        except BlockingIOError:
            logger.warning('client %d sock not ready', client_id)
        except BrokenPipeError:
            self.handle_client_disconnect(sock)
            return

    def set_timeout(self, client_id, filepath):
        """Add a task to say the client is done with the file after a timeout"""
        task = Task(FILE_CLOSE_TIMEOUT, self.do_timeout, client_id, filepath)
        self.tasks.add(task)
        self.timeout_tasks[client_id, filepath] = task

    def cancel_timeout(self, client_id, filepath):
        """Cancel the scheduled auto-closing of the file for the client"""
        task = self.timeout_tasks.pop((client_id, filepath), None)
        if task is not None:
            self.tasks.cancel(task)

    def do_timeout(self, client_id, filepath):
        logger.info("Client %d timed out for %s", client_id, filepath)
        handler = FileHandler.instance(filepath)
        handler.client_done(client_id)
        del self.timeout_tasks[client_id, filepath]

    def handle_write_request(self):
        msg = self.data_sock.recv(PIPE_BUF)
        args = msg.split(b'\0', 2)
        # Validate arguments, but we can't reply if they are invalid. We just do nothing
        # in that case.
        if len(args) != 3:
            return
        client_id, filepath, message = args
        try:
            client_id = int(client_id)
        except (ValueError, SyntaxError):
            # Not a valid client id
            return

        filepath = os.path.abspath(os.fsdecode(filepath))
        handler = FileHandler.instance(filepath)
        handler.write(client_id, message)
        self.cancel_timeout(client_id, filepath)
        self.set_timeout(client_id, filepath)

    def handle_client_disconnect(self, sock):
        logger.info("Client %d disconnected", self.client_ids[sock.fileno()])
        self.poller.unregister(sock)
        del self.client_socks[sock.fileno()]
        del self.client_ids[sock.fileno()]
        sock.close()

    def run(self):
        self.control_sock.listen()
        while True:
            if self.tasks:
                timeout = max(0, 1000 * self.tasks.next().due_in())
            else:
                timeout = None
            events = self.poller.poll(timeout)
            if events:
                # A request or message was received:
                for fd, _ in events:
                    if fd == self.control_sock.fileno():
                        self.accept_new_client()
                    elif fd == self.data_sock.fileno():
                        self.handle_write_request()
                    elif fd == self.selfpipe_reader:
                        # Shutting down
                        logger.info("Interrupted, shutting down")
                        self.shutdown()
                        logger.info("Exit")
                        return
                    else:
                        # Some other request from a client sock
                        self.handle_control_request(self.client_socks[fd])
            else:
                # A task is due:
                task = self.tasks.pop()
                task()

    def shutdown(self):
        # Stop accepting new connections
        self.control_sock.close()
        self.control_sock_path.unlink(missing_ok=True)

        # Stop accepting new control requests and close connections with clients:
        for sock in self.client_socks.values():
            sock.close()

        # Stop accepting new write requests
        self.data_sock_path.unlink(missing_ok=True)

        # Process remaining write requests and close data sock:
        while True:
            try:
                self.handle_write_request()
            except BlockingIOError:
                self.data_sock.close()
                break

        # Close files
        FileHandler.close_all()

    def connect_shutdown_handler(self):
        """Handle SIGINT and SIGTERM to shutdown gracefully"""
        shutdown = lambda *_: os.write(self.selfpipe_writer, b'\0')
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)
