import sys
import os
import socket
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
from datetime import datetime
import traceback
import select
import builtins
import io


DEFAULT_SOCK_PATH = '/tmp/ulog.sock'
DEFAULT_TIMEOUT = 5000  # ms

_level_names = {
    CRITICAL: 'CRITICAL',
    ERROR: 'ERROR',
    WARNING: 'WARNING',
    INFO: 'INFO',
    DEBUG: 'DEBUG',
}

_BUFSIZE = 4096
_FILE_OK = b"OK" # redefined to avoid circular import


def _make_exception(response):
    # Generate an exception object from a response from the server. Or return None if no
    # response
    try:
        exc_class_name, message = response.decode('utf8').split(': ', 1)
        exc_class = getattr(builtins, exc_class_name, ValueError)
    except ValueError:
        exc_class = ValueError
        message = response
    return exc_class(message)
    # TODO: maybe check what exceptions are actually possible and cover them
    # individually - we want to maintain compat between different Python versions.


class ProxyFile:
    """Object to proxy appending file writes via a running ulog server"""

    def __init__(
        self,
        filepath,
        sock_path=DEFAULT_SOCK_PATH,
        timeout=DEFAULT_TIMEOUT,
    ):
        self.sock_path = sock_path
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.setblocking(0)
        self.poller = select.epoll()
        self.poller.register(self.sock, select.EPOLLIN)
        self._recv_buf = io.BytesIO()
        self._connect()
        self._open(filepath, timeout=timeout)

    def _send(self, data, return_unsent=True):
        # Send as much as we can without blocking. If would block, either return unsent
        # data if `return_unsent` or raise BlockingIOError. If BrokenPipeError, check if
        # the server sent us an error and raise it if so.
        while data:
            try:
                sent = self.sock.send(data)
                data = data[sent:]
            except BlockingIOError:
                if return_unsent:
                    return data
                raise
            except BrokenPipeError:
                # Check if the server responded with an error saying why it booted us:
                response = self._recv_msg(timeout=0)
                if response:
                    raise _make_exception(response) from None
                raise

    def _recv_msg(self, timeout):
        # recv until null byte
        while self.poller.poll(timeout):
            data = self.sock.recv(_BUFSIZE)
            if not data:
                self._recv_buf = io.BytesIO()
                raise EOFError("Server unexpectedly closed connection")
            msg, null, extradata = data.partition(b'\0')
            self._recv_buf.write(msg)
            if null:
                msg = self._recv_buf.getvalue()
                self._recv_buf = io.BytesIO()
                self._recv_buf.write(extradata)
                return msg
        self._recv_buf = io.BytesIO()
        raise TimeoutError("No response from server")

    def _connect(self):
        try:
            self.sock.connect(self.sock_path)
        except FileNotFoundError:
            emsg = f"server socket {self.sock_path} not found"
            raise FileNotFoundError(emsg) from None

    def _open(self, filepath, timeout):
        filepath = os.fsencode(filepath)
        if b'\0' in filepath:
            raise ValueError("embedded null byte in filepath")
        self._send(os.path.abspath(filepath) + b'\0', return_unsent=False)
        response = self._recv_msg(timeout)
        if response != _FILE_OK:
            raise _make_exception(response)

    def write(self, msg):
        data = msg.encode('utf8')
        unsent = self._send(data, return_unsent=True)
        if unsent:
            # TODO: make config to optionally block, drop, or buffer?
            raise BlockingIOError

    def close(self):
        self.sock.close()


class Logger:
    def __init__(
        self,
        name=None,
        filepath=None,
        file_level=DEBUG,
        stdout_level=INFO,
        stderr_level=WARNING,
        local_file=False,
        ulog_socket_path=DEFAULT_SOCK_PATH,
    ):
        """Logging object to log to file, stdout and stderr, with optional proxying of
        file writes via a ulog server.

        Records with level `file_level` and above will be written to the given file.
        Records with level `stdout_level` and above, up to but not including
        `stderr_level` will be written to stdout, or with no upper limit if
        `stdout_level` is None. Records with level `stderr_level` and above will be
        written to stderr. Any of these can be set to None to disaable writing to that
        stream. `filepath=None` will also disable file logging.

        if `local_file` is True, then an ordinary file will be opened for writing.
        Otherwise `ulog_socket_path` is used to connect to a running ulog server, which
        will open the file for us, writes will be proxied through it.

        UTF-8 encoding is assumed throughout."""
        self.name = name
        self.filepath = filepath
        self.file_level = file_level
        self.stdout_level = stdout_level
        self.stderr_level = stderr_level
        self.ulog_socket_path = ulog_socket_path
        self.local_file = local_file
        self.minlevel = min(
            [l for l in [file_level, stdout_level, stderr_level] if l is not None]
        )
        self.file = self._open()

    def _open(self):
        if self.file_level is not None and self.filepath is not None:
            if self.local_file:
                return open(self.filepath, 'a', encoding='utf8')
            else:
                return ProxyFile(self.filepath, sock_path=self.ulog_socket_path)

    def close(self):
        if getattr(self, 'file', None) is not None:
            self.file.close()

    def format(self, level, msg, *args, exc_info=None):
        t = datetime.now().isoformat(sep=' ')[:-3]
        msg = f"[{t} {self.name} {_level_names[level]}] {msg}\n"
        if args:
            msg %= args
        if exc_info:
            if isinstance(exc_info, BaseException):
                exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
            elif not isinstance(exc_info, tuple):
                exc_info = sys.exc_info()
            msg += ''.join(traceback.format_exception(*exc_info))
        return msg

    def log(self, level, msg, *args, exc_info=False):
        if level < self.minlevel:
            return
        msg = self.format(level, msg, *args, exc_info=exc_info)
        if self.file is not None and level >= self.file_level:
            self.file.write(msg)
            if self.local_file:
                self.file.flush()
        if self.stderr_level is not None and level >= self.stderr_level:
            sys.stderr.write(msg)
            sys.stderr.flush()
        elif self.stdout_level is not None and level >= self.stdout_level:
            sys.stdout.write(msg)
            sys.stdout.flush()

    def debug(self, msg, *args, exc_info=False):
        self.log(DEBUG, msg, *args, exc_info=exc_info)

    def info(self, msg, *args, exc_info=False):
        self.log(INFO, msg, *args, exc_info=exc_info)

    def warning(self, msg, *args, exc_info=False):
        self.log(WARNING, msg, *args, exc_info=exc_info)

    def error(self, msg, *args, exc_info=False):
        self.log(ERROR, msg, *args, exc_info=exc_info)

    def exception(self, msg, *args, exc_info=True):
        self.log(ERROR, msg, *args, exc_info=exc_info)

    def critical(self, msg, *args, exc_info=False):
        self.log(CRITICAL, msg, *args, exc_info=exc_info)

    def __del__(self):
        self.close()
