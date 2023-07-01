import os
import builtins
import io
import socket
import select

from . import DEFAULT_TIMEOUT, DEFAULT_SOCK_PATH

BUFSIZE = 4096
FILE_OK = b"OK"
GOODBYE = b"BYE"


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


class ProxyFile:
    """Object to proxy appending file writes via a running ulog server"""

    def __init__(
        self,
        filepath,
        sock_path=DEFAULT_SOCK_PATH,
        timeout=DEFAULT_TIMEOUT,
    ):
        self.timeout = timeout
        self.sock_path = sock_path
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.setblocking(0)
        self.poller = select.epoll()
        self.poller.register(self.sock, select.EPOLLIN)
        self._recv_buf = io.BytesIO()
        self._connect()
        self._open(filepath)

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

    def _recv_msg(self, timeout=None):
        # recv until null byte. If timeout or EOF, raise but leave data read so far in
        # self._recv_buf.
        if timeout is None:
            timeout = self.timeout
        while self.poller.poll(timeout):
            data = self.sock.recv(BUFSIZE)
            msg, null, extradata = data.partition(b'\0')
            self._recv_buf.write(msg)
            if not data:
                raise EOFError
            if null:
                msg = self._recv_buf.getvalue()
                self._recv_buf = io.BytesIO()
                self._recv_buf.write(extradata)
                return msg
        raise TimeoutError("No response from server")

    def _connect(self):
        try:
            self.sock.connect(self.sock_path)
        except FileNotFoundError:
            emsg = f"server socket {self.sock_path} not found"
            raise FileNotFoundError(emsg) from None

    def _open(self, filepath):
        filepath = os.fsencode(filepath)
        if b'\0' in filepath:
            raise ValueError("embedded null byte in filepath")
        self._send(os.path.abspath(filepath) + b'\0', return_unsent=False)
        response = self._recv_msg(self.timeout)
        if response != FILE_OK:
            raise _make_exception(response)

    def write(self, msg):
        data = msg.encode('utf8')
        unsent = self._send(data, return_unsent=True)
        if unsent:
            # TODO: make config to optionally block, drop, or buffer?
            raise BlockingIOError

    def close(self):
        try:
            # TODO: send all unsent data
            self.sock.shutdown(socket.SHUT_WR)
            response = self._recv_msg(self.timeout)
            if response != GOODBYE:
                raise _make_exception(response)
        finally:
            self.sock.close()
