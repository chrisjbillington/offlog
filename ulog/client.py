import os
import socket
import select
import builtins
import weakref
from select import PIPE_BUF
from . import DEFAULT_CONTROL_SOCK_PATH, DEFAULT_DATA_SOCK_PATH, DEFAULT_TIMEOUT



class Client:
    _instances = weakref.WeakValueDictionary()

    def __init__(
        self,
        control_sock_path=DEFAULT_CONTROL_SOCK_PATH,
        data_sock_path=DEFAULT_DATA_SOCK_PATH,
    ):
        self.control_sock_path = control_sock_path
        self.data_sock_path = data_sock_path
        self.control_sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.data_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.control_sock.setblocking(0)
        self.data_sock.setblocking(0)
        self.poller = select.poll()
        self.poller.register(self.control_sock, select.POLLIN)
        self.client_id = self._connect()

    @classmethod
    def instance(
        cls,
        control_sock_path=DEFAULT_CONTROL_SOCK_PATH,
        data_sock_path=DEFAULT_DATA_SOCK_PATH,
    ):
        key = (control_sock_path, data_sock_path)
        instance = cls._instances.get(key)
        if instance is None:
            instance = cls(
                control_sock_path=control_sock_path, data_sock_path=data_sock_path
            )
            cls._instances[key] = instance
        return instance

    def _sendrcv(self, msg, timeout=DEFAULT_TIMEOUT):
        if len(msg) > PIPE_BUF:
            raise ValueError(f"Message too long ({len(msg)} > {PIPE_BUF})")
        self.control_sock.send(msg)
        events = self.poller.poll(timeout)
        if not events:
            raise TimeoutError("No response from server")
        return self.control_sock.recv(PIPE_BUF)

    def _connect(self, timeout=DEFAULT_TIMEOUT):
        try:
            self.control_sock.connect(self.control_sock_path)
        except FileNotFoundError:
            # Server is not running
            raise FileNotFoundError(
                f"Server control socket {self.control_sock_path} not found"
            ) from None
        response = self._sendrcv(b'hello', timeout).split(b'\0')
        if response[0] == b'welcome' and len(response) == 2:
            return response[1]
        else:
            raise ValueError(f'Invalid response from server: {response}')

    def check_access(self, filepath, timeout=DEFAULT_TIMEOUT):
        """Send a message to the logging server, asking it to check that it can open the
        log file in append mode. Raises an exception if the file cannot be opened."""
        filepath = os.fsencode(filepath)
        response = self._sendrcv(b'check_access\0%s' % filepath, timeout=timeout)
        if response == b'ok':
            return
        # Raise the exception returned by the server:
        exc_class_name, message = response.decode('utf8').split(': ', 1)
        exc_class = getattr(builtins, exc_class_name, ValueError)
        raise exc_class(message)

    def write(self, filepath, msg):
        msg = msg.encode('utf8')
        header = b'%s\0%s\0' % (self.client_id, os.fsencode(filepath))
        # Break into 4096-byte chunks if needed
        step = PIPE_BUF - len(header)
        for i in range(0, len(msg), step):
            chunk = header + msg[i : i + step]
            try:
                self.data_sock.sendto(chunk, self.data_sock_path)
            except BlockingIOError:
                # TODO: make config that optionally blocks, drops or buffers
                raise
            except FileNotFoundError:
                # Server has stopped running
                raise FileNotFoundError(
                    f"Server data socket {self.data_sock_path} vanished"
                ) from None


# Thoughts: Send data on the same sock, don't have separate socks. simplify protocol -
# first msg from client is just a filepath, that's opening the file. subsequent messages
# are data to be written to the file. Closing socket means close file. Server doesn't
# need timers. I wonder if SOCK_STREAM could do it? That way we wouldn't need to chunk
# the data up, and the server would be able to distinguish between EOF and an empty
# datagram. Maybe test this. Maybe just never send an empty datagram. Turns out
# SOCK_STREAM is extremely fast, let's use that instead. Use sendall? Are writes
# guaranteed to suceed in full if fd is writeable and size is < PIPE_BUF? Are small
# writes atomic? No.

# how about this? Client opens connection, server reads until it gets a null, preceding
# is filepath, everything else is data.
