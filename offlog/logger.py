import sys
import os
from datetime import datetime
from time import monotonic
import traceback
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL

from . import DEFAULT_SOCK_PATH, DEFAULT_TIMEOUT, DEFAULT_ROLL_CHECK_INTERVAL
from .client import ProxyFile

_level_names = {
    CRITICAL: 'CRITICAL',
    ERROR: 'ERROR',
    WARNING: 'WARNING',
    INFO: 'INFO',
    DEBUG: 'DEBUG',
}


class Logger:
    def __init__(
        self,
        name=None,
        filepath=None,
        file_level=DEBUG,
        stdout_level=INFO,
        stderr_level=WARNING,
        local_file=False,
        offlog_socket_path=DEFAULT_SOCK_PATH,
        offlog_timeout=DEFAULT_TIMEOUT,
        min_roll_check_interval=DEFAULT_ROLL_CHECK_INTERVAL,
    ):
        """Logging object to log to file, stdout and stderr, with optional proxying of
        file writes via a offlog server.

        Records with level `file_level` and above will be written to the given file.
        Records with level `stdout_level` and above, up to but not including
        `stderr_level` will be written to stdout, or with no upper limit if
        `stdout_level` is None. Records with level `stderr_level` and above will be
        written to stderr. Any of these can be set to None to disaable writing to that
        stream. `filepath=None` will also disable file logging.

        if `local_file` is `True`, then an ordinary file will be opened for writing. Otherwise
        `offlog_socket_path` is used to connect to a running offlog server, which will open the
        file for us, writes will be proxied through it. Any blocking operations communicating
        with the server (such as the initial file open, and flushing data at shutdown) will be
        subject to a communications timeout of `offlog_timeout` in milliseconds, default 5000.

        `min_roll_check_interval` is the minimum time in milliseconds in between checks
        for whether the file has vanished off disk, such as if the log file has been
        rolled by an external log rotating program. If this is detected, the file is
        closed and reopened.

        UTF-8 encoding is assumed throughout."""

        self.name = name
        self.filepath = filepath
        self.file_level = file_level
        self.stdout_level = stdout_level
        self.stderr_level = stderr_level
        self.offlog_socket_path = offlog_socket_path
        self.offlog_timeout = offlog_timeout
        self.local_file = local_file
        self.minlevel = min(
            [l for l in [file_level, stdout_level, stderr_level] if l is not None]
        )
        self.file = self._open()
        self._last_roll_check = monotonic()
        self.min_roll_check_interval = min_roll_check_interval

    def _open(self, block=True):
        if self.file_level is not None and self.filepath is not None:
            if self.local_file:
                return open(self.filepath, 'a', encoding='utf8')
            else:
                return ProxyFile(
                    self.filepath,
                    sock_path=self.offlog_socket_path,
                    timeout=self.offlog_timeout,
                    block_open=block
                )

    def _roll_check(self):
        # Check if the file has vanished on disk and if so, close and reopen it. This
        # will block if the unix socket to the server is full, but will not block
        # waiting for the server to confirm data was flushed on close, or waiting for
        # the server to confirm the file was reopened. Thus as long as the server is not
        # lagging so much that our unix socket buffer is full (which is like, 200kB on
        # most systems), this should be non-blocking.
        if self.file is not None:
            now = monotonic()
            if 1000 * (now - self._last_roll_check) > self.min_roll_check_interval:
                self._last_roll_check = now
                if not os.path.exists(self.filepath):
                    # Close and reopen, blocking only if the unix socket to send data is
                    # full - this should not normally happen as it's 200kB large on most
                    # systems. If there's an error reopening the file, it will be raised
                    # at next write attempt
                    self.close(block_send=True, block_close=False)
                    self.file = self._open(block=False)

    def close(self, block_send=True, block_close=True):
        """Close the file. Possibly blocking. Idempotent.

        For the case of a proxied file, optional arguments are (ignored for local file):

        If block_send=True, block for up to self.offlog_timeout (as configured at file
        open time) milliseconds sending unsent data to the server, otherwise only send
        data that can be sent without blocking.

        if block_close=True then wait for a response from the server to confirm all
        previously-sent data was written, and that the file was closed.

        block_send=True and block_close=False is a reasonable arrangement to prevent the
        application blocking due to slow file IO on a small amount of data, whilst
        accepting that it may block if there is so much data that the unix socket's
        buffer is full. This may be useful if the application is closing and reopening
        the file for log rolling purposes (in which case you'd like this to be
        non-blocking unless something is very wrong), wheras at application shutdown you
        probably want block_send=True, block_close=True"""
        if getattr(self, 'file', None) is not None:
            if self.local_file:
                self.file.close()
            else:
                self.file.close(block_send=block_send, block_close=block_close)

    def format(self, level, msg, *args, exc_info=None):
        t = datetime.now().isoformat(sep=' ')[:-3]
        if self.name is not None:
            msg = f"[{t} {self.name} {_level_names[level]}] {msg}\n"
        else:
            msg = f"[{t} {_level_names[level]}] {msg}\n"
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
        self._roll_check()
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
