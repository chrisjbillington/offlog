import sys
from datetime import datetime
import traceback
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL

from . import DEFAULT_SOCK_PATH, DEFAULT_TIMEOUT
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
        offlog_timeout=DEFAULT_TIMEOUT
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

    def _open(self):
        if self.file_level is not None and self.filepath is not None:
            if self.local_file:
                return open(self.filepath, 'a', encoding='utf8')
            else:
                return ProxyFile(
                    self.filepath,
                    sock_path=self.offlog_socket_path,
                    timeout=self.offlog_timeout,
                )

    def close(self):
        """Close the file. Possibly blocking. Idempotent."""
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
