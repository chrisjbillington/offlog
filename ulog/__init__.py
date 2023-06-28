import sys
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
from datetime import datetime
import traceback

DEFAULT_CONTROL_SOCK_PATH = '/tmp/ulog-control.sock'
DEFAULT_DATA_SOCK_PATH = '/tmp/ulog-data.sock'
DEFAULT_TIMEOUT = 5000  # ms

from .client import Client

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
        ulog_control_socket_path=DEFAULT_CONTROL_SOCK_PATH,
        ulog_data_socket_path=DEFAULT_DATA_SOCK_PATH,
        ulog_client=None
    ):
        """Logging object to log to file, stdout and stderr. All records with level
        `file_level` and higher will be logged to the given file. All records with level
        `stdout_level` or higher, up to but not including `stderr_level` will be logged
        to stdout, or with no upper limit if `stdout_level` is None. All records with
        level `stderr_level` or higher will be logged to stderr. Any of these can be set
        to None to not log to that stream. `filepath=None` will also disable file
        logging.

        Pass in an existing `ulog.Client` as `ulog_client`, otherwise one will be
        instantiated for connecting to a server via the given socket paths."""
        self.name = name
        self.filepath = filepath
        self.stdout_level = stdout_level
        self.stderr_level = stderr_level
        self.file_level = file_level
        self.minlevel = min(
            [l for l in [file_level, stdout_level, stderr_level] if l is not None]
        )
        if ulog_client is not None:
            self.client = ulog_client
        else:
            self.client = Client.instance(
                control_sock_path=ulog_control_socket_path,
                data_sock_path=ulog_data_socket_path,
            )
        if self.filepath is not None:
            self.client.check_access(self.filepath)

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
        if (
            self.filepath is not None
            and self.file_level is not None
            and level >= self.file_level
        ):
            self.client.write(self.filepath, msg)
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
