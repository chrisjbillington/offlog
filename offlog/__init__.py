__version__ = '0.2.0'
DEFAULT_SOCK_PATH = '/tmp/offlog.sock'
DEFAULT_TIMEOUT = 5000  # ms
DEFAULT_ROLL_CHECK_INTERVAL = 5000 # ms

from .client import ProxyFile
from .logger import Logger

