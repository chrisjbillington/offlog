import argparse
from .server import Server
from . import DEFAULT_CONTROL_SOCK_PATH, DEFAULT_DATA_SOCK_PATH


parser = argparse.ArgumentParser(description="ulog server.")

parser.add_argument(
    '-c',
    '--control-sock',
    type=str,
    default=DEFAULT_CONTROL_SOCK_PATH,
    help=f"Unix sock path for control messages. Default: {DEFAULT_CONTROL_SOCK_PATH}",
)

parser.add_argument(
    '-d',
    '--data-sock',
    type=str,
    default=DEFAULT_DATA_SOCK_PATH,
    help=f"Unix sock path for data messages. Default: {DEFAULT_CONTROL_SOCK_PATH}",
)

parser.add_argument(
    '-l',
    '--server-log-dir',
    type=str,
    default=None,
    help="""Directory for the (optional) log file of the ulog server itself.""",
)

args = parser.parse_args()

server = Server(
    control_sock_path = args.control_sock,
    data_sock_path = args.data_sock,
    log_dir=args.server_log_dir
)
server.connect_shutdown_handler()
server.run()
