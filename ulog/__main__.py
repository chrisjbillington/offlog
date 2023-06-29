import argparse
from .server import Server
from . import DEFAULT_SOCK_PATH

parser = argparse.ArgumentParser(description="ulog server.")

parser.add_argument(
    '-s',
    '--socket-file',
    type=str,
    default=DEFAULT_SOCK_PATH,
    help=f"Path of Unix socket the server binds to. Default: {DEFAULT_SOCK_PATH}",
)

parser.add_argument(
    '-l',
    '--server-log-path',
    type=str,
    default=None,
    help="""Path for the (optional) log file of the ulog server itself.""",
)

args = parser.parse_args()

server = Server(sock_path=args.socket_file, log_path=args.server_log_path)
server.connect_shutdown_handler()
server.run()
