import argparse
from .server import Server
from . import DEFAULT_SOCK_PATH, DEFAULT_ROTATE_CHECK_INTERVAL

def main():
    parser = argparse.ArgumentParser(
        prog="python -m offlog",
        description="offlog file-append service",
    )

    parser.add_argument(
        '-n',
        '--notify',
        action='store_true',
        help="""Notify systemd when the server has started up and is listening.""",
    )

    parser.add_argument(
        '-s',
        '--socket-file',
        type=str,
        default=DEFAULT_SOCK_PATH,
        help=f"Path of the Unix socket the server binds to. Default: {DEFAULT_SOCK_PATH}",
    )

    parser.add_argument(
        '-l',
        '--logfile',
        type=str,
        default=None,
        help="""Path of the (optional) log file for the offlog server itself.""",
    )
    parser.add_argument(
        '-r',
        '--rotate-check-interval',
        type=int,
        default=DEFAULT_ROTATE_CHECK_INTERVAL,
        help=f"""How often in milliseconds to check whether the file has been rotated
        (moved/renamed/deleted) and send a message to the client. Default:
        {DEFAULT_ROTATE_CHECK_INTERVAL}""",
    )

    args = parser.parse_args()

    server = Server(
        sock_path=args.socket_file,
        log_path=args.logfile,
        systemd_notify=args.notify,
        rotate_check_interval=args.rotate_check_interval,
    )
    server.connect_shutdown_handler()
    server.run()


if __name__ == '__main__':
    main()
