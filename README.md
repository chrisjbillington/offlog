# offlog 0.1


`offlog` is a non-blocking file-append service for Unix systems. It consists of a server
program and Python client library that interact via a Unix domain socket, allowing
applications to offload file appends to the server and mitigating the risk of
application latency caused by blocking file I/O. It has a focus on performance,
specifically on minimising overhead for the client. 

Both client and server are written in Python, but the protocol is not Python-specific so
other clients could be written as well. This package has no dependencies outside the
Python standard library.

## install

Install to your virtualenv or other Python environment using `pip`
```bash
pip install offlog
```

Or build from source
```bash
git clone https://github.com/chrisjbillington/offlog
cd offlog
pip install setuptools wheel
pip install .
```

Or build a wheel
```
git clone https://github.com/chrisjbillington/offlog
cd offlog
pip install setuptools wheel build
python -m build .
```

## example

Example:
```python
# example client code example.py
from offlog import Logger
logger = Logger("my app", '/tmp/myapp.log')
logger.info("Hello world, params are %s %d", "foo", 7)
try:
    1/0
except Exception:
    logger.exception("Got an error")
```

Start the the server in one terminal, and then run the client in another. You'll see the
following:

```shell
$ python -m offlog
[2023-07-03 10:12:21.895 offlog INFO] This is offlog server
[2023-07-03 10:12:21.895 offlog INFO] Listening on socket /tmp/offlog.sock
[2023-07-03 10:12:54.796 offlog INFO] Client 0 connected
[2023-07-03 10:12:54.796 offlog INFO] Client 0 access confirmed for /tmp/myapp.log
[2023-07-03 10:12:54.796 offlog INFO] New client 0 (total: 1) for /tmp/myapp.log
[2023-07-03 10:12:54.799 offlog INFO] Client 0 disconnected
[2023-07-03 10:12:54.799 offlog INFO] Client 0 done (remaining: 0) with /tmp/myapp.log
[2023-07-03 10:12:54.799 offlog INFO] Closed /tmp/myapp.log
```

```shell
$ python example.py
[2023-07-03 10:12:54.796 my app INFO] Hello world, params are foo 7
[2023-07-03 10:12:54.796 my app ERROR] Got an error
Traceback (most recent call last):
  File "/path/to/example.py", line 6, in <module>
    1/0
    ~^~
ZeroDivisionError: division by zero
```

And the contents of `/tmp/myapp.log` will be:
```shell
$ cat /tmp/myapp.log
[2023-07-03 10:12:54.796 my app INFO] Hello world, params are foo 7
[2023-07-03 10:12:54.796 my app ERROR] Got an error
Traceback (most recent call last):
  File "/path/to/example.py", line 6, in <module>
    1/0
    ~^~
ZeroDivisionError: division by zero
```

## Details and notes


The `Logger()` class by default outputs `DEBUG` and higher to file, `INFO` to stdout,
and `ERROR` and higher to stderr. These are configurable. Formatting and exception
options are similar to the Python standard library.

The focus of this library is more on low-overhead file appends than the `Logger()`
class, which is very basic. It is easily subclassable if you want to change some of its
behaviour, or you could feasibly plug the `FileProxy()` object that sends data to the
`offlog` server into some other logging framework. You may also use the `FileProxy`
object directly to proxy file appends, including of binary data, to a file.

Despite being offloaded to another process, all file appends are still reliable, in the
sense that all data will eventually be written if the server and client shut down in
normal ways. This includes if the client encounters a Python exception, but does not
include if the client, e.g. segfaults or is terminated via SIGTERM and Python's default
SIGTERM handler (which exits immediately without doing any normal Python cleanup).

The server does flush files after every write, so when things are running normally, logs
are written in real-time. The only reason they might be delayed is when the system is
under load and messages are getting backed up - in which case clients will not retry
sending queued data until the next time they have new data to send, or until program
shutdown, whichever comes first.

There is no facility for global look up of loggers. Pass them around yourself. They are
not thread-safe, but if you instantiate one per thread pointing to the same file, they
will both talk to server which will interleave their appends. But their data won't be
guaranteed to be in the same order or even have the same boundaries as they were fired
off in your application.

Instantiating a FileProxy is blocking - the server will verify it can open the file and
we wait for it to confirm. Writing data is not ever blocking, and for now data that
fails to send because the server is too busy will just be locally buffered indefinitely
(you can still run out of memory). Unsent data will be retried before sending new data
on subsequent calls to `write()`. At shutdown, all unsent data will attempted to be
flushed to the server, this is also blocking.

If the server is shut down or encounters an exception writing data on the client's
behalf, it will terminate the connection with the client. Upon the next attempt to write
the client will get a BrokenPipeError and read the exception message from the server,
raising it as an exception in the client code.

## Client documentation

### Logger

```python
offlog.Logger(
    name=None,
    filepath=None,
    file_level=DEBUG,
    stdout_level=INFO,
    stderr_level=WARNING,
    local_file=False,
    offlog_socket_path='/tmp/offlog.sock',
    offlog_timeout=5000
)
```

Logging object to log to file, `stdout` and `stderr`, with optional proxying of file
writes via a offlog server.

Records with level `file_level` and above will be written to the given file. Records
with level `stdout_level` and above, up to but not including `stderr_level` will be
written to stdout, or with no upper limit if `stdout_level` is None. Records with level
`stderr_level` and above will be written to stderr. Any of these can be set to None to
disaable writing to that stream. `filepath=None` will also disable file logging.

if `local_file` is `True`, then an ordinary file will be opened for writing. Otherwise
`offlog_socket_path` is used to connect to a running offlog server, which will open the
file for us, writes will be proxied through it. Any blocking operations communicating
with the server (such as the initial file open, and flushing data at shutdown) will be
subject to a communications timeout of `offlog_timeout` in milliseconds, default 5000.

UTF-8 encoding is assumed throughout.

```python
offlog.Logger.close()
```
Close the file. Possibly blocking. Idempotent.

Logging methods work similarly to the Python standard library:

```python
offlog.Logger.log(level, msg, *args, exc_info=False):

offlog.Logger.debug(msg, *args, exc_info=False):

offlog.Logger.info(msg, *args, exc_info=False):

offlog.Logger.warning(msg, *args, exc_info=False):

offlog.Logger.error(msg, *args, exc_info=False):

offlog.Logger.exception(msg, *args, exc_info=True)

offlog.Logger.critical(msg, *args, exc_info=False)
```


### ProxyFile

```python
offlog.ProxyFile(
    filepath,
    sock_path='/tmp/offlog.sock',
    timeout=5000,
)
```

Object to proxy appending file writes via a running offlog server.

```python
offlog.ProxyFile.write( data):
```
Send as much as we can without blocking. If sending would block, queue unsent data for
later. On `BrokenPipeError`, check if the server sent us an error and raise it if so.
This method will always attempt to re-send previously-queued data before attempting to
send new data.

```python
offlog.ProxyFile.close():
```
Close the socket. Attempt to send all queued unsent data to the server and
cleanly close the connection to it, raising exceptions if anything goes wrong

## Server documentation

```shell
$ python -m offlog -h
usage: python -m offlog [-h] [-n] [-s SOCKET_FILE] [-l LOGFILE]

offlog file-append service

options:
  -h, --help            show this help message and exit
  -n, --notify          Notify systemd when the server has started up and is
                        listening.
  -s SOCKET_FILE, --socket-file SOCKET_FILE
                        Path of the Unix socket the server binds to. Default:
                        /tmp/offlog.sock
  -l LOGFILE, --logfile LOGFILE
                        Path of the (optional) log file for the offlog server
                        itself.
```

You may wish to start the service as a systemd service. Here is an example unit file:
```ini
# offlog.service
[Unit]
Description=offlog file-append service
After=network.target

[Service]
Type=notify
ExecStart=/path/to/your/.venv/bin/python -u -m offlog --notify
Restart=always
RestartSec=5s
User=<user_to_run_as>

[Install]
WantedBy=multi-user.target
```

Note: the server will run with permissions of the specific user, which means all file
operations will be performed as that user. The server's unix socket file will be created
with default access permissions of that user, and thus only users will permission to
access that socket will be able to communicate with the server. This means by default,
clients will not be able to access an offlog server running as root in order to write to
files they would not have otherwise had permission to write to.

To avoid race conditions in systemd starting other services that depend on the offlog
server, ensure you use the `--notify` flag to the offlog server, which will notify
systemd when the server's socket is bound and it is ready to accept clients. Mark other
units that require the offlog server by adding `Requires` and `After` lines to the
`[Unit]` section of their service files:

```ini
[Unit]
Description=Some other service that needs an offlog server
...
Requires=offlog.service
After=offlog.service
```

The server shuts down cleanly upon `SIGINT` (i.e. pressing ctrl-C when run from a
terminal) or `SIGTERM` (i.e. what systemd will send by default if you run `systemctl
stop`, or at shutdown). It will close all client connections, and write any data clients
have already sent to files. Running clients will get an exception upon their next
attempt to write data.

protocol
========

TODO - document the protocol

