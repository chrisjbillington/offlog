offlog 0.1
----------

`offlog` is a non-blocking file-append service for Unix systems. It consists of a server
program and client library that interact via a Unix domain socket, allowing applications
to offload file appends to the server, mitigating the risk of application latency caused
by blocking file I/O.
