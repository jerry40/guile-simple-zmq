# guile-simple-zmq
Guile wrapper over ZeroMQ library

## Example
A server waits for a client message and answers to it. They executed in parallel in two different Jupyter Notebooks sessions.
Server:
![](guile-zmq-server.png)
Client:
![](guile-zmq-client.png)

## Installation
Discover your guile load paths (some help is here: https://www.gnu.org/software/guile/manual/html_node/Load-Paths.html), then put [simple-zmq.scm](src/simple-zmq.scm) to (%library-dir) or to any path from the %load-path list.

I test it with ZeroMQ-4.2.1:
```
$ wget https://github.com/zeromq/libzmq/releases/download/v4.2.1/zeromq-4.2.1.tar.gz
$ tar xvf zeromq-4.2.1.tar.gz
$ cd zeromq-4.2.1/
$ ./configure
$ make
$ sudo make install
```
