import os
import sys
import multiprocessing
from errno import EINTR, ECHILD
from socket import socket, AF_INET, SOCK_STREAM, SOMAXCONN
from signal import signal, SIGHUP, SIGTERM, SIGCHLD, SIG_DFL, SIG_IGN
from pyspark.worker import main as worker_main
from pyspark.serializers import write_int

try:
    POOLSIZE = multiprocessing.cpu_count()
except NotImplementedError:
    POOLSIZE = 4

should_exit = False


def worker(listen_sock):
    # Redirect stdout to stderr
    os.dup2(2, 1)

    # Manager sends SIGHUP to request termination of workers in the pool
    def handle_sighup(signum, frame):
        global should_exit
        should_exit = True
    signal(SIGHUP, handle_sighup)

    while not should_exit:
        # Wait until a client arrives or we have to exit
        sock = None
        while not should_exit and sock is None:
            try:
                sock, addr = listen_sock.accept()
            except EnvironmentError as err:
                if err.errno != EINTR:
                    raise

        if sock is not None:
            # Fork a child to handle the client
            if os.fork() == 0:
                # Leave the worker pool
                signal(SIGHUP, SIG_DFL)
                listen_sock.close()
                # Handle the client then exit
                sockfile = sock.makefile()
                worker_main(sockfile, sockfile)
                sockfile.close()
                sock.close()
                os._exit(0)
            else:
                sock.close()

    assert should_exit
    os._exit(0)


def manager():
    # Create a new process group to corral our children
    os.setpgid(0, 0)

    # Create a listening socket on the AF_INET loopback interface
    listen_sock = socket(AF_INET, SOCK_STREAM)
    listen_sock.bind(('127.0.0.1', 0))
    listen_sock.listen(max(1024, 2 * POOLSIZE, SOMAXCONN))
    listen_host, listen_port = listen_sock.getsockname()
    write_int(listen_port, sys.stdout)

    # Launch initial worker pool
    for idx in range(POOLSIZE):
        if os.fork() == 0:
            worker(listen_sock)
            raise RuntimeError("worker() unexpectedly returned")
    listen_sock.close()

    def shutdown():
        global should_exit
        os.kill(0, SIGHUP)
        should_exit = True

    # Gracefully exit on SIGTERM, don't die on SIGHUP
    signal(SIGTERM, lambda signum, frame: shutdown())
    signal(SIGHUP, SIG_IGN)

    # Cleanup zombie children
    def handle_sigchld(signum, frame):
        try:
            pid, status = os.waitpid(0, os.WNOHANG)
            if (pid, status) != (0, 0) and not should_exit:
                raise RuntimeError("pool member crashed: %s, %s" % (pid, status))
        except EnvironmentError as err:
            if err.errno not in (ECHILD, EINTR):
                raise
    signal(SIGCHLD, handle_sigchld)

    # Initialization complete
    sys.stdout.close()
    while not should_exit:
        try:
            # Spark tells us to exit by closing stdin
            if sys.stdin.read() == '':
                shutdown()
        except EnvironmentError as err:
            if err.errno != EINTR:
                shutdown()
                raise


if __name__ == '__main__':
    manager()
