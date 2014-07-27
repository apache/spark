#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import signal
import socket
import sys
import traceback
import multiprocessing
from ctypes import c_bool
from errno import EINTR, ECHILD
from socket import AF_INET, SOCK_STREAM, SOMAXCONN
from signal import SIGHUP, SIGTERM, SIGCHLD, SIG_DFL, SIG_IGN
from pyspark.worker import main as worker_main
from pyspark.serializers import write_int

try:
    POOLSIZE = multiprocessing.cpu_count()
except NotImplementedError:
    POOLSIZE = 4

exit_flag = multiprocessing.Value(c_bool, False)


def should_exit():
    global exit_flag
    return exit_flag.value


def compute_real_exit_code(exit_code):
    # SystemExit's code can be integer or string, but os._exit only accepts integers
    import numbers
    if isinstance(exit_code, numbers.Integral):
        return exit_code
    else:
        return 1


def worker(listen_sock):
    # Redirect stdout to stderr
    os.dup2(2, 1)
    sys.stdout = sys.stderr   # The sys.stdout object is different from file descriptor 1

    # Manager sends SIGHUP to request termination of workers in the pool
    def handle_sighup(*args):
        assert should_exit()
    signal.signal(SIGHUP, handle_sighup)

    # Cleanup zombie children
    def handle_sigchld(*args):
        pid = status = None
        try:
            while (pid, status) != (0, 0):
                pid, status = os.waitpid(0, os.WNOHANG)
        except EnvironmentError as err:
            if err.errno == EINTR:
                # retry
                handle_sigchld()
            elif err.errno != ECHILD:
                raise
    signal.signal(SIGCHLD, handle_sigchld)

    # Blocks until the socket is closed by draining the input stream
    # until it raises an exception or returns EOF.
    def waitSocketClose(sock):
        try:
            while True:
                # Empty string is returned upon EOF (and only then).
                if sock.recv(4096) == '':
                    return
        except:
            pass

    # Handle clients
    while not should_exit():
        # Wait until a client arrives or we have to exit
        sock = None
        while not should_exit() and sock is None:
            try:
                sock, addr = listen_sock.accept()
            except EnvironmentError as err:
                if err.errno != EINTR:
                    raise

        if sock is not None:
            # Fork a child to handle the client.
            # The client is handled in the child so that the manager
            # never receives SIGCHLD unless a worker crashes.
            if os.fork() == 0:
                # Leave the worker pool
                signal.signal(SIGHUP, SIG_DFL)
                signal.signal(SIGCHLD, SIG_DFL)
                listen_sock.close()
                # Read the socket using fdopen instead of socket.makefile() because the latter
                # seems to be very slow; note that we need to dup() the file descriptor because
                # otherwise writes also cause a seek that makes us miss data on the read side.
                infile = os.fdopen(os.dup(sock.fileno()), "a+", 65536)
                outfile = os.fdopen(os.dup(sock.fileno()), "a+", 65536)
                exit_code = 0
                try:
                    worker_main(infile, outfile)
                except SystemExit as exc:
                    exit_code = exc.code
                finally:
                    outfile.flush()
                    # The Scala side will close the socket upon task completion.
                    waitSocketClose(sock)
                    os._exit(compute_real_exit_code(exit_code))
            else:
                sock.close()


def launch_worker(listen_sock):
    if os.fork() == 0:
        try:
            worker(listen_sock)
        except Exception as err:
            traceback.print_exc()
            os._exit(1)
        else:
            assert should_exit()
            os._exit(0)


def manager():
    # Create a new process group to corral our children
    os.setpgid(0, 0)

    # Create a listening socket on the AF_INET loopback interface
    listen_sock = socket.socket(AF_INET, SOCK_STREAM)
    listen_sock.bind(('127.0.0.1', 0))
    listen_sock.listen(max(1024, 2 * POOLSIZE, SOMAXCONN))
    listen_host, listen_port = listen_sock.getsockname()
    write_int(listen_port, sys.stdout)

    # Launch initial worker pool
    for idx in range(POOLSIZE):
        launch_worker(listen_sock)
    listen_sock.close()

    def shutdown():
        global exit_flag
        exit_flag.value = True

    # Gracefully exit on SIGTERM, don't die on SIGHUP
    signal.signal(SIGTERM, lambda signum, frame: shutdown())
    signal.signal(SIGHUP, SIG_IGN)

    # Cleanup zombie children
    def handle_sigchld(*args):
        try:
            pid, status = os.waitpid(0, os.WNOHANG)
            if status != 0 and not should_exit():
                raise RuntimeError("worker crashed: %s, %s" % (pid, status))
        except EnvironmentError as err:
            if err.errno not in (ECHILD, EINTR):
                raise
    signal.signal(SIGCHLD, handle_sigchld)

    # Initialization complete
    sys.stdout.close()
    try:
        while not should_exit():
            try:
                # Spark tells us to exit by closing stdin
                if os.read(0, 512) == '':
                    shutdown()
            except EnvironmentError as err:
                if err.errno != EINTR:
                    shutdown()
                    raise
    finally:
        signal.signal(SIGTERM, SIG_DFL)
        exit_flag.value = True
        # Send SIGHUP to notify workers of shutdown
        os.kill(0, SIGHUP)


if __name__ == '__main__':
    manager()
