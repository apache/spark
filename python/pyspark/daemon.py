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

import numbers
import os
import signal
import select
import socket
import sys
import traceback
from errno import EINTR, ECHILD
from socket import AF_INET, SOCK_STREAM, SOMAXCONN
from signal import SIGHUP, SIGTERM, SIGCHLD, SIG_DFL, SIG_IGN
from pyspark.worker import main as worker_main
from pyspark.serializers import read_int, write_int


def compute_real_exit_code(exit_code):
    # SystemExit's code can be integer or string, but os._exit only accepts integers
    if isinstance(exit_code, numbers.Integral):
        return exit_code
    else:
        return 1


def worker(sock):
    """
    Called by a worker process after the fork().
    """
    # Redirect stdout to stderr
    os.dup2(2, 1)
    sys.stdout = sys.stderr  # The sys.stdout object is different from file descriptor 1

    signal.signal(SIGHUP, SIG_DFL)
    signal.signal(SIGCHLD, SIG_DFL)
    signal.signal(SIGTERM, SIG_DFL)

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

    # Read the socket using fdopen instead of socket.makefile() because the latter
    # seems to be very slow; note that we need to dup() the file descriptor because
    # otherwise writes also cause a seek that makes us miss data on the read side.
    infile = os.fdopen(os.dup(sock.fileno()), "a+", 65536)
    outfile = os.fdopen(os.dup(sock.fileno()), "a+", 65536)
    exit_code = 0
    try:
        # Acknowledge that the fork was successful
        write_int(os.getpid(), outfile)
        outfile.flush()
        worker_main(infile, outfile)
    except SystemExit as exc:
        exit_code = exc.code
    finally:
        outfile.flush()
        # The Scala side will close the socket upon task completion.
        waitSocketClose(sock)
        os._exit(compute_real_exit_code(exit_code))


def manager():
    # Create a new process group to corral our children
    os.setpgid(0, 0)

    # Create a listening socket on the AF_INET loopback interface
    listen_sock = socket.socket(AF_INET, SOCK_STREAM)
    listen_sock.bind(('127.0.0.1', 0))
    listen_sock.listen(max(1024, SOMAXCONN))
    listen_host, listen_port = listen_sock.getsockname()
    write_int(listen_port, sys.stdout)

    def shutdown(code):
        signal.signal(SIGTERM, SIG_DFL)
        # Send SIGHUP to notify workers of shutdown
        os.kill(0, SIGHUP)
        exit(code)

    def handle_sigterm(*args):
        shutdown(1)
    signal.signal(SIGTERM, handle_sigterm)  # Gracefully exit on SIGTERM
    signal.signal(SIGHUP, SIG_IGN)  # Don't die on SIGHUP

    # Cleanup zombie children
    def handle_sigchld(*args):
        try:
            pid, status = os.waitpid(0, os.WNOHANG)
            if status != 0:
                msg = "worker %s crashed abruptly with exit status %s" % (pid, status)
                print >> sys.stderr, msg
        except EnvironmentError as err:
            if err.errno not in (ECHILD, EINTR):
                raise
    signal.signal(SIGCHLD, handle_sigchld)

    # Initialization complete
    sys.stdout.close()
    try:
        while True:
            try:
                ready_fds = select.select([0, listen_sock], [], [])[0]
            except select.error as ex:
                if ex[0] == EINTR:
                    continue
                else:
                    raise
            if 0 in ready_fds:
                try:
                    worker_pid = read_int(sys.stdin)
                except EOFError:
                    # Spark told us to exit by closing stdin
                    shutdown(0)
                try:
                    os.kill(worker_pid, signal.SIGKILL)
                except OSError:
                    pass  # process already died

            if listen_sock in ready_fds:
                sock, addr = listen_sock.accept()
                # Launch a worker process
                try:
                    pid = os.fork()
                    if pid == 0:
                        listen_sock.close()
                        try:
                            worker(sock)
                        except:
                            traceback.print_exc()
                            os._exit(1)
                        else:
                            os._exit(0)
                    else:
                        sock.close()

                except OSError as e:
                    print >> sys.stderr, "Daemon failed to fork PySpark worker: %s" % e
                    outfile = os.fdopen(os.dup(sock.fileno()), "a+", 65536)
                    write_int(-1, outfile)  # Signal that the fork failed
                    outfile.flush()
                    outfile.close()
                    sock.close()
    finally:
        shutdown(1)


if __name__ == '__main__':
    manager()
