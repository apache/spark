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
import time
import gc
from errno import EINTR, EAGAIN
from socket import AF_INET, AF_INET6, SOCK_STREAM, SOMAXCONN
from signal import SIGHUP, SIGTERM, SIGCHLD, SIG_DFL, SIG_IGN, SIGINT

from pyspark.serializers import read_int, write_int, write_with_length, UTF8Deserializer

if len(sys.argv) > 1 and sys.argv[1].startswith("pyspark"):
    import importlib

    worker_module = importlib.import_module(sys.argv[1])
    worker_main = worker_module.main
else:
    from pyspark.worker import main as worker_main


def compute_real_exit_code(exit_code):
    # SystemExit's code can be integer or string, but os._exit only accepts integers
    if isinstance(exit_code, numbers.Integral):
        return exit_code
    else:
        return 1


def worker(sock, authenticated):
    """
    Called by a worker process after the fork().
    """
    signal.signal(SIGHUP, SIG_DFL)
    signal.signal(SIGCHLD, SIG_DFL)
    signal.signal(SIGTERM, SIG_DFL)
    # restore the handler for SIGINT,
    # it's useful for debugging (show the stacktrace before exit)
    signal.signal(SIGINT, signal.default_int_handler)

    # Read the socket using fdopen instead of socket.makefile() because the latter
    # seems to be very slow; note that we need to dup() the file descriptor because
    # otherwise writes also cause a seek that makes us miss data on the read side.
    buffer_size = int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
    infile = os.fdopen(os.dup(sock.fileno()), "rb", buffer_size)
    outfile = os.fdopen(os.dup(sock.fileno()), "wb", buffer_size)

    if not authenticated:
        client_secret = UTF8Deserializer().loads(infile)
        if os.environ["PYTHON_WORKER_FACTORY_SECRET"] == client_secret:
            write_with_length("ok".encode("utf-8"), outfile)
            outfile.flush()
        else:
            write_with_length("err".encode("utf-8"), outfile)
            outfile.flush()
            sock.close()
            return 1

    exit_code = 0
    try:
        worker_main(infile, outfile)
    except SystemExit as exc:
        exit_code = compute_real_exit_code(exc.code)
    finally:
        try:
            outfile.flush()
        except Exception:
            pass
    return exit_code


def manager():
    # Create a new process group to corral our children
    os.setpgid(0, 0)

    # Create a listening socket on the loopback interface
    if os.environ.get("SPARK_PREFER_IPV6", "false").lower() == "true":
        listen_sock = socket.socket(AF_INET6, SOCK_STREAM)
        listen_sock.bind(("::1", 0, 0, 0))
        listen_sock.listen(max(1024, SOMAXCONN))
        listen_host, listen_port, _, _ = listen_sock.getsockname()
    else:
        listen_sock = socket.socket(AF_INET, SOCK_STREAM)
        listen_sock.bind(("127.0.0.1", 0))
        listen_sock.listen(max(1024, SOMAXCONN))
        listen_host, listen_port = listen_sock.getsockname()

    # re-open stdin/stdout in 'wb' mode
    stdin_bin = os.fdopen(sys.stdin.fileno(), "rb", 4)
    stdout_bin = os.fdopen(sys.stdout.fileno(), "wb", 4)
    write_int(listen_port, stdout_bin)
    stdout_bin.flush()

    def shutdown(code):
        signal.signal(SIGTERM, SIG_DFL)
        # Send SIGHUP to notify workers of shutdown
        os.kill(0, SIGHUP)
        sys.exit(code)

    def handle_sigterm(*args):
        shutdown(1)

    signal.signal(SIGTERM, handle_sigterm)  # Gracefully exit on SIGTERM
    signal.signal(SIGHUP, SIG_IGN)  # Don't die on SIGHUP
    signal.signal(SIGCHLD, SIG_IGN)

    reuse = os.environ.get("SPARK_REUSE_WORKER")

    # Initialization complete
    try:
        while True:
            try:
                ready_fds = select.select([0, listen_sock], [], [], 1)[0]
            except select.error as ex:
                if ex[0] == EINTR:
                    continue
                else:
                    raise

            if 0 in ready_fds:
                try:
                    worker_pid = read_int(stdin_bin)
                except EOFError:
                    # Spark told us to exit by closing stdin
                    shutdown(0)
                try:
                    os.kill(worker_pid, signal.SIGKILL)
                except OSError:
                    pass  # process already died

            if listen_sock in ready_fds:
                try:
                    sock, _ = listen_sock.accept()
                except OSError as e:
                    if e.errno == EINTR:
                        continue
                    raise

                # Launch a worker process
                try:
                    pid = os.fork()
                except OSError as e:
                    if e.errno in (EAGAIN, EINTR):
                        time.sleep(1)
                        pid = os.fork()  # error here will shutdown daemon
                    else:
                        outfile = sock.makefile(mode="wb")
                        write_int(e.errno, outfile)  # Signal that the fork failed
                        outfile.flush()
                        outfile.close()
                        sock.close()
                        continue

                if pid == 0:
                    # in child process
                    listen_sock.close()

                    # It should close the standard input in the child process so that
                    # Python native function executions stay intact.
                    #
                    # Note that if we just close the standard input (file descriptor 0),
                    # the lowest file descriptor (file descriptor 0) will be allocated,
                    # later when other file descriptors should happen to open.
                    #
                    # Therefore, here we redirects it to '/dev/null' by duplicating
                    # another file descriptor for '/dev/null' to the standard input (0).
                    # See SPARK-26175.
                    devnull = open(os.devnull, "r")
                    os.dup2(devnull.fileno(), 0)
                    devnull.close()

                    try:
                        # Acknowledge that the fork was successful
                        outfile = sock.makefile(mode="wb")
                        write_int(os.getpid(), outfile)
                        outfile.flush()
                        outfile.close()
                        authenticated = False
                        while True:
                            code = worker(sock, authenticated)
                            if code == 0:
                                authenticated = True
                            if not reuse or code:
                                # wait for closing
                                try:
                                    while sock.recv(1024):
                                        pass
                                except Exception:
                                    pass
                                break
                            gc.collect()
                    except BaseException:
                        traceback.print_exc()
                        os._exit(1)
                    else:
                        os._exit(0)
                else:
                    sock.close()

    finally:
        shutdown(1)


if __name__ == "__main__":
    manager()
