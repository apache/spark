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

from __future__ import print_function


import atexit
import os
import sys
import select
import signal
import shlex
import socket
import platform
from subprocess import Popen, PIPE
from threading import Thread


if sys.version >= '3':
    xrange = range

from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from pyspark.find_spark_home import _find_spark_home
from pyspark.serializers import read_int


def launch_gateway(conf=None):
    """
    launch jvm gateway
    :param conf: spark configuration passed to spark-submit
    :return:
    """
    # If sys.stdout has been changed the child processes JVM will not respect that
    # so grab the jvm output and copy it over if we are in a notebook.
    redirect_shells = ["ZMQInteractiveShell", "StringIO"]
    grab_jvm_output = (sys.stdout != sys.__stdout__ and
                       sys.stdout.__class__.__name__ in redirect_shells)
    if grab_jvm_output:
        print("Grabbing JVM output cause magic.....")

    if "PYSPARK_GATEWAY_PORT" in os.environ:
        gateway_port = int(os.environ["PYSPARK_GATEWAY_PORT"])
        if grab_jvm_output:
            print("Gateway already launched, can not grab output")
    else:
        SPARK_HOME = _find_spark_home()
        # Launch the Py4j gateway using Spark's run command so that we pick up the
        # proper classpath and settings from spark-env.sh
        on_windows = platform.system() == "Windows"
        script = "./bin/spark-submit.cmd" if on_windows else "./bin/spark-submit"
        command = [os.path.join(SPARK_HOME, script)]
        if conf:
            for k, v in conf.getAll():
                command += ['--conf', '%s=%s' % (k, v)]
        submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        if os.environ.get("SPARK_TESTING"):
            submit_args = ' '.join([
                "--conf spark.ui.enabled=false",
                submit_args
            ])
        command = command + shlex.split(submit_args)

        # Start a socket that will be used by PythonGatewayServer to communicate its port to us
        callback_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        callback_socket.bind(('127.0.0.1', 0))
        callback_socket.listen(1)
        callback_host, callback_port = callback_socket.getsockname()
        env = dict(os.environ)
        env['_PYSPARK_DRIVER_CALLBACK_HOST'] = callback_host
        env['_PYSPARK_DRIVER_CALLBACK_PORT'] = str(callback_port)

        # Launch the Java gateway.
        # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
        proc_kwargs = {"env": env, "stdin": PIPE}
        if not on_windows:
            # Don't send ctrl-c / SIGINT to the Java gateway:
            # However, preexec_fn not supported on Windows
            def preexec_func():
                signal.signal(signal.SIGINT, signal.SIG_IGN)

            proc_kwargs["preexec_fn"] = preexec_func

        # If we need to copy stderr/stdout through, set up a pipe.
        if grab_jvm_output:
            proc_kwargs["stderr"] = PIPE
            proc_kwargs["stdout"] = PIPE
            proc_kwargs["bufsize"] = 1
            proc_kwargs["close_fds"] = True

        proc = Popen(command, **proc_kwargs)

        def connect(input_pipe, out_pipe):
            """Connect the input pipe to the output. We can't use os.dup for IPython
            or directly write to them (see https://github.com/ipython/ipython/pull/3072/)."""
            print("Connecting pipes....")
            for line in iter(input_pipe.readline, b''):
                print(line, file=out_pipe)
            print("Pipe finished...")
            input_pipe.close()

        if grab_jvm_output:
            t = Thread(target=connect, args=(proc.stdout, sys.stdout))
            t.daemon = True
            t.start()
            t = Thread(target=connect, args=(proc.stderr, sys.stderr))
            t.daemon = True
            t.start()

        gateway_port = None
        # We use select() here in order to avoid blocking indefinitely if the subprocess dies
        # before connecting
        while gateway_port is None and proc.poll() is None:
            timeout = 1  # (seconds)
            readable, _, _ = select.select([callback_socket], [], [], timeout)
            if callback_socket in readable:
                gateway_connection = callback_socket.accept()[0]
                # Determine which ephemeral port the server started on:
                gateway_port = read_int(gateway_connection.makefile(mode="rb"))
                gateway_connection.close()
                callback_socket.close()
        if gateway_port is None:
            raise Exception("Java gateway process exited before sending the driver its port number")

        # In Windows, ensure the Java child processes do not linger after Python has exited.
        # In UNIX-based systems, the child process can kill itself on broken pipe (i.e. when
        # the parent process' stdin sends an EOF). In Windows, however, this is not possible
        # because java.lang.Process reads directly from the parent process' stdin, contending
        # with any opportunity to read an EOF from the parent. Note that this is only best
        # effort and will not take effect if the python process is violently terminated.
        if on_windows:
            # In Windows, the child process here is "spark-submit.cmd", not the JVM itself
            # (because the UNIX "exec" command is not available). This means we cannot simply
            # call proc.kill(), which kills only the "spark-submit.cmd" process but not the
            # JVMs. Instead, we use "taskkill" with the tree-kill option "/t" to terminate all
            # child processes in the tree (http://technet.microsoft.com/en-us/library/bb491009.aspx)
            def killChild():
                Popen(["cmd", "/c", "taskkill", "/f", "/t", "/pid", str(proc.pid)])
            atexit.register(killChild)

    # Connect to the gateway
    gateway = JavaGateway(GatewayClient(port=gateway_port), auto_convert=True)

    # Import the classes used by PySpark
    java_import(gateway.jvm, "org.apache.spark.SparkConf")
    java_import(gateway.jvm, "org.apache.spark.api.java.*")
    java_import(gateway.jvm, "org.apache.spark.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.ml.python.*")
    java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
    # TODO(davies): move into sql
    java_import(gateway.jvm, "org.apache.spark.sql.*")
    java_import(gateway.jvm, "org.apache.spark.sql.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
    java_import(gateway.jvm, "scala.Tuple2")

    return gateway
