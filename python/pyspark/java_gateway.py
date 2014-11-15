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

import atexit
import os
import sys
import signal
import shlex
import platform
from subprocess import Popen, PIPE
from threading import Thread
from py4j.java_gateway import java_import, JavaGateway, GatewayClient


def launch_gateway():
    SPARK_HOME = os.environ["SPARK_HOME"]

    gateway_port = -1
    if "PYSPARK_GATEWAY_PORT" in os.environ:
        gateway_port = int(os.environ["PYSPARK_GATEWAY_PORT"])
    else:
        # Launch the Py4j gateway using Spark's run command so that we pick up the
        # proper classpath and settings from spark-env.sh
        on_windows = platform.system() == "Windows"
        script = "./bin/spark-submit.cmd" if on_windows else "./bin/spark-submit"
        submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS")
        submit_args = submit_args if submit_args is not None else ""
        submit_args = shlex.split(submit_args)
        command = [os.path.join(SPARK_HOME, script)] + submit_args + ["pyspark-shell"]
        if not on_windows:
            # Don't send ctrl-c / SIGINT to the Java gateway:
            def preexec_func():
                signal.signal(signal.SIGINT, signal.SIG_IGN)
            env = dict(os.environ)
            env["IS_SUBPROCESS"] = "1"  # tell JVM to exit after python exits
            proc = Popen(command, stdout=PIPE, stdin=PIPE, preexec_fn=preexec_func, env=env)
        else:
            # preexec_fn not supported on Windows
            proc = Popen(command, stdout=PIPE, stdin=PIPE)

        try:
            # Determine which ephemeral port the server started on:
            gateway_port = proc.stdout.readline()
            gateway_port = int(gateway_port)
        except ValueError:
            # Grab the remaining lines of stdout
            (stdout, _) = proc.communicate()
            exit_code = proc.poll()
            error_msg = "Launching GatewayServer failed"
            error_msg += " with exit code %d!\n" % exit_code if exit_code else "!\n"
            error_msg += "Warning: Expected GatewayServer to output a port, but found "
            if gateway_port == "" and stdout == "":
                error_msg += "no output.\n"
            else:
                error_msg += "the following:\n\n"
                error_msg += "--------------------------------------------------------------\n"
                error_msg += gateway_port + stdout
                error_msg += "--------------------------------------------------------------\n"
            raise Exception(error_msg)

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

        # Create a thread to echo output from the GatewayServer, which is required
        # for Java log output to show up:
        class EchoOutputThread(Thread):

            def __init__(self, stream):
                Thread.__init__(self)
                self.daemon = True
                self.stream = stream

            def run(self):
                while True:
                    line = self.stream.readline()
                    sys.stderr.write(line)
        EchoOutputThread(proc.stdout).start()

    # Connect to the gateway
    gateway = JavaGateway(GatewayClient(port=gateway_port), auto_convert=False)

    # Import the classes used by PySpark
    java_import(gateway.jvm, "org.apache.spark.SparkConf")
    java_import(gateway.jvm, "org.apache.spark.api.java.*")
    java_import(gateway.jvm, "org.apache.spark.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.sql.SQLContext")
    java_import(gateway.jvm, "org.apache.spark.sql.hive.HiveContext")
    java_import(gateway.jvm, "org.apache.spark.sql.hive.LocalHiveContext")
    java_import(gateway.jvm, "org.apache.spark.sql.hive.TestHiveContext")
    java_import(gateway.jvm, "scala.Tuple2")

    return gateway
