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
import select
import signal
import shlex
import shutil
import socket
import platform
import tempfile
import time
from subprocess import Popen, PIPE

if sys.version >= '3':
    xrange = range

from py4j.java_gateway import java_import, JavaGateway, GatewayParameters
from pyspark.find_spark_home import _find_spark_home
from pyspark.serializers import read_int, write_with_length, UTF8Deserializer
from pyspark.util import _exception_message


def launch_gateway(conf=None):
    """
    launch jvm gateway
    :param conf: spark configuration passed to spark-submit
    :return:
    """
    if "PYSPARK_GATEWAY_PORT" in os.environ:
        gateway_port = int(os.environ["PYSPARK_GATEWAY_PORT"])
        gateway_secret = os.environ["PYSPARK_GATEWAY_SECRET"]
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

        # Create a temporary directory where the gateway server should write the connection
        # information.
        conn_info_dir = tempfile.mkdtemp()
        try:
            fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
            os.close(fd)
            os.unlink(conn_info_file)

            env = dict(os.environ)
            env["_PYSPARK_DRIVER_CONN_INFO_PATH"] = conn_info_file

            # Launch the Java gateway.
            # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
            if not on_windows:
                # Don't send ctrl-c / SIGINT to the Java gateway:
                def preexec_func():
                    signal.signal(signal.SIGINT, signal.SIG_IGN)
                proc = Popen(command, stdin=PIPE, preexec_fn=preexec_func, env=env)
            else:
                # preexec_fn not supported on Windows
                proc = Popen(command, stdin=PIPE, env=env)

            # Wait for the file to appear, or for the process to exit, whichever happens first.
            while not proc.poll() and not os.path.isfile(conn_info_file):
                time.sleep(0.1)

            if not os.path.isfile(conn_info_file):
                raise Exception("Java gateway process exited before sending its port number")

            with open(conn_info_file, "rb") as info:
                gateway_port = read_int(info)
                gateway_secret = UTF8Deserializer().loads(info)
        finally:
            shutil.rmtree(conn_info_dir)

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
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(port=gateway_port, auth_token=gateway_secret,
                                             auto_convert=True))

    # Import the classes used by PySpark
    java_import(gateway.jvm, "org.apache.spark.SparkConf")
    java_import(gateway.jvm, "org.apache.spark.api.java.*")
    java_import(gateway.jvm, "org.apache.spark.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.ml.python.*")
    java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
    # TODO(davies): move into sql
    java_import(gateway.jvm, "org.apache.spark.sql.*")
    java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
    java_import(gateway.jvm, "scala.Tuple2")

    return gateway


def _do_server_auth(conn, auth_secret):
    """
    Performs the authentication protocol defined by the SocketAuthHelper class on the given
    file-like object 'conn'.
    """
    write_with_length(auth_secret.encode("utf-8"), conn)
    conn.flush()
    reply = UTF8Deserializer().loads(conn)
    if reply != "ok":
        conn.close()
        raise Exception("Unexpected reply from iterator server.")


def local_connect_and_auth(port, auth_secret):
    """
    Connect to local host, authenticate with it, and return a (sockfile,sock) for that connection.
    Handles IPV4 & IPV6, does some error handling.
    :param port
    :param auth_secret
    :return: a tuple with (sockfile, sock)
    """
    sock = None
    errors = []
    # Support for both IPv4 and IPv6.
    # On most of IPv6-ready systems, IPv6 will take precedence.
    for res in socket.getaddrinfo("127.0.0.1", port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, _, sa = res
        try:
            sock = socket.socket(af, socktype, proto)
            sock.settimeout(15)
            sock.connect(sa)
            sockfile = sock.makefile("rwb", 65536)
            _do_server_auth(sockfile, auth_secret)
            return (sockfile, sock)
        except socket.error as e:
            emsg = _exception_message(e)
            errors.append("tried to connect to %s, but an error occured: %s" % (sa, emsg))
            sock.close()
            sock = None
    else:
        raise Exception("could not open socket: %s" % errors)
