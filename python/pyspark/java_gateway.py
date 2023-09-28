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
import signal
import shlex
import shutil
import socket
import platform
import tempfile
import time
from subprocess import Popen, PIPE

from py4j.java_gateway import java_import, JavaGateway, JavaObject, GatewayParameters
from py4j.clientserver import ClientServer, JavaParameters, PythonParameters
from pyspark.find_spark_home import _find_spark_home
from pyspark.serializers import read_int, write_with_length, UTF8Deserializer
from pyspark.errors import PySparkRuntimeError


def launch_gateway(conf=None, popen_kwargs=None):
    """
    launch jvm gateway

    Parameters
    ----------
    conf : :py:class:`pyspark.SparkConf`
        spark configuration passed to spark-submit
    popen_kwargs : dict
        Dictionary of kwargs to pass to Popen when spawning
        the py4j JVM. This is a developer feature intended for use in
        customizing how pyspark interacts with the py4j JVM (e.g., capturing
        stdout/stderr).

    Returns
    -------
    ClientServer or JavaGateway
    """
    if "PYSPARK_GATEWAY_PORT" in os.environ:
        gateway_port = int(os.environ["PYSPARK_GATEWAY_PORT"])
        gateway_secret = os.environ["PYSPARK_GATEWAY_SECRET"]
        # Process already exists
        proc = None
    else:
        SPARK_HOME = _find_spark_home()
        # Launch the Py4j gateway using Spark's run command so that we pick up the
        # proper classpath and settings from spark-env.sh
        on_windows = platform.system() == "Windows"
        script = "./bin/spark-submit.cmd" if on_windows else "./bin/spark-submit"
        command = [os.path.join(SPARK_HOME, script)]
        if conf:
            for k, v in conf.getAll():
                command += ["--conf", "%s=%s" % (k, v)]
        submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        if os.environ.get("SPARK_TESTING"):
            submit_args = " ".join(["--conf spark.ui.enabled=false", submit_args])
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
            popen_kwargs = {} if popen_kwargs is None else popen_kwargs
            # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
            popen_kwargs["stdin"] = PIPE
            # We always set the necessary environment variables.
            popen_kwargs["env"] = env
            if not on_windows:
                # Don't send ctrl-c / SIGINT to the Java gateway:
                def preexec_func():
                    signal.signal(signal.SIGINT, signal.SIG_IGN)

                popen_kwargs["preexec_fn"] = preexec_func
                proc = Popen(command, **popen_kwargs)
            else:
                # preexec_fn not supported on Windows
                proc = Popen(command, **popen_kwargs)

            # Wait for the file to appear, or for the process to exit, whichever happens first.
            while not proc.poll() and not os.path.isfile(conn_info_file):
                time.sleep(0.1)

            if not os.path.isfile(conn_info_file):
                raise PySparkRuntimeError(
                    error_class="JAVA_GATEWAY_EXITED",
                    message_parameters={},
                )

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

    # Connect to the gateway (or client server to pin the thread between JVM and Python)
    if os.environ.get("PYSPARK_PIN_THREAD", "true").lower() == "true":
        gateway = ClientServer(
            java_parameters=JavaParameters(
                port=gateway_port, auth_token=gateway_secret, auto_convert=True
            ),
            python_parameters=PythonParameters(port=0, eager_load=False),
        )
    else:
        gateway = JavaGateway(
            gateway_parameters=GatewayParameters(
                port=gateway_port, auth_token=gateway_secret, auto_convert=True
            )
        )

    # Store a reference to the Popen object for use by the caller (e.g., in reading stdout/stderr)
    gateway.proc = proc

    # Import the classes used by PySpark
    java_import(gateway.jvm, "org.apache.spark.SparkConf")
    java_import(gateway.jvm, "org.apache.spark.api.java.*")
    java_import(gateway.jvm, "org.apache.spark.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.ml.python.*")
    java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.resource.*")
    # TODO(davies): move into sql
    java_import(gateway.jvm, "org.apache.spark.sql.*")
    java_import(gateway.jvm, "org.apache.spark.sql.api.python.*")
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
        raise PySparkRuntimeError(
            error_class="UNEXPECTED_RESPONSE_FROM_SERVER",
            message_parameters={},
        )


def local_connect_and_auth(port, auth_secret):
    """
    Connect to local host, authenticate with it, and return a (sockfile,sock) for that connection.
    Handles IPV4 & IPV6, does some error handling.

    Parameters
    ----------
    port : str or int or None
    auth_secret : str

    Returns
    -------
    tuple
        with (sockfile, sock)
    """
    sock = None
    errors = []
    # Support for both IPv4 and IPv6.
    addr = "127.0.0.1"
    if os.environ.get("SPARK_PREFER_IPV6", "false").lower() == "true":
        addr = "::1"
    for res in socket.getaddrinfo(addr, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, _, sa = res
        try:
            sock = socket.socket(af, socktype, proto)
            sock.settimeout(int(os.environ.get("SPARK_AUTH_SOCKET_TIMEOUT", 15)))
            sock.connect(sa)
            sockfile = sock.makefile("rwb", int(os.environ.get("SPARK_BUFFER_SIZE", 65536)))
            _do_server_auth(sockfile, auth_secret)
            return (sockfile, sock)
        except socket.error as e:
            emsg = str(e)
            errors.append("tried to connect to %s, but an error occurred: %s" % (sa, emsg))
            sock.close()
            sock = None
    raise PySparkRuntimeError(
        error_class="CANNOT_OPEN_SOCKET",
        message_parameters={
            "errors": str(errors),
        },
    )


def ensure_callback_server_started(gw):
    """
    Start callback server if not already started. The callback server is needed if the Java
    driver process needs to callback into the Python driver process to execute Python code.
    """

    # getattr will fallback to JVM, so we cannot test by hasattr()
    if "_callback_server" not in gw.__dict__ or gw._callback_server is None:
        gw.callback_server_parameters.eager_load = True
        gw.callback_server_parameters.daemonize = True
        gw.callback_server_parameters.daemonize_connections = True
        gw.callback_server_parameters.port = 0
        gw.start_callback_server(gw.callback_server_parameters)
        cbport = gw._callback_server.server_socket.getsockname()[1]
        gw._callback_server.port = cbport
        # gateway with real port
        gw._python_proxy_port = gw._callback_server.port
        # get the GatewayServer object in JVM by ID
        jgws = JavaObject("GATEWAY_SERVER", gw._gateway_client)
        # update the port of CallbackClient with real port
        jgws.resetCallbackClient(jgws.getCallbackClient().getAddress(), gw._python_proxy_port)
