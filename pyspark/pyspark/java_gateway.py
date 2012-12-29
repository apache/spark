import os
import sys
from subprocess import Popen, PIPE
from threading import Thread
from py4j.java_gateway import java_import, JavaGateway, GatewayClient


SPARK_HOME = os.environ["SPARK_HOME"]


def launch_gateway():
    # Launch the Py4j gateway using Spark's run command so that we pick up the
    # proper classpath and SPARK_MEM settings from spark-env.sh
    command = [os.path.join(SPARK_HOME, "run"), "py4j.GatewayServer",
               "--die-on-broken-pipe", "0"]
    proc = Popen(command, stdout=PIPE, stdin=PIPE)
    # Determine which ephemeral port the server started on:
    port = int(proc.stdout.readline())
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
    gateway = JavaGateway(GatewayClient(port=port), auto_convert=False)
    # Import the classes used by PySpark
    java_import(gateway.jvm, "spark.api.java.*")
    java_import(gateway.jvm, "spark.api.python.*")
    java_import(gateway.jvm, "scala.Tuple2")
    return gateway
