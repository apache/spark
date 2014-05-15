#!/usr/bin/env python

import code
import os
import socket
import subprocess
import sys
import time
from pyspark.java_gateway import ignoreInterrupt

# Launch spark submit process
submitArgs = sys.argv[1:]
command = ["bin/spark-submit", "pyspark-shell"] + submitArgs
process = subprocess.Popen(command, stdout=sys.stdout, preexec_fn=ignoreInterrupt)

try:
    # Read py4j port from the PythonShellRunner server
    serverPort = 7744
    retrySeconds = 0.1
    maxAttempts = 20
    numAttempts = 0
    py4jPort = -1
    while py4jPort < 0:
        try:
            s = socket.socket()
            s.connect(("127.0.0.1", serverPort))
            py4jPort = s.recv(1024)
            s.close()
        except socket.error as se:
            if numAttempts < maxAttempts:
                numAttempts += 1
                time.sleep(retrySeconds)
            else:
                raise Exception("Failed to retrieve Py4j gateway server port from server!")

    # Set up Spark environment for python
    os.environ["PYSPARK_GATEWAY_PORT"] = py4jPort
    _pythonstartup = os.environ.get('PYTHONSTARTUP')
    if _pythonstartup and os.path.isfile(_pythonstartup):
        execfile(_pythonstartup)

    # Start the REPL
    code.interact(local=locals())

finally:
    process.terminate()
