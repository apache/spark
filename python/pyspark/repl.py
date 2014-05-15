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

import code
import os
import socket
import subprocess
import sys
import time
from pyspark.java_gateway import ignoreInterrupt

# Launch spark submit process
sparkHome = os.getcwd()
sparkSubmit = sparkHome + "/bin/spark-submit"
submitArgs = sys.argv[1:]
command = [sparkSubmit, "pyspark-shell"] + submitArgs
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
