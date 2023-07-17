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

"""
Worker that receives input from Piped RDD.
"""
import os
import sys
import time
from inspect import currentframe, getframeinfo, getfullargspec
import importlib
import json

# 'resource' is a Unix specific module.
has_resource_module = True
try:
    import resource
except ImportError:
    has_resource_module = False

from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import (
    write_int,
    read_long,
    SpecialLengths,
    UTF8Deserializer,
    CPickleSerializer,
)
from pyspark import worker
from pyspark.sql import SparkSession

pickleSer = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def main(infile, outfile):
    log_name = "Streaming ForeachBatch worker"
    connect_url = os.environ["SPARK_CONNECT_LOCAL_URL"]
    sessionId = utf8_deserializer.loads(infile)

    print(f"{log_name} is starting with url {connect_url} and sessionId {sessionId}.")

    sparkConnectSession = SparkSession.builder.remote(connect_url).getOrCreate()
    sparkConnectSession._client._session_id = sessionId

    # TODO(SPARK-44460): Pass credentials.
    # TODO(SPARK-44461): Enable Process Isolation

    func = worker.read_command(pickleSer, infile)
    write_int(0, outfile)  # Indicate successful initialization

    outfile.flush()

    def process(dfId, batchId):
        print(f"{log_name} Started batch {batchId} with DF id {dfId}")
        batchDf = sparkConnectSession._createRemoteDataFrame(dfId)
        func(batchDf, batchId)
        print(f"{log_name} Completed batch {batchId} with DF id {dfId}")

    while True:
        dfRefId = utf8_deserializer.loads(infile)
        batchId = read_long(infile)
        process(dfRefId, int(batchId))  # TODO:     Propagate error better to the user.
        write_int(0, outfile)
        outfile.flush()


if __name__ == "__main__":
    print("Starting streaming worker")

    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)
