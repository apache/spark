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

from pyspark.sql.streaming.listener import (
    QueryStartedEvent,
    QueryProgressEvent,
    QueryTerminatedEvent,
    QueryIdleEvent,
)

# 'resource' is a Unix specific module.
has_resource_module = True
try:
    import resource
except ImportError:
    has_resource_module = False
import traceback
import warnings
import faulthandler

from pyspark.accumulators import _accumulatorRegistry
from pyspark.broadcast import Broadcast, _broadcastRegistry
from pyspark.java_gateway import local_connect_and_auth
from pyspark.taskcontext import BarrierTaskContext, TaskContext
from pyspark.files import SparkFiles
from pyspark.resource import ResourceInformation
from pyspark.rdd import PythonEvalType
from pyspark.serializers import (
    write_with_length,
    write_int,
    read_long,
    read_bool,
    write_long,
    read_int,
    SpecialLengths,
    UTF8Deserializer,
    CPickleSerializer,
    BatchedSerializer,
)
from pyspark.sql.pandas.serializers import (
    ArrowStreamPandasUDFSerializer,
    CogroupUDFSerializer,
    ArrowStreamUDFSerializer,
    ApplyInPandasWithStateSerializer,
)
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.types import StructType
from pyspark.util import fail_on_stopiteration, try_simplify_traceback
from pyspark import shuffle
from pyspark import worker
from pyspark.sql import SparkSession

pickleSer = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def main(infile, outfile):
    sessionId = utf8_deserializer.loads(infile)
    print("##### sessionId in python process is", sessionId)
    sparkConnectSession = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    sparkConnectSession._client._session_id = sessionId

    listener = worker.read_command(pickleSer, infile)
    print("##### listener test in python process is", listener)

    write_int(9999, outfile)
    print("##### write data 9999 to server", listener)

    outfile.flush()

    listener.setSparkSession(sparkConnectSession)

    # TODO: move the logic to streaming_worker
    def process(listener_event_str, listener_event_type):
        listener_event = json.loads(listener_event_str)
        if listener_event_type == 0:
            listener.onQueryStarted(QueryStartedEvent.fromJson(listener_event))
        elif listener_event_type == 1:
            listener.onQueryProgress(QueryProgressEvent.fromJson(listener_event))
        elif listener_event_type == 2:
            listener.onQueryIdle(QueryIdleEvent.fromJson(listener_event))
        elif listener_event_type == 3:
            listener.onQueryTerminated(QueryTerminatedEvent.fromJson(listener_event))

    while True:
        event = utf8_deserializer.loads(infile)
        print("##### event received from python process is", event)
        event_type = read_int(infile)
        print("##### event_type received from python process is", event_type)
        process(event, int(event_type))
        write_int(SpecialLengths.END_OF_MICRO_BATCH, outfile)
        outfile.flush()


if __name__ == "__main__":
    print("##### start streaming worker.py")

    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
    # TODO: Remove the following two lines and use `Process.pid()` when we drop JDK 8.
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)
