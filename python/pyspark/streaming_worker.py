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
A worker for streaming foreachBatch and query listener in Spark Connect.
"""
import os
import json

from pyspark.java_gateway import local_connect_and_auth
from pyspark.rdd import PythonEvalType
from pyspark.serializers import (
    read_int,
    write_int,
    read_long,
    UTF8Deserializer,
    CPickleSerializer,
)
from pyspark import worker
from pyspark.sql import SparkSession

from pyspark.sql.streaming.listener import (
    QueryStartedEvent,
    QueryProgressEvent,
    QueryTerminatedEvent,
    QueryIdleEvent,
)

pickleSer = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def main(infile, outfile):  # type: ignore[no-untyped-def]
    connect_url = os.environ["SPARK_CONNECT_LOCAL_URL"]
    session_id = utf8_deserializer.loads(infile)

    print(f"Streaming worker is starting with url {connect_url} and sessionId {session_id}.")

    spark_connect_session = SparkSession.builder.remote(connect_url).getOrCreate()
    spark_connect_session._client._session_id = session_id

    # TODO(SPARK-44460): Pass credentials.
    # TODO(SPARK-44461): Enable Process Isolation

    eval_type = read_int(infile)

    func = worker.read_command(pickleSer, infile)
    write_int(0, outfile)  # Indicate successful initialization

    outfile.flush()

    if eval_type == PythonEvalType.SQL_STREAMING_FOREACH_BATCH:
        foreach_batch_fcn(infile, outfile, spark_connect_session, func)
    elif eval_type == PythonEvalType.SQL_STREAMING_LISTENER:
        streaming_listener_fcn(infile, outfile, spark_connect_session, func)
    else:  # unreachable
        raise ValueError("Unrecognized streaming function type")


def streaming_listener_fcn(infile, outfile, spark_connect_session, listener):
    listener._set_spark_session(spark_connect_session)
    assert listener.spark == spark_connect_session

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
        outfile.flush()


def foreach_batch_fcn(infile, outfile, spark_connect_session, func):
    log_name = "Streaming ForeachBatch worker"

    def process(dfId, batchId):  # type: ignore[no-untyped-def]
        print(f"{log_name} Started batch {batchId} with DF id {dfId}")
        batch_df = spark_connect_session._createRemoteDataFrame(dfId)
        func(batch_df, batchId)
        print(f"{log_name} Completed batch {batchId} with DF id {dfId}")

    while True:
        df_ref_id = utf8_deserializer.loads(infile)
        batch_id = read_long(infile)
        process(df_ref_id, int(batch_id))  # TODO(SPARK-44463): Propagate error to the user.
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
