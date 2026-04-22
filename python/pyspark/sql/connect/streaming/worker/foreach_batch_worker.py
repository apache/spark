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
A worker for streaming foreachBatch in Spark Connect.
Usually this is ran on the driver side of the Spark Connect Server.
"""

import os

from pyspark.worker_util import get_sock_file_to_executor
from pyspark.serializers import (
    write_int,
    read_long,
    UTF8Deserializer,
    CPickleSerializer,
)
from pyspark import worker
from pyspark.errors import PySparkAssertionError
from pyspark.sql.connect.session import SparkSession
from pyspark.util import handle_worker_exception
from typing import IO
from pyspark.worker_util import check_python_version

pickle_ser = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


spark = None


def main(infile: IO, outfile: IO) -> None:
    global spark

    log_name = "Streaming ForeachBatch worker"

    def process(df_id, batch_id, cloned_session_id):  # type: ignore[no-untyped-def]
        global spark
        # Lazily create or switch the SparkSession when the cloned session changes.
        # Use create() instead of getOrCreate() because the latter returns the cached
        # active/default session, ignoring the cloned session ID in the URL.
        if spark is None or spark.session_id != cloned_session_id:
            cloned_url = connect_url + ";session_id=" + cloned_session_id
            spark = SparkSession.builder.remote(cloned_url).create()
            if spark.session_id != cloned_session_id:
                raise PySparkAssertionError(
                    f"Cloned session ID mismatch: expected {cloned_session_id}, "
                    f"got {spark.session_id}"
                )
            print(f"{log_name} Created new session for cloned_session_id {cloned_session_id}")
        print(
            f"{log_name} Started batch {batch_id} with DF id {df_id} "
            f"and session id {cloned_session_id}"
        )
        batch_df = spark._create_remote_dataframe(df_id)
        func(batch_df, batch_id)
        print(
            f"{log_name} Completed batch {batch_id} with DF id {df_id} "
            f"and session id {cloned_session_id}"
        )

    try:
        check_python_version(infile)

        # Enable Spark Connect Mode
        os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"

        connect_url = os.environ["SPARK_CONNECT_LOCAL_URL"]
        root_session_id = utf8_deserializer.loads(infile)

        print(f"{log_name} is starting with url {connect_url} and sessionId {root_session_id}.")

        # Bootstrap SparkSession on the root session id: needed to unpickle the user's
        # foreachBatch function before any batch (and the cloned session id) arrives.
        # Per-batch work runs against the cloned session created in `process()`.
        connect_url_init = connect_url + ";session_id=" + root_session_id
        spark_connect_session = SparkSession.builder.remote(connect_url_init).getOrCreate()
        if spark_connect_session.session_id != root_session_id:
            raise PySparkAssertionError(
                f"Root session ID mismatch: expected {root_session_id}, "
                f"got {spark_connect_session.session_id}"
            )

        func = worker.read_command(pickle_ser, infile)
        write_int(0, outfile)
        outfile.flush()

        while True:
            df_ref_id = utf8_deserializer.loads(infile)
            batch_id = read_long(infile)
            cloned_session_id = utf8_deserializer.loads(infile)
            # Handle errors inside Python worker. Write 0 to outfile if no errors and write -2
            # with traceback string if error occurs.
            process(df_ref_id, int(batch_id), cloned_session_id)
            write_int(0, outfile)
            outfile.flush()
    except Exception as e:
        handle_worker_exception(e, outfile)
        outfile.flush()


if __name__ == "__main__":
    with get_sock_file_to_executor(timeout=None) as sock_file:
        main(sock_file, sock_file)
