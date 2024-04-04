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

from pyspark.util import local_connect_and_auth
from pyspark.serializers import (
    write_int,
    read_long,
    UTF8Deserializer,
    CPickleSerializer,
)
from pyspark import worker
from pyspark.sql import SparkSession
from pyspark.util import handle_worker_exception
from typing import IO
from pyspark.worker_util import check_python_version

pickle_ser = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def main(infile: IO, outfile: IO) -> None:
    check_python_version(infile)

    connect_url = os.environ["SPARK_CONNECT_LOCAL_URL"]
    session_id = utf8_deserializer.loads(infile)

    print(
        "Streaming foreachBatch worker is starting with "
        f"url {connect_url} and sessionId {session_id}."
    )

    spark_connect_session = SparkSession.builder.remote(connect_url).getOrCreate()
    spark_connect_session._client._session_id = session_id  # type: ignore[attr-defined]

    # TODO(SPARK-44461): Enable Process Isolation

    func = worker.read_command(pickle_ser, infile)
    write_int(0, outfile)  # Indicate successful initialization

    outfile.flush()

    log_name = "Streaming ForeachBatch worker"

    def process(df_id, batch_id):  # type: ignore[no-untyped-def]
        print(f"{log_name} Started batch {batch_id} with DF id {df_id}")
        batch_df = spark_connect_session._create_remote_dataframe(df_id)
        func(batch_df, batch_id)
        print(f"{log_name} Completed batch {batch_id} with DF id {df_id}")

    while True:
        df_ref_id = utf8_deserializer.loads(infile)
        batch_id = read_long(infile)
        # Handle errors inside Python worker. Write 0 to outfile if no errors and write -2 with
        # traceback string if error occurs.
        try:
            process(df_ref_id, int(batch_id))
            write_int(0, outfile)
        except BaseException as e:
            handle_worker_exception(e, outfile)
        outfile.flush()


if __name__ == "__main__":
    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, sock) = local_connect_and_auth(java_port, auth_secret)
    # There could be a long time between each micro batch.
    sock.settimeout(None)
    main(sock_file, sock_file)
