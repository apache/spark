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

    def process(df_id, batch_id):  # type: ignore[no-untyped-def]
        global spark
        print(f"{log_name} Started batch {batch_id} with DF id {df_id} and session id {session_id}")
        batch_df = spark_connect_session._create_remote_dataframe(df_id)
        func(batch_df, batch_id)
        print(
            f"{log_name} Completed batch {batch_id} with DF id {df_id} and session id {session_id}"
        )

    try:
        check_python_version(infile)

        # Enable Spark Connect Mode
        os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"

        connect_url = os.environ["SPARK_CONNECT_LOCAL_URL"]
        session_id = utf8_deserializer.loads(infile)

        print(f"{log_name} is starting with " f"url {connect_url} and sessionId {session_id}.")

        # To attach to the existing SparkSession, we're setting the session_id in the URL.
        connect_url = connect_url + ";session_id=" + session_id
        spark_connect_session = SparkSession.builder.remote(connect_url).getOrCreate()
        assert spark_connect_session.session_id == session_id
        spark = spark_connect_session

        func = worker.read_command(pickle_ser, infile)
        write_int(0, outfile)
        outfile.flush()

        while True:
            df_ref_id = utf8_deserializer.loads(infile)
            batch_id = read_long(infile)
            # Handle errors inside Python worker. Write 0 to outfile if no errors and write -2 with
            # traceback string if error occurs.
            process(df_ref_id, int(batch_id))
            write_int(0, outfile)
            outfile.flush()
    except Exception as e:
        handle_worker_exception(e, outfile)
        outfile.flush()


if __name__ == "__main__":
    # Read information about how to connect back to the JVM from the environment.
    conn_info = os.environ.get(
        "PYTHON_WORKER_FACTORY_SOCK_PATH", int(os.environ.get("PYTHON_WORKER_FACTORY_PORT", -1))
    )
    auth_secret = os.environ.get("PYTHON_WORKER_FACTORY_SECRET")
    (sock_file, sock) = local_connect_and_auth(conn_info, auth_secret)
    # There could be a long time between each micro batch.
    sock.settimeout(None)
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)
