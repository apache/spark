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

from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import (
    write_int,
    UTF8Deserializer,
    CPickleSerializer,
)
from typing import IO
from pyspark.worker_util import check_python_version

pickle_ser = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def main(infile: IO, outfile: IO) -> None:
    check_python_version(infile)

    connect_url = os.environ["SPARK_CONNECT_LOCAL_URL"]
    session_id = utf8_deserializer.loads(infile)

    print(
        "Streaming test worker is starting with " f"url {connect_url} and sessionId {session_id}."
    )

    write_int(0, outfile)  # Indicate successful initialization
    outfile.flush()

    while True:
        continue


if __name__ == "__main__":
    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, sock) = local_connect_and_auth(java_port, auth_secret)
    # There could be a long time between each micro batch.
    sock.settimeout(None)
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)
