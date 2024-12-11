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

import os
import json

from pyspark.util import local_connect_and_auth
from pyspark.serializers import (
    write_int,
    read_int,
    UTF8Deserializer,
    CPickleSerializer,
)
from pyspark import worker
from pyspark.util import handle_worker_exception
from typing import IO
from pyspark.worker_util import check_python_version
from pyspark.sql.streaming.stateful_processor_api_client import (
    StatefulProcessorApiClient,
    StatefulProcessorHandleState,
)
from pyspark.sql.streaming.stateful_processor_util import TransformWithStateInPandasFuncMode
from pyspark.sql.types import StructType, _parse_datatype_string

pickle_ser = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def main(infile: IO, outfile: IO) -> None:
    check_python_version(infile)

    log_name = "Streaming TransformWithStateInPandas Python worker"

    def process(processor, mode, key, input):
        # raise Exception(f"I am inside process, key: {key}, func: {func}")
        func(processor, mode, key, input)
        print(f"{log_name} Completed tws udf\n")

    try:
        func, return_type = worker.read_command(pickle_ser, infile)
        # send signal for getting args
        write_int(0, outfile)
        outfile.flush()

        while True:
            # Handle errors inside Python worker. Write 0 to outfile if no errors and write -2 with
            # traceback string if error occurs.
            state_server_port = read_int(infile)
            key_schema = StructType.fromJson(json.loads(utf8_deserializer.loads(infile)))

            stateful_processor_api_client = StatefulProcessorApiClient(state_server_port, key_schema)
            process(stateful_processor_api_client, TransformWithStateInPandasFuncMode.PRE_INIT,
                    None, iter([]))
            # raise Exception(f"{log_name} after process tws, stateful_processor_api_client: {stateful_processor_api_client}\n")
            write_int(0, outfile)
            outfile.flush()
    except Exception as e:
        handle_worker_exception(e, outfile)
        outfile.flush()


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
