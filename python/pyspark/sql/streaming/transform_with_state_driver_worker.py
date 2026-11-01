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

import json
from typing import IO, TYPE_CHECKING, Any, Iterator

from pyspark import worker
from pyspark.serializers import (
    CPickleSerializer,
    UTF8Deserializer,
    read_int,
    write_int,
)
from pyspark.sql.streaming.stateful_processor_api_client import StatefulProcessorApiClient
from pyspark.sql.streaming.stateful_processor_util import TransformWithStateInPandasFuncMode
from pyspark.sql.types import StructType
from pyspark.util import handle_worker_exception
from pyspark.worker_util import check_python_version, get_sock_file_to_executor

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import (
        DataFrameLike as PandasDataFrameLike,
    )

pickle_ser = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def main(infile: IO, outfile: IO) -> None:
    check_python_version(infile)

    log_name = "Streaming TransformWithStateInPandas Python worker"
    print(f"Starting {log_name}.\n")

    def process(
        processor: StatefulProcessorApiClient,
        mode: TransformWithStateInPandasFuncMode,
        key: Any,
        input: Iterator["PandasDataFrameLike"],
    ) -> None:
        print(f"{log_name} Starting execution of UDF: {func}.\n")
        func(processor, mode, key, input)
        print(f"{log_name} Completed execution of UDF: {func}.\n")

    try:
        func, return_type = worker.read_command(pickle_ser, infile)
        print(
            f"{log_name} finish init stage of Python runner. Received UDF from JVM: {func}, "
            f"received return type of UDF: {return_type}.\n"
        )
        # send signal for getting args
        write_int(0, outfile)
        outfile.flush()

        # This driver runner will only be used on the first batch of a query,
        # and the following code block should be only run once for each query run
        state_server_port = read_int(infile)
        auth_secret = None
        if state_server_port == -1:
            state_server_port = utf8_deserializer.loads(infile)
        else:
            # For TCP state servers the connection secret follows the port; Unix domain
            # socket paths (-1 marker above) rely on filesystem permissions instead.
            auth_secret = utf8_deserializer.loads(infile)
        key_schema = StructType.fromJson(json.loads(utf8_deserializer.loads(infile)))
        print(
            f"{log_name} received parameters for UDF. State server port/path: {state_server_port}, "
            f"key schema: {key_schema}.\n"
        )

        stateful_processor_api_client = StatefulProcessorApiClient(
            state_server_port, key_schema, auth_secret=auth_secret
        )
        process(
            stateful_processor_api_client,
            TransformWithStateInPandasFuncMode.PRE_INIT,
            None,
            iter([]),
        )
        write_int(0, outfile)
        outfile.flush()
    except Exception as e:
        handle_worker_exception(e, outfile)
        outfile.flush()


if __name__ == "__main__":
    with get_sock_file_to_executor() as sock_file:
        main(sock_file, sock_file)
