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
import sys
from typing import Any, IO, Iterator

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkAssertionError, PySparkRuntimeError
from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import (
    read_int,
    write_int,
    SpecialLengths,
    CloudPickleSerializer,
)
from pyspark.sql.datasource import DataSource
from pyspark.sql.types import _parse_datatype_json_string, StructType
from pyspark.util import handle_worker_exception
from pyspark.worker_util import (
    check_python_version,
    read_command,
    pickleSer,
    send_accumulator_updates,
    setup_broadcasts,
    setup_memory_limits,
    setup_spark_files,
    utf8_deserializer,
)


def main(infile: IO, outfile: IO) -> None:
    """
    Main method for planning a data source read.

    This process is invoked from the `UserDefinedPythonDataSourceReadRunner.runInPython`
    method in the optimizer rule `PlanPythonDataSourceScan` in JVM. This process is responsible
    for creating a `DataSourceReader` object and send the information needed back to the JVM.

    The infile and outfile are connected to the JVM via a socket. The JVM sends the following
    information to this process via the socket:
    - a `DataSource` instance representing the data source
    - a `StructType` instance representing the output schema of the data source

    This process then creates a `DataSourceReader` instance by calling the `reader` method
    on the `DataSource` instance. Then it calls the `partitions()` method of the reader and
    constructs a Python UDTF using the `read()` method of the reader.

    The partition values and the UDTF are then serialized and sent back to the JVM via the socket.
    """
    try:
        check_python_version(infile)

        memory_limit_mb = int(os.environ.get("PYSPARK_PLANNER_MEMORY_MB", "-1"))
        setup_memory_limits(memory_limit_mb)

        setup_spark_files(infile)
        setup_broadcasts(infile)

        _accumulatorRegistry.clear()

        # Receive the data source instance.
        data_source = read_command(pickleSer, infile)
        if not isinstance(data_source, DataSource):
            raise PySparkAssertionError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": "a Python data source instance of type 'DataSource'",
                    "actual": f"'{type(data_source).__name__}'",
                },
            )

        # Receive the data source output schema.
        schema_json = utf8_deserializer.loads(infile)
        schema = _parse_datatype_json_string(schema_json)
        if not isinstance(schema, StructType):
            raise PySparkAssertionError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": "a Python data source schema of type 'StructType'",
                    "actual": f"'{type(schema).__name__}'",
                },
            )

        # Instantiate data source reader.
        try:
            reader = data_source.reader(schema=schema)
        except NotImplementedError:
            raise PySparkRuntimeError(
                error_class="PYTHON_DATA_SOURCE_METHOD_NOT_IMPLEMENTED",
                message_parameters={"type": "reader", "method": "reader"},
            )
        except Exception as e:
            raise PySparkRuntimeError(
                error_class="PYTHON_DATA_SOURCE_CREATE_ERROR",
                message_parameters={"type": "reader", "error": str(e)},
            )

        # Generate all partitions.
        partitions = list(reader.partitions() or [])
        if len(partitions) == 0:
            partitions = [None]

        # Construct a UDTF.
        class PythonDataSourceReaderUDTF:
            def __init__(self) -> None:
                self.ser = CloudPickleSerializer()

            def eval(self, partition_bytes: Any) -> Iterator:
                partition = self.ser.loads(partition_bytes)
                yield from reader.read(partition)

        command = PythonDataSourceReaderUDTF
        pickleSer._write_with_length(command, outfile)

        # Return the serialized partition values.
        write_int(len(partitions), outfile)
        for partition in partitions:
            pickleSer._write_with_length(partition, outfile)

    except BaseException as e:
        handle_worker_exception(e, outfile)
        sys.exit(-1)

    send_accumulator_updates(outfile)

    # check end of stream
    if read_int(infile) == SpecialLengths.END_OF_STREAM:
        write_int(SpecialLengths.END_OF_STREAM, outfile)
    else:
        # write a different value to tell JVM to not reuse this worker
        write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
        sys.exit(-1)


if __name__ == "__main__":
    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
    main(sock_file, sock_file)
