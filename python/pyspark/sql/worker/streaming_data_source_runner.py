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
import functools
from itertools import islice
from typing import IO, List, Iterator, Iterable

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkAssertionError, PySparkRuntimeError
from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import (
    read_int,
    write_int,
    write_with_length,
    SpecialLengths,
)
from pyspark.sql.connect.conversion import ArrowTableToRowsConversion, LocalDataToArrowConversion
from pyspark.sql.datasource import DataSource, DataStreamOffset, InputPartition
from pyspark.sql.pandas.types import to_arrow_schema
from pyspark.sql.types import (
    _parse_datatype_json_string,
    BinaryType,
    StructType,
)
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

# Get the partitions if any.
partitions_func_id = 886
latest_offsets_func_id = 887
read_func_id = 888

def latest_offset_func(reader, outfile):
    reader.latest_offset()
    write_with_length(reader.offset_to_json(reader.latest_offset()).encode("utf-8"), outfile)

def partitions_func(reader, infile, outfile):
    start_offset_json = utf8_deserializer.loads(infile)
    end_offset_json = utf8_deserializer.loads(infile)
    partitions = reader.partitions(reader.json_to_offset(start_offset_json), reader.json_to_offset(end_offset_json))
    # Return the serialized partition values.
    write_int(len(partitions), outfile)
    for partition in partitions:
        pickleSer._write_with_length(partition, outfile)


def read_func(reader, infile, outfile):
    write_int(read_func_id, outfile)

def main(infile: IO, outfile: IO) -> None:
    try:

        check_python_version(infile)
        setup_spark_files(infile)

        memory_limit_mb = int(os.environ.get("PYSPARK_PLANNER_MEMORY_MB", "-1"))
        setup_memory_limits(memory_limit_mb)

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

        # Receive the output schema from its child plan.
        input_schema_json = utf8_deserializer.loads(infile)
        input_schema = _parse_datatype_json_string(input_schema_json)
        if not isinstance(input_schema, StructType):
            raise PySparkAssertionError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": "an input schema of type 'StructType'",
                    "actual": f"'{type(input_schema).__name__}'",
                },
            )
        assert len(input_schema) == 1 and isinstance(input_schema[0].dataType, BinaryType), (
            "The input schema of Python data source read should contain only one column of type "
            f"'BinaryType', but got '{input_schema}'"
        )

        # Receive the data source output schema.
        schema_json = utf8_deserializer.loads(infile)
        schema = _parse_datatype_json_string(schema_json)
        if not isinstance(schema, StructType):
            raise PySparkAssertionError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": "an output schema of type 'StructType'",
                    "actual": f"'{type(schema).__name__}'",
                },
            )

        # Receive the configuration values.
        max_arrow_batch_size = read_int(infile)
        assert max_arrow_batch_size > 0, (
            "The maximum arrow batch size should be greater than 0, but got "
            f"'{max_arrow_batch_size}'"
        )

        # Instantiate data source reader.
        try:
            reader = data_source.stream_reader(schema=schema)
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

        # Initialization succeed.
        write_int(0, outfile)
        outfile.flush()

        # handle method call from socket
        while True:
            func_id = read_int(infile)
            if func_id == latest_offsets_func_id:
                latest_offset_func(reader, outfile)
            elif func_id == partitions_func_id:
                partitions_func(reader, infile, outfile)
            elif func_id == read_func_id:
                read_func(reader, infile, outfile)
            outfile.flush()

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
