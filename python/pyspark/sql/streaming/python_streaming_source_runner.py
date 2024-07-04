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
import json
from typing import IO, Iterator, Tuple

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import IllegalArgumentException, PySparkAssertionError, PySparkRuntimeError
from pyspark.serializers import (
    read_int,
    write_int,
    write_with_length,
    SpecialLengths,
)
from pyspark.sql.datasource import DataSource, DataSourceStreamReader
from pyspark.sql.datasource_internal import _SimpleStreamReaderWrapper, _streamReader
from pyspark.sql.pandas.serializers import ArrowStreamSerializer
from pyspark.sql.types import (
    _parse_datatype_json_string,
    StructType,
)
from pyspark.sql.worker.plan_data_source_read import records_to_arrow_batches
from pyspark.util import handle_worker_exception, local_connect_and_auth
from pyspark.worker_util import (
    check_python_version,
    read_command,
    pickleSer,
    send_accumulator_updates,
    setup_memory_limits,
    setup_spark_files,
    utf8_deserializer,
)

INITIAL_OFFSET_FUNC_ID = 884
LATEST_OFFSET_FUNC_ID = 885
PARTITIONS_FUNC_ID = 886
COMMIT_FUNC_ID = 887

PREFETCHED_RECORDS_NOT_FOUND = 0
NON_EMPTY_PYARROW_RECORD_BATCHES = 1
EMPTY_PYARROW_RECORD_BATCHES = 2


def initial_offset_func(reader: DataSourceStreamReader, outfile: IO) -> None:
    offset = reader.initialOffset()
    write_with_length(json.dumps(offset).encode("utf-8"), outfile)


def latest_offset_func(reader: DataSourceStreamReader, outfile: IO) -> None:
    offset = reader.latestOffset()
    write_with_length(json.dumps(offset).encode("utf-8"), outfile)


def partitions_func(
    reader: DataSourceStreamReader,
    data_source: DataSource,
    schema: StructType,
    max_arrow_batch_size: int,
    infile: IO,
    outfile: IO,
) -> None:
    start_offset = json.loads(utf8_deserializer.loads(infile))
    end_offset = json.loads(utf8_deserializer.loads(infile))
    partitions = reader.partitions(start_offset, end_offset)
    # Return the serialized partition values.
    write_int(len(partitions), outfile)
    for partition in partitions:
        pickleSer._write_with_length(partition, outfile)
    if isinstance(reader, _SimpleStreamReaderWrapper):
        it = reader.getCache(start_offset, end_offset)
        if it is None:
            write_int(PREFETCHED_RECORDS_NOT_FOUND, outfile)
        else:
            send_batch_func(it, outfile, schema, max_arrow_batch_size, data_source)
    else:
        write_int(PREFETCHED_RECORDS_NOT_FOUND, outfile)


def commit_func(reader: DataSourceStreamReader, infile: IO, outfile: IO) -> None:
    end_offset = json.loads(utf8_deserializer.loads(infile))
    reader.commit(end_offset)
    write_int(0, outfile)


def send_batch_func(
    rows: Iterator[Tuple],
    outfile: IO,
    schema: StructType,
    max_arrow_batch_size: int,
    data_source: DataSource,
) -> None:
    batches = list(records_to_arrow_batches(rows, max_arrow_batch_size, schema, data_source))
    if len(batches) != 0:
        write_int(NON_EMPTY_PYARROW_RECORD_BATCHES, outfile)
        write_int(SpecialLengths.START_ARROW_STREAM, outfile)
        serializer = ArrowStreamSerializer()
        serializer.dump_stream(batches, outfile)
    else:
        write_int(EMPTY_PYARROW_RECORD_BATCHES, outfile)


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
                error_class="DATA_SOURCE_TYPE_MISMATCH",
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
                error_class="DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": "an output schema of type 'StructType'",
                    "actual": f"'{type(schema).__name__}'",
                },
            )

        max_arrow_batch_size = read_int(infile)
        assert max_arrow_batch_size > 0, (
            "The maximum arrow batch size should be greater than 0, but got "
            f"'{max_arrow_batch_size}'"
        )

        # Instantiate data source reader.
        try:
            reader = _streamReader(data_source, schema)
            # Initialization succeed.
            write_int(0, outfile)
            outfile.flush()

            # handle method call from socket
            while True:
                func_id = read_int(infile)
                if func_id == INITIAL_OFFSET_FUNC_ID:
                    initial_offset_func(reader, outfile)
                elif func_id == LATEST_OFFSET_FUNC_ID:
                    latest_offset_func(reader, outfile)
                elif func_id == PARTITIONS_FUNC_ID:
                    partitions_func(
                        reader, data_source, schema, max_arrow_batch_size, infile, outfile
                    )
                elif func_id == COMMIT_FUNC_ID:
                    commit_func(reader, infile, outfile)
                else:
                    raise IllegalArgumentException(
                        error_class="UNSUPPORTED_OPERATION",
                        message_parameters={
                            "operation": "Function call id not recognized by stream reader"
                        },
                    )
                outfile.flush()
        except Exception as e:
            error_msg = "data source {} throw exception: {}".format(data_source.name, e)
            raise PySparkRuntimeError(
                error_class="PYTHON_STREAMING_DATA_SOURCE_RUNTIME_ERROR",
                message_parameters={"msg": error_msg},
            )
        finally:
            reader.stop()
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
    (sock_file, sock) = local_connect_and_auth(java_port, auth_secret)
    # Prevent the socket from timeout error when query trigger interval is large.
    sock.settimeout(None)
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)
