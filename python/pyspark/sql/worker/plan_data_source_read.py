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
import pyarrow as pa
from itertools import islice
from typing import IO, List, Iterator, Iterable, Tuple

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkAssertionError, PySparkRuntimeError
from pyspark.serializers import (
    read_bool,
    read_int,
    write_int,
    SpecialLengths,
)
from pyspark.sql import Row
from pyspark.sql.connect.conversion import ArrowTableToRowsConversion, LocalDataToArrowConversion
from pyspark.sql.datasource import DataSource, InputPartition
from pyspark.sql.datasource_internal import _streamReader
from pyspark.sql.pandas.types import to_arrow_schema
from pyspark.sql.types import (
    _parse_datatype_json_string,
    BinaryType,
    StructType,
)
from pyspark.util import handle_worker_exception, local_connect_and_auth
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


def records_to_arrow_batches(
    output_iter: Iterator[Tuple],
    max_arrow_batch_size: int,
    return_type: StructType,
    data_source: DataSource,
) -> Iterable[pa.RecordBatch]:
    """
    Convert an iterator of Python tuples to an iterator of pyarrow record batches.

    For each python tuple, check the types of each field and append it to the records batch.

    """

    def batched(iterator: Iterator, n: int) -> Iterator:
        return iter(functools.partial(lambda it: list(islice(it, n)), iterator), [])

    pa_schema = to_arrow_schema(return_type)
    column_names = return_type.fieldNames()
    column_converters = [
        LocalDataToArrowConversion._create_converter(field.dataType) for field in return_type.fields
    ]
    # Convert the results from the `reader.read` method to an iterator of arrow batches.
    num_cols = len(column_names)
    col_mapping = {name: i for i, name in enumerate(column_names)}
    col_name_set = set(column_names)
    for batch in batched(output_iter, max_arrow_batch_size):
        pylist: List[List] = [[] for _ in range(num_cols)]
        for result in batch:
            # Validate the output row schema.
            if hasattr(result, "__len__") and len(result) != num_cols:
                raise PySparkRuntimeError(
                    error_class="DATA_SOURCE_RETURN_SCHEMA_MISMATCH",
                    message_parameters={
                        "expected": str(num_cols),
                        "actual": str(len(result)),
                    },
                )

            # Validate the output row type.
            if not isinstance(result, (list, tuple)):
                raise PySparkRuntimeError(
                    error_class="DATA_SOURCE_INVALID_RETURN_TYPE",
                    message_parameters={
                        "type": type(result).__name__,
                        "name": data_source.name(),
                        "supported_types": "tuple, list, `pyspark.sql.types.Row`",
                    },
                )

            # Assign output values by name of the field, not position, if the result is a
            # named `Row` object.
            if isinstance(result, Row) and hasattr(result, "__fields__"):
                # Check if the names are the same as the schema.
                if set(result.__fields__) != col_name_set:
                    raise PySparkRuntimeError(
                        error_class="PYTHON_DATA_SOURCE_READ_RETURN_SCHEMA_MISMATCH",
                        message_parameters={
                            "expected": str(column_names),
                            "actual": str(result.__fields__),
                        },
                    )
                # Assign the values by name.
                for name in column_names:
                    idx = col_mapping[name]
                    pylist[idx].append(column_converters[idx](result[name]))
            else:
                for col in range(num_cols):
                    pylist[col].append(column_converters[col](result[col]))
        batch = pa.RecordBatch.from_arrays(pylist, schema=pa_schema)
        yield batch


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
                error_class="DATA_SOURCE_TYPE_MISMATCH",
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
                error_class="DATA_SOURCE_TYPE_MISMATCH",
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

        is_streaming = read_bool(infile)

        # Instantiate data source reader.
        reader = (
            _streamReader(data_source, schema)
            if is_streaming
            else data_source.reader(schema=schema)
        )

        # Create input converter.
        converter = ArrowTableToRowsConversion._create_converter(BinaryType())

        # Create output converter.
        return_type = schema

        def data_source_read_func(iterator: Iterable[pa.RecordBatch]) -> Iterable[pa.RecordBatch]:
            partition_bytes = None

            # Get the partition value from the input iterator.
            for batch in iterator:
                # There should be only one row/column in the batch.
                assert batch.num_columns == 1 and batch.num_rows == 1, (
                    "Expected each batch to have exactly 1 column and 1 row, "
                    f"but found {batch.num_columns} columns and {batch.num_rows} rows."
                )
                columns = [column.to_pylist() for column in batch.columns]
                partition_bytes = converter(columns[0][0])

            assert (
                partition_bytes is not None
            ), "The input iterator for Python data source read function is empty."

            # Deserialize the partition value.
            partition = pickleSer.loads(partition_bytes)

            assert partition is None or isinstance(partition, InputPartition), (
                "Expected the partition value to be of type 'InputPartition', "
                f"but found '{type(partition).__name__}'."
            )

            output_iter = reader.read(partition)  # type: ignore[attr-defined]

            # Validate the output iterator.
            if not isinstance(output_iter, Iterator):
                raise PySparkRuntimeError(
                    error_class="DATA_SOURCE_INVALID_RETURN_TYPE",
                    message_parameters={
                        "type": type(output_iter).__name__,
                        "name": data_source.name(),
                        "supported_types": "iterator",
                    },
                )

            return records_to_arrow_batches(
                output_iter, max_arrow_batch_size, return_type, data_source
            )

        command = (data_source_read_func, return_type)
        pickleSer._write_with_length(command, outfile)

        if not is_streaming:
            # The partitioning of python batch source read is determined before query execution.
            try:
                partitions = reader.partitions()  # type: ignore[attr-defined]
                if not isinstance(partitions, list):
                    raise PySparkRuntimeError(
                        error_class="DATA_SOURCE_TYPE_MISMATCH",
                        message_parameters={
                            "expected": "'partitions' to return a list",
                            "actual": f"'{type(partitions).__name__}'",
                        },
                    )
                if not all(isinstance(p, InputPartition) for p in partitions):
                    partition_types = ", ".join([f"'{type(p).__name__}'" for p in partitions])
                    raise PySparkRuntimeError(
                        error_class="DATA_SOURCE_TYPE_MISMATCH",
                        message_parameters={
                            "expected": "elements in 'partitions' to be of type 'InputPartition'",
                            "actual": partition_types,
                        },
                    )
                if len(partitions) == 0:
                    partitions = [None]
            except NotImplementedError:
                partitions = [None]

            # Return the serialized partition values.
            write_int(len(partitions), outfile)
            for partition in partitions:
                pickleSer._write_with_length(partition, outfile)
        else:
            # Send an empty list of partition for stream reader because partitions are planned
            # in each microbatch during query execution.
            write_int(0, outfile)
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
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)
