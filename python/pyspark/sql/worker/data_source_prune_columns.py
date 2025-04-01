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

import faulthandler
from functools import cached_property
import os
import sys
from dataclasses import dataclass, field
from typing import IO, List

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkAssertionError, PySparkValueError
from pyspark.serializers import (
    SpecialLengths,
    UTF8Deserializer,
    read_int,
    write_int,
    write_with_length,
)
from pyspark.sql.datasource import ColumnPruning, DataSource, DataSourceReader
from pyspark.sql.types import ArrayType, DataType, StructType, _parse_datatype_json_string
from pyspark.sql.worker.plan_data_source_read import write_read_func_and_partitions
from pyspark.util import handle_worker_exception, local_connect_and_auth
from pyspark.worker_util import (
    check_python_version,
    pickleSer,
    read_command,
    send_accumulator_updates,
    setup_broadcasts,
    setup_memory_limits,
    setup_spark_files,
)

utf8_deserializer = UTF8Deserializer()


def validate_schema(reader: DataSourceReader, actual: StructType, required: StructType) -> None:
    path: List[str] = []

    def check_nullable(actual: bool, required: bool) -> None:
        if actual != required:
            raise PySparkValueError(
                errorClass="DATA_SOURCE_PRUNED_SCHEMA_NULLABILITY",
                messageParameters={
                    "type": type(reader).__name__,
                    "path": ".".join(path),
                    "actual": str(actual),
                    "required": str(required),
                },
            )

    def check(actual: DataType, required: DataType) -> None:
        if isinstance(actual, StructType) and isinstance(required, StructType):
            actual_fields = {f.name: f for f in actual.fields}
            for field in required.fields:
                path.append(field.name)
                if field.name not in actual_fields:
                    raise PySparkValueError(
                        errorClass="DATA_SOURCE_PRUNED_SCHEMA_MISSING",
                        messageParameters={
                            "type": type(reader).__name__,
                            "path": ".".join(path),
                        },
                    )
                check(actual_fields[field.name].dataType, field.dataType)
                check_nullable(actual_fields[field.name].nullable, field.nullable)
                path.pop()
        elif isinstance(actual, ArrayType) and isinstance(required, ArrayType):
            check(actual.elementType, required.elementType)
            check_nullable(actual.containsNull, required.containsNull)
        elif actual != required:
            raise PySparkValueError(
                errorClass="DATA_SOURCE_PRUNED_SCHEMA_MISMATCH",
                messageParameters={
                    "type": type(reader).__name__,
                    "path": ".".join(path),
                    "actual": actual.json(),
                    "required": required.json(),
                },
            )

    check(actual, required)


def main(infile: IO, outfile: IO) -> None:
    """
    TODO: Add docstring.
    """
    faulthandler_log_path = os.environ.get("PYTHON_FAULTHANDLER_DIR", None)
    try:
        if faulthandler_log_path:
            faulthandler_log_path = os.path.join(faulthandler_log_path, str(os.getpid()))
            faulthandler_log_file = open(faulthandler_log_path, "w")
            faulthandler.enable(file=faulthandler_log_file)

        check_python_version(infile)

        memory_limit_mb = int(os.environ.get("PYSPARK_PLANNER_MEMORY_MB", "-1"))
        setup_memory_limits(memory_limit_mb)

        setup_spark_files(infile)
        setup_broadcasts(infile)

        _accumulatorRegistry.clear()

        # ----------------------------------------------------------------------
        # Start of worker logic
        # ----------------------------------------------------------------------

        def receive_schema() -> StructType:
            schema_json = utf8_deserializer.loads(infile)
            schema = _parse_datatype_json_string(schema_json)
            if not isinstance(schema, StructType):
                raise PySparkAssertionError(
                    errorClass="DATA_SOURCE_TYPE_MISMATCH",
                    messageParameters={
                        "expected": "a schema of type 'StructType'",
                        "actual": f"'{type(schema).__name__}'",
                    },
                )
            return schema

        # Receive the data source instance.
        data_source = read_command(pickleSer, infile)
        if not isinstance(data_source, DataSource):
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": "a Python data source instance of type 'DataSource'",
                    "actual": f"'{type(data_source).__name__}'",
                },
            )

        # Receive the schemas.
        full_schema = receive_schema()
        required_schema = receive_schema()
        required_top_level_schema = receive_schema()

        # Get the reader.
        reader = data_source.reader(schema=full_schema)
        # Validate the reader.
        if not isinstance(reader, DataSourceReader):
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": "an instance of DataSourceReader",
                    "actual": f"'{type(reader).__name__}'",
                },
            )

        # Push down the required schema and get the indices of the unsupported filters.
        actual_schema = reader.pruneColumns(
            ColumnPruning(full_schema, required_schema, required_top_level_schema)
        )
        if not isinstance(actual_schema, StructType):
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": "a schema of type 'StructType'",
                    "actual": f"'{type(actual_schema).__name__}'",
                },
            )

        # Validate the actual schema to make sure it's a superset of the required schema.
        validate_schema(reader, actual_schema, required_schema)

        # Receive the max arrow batch size.
        max_arrow_batch_size = read_int(infile)
        assert max_arrow_batch_size > 0, (
            "The maximum arrow batch size should be greater than 0, but got "
            f"'{max_arrow_batch_size}'"
        )

        # Return the read function and partitions. Doing this in the same worker as column pruning
        # helps reduce the number of Python worker calls.
        write_read_func_and_partitions(
            outfile,
            reader=reader,
            data_source=data_source,
            schema=actual_schema,
            max_arrow_batch_size=max_arrow_batch_size,
        )

        # Return the actual schema.
        write_with_length(actual_schema.json().encode("utf-8"), outfile)

        # ----------------------------------------------------------------------
        # End of worker logic
        # ----------------------------------------------------------------------
    except BaseException as e:
        handle_worker_exception(e, outfile)
        sys.exit(-1)
    finally:
        if faulthandler_log_path:
            faulthandler.disable()
            faulthandler_log_file.close()
            os.remove(faulthandler_log_path)

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
