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
import inspect
import os
import sys
from itertools import chain
from typing import IO

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkAssertionError, PySparkRuntimeError, PySparkTypeError
from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import (
    read_int,
    write_int,
    SpecialLengths,
)
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource
from pyspark.sql.types import _parse_datatype_json_string, StructType, StructField, BinaryType
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
    Main method for saving into a Python data source.
    """
    try:
        check_python_version(infile)

        memory_limit_mb = int(os.environ.get("PYSPARK_PLANNER_MEMORY_MB", "-1"))
        setup_memory_limits(memory_limit_mb)

        setup_spark_files(infile)
        setup_broadcasts(infile)

        _accumulatorRegistry.clear()

        # Receive the data source class.
        data_source_cls = read_command(pickleSer, infile)
        if not (isinstance(data_source_cls, type) and issubclass(data_source_cls, DataSource)):
            raise PySparkAssertionError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": "a subclass of DataSource",
                    "actual": f"'{type(data_source_cls).__name__}'",
                },
            )

        # Check the name method is a class method.
        if not inspect.ismethod(data_source_cls.name):
            raise PySparkTypeError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": "'name()' method to be a classmethod",
                    "actual": f"'{type(data_source_cls.name).__name__}'",
                },
            )

        # Receive the provider name.
        provider = utf8_deserializer.loads(infile)

        # Check if the provider name matches the data source's name.
        if provider.lower() != data_source_cls.name().lower():
            raise PySparkAssertionError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": f"provider with name {data_source_cls.name()}",
                    "actual": f"'{provider}'",
                },
            )

        # Receive the output schema
        schema = _parse_datatype_json_string(utf8_deserializer.loads(infile))
        if not isinstance(schema, StructType):
            raise PySparkAssertionError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": "the user-defined schema to be a 'StructType'",
                    "actual": f"'{type(data_source_cls).__name__}'",
                },
            )

        # Receive the options.
        options = dict()
        num_options = read_int(infile)
        for _ in range(num_options):
            key = utf8_deserializer.loads(infile)
            value = utf8_deserializer.loads(infile)
            options[key] = value

        # Receive the save mode.
        save_mode = utf8_deserializer.loads(infile)
        # TODO: check if the save mode is valid

        # Instantiate a data source.
        # TODO: remove `paths` from DataSource constructor and rename userSpecifiedSchema.
        try:
            data_source = data_source_cls(
                paths=[],
                userSpecifiedSchema=schema,
                options=options,
            )
        except Exception as e:
            raise PySparkRuntimeError(
                error_class="PYTHON_DATA_SOURCE_CREATE_ERROR",
                message_parameters={"type": "instance", "error": str(e)},
            )

        # Instantiate the data source writer.
        try:
            writer = data_source.writer(schema, save_mode)
        except Exception as e:
            # TODO: Change this to be generic error class
            raise PySparkRuntimeError(
                error_class="PYTHON_DATA_SOURCE_CREATE_ERROR",
                message_parameters={"type": "instance", "error": str(e)},
            )

        # Create a UDF to be used in mapInPandas.
        # The purpose of this UDF is to change the input type from an iterator of
        # pandas dataframe to an iterator of rows.
        import pandas as pd

        def data_source_write_func(iterator):  # type: ignore
            rows = (Row(*record) for df in iterator for record in df.to_records(index=False))
            row_iterator = chain.from_iterable(rows)
            res = writer.write(row_iterator)
            # Yield the pickled commit message.
            picked_res = pickleSer.dumps(res)
            yield pd.DataFrame({"message": [picked_res]})

        return_type = StructType([StructField("message", BinaryType(), True)])

        # Return the pickled write UDF.
        command = (data_source_write_func, return_type)
        pickleSer._write_with_length(command, outfile)

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
