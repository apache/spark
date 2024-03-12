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
from typing import IO

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkAssertionError, PySparkRuntimeError, PySparkTypeError
from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import (
    read_bool,
    read_int,
    write_int,
    write_with_length,
    SpecialLengths,
)
from pyspark.sql.datasource import DataSource, CaseInsensitiveDict
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
    Main method for creating a Python data source instance.

    This process is invoked from the `UserDefinedPythonDataSourceRunner.runInPython` method
    in JVM. This process is responsible for creating a `DataSource` object and send the
    information needed back to the JVM.

    The JVM sends the following information to this process:
    - a `DataSource` class representing the data source to be created.
    - a provider name in string.
    - an optional user-specified schema in json string.
    - a dictionary of options in string.

    This process then creates a `DataSource` instance using the above information and
    sends the pickled instance as well as the schema back to the JVM.
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

        # Receive the user-specified schema
        user_specified_schema = None
        if read_bool(infile):
            user_specified_schema = _parse_datatype_json_string(utf8_deserializer.loads(infile))
            if not isinstance(user_specified_schema, StructType):
                raise PySparkAssertionError(
                    error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                    message_parameters={
                        "expected": "the user-defined schema to be a 'StructType'",
                        "actual": f"'{type(data_source_cls).__name__}'",
                    },
                )

        # Receive the options.
        options = CaseInsensitiveDict()
        num_options = read_int(infile)
        for _ in range(num_options):
            key = utf8_deserializer.loads(infile)
            value = utf8_deserializer.loads(infile)
            options[key] = value

        # Instantiate a data source.
        try:
            data_source = data_source_cls(options=options)  # type: ignore
        except Exception as e:
            raise PySparkRuntimeError(
                error_class="DATA_SOURCE_CREATE_ERROR",
                message_parameters={"error": str(e)},
            )

        # Get the schema of the data source.
        # If user_specified_schema is not None, use user_specified_schema.
        # Otherwise, use the schema of the data source.
        # Throw exception if the data source does not implement schema().
        is_ddl_string = False
        if user_specified_schema is None:
            try:
                schema = data_source.schema()
                if isinstance(schema, str):
                    # Here we cannot use _parse_datatype_string to parse the DDL string schema.
                    # as it requires an active Spark session.
                    is_ddl_string = True
            except NotImplementedError:
                raise PySparkRuntimeError(
                    error_class="NOT_IMPLEMENTED",
                    message_parameters={"feature": "DataSource.schema"},
                )
        else:
            schema = user_specified_schema  # type: ignore

        assert schema is not None

        # Return the pickled data source instance.
        pickleSer._write_with_length(data_source, outfile)

        # Return the schema of the data source.
        write_int(int(is_ddl_string), outfile)
        if is_ddl_string:
            write_with_length(schema.encode("utf-8"), outfile)  # type: ignore
        else:
            write_with_length(schema.json().encode("utf-8"), outfile)  # type: ignore

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
