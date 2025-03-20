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
import os
import sys
from dataclasses import dataclass, field
from typing import IO, List

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkAssertionError, PySparkValueError
from pyspark.serializers import SpecialLengths, UTF8Deserializer, read_int, write_int
from pyspark.sql.datasource import DataSource, DataSourceReader, EqualTo, Filter
from pyspark.sql.types import StructType, _parse_datatype_json_string
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


@dataclass(frozen=True)
class FilterRef:
    filter: Filter = field(compare=False)
    id: int = field(init=False)  # only id is used for comparison

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", id(self.filter))


def main(infile: IO, outfile: IO) -> None:
    """
    Main method for planning a data source read with filter pushdown.

    This process is invoked from the `UserDefinedPythonDataSourceReadRunner.runInPython`
    method in the optimizer rule `PlanPythonDataSourceScan` in JVM. This process is responsible
    for creating a `DataSourceReader` object, applying filter pushdown, and sending the
    information needed back to the JVM.

    The infile and outfile are connected to the JVM via a socket. The JVM sends the following
    information to this process via the socket:
    - a `DataSource` instance representing the data source
    - a `StructType` instance representing the output schema of the data source
    - a list of filters to be pushed down

    This process then creates a `DataSourceReader` instance by calling the `reader` method
    on the `DataSource` instance. It applies the filters by calling the `pushFilters` method
    on the reader and determines which filters are supported. The data source with updated reader
    is then sent back to the JVM along with the indices of the supported filters.
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

        # Receive the data source output schema.
        schema_json = utf8_deserializer.loads(infile)
        schema = _parse_datatype_json_string(schema_json)
        if not isinstance(schema, StructType):
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": "an output schema of type 'StructType'",
                    "actual": f"'{type(schema).__name__}'",
                },
            )

        # Get the reader.
        reader = data_source.reader(schema=schema)
        # Validate the reader.
        if not isinstance(reader, DataSourceReader):
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": "an instance of DataSourceReader",
                    "actual": f"'{type(reader).__name__}'",
                },
            )

        # Receive the pushdown filters.
        num_filters = read_int(infile)
        filters: List[FilterRef] = []
        for _ in range(num_filters):
            name = utf8_deserializer.loads(infile)
            if name == "EqualTo":
                num_parts = read_int(infile)
                column_path = tuple(utf8_deserializer.loads(infile) for _ in range(num_parts))
                value = read_int(infile)
                filters.append(FilterRef(EqualTo(column_path, value)))
            else:
                raise PySparkAssertionError(
                    errorClass="DATA_SOURCE_UNSUPPORTED_FILTER",
                    messageParameters={
                        "name": name,
                    },
                )

        # Push down the filters and get the indices of the unsupported filters.
        unsupported_filters = set(
            FilterRef(f) for f in reader.pushFilters([ref.filter for ref in filters])
        )
        supported_filter_indices = []
        for i, filter in enumerate(filters):
            if filter in unsupported_filters:
                unsupported_filters.remove(filter)
            else:
                supported_filter_indices.append(i)

        # If it returned any filters that are not in the original filters, raise an error.
        if len(unsupported_filters) > 0:
            raise PySparkValueError(
                errorClass="DATA_SOURCE_EXTRANEOUS_FILTERS",
                messageParameters={
                    "type": type(reader).__name__,
                    "input": str(list(filters)),
                    "extraneous": str(list(unsupported_filters)),
                },
            )

        # Monkey patch the data source instance
        # to return the existing reader with the pushed down filters.
        data_source.reader = lambda schema: reader  # type: ignore[method-assign]
        pickleSer._write_with_length(data_source, outfile)

        # Return the supported filter indices.
        write_int(len(supported_filter_indices), outfile)
        for index in supported_filter_indices:
            write_int(index, outfile)

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
