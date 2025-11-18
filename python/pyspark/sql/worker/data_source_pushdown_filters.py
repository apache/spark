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

import base64
import json
import os
import sys
import typing
from dataclasses import dataclass, field
from typing import IO, Type, Union

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkAssertionError, PySparkValueError
from pyspark.errors.exceptions.base import PySparkNotImplementedError
from pyspark.logger.worker_io import capture_outputs
from pyspark.serializers import SpecialLengths, UTF8Deserializer, read_int, read_bool, write_int
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    EqualNullSafe,
    EqualTo,
    Filter,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNotNull,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    StringContains,
    StringEndsWith,
    StringStartsWith,
)
from pyspark.sql.types import StructType, VariantVal, _parse_datatype_json_string
from pyspark.sql.worker.plan_data_source_read import write_read_func_and_partitions
from pyspark.util import (
    handle_worker_exception,
    local_connect_and_auth,
    with_faulthandler,
    start_faulthandler_periodic_traceback,
)
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

BinaryFilter = Union[
    EqualTo,
    EqualNullSafe,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    In,
    StringStartsWith,
    StringEndsWith,
    StringContains,
]

binary_filters = {cls.__name__: cls for cls in typing.get_args(BinaryFilter)}

UnaryFilter = Union[IsNotNull, IsNull]

unary_filters = {cls.__name__: cls for cls in typing.get_args(UnaryFilter)}


@dataclass(frozen=True)
class FilterRef:
    filter: Filter = field(compare=False)
    id: int = field(init=False)  # only id is used for comparison

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", id(self.filter))


def deserializeVariant(variantDict: dict) -> VariantVal:
    value = base64.b64decode(variantDict["value"])
    metadata = base64.b64decode(variantDict["metadata"])
    return VariantVal(value, metadata)


def deserializeFilter(jsonDict: dict) -> Filter:
    name = jsonDict["name"]
    filter: Filter
    if name in binary_filters:
        binary_filter_cls: Type[BinaryFilter] = binary_filters[name]
        filter = binary_filter_cls(
            attribute=tuple(jsonDict["columnPath"]),
            value=deserializeVariant(jsonDict["value"]).toPython(),
        )
    elif name in unary_filters:
        unary_filter_cls: Type[UnaryFilter] = unary_filters[name]
        filter = unary_filter_cls(attribute=tuple(jsonDict["columnPath"]))
    else:
        raise PySparkNotImplementedError(
            errorClass="UNSUPPORTED_FILTER",
            messageParameters={"name": name},
        )
    if jsonDict["isNegated"]:
        filter = Not(filter)
    return filter


@with_faulthandler
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
    - configuration values

    This process then creates a `DataSourceReader` instance by calling the `reader` method
    on the `DataSource` instance. It applies the filters by calling the `pushFilters` method
    on the reader and determines which filters are supported. The indices of the supported
    filters are sent back to the JVM, along with the list of partitions and the read function.
    """
    try:
        check_python_version(infile)

        start_faulthandler_periodic_traceback()

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

        with capture_outputs():
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
            json_str = utf8_deserializer.loads(infile)
            filter_dicts = json.loads(json_str)
            filters = [FilterRef(deserializeFilter(f)) for f in filter_dicts]

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

            # Receive the max arrow batch size.
            max_arrow_batch_size = read_int(infile)
            assert max_arrow_batch_size > 0, (
                "The maximum arrow batch size should be greater than 0, but got "
                f"'{max_arrow_batch_size}'"
            )
            binary_as_bytes = read_bool(infile)

            # Return the read function and partitions. Doing this in the same worker
            # as filter pushdown helps reduce the number of Python worker calls.
            write_read_func_and_partitions(
                outfile,
                reader=reader,
                data_source=data_source,
                schema=schema,
                max_arrow_batch_size=max_arrow_batch_size,
                binary_as_bytes=binary_as_bytes,
            )

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
    conn_info = os.environ.get(
        "PYTHON_WORKER_FACTORY_SOCK_PATH", int(os.environ.get("PYTHON_WORKER_FACTORY_PORT", -1))
    )
    auth_secret = os.environ.get("PYTHON_WORKER_FACTORY_SECRET")
    (sock_file, _) = local_connect_and_auth(conn_info, auth_secret)
    main(sock_file, sock_file)
