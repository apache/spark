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
import typing
from dataclasses import dataclass, field
from typing import IO, Type, Union

from pyspark.errors import PySparkAssertionError, PySparkValueError
from pyspark.errors.exceptions.base import PySparkNotImplementedError
from pyspark.logger.worker_io import capture_outputs
from pyspark.serializers import UTF8Deserializer, read_int, read_bool, write_int
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
from pyspark.sql.worker.utils import is_method_overridden, worker_run
from pyspark.worker_util import (
    get_sock_file_to_executor,
    pickleSer,
    read_command,
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


def _main(infile: IO, outfile: IO) -> None:
    """
    Main method for planning a data source read with filter and limit pushdown.

    This process is invoked from the `UserDefinedPythonDataSourceReadRunner.runInPython`
    method in the optimizer rule `PlanPythonDataSourceScan` in JVM. This process is responsible
    for creating a `DataSourceReader` object, applying filter and limit pushdown, and sending
    the information needed back to the JVM.

    The infile and outfile are connected to the JVM via a socket. The JVM sends the following
    information to this process via the socket:
    - a `DataSource` instance representing the data source
    - a `StructType` instance representing the output schema of the data source
    - a list of filters to be pushed down
    - the limit to be pushed down, or -1 if there is none
    - configuration values

    This process then creates a `DataSourceReader` instance by calling the `reader` method
    on the `DataSource` instance. It applies the filters by calling the `pushFilters` method
    on the reader and determines which filters are supported. The indices of the supported
    filters are sent back to the JVM, along with the list of partitions and the read function.

    When a limit is sent, it is pushed down by calling the `pushLimit` method on the reader
    after `pushFilters`, and whether the reader accepted it is sent back to the JVM. The JVM
    replays the same filters when pushing down a limit, so that the reader reaches the same
    state as it did during filter pushdown before `pushLimit` is called on it.
    """
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

        # Push down the filters and get the indices of the unsupported filters. `pushFilters` is
        # not called when there is nothing to push, so that a reader planning a limit-only scan
        # does not observe a spurious empty pushFilters call.
        unsupported_filters = set(
            FilterRef(f)
            for f in (reader.pushFilters([ref.filter for ref in filters]) if filters else [])
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

        # Receive the limit to push down. -1 means there is no limit.
        limit = read_int(infile)

        # Receive the max arrow batch size.
        max_arrow_batch_size = read_int(infile)
        assert max_arrow_batch_size > 0, (
            "The maximum arrow batch size should be greater than 0, but got "
            f"'{max_arrow_batch_size}'"
        )
        enable_limit_pushdown = read_bool(infile)
        binary_as_bytes = read_bool(infile)

        if not enable_limit_pushdown and is_method_overridden(reader, "pushLimit"):
            # Do not silently ignore pushLimit when limit pushdown is disabled. This worker also
            # runs for filter-only pushdown, where no limit is ever sent, so the check has to
            # happen here as well as in `plan_data_source_read`.
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_PUSHDOWN_DISABLED",
                messageParameters={
                    "type": type(reader).__name__,
                    "method": "pushLimit",
                    "conf": "spark.sql.python.limitPushdown.enabled",
                },
            )

        # Push down the limit, if any. This must happen after pushFilters, matching the
        # operator order that DSv2 uses on the JVM side.
        is_limit_pushed = False
        if limit >= 0:
            is_limit_pushed = reader.pushLimit(limit)
            if not isinstance(is_limit_pushed, bool):
                raise PySparkValueError(
                    errorClass="DATA_SOURCE_INVALID_RETURN_TYPE",
                    messageParameters={
                        "type": type(is_limit_pushed).__name__,
                        "name": type(reader).__name__ + ".pushLimit",
                        "supported_types": "bool",
                    },
                )

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

    # Return whether the limit was pushed down, as 1 or 0.
    write_int(int(is_limit_pushed), outfile)


def main(infile: IO, outfile: IO) -> None:
    worker_run(_main, infile, outfile)


if __name__ == "__main__":
    with get_sock_file_to_executor() as sock_file:
        main(sock_file, sock_file)
