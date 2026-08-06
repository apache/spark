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

import json
from typing import IO, Any, Sequence

from pyspark.errors import PySparkAssertionError, PySparkValueError
from pyspark.logger.worker_io import capture_outputs
from pyspark.serializers import UTF8Deserializer, read_bool, read_int, write_with_length
from pyspark.sql.datasource import (
    CaseInsensitiveDict,
    ClusteredDistribution,
    DataSource,
    DataSourceWriter,
    Distribution,
    NullOrdering,
    OrderedDistribution,
    SortDirection,
    SortOrder,
    UnspecifiedDistribution,
)
from pyspark.sql.types import StructType, _parse_datatype_json_string
from pyspark.sql.worker.utils import worker_run
from pyspark.worker_util import (
    get_sock_file_to_executor,
    pickleSer,
    read_command,
)

utf8_deserializer = UTF8Deserializer()

_JVM_INT_MAX = 2**31 - 1
_JVM_LONG_MAX = 2**63 - 1


def serialize_column_path(writer: DataSourceWriter, column: Any) -> list:
    """Validates a `ColumnPath` and converts it to a JSON-serializable list."""
    if isinstance(column, str) or not isinstance(column, (tuple, list)):
        raise PySparkValueError(
            errorClass="DATA_SOURCE_INVALID_COLUMN_PATH",
            messageParameters={
                "type": type(writer).__name__,
                "column": repr(column),
            },
        )
    if len(column) == 0 or not all(isinstance(part, str) for part in column):
        raise PySparkValueError(
            errorClass="DATA_SOURCE_INVALID_COLUMN_PATH",
            messageParameters={
                "type": type(writer).__name__,
                "column": repr(column),
            },
        )
    return list(column)


def serialize_sort_order(writer: DataSourceWriter, order: Any) -> dict:
    """Converts a `SortOrder` to a JSON-serializable dict."""
    if not isinstance(order, SortOrder):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "an instance of SortOrder",
                "actual": f"'{type(order).__name__}'",
            },
        )
    if not isinstance(order.direction, SortDirection):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "an instance of SortDirection",
                "actual": f"'{type(order.direction).__name__}'",
            },
        )
    if order.nullOrdering is not None and not isinstance(order.nullOrdering, NullOrdering):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "an instance of NullOrdering or None",
                "actual": f"'{type(order.nullOrdering).__name__}'",
            },
        )
    return {
        "columnPath": serialize_column_path(writer, order.column),
        "direction": order.direction.value,
        # None lets the JVM apply the default null ordering for the direction.
        "nullOrdering": order.nullOrdering.value if order.nullOrdering is not None else None,
    }


def serialize_distribution(writer: DataSourceWriter, distribution: Any) -> dict:
    """Converts a `Distribution` to a JSON-serializable dict."""
    if not isinstance(distribution, Distribution):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "an instance of Distribution",
                "actual": f"'{type(distribution).__name__}'",
            },
        )
    if isinstance(distribution, UnspecifiedDistribution):
        return {"name": "UnspecifiedDistribution"}
    if isinstance(distribution, ClusteredDistribution):
        clustering = distribution.clustering
        if not clustering:
            raise PySparkValueError(
                errorClass="DATA_SOURCE_EMPTY_DISTRIBUTION",
                messageParameters={
                    "type": type(writer).__name__,
                    "distribution": "ClusteredDistribution",
                    "field": "clustering",
                },
            )
        return {
            "name": "ClusteredDistribution",
            "clustering": [serialize_column_path(writer, c) for c in clustering],
        }
    if isinstance(distribution, OrderedDistribution):
        ordering = distribution.ordering
        if not ordering:
            raise PySparkValueError(
                errorClass="DATA_SOURCE_EMPTY_DISTRIBUTION",
                messageParameters={
                    "type": type(writer).__name__,
                    "distribution": "OrderedDistribution",
                    "field": "ordering",
                },
            )
        return {
            "name": "OrderedDistribution",
            "ordering": [serialize_sort_order(writer, o) for o in ordering],
        }
    # A user-defined Distribution subclass Spark has no planner support for.
    raise PySparkAssertionError(
        errorClass="DATA_SOURCE_TYPE_MISMATCH",
        messageParameters={
            "expected": "one of UnspecifiedDistribution, ClusteredDistribution, "
            "or OrderedDistribution",
            "actual": f"'{type(distribution).__name__}'",
        },
    )


def serialize_ordering(writer: DataSourceWriter, ordering: Any) -> list:
    """Validates the result of `requiredOrdering` and converts it to a JSON-serializable list."""
    if isinstance(ordering, (str, bytes)) or not isinstance(ordering, Sequence):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "a list of SortOrder",
                "actual": f"'{type(ordering).__name__}'",
            },
        )
    return [serialize_sort_order(writer, o) for o in ordering]


def read_bool_requirement(method: str, value: Any) -> bool:
    """Validates that a bool-returning requirement method returned a bool."""
    if not isinstance(value, bool):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": f"a bool from '{method}'",
                "actual": f"'{type(value).__name__}'",
            },
        )
    return value


def read_int_requirement(writer: DataSourceWriter, method: str, value: Any, max_value: int) -> int:
    """Validates that an int-returning requirement method returned an int the JVM can hold."""
    # bool is a subclass of int, but is never a meaningful partition count or size.
    if not isinstance(value, int) or isinstance(value, bool):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": f"an int from '{method}'",
                "actual": f"'{type(value).__name__}'",
            },
        )
    if value > max_value:
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": f"an int from '{method}' of at most {max_value}",
                "actual": str(value),
            },
        )
    return value


def _main(infile: IO, outfile: IO) -> None:
    """
    Main method for planning the required distribution and ordering of a data source write.

    This process is invoked from `UserDefinedPythonDataSourceWriteDistributionAndOrderingRunner`,
    which `PythonWrite` runs when the optimizer rule `V2Writes` asks the write for its required
    distribution and ordering.

    The JVM sends the data source class, the input schema of the write, the write options and
    the overwrite flag. This process instantiates the data source, creates its
    `DataSourceWriter`, and sends the writer's requirements back to the JVM as JSON.
    """
    # Receive the data source class.
    data_source_cls = read_command(pickleSer, infile)
    if not (isinstance(data_source_cls, type) and issubclass(data_source_cls, DataSource)):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "a subclass of DataSource",
                "actual": f"'{type(data_source_cls).__name__}'",
            },
        )

    # Receive the input schema of the write.
    schema_json = utf8_deserializer.loads(infile)
    schema = _parse_datatype_json_string(schema_json)
    if not isinstance(schema, StructType):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "an input schema of type 'StructType'",
                "actual": f"'{type(schema).__name__}'",
            },
        )

    # Receive the options.
    options = CaseInsensitiveDict()
    for _ in range(read_int(infile)):
        key = utf8_deserializer.loads(infile)
        options[key] = utf8_deserializer.loads(infile)

    # Receive the overwrite flag.
    overwrite = read_bool(infile)

    with capture_outputs():
        writer = data_source_cls(options=options).writer(schema, overwrite)
        if not isinstance(writer, DataSourceWriter):
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": "an instance of DataSourceWriter",
                    "actual": f"'{type(writer).__name__}'",
                },
            )

        requirements = {
            "distribution": serialize_distribution(writer, writer.requiredDistribution()),
            "distributionStrictlyRequired": read_bool_requirement(
                "distributionStrictlyRequired", writer.distributionStrictlyRequired()
            ),
            "requiredNumPartitions": read_int_requirement(
                writer, "requiredNumPartitions", writer.requiredNumPartitions(), _JVM_INT_MAX
            ),
            "advisoryPartitionSizeInBytes": read_int_requirement(
                writer,
                "advisoryPartitionSizeInBytes",
                writer.advisoryPartitionSizeInBytes(),
                _JVM_LONG_MAX,
            ),
            "ordering": serialize_ordering(writer, writer.requiredOrdering()),
        }

    # Return the write requirements as JSON.
    write_with_length(json.dumps(requirements).encode("utf-8"), outfile)


def main(infile: IO, outfile: IO) -> None:
    worker_run(_main, infile, outfile)


if __name__ == "__main__":
    with get_sock_file_to_executor() as sock_file:
        main(sock_file, sock_file)
