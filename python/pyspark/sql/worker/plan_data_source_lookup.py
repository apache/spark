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
from collections.abc import Sequence
from typing import IO, Any, Dict, List, Optional, Type

from pyspark.errors import PySparkAssertionError
from pyspark.logger.worker_io import capture_outputs
from pyspark.serializers import UTF8Deserializer, read_bool, write_int, write_with_length
from pyspark.sql.datasource import (
    DataSource,
    DataSourceLookupReader,
    EqualNullSafe,
    EqualTo,
    Filter,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    LookupParameter,
)
from pyspark.sql.types import StructType, _parse_datatype_json_string
from pyspark.sql.worker.data_source_pushdown_filters import push_filters, read_filters
from pyspark.sql.worker.utils import check_pushdown_not_disabled, worker_run
from pyspark.worker_util import (
    get_sock_file_to_executor,
    pickleSer,
    read_command,
)

utf8_deserializer = UTF8Deserializer()

# The filter class of a candidate lookup predicate, by the name of the DSv2 predicate that Spark
# offers for it.
lookup_predicates: Dict[str, Type[Filter]] = {
    "=": EqualTo,
    "<=>": EqualNullSafe,
    "<": LessThan,
    "<=": LessThanOrEqual,
    ">": GreaterThan,
    ">=": GreaterThanOrEqual,
}

# The bounds of a Java int, to which the maximum number of parameter rows per request is clamped.
INT_MIN = -(2**31)
INT_MAX = 2**31 - 1


# The reply, in place of the planned reader, that reports a broken contract of the lookup API.
# It must match `UserDefinedPythonDataSourceLookupPlanRunner.LOOKUP_CONTRACT_VIOLATION`.
LOOKUP_CONTRACT_VIOLATION = -100


class LookupContractViolation(Exception):
    """
    A broken contract of the lookup API, such as a selected position that is not one of a
    candidate. The JVM reports it with the error `DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.<subclass>`
    that it raises for a Scala connector, so that a streaming query stops when it starts.
    """

    def __init__(self, subclass: str, parameters: Dict[str, str]):
        super().__init__(subclass, parameters)
        self.subclass = subclass
        self.parameters = parameters

    @staticmethod
    def of_result(
        method: str, value: Any, expected: str, actual: Optional[str] = None
    ) -> "LookupContractViolation":
        """
        The violation of a method that returned `None`, or a value of the wrong type. `actual`
        describes the type of `value`, by default its name.
        """
        if value is None:
            return LookupContractViolation("NULL_RESULT", {"method": method})
        return LookupContractViolation(
            "INVALID_RESULT_TYPE",
            {
                "method": method,
                "type": actual if actual is not None else type(value).__name__,
                "expected": expected,
            },
        )


def deserializeCandidate(jsonDict: dict) -> Filter:
    """
    Builds a candidate lookup predicate, a filter whose value is a new `LookupParameter`, from
    its JSON form `{"op": "=", "columnPath": ["id"], "dataType": "<data type JSON>"}`.
    """
    filter_cls: Any = lookup_predicates[jsonDict["op"]]
    return filter_cls(
        attribute=tuple(jsonDict["columnPath"]),
        value=LookupParameter(_parse_datatype_json_string(jsonDict["dataType"])),
    )


def select_lookup_predicates(reader: DataSourceLookupReader, candidates: List[Filter]) -> List[int]:
    """
    Calls `reader.selectLookupPredicates` and returns the positions that it selects among the
    candidates, in the order that the reader returned them.

    Python checks what the types of the Scala method already guarantee: a sequence of `int`s.
    The JVM checks that its copy of the candidates is unchanged, but the reader receives this
    list, so Python checks it, as the JVM does: the positions refer to the offered order. It
    also checks that each position is one of a candidate, as a Java `int` cannot hold every
    Python `int`. The JVM then checks the rest with the same errors as for a Scala connector: it
    rejects a duplicate and an empty selection.
    """
    method = "DataSourceLookupReader.selectLookupPredicates()"
    expected = "a sequence of ints"
    offered = list(candidates)
    selected = reader.selectLookupPredicates(candidates)
    if not isinstance(selected, Sequence):
        raise LookupContractViolation.of_result(method, selected, expected)
    for position in selected:
        # A bool is an int in Python, but never a meaningful position.
        if not isinstance(position, int) or isinstance(position, bool):
            raise LookupContractViolation.of_result(
                method,
                selected,
                expected,
                f"{type(selected).__name__} holding {type(position).__name__}",
            )
    if len(selected) > 0:
        check_candidates_unchanged(offered, candidates)
    # The positions refer to the offered candidates, not to any that the reader appended.
    for position in selected:
        if not 0 <= position < len(offered):
            raise LookupContractViolation(
                "INVALID_SELECTED_PREDICATE_POSITION",
                {"position": str(position), "maxPosition": str(len(offered) - 1)},
            )
    return list(selected)


def check_candidates_unchanged(offered: List[Filter], candidates: List[Filter]) -> None:
    """
    Checks that `candidates` still holds the `offered` candidates in the offered order, after
    the reader selected a non-empty access path. As each candidate has its own
    `LookupParameter`, which equals only itself, `==` tells every reorder apart, and it accepts
    an equal rebuilt candidate, as the JVM's check does. Candidates that the reader appended
    change no offered position, so they are not checked here, and a position of one fails the
    range check.
    """
    for position, candidate in enumerate(offered):
        if position >= len(candidates) or candidates[position] != candidate:
            raise LookupContractViolation("MODIFIED_CANDIDATES", {"position": str(position)})


def max_parameter_rows_per_request(reader: DataSourceLookupReader) -> int:
    """
    Calls `reader.maxParameterRowsPerRequest` and checks that it returns an `int`. The JVM checks
    that the value is positive, as for a Scala connector, so a value out of the range of a Java
    int is clamped to it, keeping its sign.
    """
    max_rows = reader.maxParameterRowsPerRequest()
    # A bool is an int in Python, but never a meaningful number of rows.
    if not isinstance(max_rows, int) or isinstance(max_rows, bool):
        raise LookupContractViolation.of_result(
            "DataSourceLookupReader.maxParameterRowsPerRequest()", max_rows, "an int"
        )
    return min(max(max_rows, INT_MIN), INT_MAX)


def _main(infile: IO, outfile: IO) -> None:
    """
    Main method for planning a data source lookup.

    This process is invoked from the `UserDefinedPythonDataSourceLookupPlanRunner.runInPython`
    method, which `PythonScanBuilder.selectLookupPredicates` runs on the JVM while Spark plans a
    lookup join. This process is responsible for creating a `DataSourceLookupReader` object,
    planning the lookup with it, and sending the planned reader back to the JVM.

    The infile and outfile are connected to the JVM via a socket. The JVM sends the following
    information to this process via the socket:
    - a `DataSource` instance representing the data source
    - a `StructType` instance representing the schema of the data source
    - a list of filters to be pushed down, in the format of `data_source_pushdown_filters`
    - a list of candidate lookup predicates
    - whether filter pushdown is enabled

    This process then creates a `DataSourceLookupReader` instance by calling the `lookupReader`
    method on the `DataSource` instance. It pushes the filters down with `pushFilters`, when
    there are any, then calls `selectLookupPredicates` with the candidates, and then
    `maxParameterRowsPerRequest`, in the order in which Spark calls the methods of a Scala
    connector.

    The values sent back to the JVM, in order, are:
    - the pickled reader, as planned, which the executors use to look up rows.
    - the indices of the filters that the reader pushes.
    - the positions of the selected candidates, in the order of the reader's selection.
    - the maximum number of parameter rows per request.

    When the data source breaks the contract of the lookup API, the reply is instead
    `LOOKUP_CONTRACT_VIOLATION`, the subclass of the JVM's error and its parameters.
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

    # Receive the data source schema.
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

    # Receive the filters and the candidate lookup predicates.
    filters = read_filters(infile)
    candidates = [deserializeCandidate(c) for c in json.loads(utf8_deserializer.loads(infile))]
    enable_filter_pushdown = read_bool(infile)

    try:
        with capture_outputs():
            # Get the lookup reader.
            reader = data_source.lookupReader(schema=schema)
            if not isinstance(reader, DataSourceLookupReader):
                raise LookupContractViolation.of_result(
                    "DataSource.lookupReader()", reader, "a DataSourceLookupReader"
                )

            # Do not silently ignore a pushFilters method while filter pushdown is disabled.
            # Spark never pushes a limit into a lookup, and a lookup reader has no pushLimit.
            check_pushdown_not_disabled(
                reader, enable_filter_pushdown=enable_filter_pushdown, enable_limit_pushdown=False
            )

            # Plan the lookup, in the order in which Spark calls a Scala connector: filter
            # pushdown, then the selection of the lookup predicates, then the request size.
            pushed_filter_indices = push_filters(reader, filters)
            selected_positions = select_lookup_predicates(reader, candidates)
            max_rows = max_parameter_rows_per_request(reader)
    except LookupContractViolation as violation:
        write_int(LOOKUP_CONTRACT_VIOLATION, outfile)
        write_with_length(violation.subclass.encode("utf-8"), outfile)
        write_int(len(violation.parameters), outfile)
        for key, value in violation.parameters.items():
            write_with_length(key.encode("utf-8"), outfile)
            write_with_length(value.encode("utf-8"), outfile)
        return

    # Return the planned reader. It is pickled before anything else is written, so that a reader
    # that cannot be pickled fails with the error of the pickler instead of a malformed reply.
    pickleSer._write_with_length(reader, outfile)

    # Return the pushed filter indices.
    write_int(len(pushed_filter_indices), outfile)
    for index in pushed_filter_indices:
        write_int(index, outfile)

    # Return the selected candidate positions, in order.
    write_int(len(selected_positions), outfile)
    for position in selected_positions:
        write_int(position, outfile)

    # Return the maximum number of parameter rows per request.
    write_int(max_rows, outfile)


def main(infile: IO, outfile: IO) -> None:
    worker_run(_main, infile, outfile)


if __name__ == "__main__":
    with get_sock_file_to_executor() as sock_file:
        main(sock_file, sock_file)
