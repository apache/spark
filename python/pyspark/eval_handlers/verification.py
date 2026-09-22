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

"""Verification helpers for a UDF's result before it is written back to the JVM.

Used by the eval type handlers and by the worker's ``read_udfs``.
"""

from collections.abc import Iterator
from typing import Any, Callable, Type, TypeVar, Union, cast, get_args, get_origin

from pyspark.errors import PySparkRuntimeError, PySparkTypeError

T = TypeVar("T")


def _top_level_package(t: type) -> str:
    """Return the top-level package of ``t`` (``pandas`` for ``pd.DataFrame``)."""
    return (t.__module__ or "").split(".", 1)[0]


def verify_iter_result_row_count(
    iterator: Iterator,
    expected_rows: Callable[[], int],
) -> Iterator:
    """Yield elements and verify final row count matches expected exactly.

    ``expected_rows`` is a callable because the expected count is only known once
    the iterator is fully consumed (input rows are counted lazily as a side effect
    of pulling batches), so it must be read after this generator is exhausted.
    """
    actual_rows = 0
    for element in iterator:
        actual_rows += len(element)
        yield element

    verify_result_row_count(actual_rows, expected_rows())


def verify_iterator_exhausted(iterator: Iterator) -> None:
    """Verify that an iterator has been fully consumed."""
    try:
        next(iterator)
    except StopIteration:
        pass
    else:
        raise PySparkRuntimeError(errorClass="INPUT_NOT_FULLY_CONSUMED", messageParameters={})


def verify_output_row_limit(
    iterator: Iterator,
    max_rows: Union[int, Callable[[], int]],
) -> Iterator:
    """Yield elements while verifying total rows do not exceed a limit (fail-fast)."""
    total_rows = 0
    for element in iterator:
        total_rows += len(element)
        if total_rows > (max_rows() if callable(max_rows) else max_rows):
            raise PySparkRuntimeError(errorClass="OUTPUT_EXCEEDS_INPUT_ROWS", messageParameters={})
        yield element


def verify_result_row_count(result_length: int, expected: int) -> None:
    """Raise if the result row count doesn't match the expected input row count."""
    if result_length != expected:
        raise PySparkRuntimeError(
            errorClass="RESULT_ROWS_MISMATCH",
            messageParameters={
                "output_length": str(result_length),
                "input_length": str(expected),
            },
        )


def verify_return_type(result: T, expected_type: Type[T]) -> T:
    """
    Verify a UDF return value against an expected type.

    Returns ``result`` unchanged if ``isinstance(result, expected_type)``.
    For ``Iterator[T]``, returns a lazy iterator that checks each element
    against ``T`` on consumption. Raises ``PySparkTypeError`` on mismatch.
    """
    if get_origin(expected_type) is Iterator:
        (element_type,) = get_args(expected_type)
        label = f"iterator of {_top_level_package(element_type)}.{element_type.__name__}"

        if not isinstance(result, Iterator):
            raise PySparkTypeError(
                errorClass="UDF_RETURN_TYPE",
                messageParameters={"expected": label, "actual": type(result).__name__},
            )

        def check_element(element: T) -> T:
            if not isinstance(element, element_type):
                raise PySparkTypeError(
                    errorClass="UDF_RETURN_TYPE",
                    messageParameters={
                        "expected": label,
                        "actual": f"iterator of {type(element).__name__}",
                    },
                )
            return element

        return cast(T, map(check_element, result))

    if not isinstance(result, expected_type):
        raise PySparkTypeError(
            errorClass="UDF_RETURN_TYPE",
            messageParameters={
                "expected": f"{_top_level_package(expected_type)}.{expected_type.__name__}",
                "actual": type(result).__name__,
            },
        )
    return result


def verify_scalar_result(result: Any, num_rows: int) -> Any:
    """
    Verify a scalar UDF result is array-like and has the expected number of rows.

    Parameters
    ----------
    result : Any
        The UDF result to verify.
    num_rows : int
        Expected number of rows (must match input batch size).
    """
    try:
        result_length = len(result)
    except TypeError:
        raise PySparkTypeError(
            errorClass="UDF_RETURN_TYPE",
            messageParameters={
                "expected": "array-like object",
                "actual": type(result).__name__,
            },
        )
    verify_result_row_count(result_length, num_rows)
    return result
