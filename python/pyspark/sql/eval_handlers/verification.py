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

"""Verification helpers shared by the eval type handlers and the worker.

These validate a UDF's result before it is written back to the JVM. Handlers
call them from ``post_process``; the worker still calls them from the eval-type
branches that have not been migrated yet.
"""

from typing import Any

from pyspark.errors import PySparkRuntimeError, PySparkTypeError


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
