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
from typing import Any, Iterator, Type, TypeVar, overload

from pyspark.errors import PySparkTypeError

T = TypeVar("T")


@overload
def verify_return_type(result: Any, expected_type: Type[T]) -> T: ...


@overload
def verify_return_type(result: Any, expected_type: Any) -> Any: ...


def verify_return_type(result: Any, expected_type: Any) -> Any:
    """
    Verify a UDF return value against an expected type.

    Returns ``result`` unchanged if ``isinstance(result, expected_type)``.
    For ``Iterator[T]``, returns a lazy iterator that checks each element
    against ``T`` on consumption. Raises ``PySparkTypeError`` on mismatch.
    """
    if getattr(expected_type, "_name", None) == "Iterator":
        (element_type,) = expected_type.__args__
        package = getattr(inspect.getmodule(element_type), "__package__", "")
        label = f"iterator of {package}.{element_type.__name__}"

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

        return map(check_element, result)

    if not isinstance(result, expected_type):
        package = getattr(inspect.getmodule(expected_type), "__package__", "")
        raise PySparkTypeError(
            errorClass="UDF_RETURN_TYPE",
            messageParameters={
                "expected": f"{package}.{expected_type.__name__}",
                "actual": type(result).__name__,
            },
        )
    return result
