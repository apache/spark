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

from __future__ import annotations

import math
import re
import unittest
from collections.abc import Callable
from typing import Any


class _Approx:
    def __init__(self, expected: float, abs: float = 1e-12, rel: float = 1e-6) -> None:
        self.expected = expected
        self.abs = abs
        self.rel = rel

    def __eq__(self, actual: object) -> bool:
        return isinstance(actual, (int, float)) and math.isclose(
            actual, self.expected, abs_tol=self.abs, rel_tol=self.rel
        )


class _Raises:
    def __init__(self, exception: type[BaseException], match: str | None = None) -> None:
        self.exception = exception
        self.match = match

    def __enter__(self) -> "_Raises":
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object,
    ) -> bool:
        if exception_type is None:
            raise AssertionError(f"{self.exception.__name__} was not raised")
        if not issubclass(exception_type, self.exception):
            return False
        if self.match is not None and (
            exception is None or re.search(self.match, str(exception)) is None
        ):
            raise AssertionError(
                f"{self.match!r} does not match exception message {str(exception)!r}"
            )
        return True


class _Mark:
    @staticmethod
    def parametrize(
        *args: Any, **kwargs: Any
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return lambda function: function

    @staticmethod
    def skipif(
        condition: bool, *, reason: str
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
            if not condition:
                return function

            def skipped(*args: Any, **kwargs: Any) -> None:
                raise unittest.SkipTest(reason)

            return skipped

        return decorate


class _PytestCompatibility:
    mark = _Mark()

    @staticmethod
    def approx(expected: float, *, abs: float = 1e-12, rel: float = 1e-6) -> _Approx:
        return _Approx(expected, abs=abs, rel=rel)

    @staticmethod
    def fixture(*args: Any, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return lambda function: function

    @staticmethod
    def raises(exception: type[BaseException], *, match: str | None = None) -> _Raises:
        return _Raises(exception, match)

    @staticmethod
    def skip(reason: str) -> None:
        raise unittest.SkipTest(reason)


pytest = _PytestCompatibility()
