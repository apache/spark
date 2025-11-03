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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import TYPE_CHECKING, Any, Union, Sequence, List, Optional, Tuple, cast, Iterable

from pyspark.sql.column import Column
from pyspark.sql.window import (
    Window as ParentWindow,
    WindowSpec as ParentWindowSpec,
)
from pyspark.sql.connect.expressions import Expression, SortOrder
from pyspark.sql.connect.functions import builtin as F

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName

__all__ = ["Window", "WindowSpec"]


def _to_cols(cols: Tuple[Union["ColumnOrName", Sequence["ColumnOrName"]], ...]) -> List[Column]:
    if len(cols) == 1 and isinstance(cols[0], list):
        cols = cols[0]  # type: ignore[assignment]
    return [F._to_col(c) for c in cast(Iterable["ColumnOrName"], cols)]


class WindowFrame:
    def __init__(self, isRowFrame: bool, start: int, end: int) -> None:
        super().__init__()

        assert isinstance(isRowFrame, bool)
        assert isinstance(start, int)
        assert isinstance(end, int)
        self._isRowFrame = isRowFrame
        self._start = start
        self._end = end

    def __repr__(self) -> str:
        if self._isRowFrame:
            return f"WindowFrame(ROW_FRAME, {self._start}, {self._end})"
        else:
            return f"WindowFrame(RANGE_FRAME, {self._start}, {self._end})"


class WindowSpec(ParentWindowSpec):
    def __new__(
        cls,
        partitionSpec: Sequence[Expression],
        orderSpec: Sequence[SortOrder],
        frame: Optional[WindowFrame],
    ) -> "WindowSpec":
        self = object.__new__(cls)
        self.__init__(partitionSpec, orderSpec, frame)  # type: ignore[misc]
        return self

    def __getnewargs__(self) -> Tuple[Any, ...]:
        return (self._partitionSpec, self._orderSpec, self._frame)

    def __init__(
        self,
        partitionSpec: Sequence[Expression],
        orderSpec: Sequence[SortOrder],
        frame: Optional[WindowFrame],
    ) -> None:
        assert isinstance(partitionSpec, list) and all(
            isinstance(p, Expression) for p in partitionSpec
        )
        assert isinstance(orderSpec, list) and all(isinstance(s, SortOrder) for s in orderSpec)
        assert frame is None or isinstance(frame, WindowFrame)
        self._partitionSpec = partitionSpec
        self._orderSpec = orderSpec
        self._frame = frame

    def partitionBy(self, *cols: Union["ColumnOrName", Sequence["ColumnOrName"]]) -> "WindowSpec":
        return WindowSpec(
            partitionSpec=[c._expr for c in _to_cols(cols)],  # type: ignore[misc]
            orderSpec=self._orderSpec,
            frame=self._frame,
        )

    def orderBy(self, *cols: Union["ColumnOrName", Sequence["ColumnOrName"]]) -> "WindowSpec":
        return WindowSpec(
            partitionSpec=self._partitionSpec,
            orderSpec=[cast(SortOrder, F._sort_col(c)._expr) for c in _to_cols(cols)],
            frame=self._frame,
        )

    def rowsBetween(self, start: int, end: int) -> "WindowSpec":
        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing

        return WindowSpec(
            partitionSpec=self._partitionSpec,
            orderSpec=self._orderSpec,
            frame=WindowFrame(isRowFrame=True, start=start, end=end),
        )

    def rangeBetween(self, start: int, end: int) -> "WindowSpec":
        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing

        return WindowSpec(
            partitionSpec=self._partitionSpec,
            orderSpec=self._orderSpec,
            frame=WindowFrame(isRowFrame=False, start=start, end=end),
        )

    def __repr__(self) -> str:
        strs: List[str] = []
        if len(self._partitionSpec) > 0:
            str_p = ", ".join([str(p) for p in self._partitionSpec])
            strs.append(f"PartitionBy({str_p})")
        if len(self._orderSpec) > 0:
            str_s = ", ".join([str(s) for s in self._orderSpec])
            strs.append(f"OrderBy({str_s})")
        if self._frame is not None:
            strs.append(str(self._frame))
        return "WindowSpec(" + ", ".join(strs) + ")"


class Window(ParentWindow):
    _spec = WindowSpec(partitionSpec=[], orderSpec=[], frame=None)

    @staticmethod
    def partitionBy(*cols: Union["ColumnOrName", Sequence["ColumnOrName"]]) -> "WindowSpec":
        return Window._spec.partitionBy(*cols)

    @staticmethod
    def orderBy(*cols: Union["ColumnOrName", Sequence["ColumnOrName"]]) -> "WindowSpec":
        return Window._spec.orderBy(*cols)

    @staticmethod
    def rowsBetween(start: int, end: int) -> "WindowSpec":
        return Window._spec.rowsBetween(start, end)

    @staticmethod
    def rangeBetween(start: int, end: int) -> "WindowSpec":
        return Window._spec.rangeBetween(start, end)


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.window

    globs = pyspark.sql.window.__dict__.copy()
    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.window tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.window,
        globs=globs,
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL,
    )

    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
