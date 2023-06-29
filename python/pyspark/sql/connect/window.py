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

import sys
from typing import TYPE_CHECKING, Union, Sequence, List, Optional

from pyspark.sql.connect.column import Column
from pyspark.sql.connect.expressions import (
    ColumnReference,
    Expression,
    SortOrder,
)
from pyspark.sql.connect.types import (
    JVM_LONG_MIN,
    JVM_LONG_MAX,
)
from pyspark.sql.window import Window as PySparkWindow, WindowSpec as PySparkWindowSpec
from pyspark.errors import PySparkTypeError

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName

__all__ = ["Window", "WindowSpec"]


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


class WindowSpec:
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

    def partitionBy(self, *cols: Union["ColumnOrName", List["ColumnOrName"]]) -> "WindowSpec":
        _cols: List[ColumnOrName] = []
        for col in cols:
            if isinstance(col, (str, Column)):
                _cols.append(col)
            elif isinstance(col, list):
                for c in col:
                    if isinstance(c, (str, Column)):
                        _cols.append(c)
                    else:
                        raise PySparkTypeError(
                            error_class="NOT_COLUMN_OR_LIST_OR_STR",
                            message_parameters={"arg_name": "cols", "arg_type": type(c).__name__},
                        )
            else:
                raise PySparkTypeError(
                    error_class="NOT_COLUMN_OR_LIST_OR_STR",
                    message_parameters={"arg_name": "cols", "arg_type": type(col).__name__},
                )

        newPartitionSpec: List[Expression] = []
        for c in _cols:
            if isinstance(c, Column):
                newPartitionSpec.append(c._expr)
            else:
                newPartitionSpec.append(ColumnReference(c))

        return WindowSpec(
            partitionSpec=newPartitionSpec,
            orderSpec=self._orderSpec,
            frame=self._frame,
        )

    def orderBy(self, *cols: Union["ColumnOrName", List["ColumnOrName"]]) -> "WindowSpec":
        _cols: List[ColumnOrName] = []
        for col in cols:
            if isinstance(col, (str, Column)):
                _cols.append(col)
            elif isinstance(col, list):
                for c in col:
                    if isinstance(c, (str, Column)):
                        _cols.append(c)
                    else:
                        raise PySparkTypeError(
                            error_class="NOT_COLUMN_OR_LIST_OR_STR",
                            message_parameters={"arg_name": "cols", "arg_type": type(c).__name__},
                        )
            else:
                raise PySparkTypeError(
                    error_class="NOT_COLUMN_OR_LIST_OR_STR",
                    message_parameters={"arg_name": "cols", "arg_type": type(col).__name__},
                )

        newOrderSpec: List[SortOrder] = []
        for c in _cols:
            if isinstance(c, Column):
                if isinstance(c._expr, SortOrder):
                    newOrderSpec.append(c._expr)
                else:
                    newOrderSpec.append(SortOrder(c._expr))
            else:
                newOrderSpec.append(SortOrder(ColumnReference(c)))

        return WindowSpec(
            partitionSpec=self._partitionSpec,
            orderSpec=newOrderSpec,
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


WindowSpec.rangeBetween.__doc__ = PySparkWindowSpec.rangeBetween.__doc__
WindowSpec.rowsBetween.__doc__ = PySparkWindowSpec.rowsBetween.__doc__
WindowSpec.orderBy.__doc__ = PySparkWindowSpec.orderBy.__doc__
WindowSpec.partitionBy.__doc__ = PySparkWindowSpec.partitionBy.__doc__
WindowSpec.__doc__ = PySparkWindowSpec.__doc__


class Window:
    _PRECEDING_THRESHOLD = max(-sys.maxsize, JVM_LONG_MIN)
    _FOLLOWING_THRESHOLD = min(sys.maxsize, JVM_LONG_MAX)

    unboundedPreceding: int = JVM_LONG_MIN

    unboundedFollowing: int = JVM_LONG_MAX

    currentRow: int = 0

    _spec = WindowSpec(partitionSpec=[], orderSpec=[], frame=None)

    @staticmethod
    def partitionBy(*cols: Union["ColumnOrName", List["ColumnOrName"]]) -> "WindowSpec":
        return Window._spec.partitionBy(*cols)

    @staticmethod
    def orderBy(*cols: Union["ColumnOrName", List["ColumnOrName"]]) -> "WindowSpec":
        return Window._spec.orderBy(*cols)

    @staticmethod
    def rowsBetween(start: int, end: int) -> "WindowSpec":
        return Window._spec.rowsBetween(start, end)

    @staticmethod
    def rangeBetween(start: int, end: int) -> "WindowSpec":
        return Window._spec.rangeBetween(start, end)


Window.orderBy.__doc__ = PySparkWindow.orderBy.__doc__
Window.rowsBetween.__doc__ = PySparkWindow.rowsBetween.__doc__
Window.rangeBetween.__doc__ = PySparkWindow.rangeBetween.__doc__
Window.partitionBy.__doc__ = PySparkWindow.partitionBy.__doc__
Window.__doc__ = PySparkWindow.__doc__


def _test() -> None:
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.window

    globs = pyspark.sql.connect.window.__dict__.copy()
    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.window tests").remote("local[4]").getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.window,
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
