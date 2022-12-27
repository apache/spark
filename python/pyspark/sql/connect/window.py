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

import sys
from typing import TYPE_CHECKING, Union, Sequence, List, Optional

from pyspark import SparkContext, SparkConf
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
                        raise TypeError(
                            f"cols must be str or Column or list, but got {type(c).__name__} {c}"
                        )
            else:
                raise TypeError(
                    f"cols must be str or Column or list, but got {type(col).__name__} {col}"
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

    partitionBy.__doc__ = PySparkWindowSpec.partitionBy.__doc__

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
                        raise TypeError(
                            f"cols must be str or Column or list, but got {type(c).__name__} {c}"
                        )
            else:
                raise TypeError(
                    f"cols must be str or Column or list, but got {type(col).__name__} {col}"
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

    orderBy.__doc__ = PySparkWindowSpec.orderBy.__doc__

    def rowsBetween(self, start: int, end: int) -> "WindowSpec":
        if not isinstance(start, int):
            raise TypeError(f"start must be a int, but got {type(start).__name__}")
        if not isinstance(end, int):
            raise TypeError(f"end must be a int, but got {type(end).__name__}")

        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing

        return WindowSpec(
            partitionSpec=self._partitionSpec,
            orderSpec=self._orderSpec,
            frame=WindowFrame(isRowFrame=True, start=start, end=end),
        )

    rowsBetween.__doc__ = PySparkWindowSpec.rowsBetween.__doc__

    def rangeBetween(self, start: int, end: int) -> "WindowSpec":
        if not isinstance(start, int):
            raise TypeError(f"start must be a int, but got {type(start).__name__}")
        if not isinstance(end, int):
            raise TypeError(f"end must be a int, but got {type(end).__name__}")

        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing

        return WindowSpec(
            partitionSpec=self._partitionSpec,
            orderSpec=self._orderSpec,
            frame=WindowFrame(isRowFrame=False, start=start, end=end),
        )

    rangeBetween.__doc__ = PySparkWindowSpec.rangeBetween.__doc__

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

    partitionBy.__doc__ = PySparkWindow.partitionBy.__doc__

    @staticmethod
    def orderBy(*cols: Union["ColumnOrName", List["ColumnOrName"]]) -> "WindowSpec":
        return Window._spec.orderBy(*cols)

    orderBy.__doc__ = PySparkWindow.orderBy.__doc__

    @staticmethod
    def rowsBetween(start: int, end: int) -> "WindowSpec":
        return Window._spec.rowsBetween(start, end)

    rowsBetween.__doc__ = PySparkWindow.rowsBetween.__doc__

    @staticmethod
    def rangeBetween(start: int, end: int) -> "WindowSpec":
        return Window._spec.rangeBetween(start, end)

    rangeBetween.__doc__ = PySparkWindow.rangeBetween.__doc__


Window.__doc__ = PySparkWindow.__doc__


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

    os.chdir(os.environ["SPARK_HOME"])

    if should_test_connect:
        import pyspark.sql.connect.window

        globs = pyspark.sql.window.__dict__.copy()
        # Works around to create a regular Spark session
        sc = SparkContext("local[4]", "sql.connect.window tests", conf=SparkConf())
        globs["_spark"] = PySparkSession(sc, options={"spark.app.name": "sql.connect.window tests"})

        # Creates a remote Spark session.
        globs["spark"] = PySparkSession.builder.remote("sc://localhost").getOrCreate()

        (failure_count, test_count) = doctest.testmod(
            pyspark.sql.connect.window,
            globs=globs,
            optionflags=doctest.ELLIPSIS
            | doctest.NORMALIZE_WHITESPACE
            | doctest.IGNORE_EXCEPTION_DETAIL,
        )

        globs["spark"].stop()
        globs["_spark"].stop()
        if failure_count:
            sys.exit(-1)
    else:
        print(
            f"Skipping pyspark.sql.connect.window doctests: {connect_requirement_message}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    _test()
