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
from typing import cast, Iterable, List, Tuple, TYPE_CHECKING, Union

from pyspark.sql.window import (
    Window as ParentWindow,
    WindowSpec as ParentWindowSpec,
)
from pyspark.sql.utils import get_active_spark_context

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.sql._typing import ColumnOrName, ColumnOrName_


__all__ = ["Window", "WindowSpec"]


def _to_java_cols(cols: Tuple[Union["ColumnOrName", List["ColumnOrName_"]], ...]) -> "JavaObject":
    from pyspark.sql.classic.column import _to_seq, _to_java_column

    if len(cols) == 1 and isinstance(cols[0], list):
        cols = cols[0]  # type: ignore[assignment]
    sc = get_active_spark_context()
    return _to_seq(sc, cast(Iterable["ColumnOrName"], cols), _to_java_column)


class Window(ParentWindow):
    @staticmethod
    def partitionBy(*cols: Union["ColumnOrName", List["ColumnOrName_"]]) -> ParentWindowSpec:
        from py4j.java_gateway import JVMView

        sc = get_active_spark_context()
        jspec = cast(JVMView, sc._jvm).org.apache.spark.sql.expressions.Window.partitionBy(
            _to_java_cols(cols)
        )
        return WindowSpec(jspec)

    @staticmethod
    def orderBy(*cols: Union["ColumnOrName", List["ColumnOrName_"]]) -> ParentWindowSpec:
        from py4j.java_gateway import JVMView

        sc = get_active_spark_context()
        jspec = cast(JVMView, sc._jvm).org.apache.spark.sql.expressions.Window.orderBy(
            _to_java_cols(cols)
        )
        return WindowSpec(jspec)

    @staticmethod
    def rowsBetween(start: int, end: int) -> ParentWindowSpec:
        from py4j.java_gateway import JVMView

        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing
        sc = get_active_spark_context()
        jspec = cast(JVMView, sc._jvm).org.apache.spark.sql.expressions.Window.rowsBetween(
            start, end
        )
        return WindowSpec(jspec)

    @staticmethod
    def rangeBetween(start: int, end: int) -> ParentWindowSpec:
        from py4j.java_gateway import JVMView

        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing
        sc = get_active_spark_context()
        jspec = cast(JVMView, sc._jvm).org.apache.spark.sql.expressions.Window.rangeBetween(
            start, end
        )
        return WindowSpec(jspec)


class WindowSpec(ParentWindowSpec):
    def __new__(cls, jspec: "JavaObject") -> "WindowSpec":
        self = object.__new__(cls)
        self.__init__(jspec)  # type: ignore[misc]
        return self

    def __init__(self, jspec: "JavaObject") -> None:
        self._jspec = jspec

    def partitionBy(self, *cols: Union["ColumnOrName", List["ColumnOrName_"]]) -> ParentWindowSpec:
        return WindowSpec(self._jspec.partitionBy(_to_java_cols(cols)))

    def orderBy(self, *cols: Union["ColumnOrName", List["ColumnOrName_"]]) -> ParentWindowSpec:
        return WindowSpec(self._jspec.orderBy(_to_java_cols(cols)))

    def rowsBetween(self, start: int, end: int) -> ParentWindowSpec:
        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing
        return WindowSpec(self._jspec.rowsBetween(start, end))

    def rangeBetween(self, start: int, end: int) -> ParentWindowSpec:
        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing
        return WindowSpec(self._jspec.rangeBetween(start, end))


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.window

    # It inherits docstrings but doctests cannot detect them so we run
    # the parent classe's doctests here directly.
    globs = pyspark.sql.window.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[4]").appName("sql.classic.window tests").getOrCreate()
    )
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.window,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
