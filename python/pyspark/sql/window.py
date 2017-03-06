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

from pyspark import since, SparkContext
from pyspark.sql.column import _to_seq, _to_java_column

__all__ = ["Window", "WindowSpec"]


def _to_java_cols(cols):
    sc = SparkContext._active_spark_context
    if len(cols) == 1 and isinstance(cols[0], list):
        cols = cols[0]
    return _to_seq(sc, cols, _to_java_column)


class Window(object):
    """
    Utility functions for defining window in DataFrames.

    For example:

    >>> # ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    >>> window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    >>> # PARTITION BY country ORDER BY date RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING
    >>> window = Window.orderBy("date").partitionBy("country").rangeBetween(-3, 3)

    .. note:: Experimental

    .. versionadded:: 1.4
    """

    _JAVA_MIN_LONG = -(1 << 63)  # -9223372036854775808
    _JAVA_MAX_LONG = (1 << 63) - 1  # 9223372036854775807
    _PRECEDING_THRESHOLD = max(-sys.maxsize, _JAVA_MIN_LONG)
    _FOLLOWING_THRESHOLD = min(sys.maxsize, _JAVA_MAX_LONG)

    unboundedPreceding = _JAVA_MIN_LONG

    unboundedFollowing = _JAVA_MAX_LONG

    currentRow = 0

    @staticmethod
    @since(1.4)
    def partitionBy(*cols):
        """
        Creates a :class:`WindowSpec` with the partitioning defined.
        """
        sc = SparkContext._active_spark_context
        jspec = sc._jvm.org.apache.spark.sql.expressions.Window.partitionBy(_to_java_cols(cols))
        return WindowSpec(jspec)

    @staticmethod
    @since(1.4)
    def orderBy(*cols):
        """
        Creates a :class:`WindowSpec` with the ordering defined.
        """
        sc = SparkContext._active_spark_context
        jspec = sc._jvm.org.apache.spark.sql.expressions.Window.orderBy(_to_java_cols(cols))
        return WindowSpec(jspec)

    @staticmethod
    @since(2.1)
    def rowsBetween(start, end):
        """
        Creates a :class:`WindowSpec` with the frame boundaries defined,
        from `start` (inclusive) to `end` (inclusive).

        Both `start` and `end` are relative positions from the current row.
        For example, "0" means "current row", while "-1" means the row before
        the current row, and "5" means the fifth row after the current row.

        We recommend users use ``Window.unboundedPreceding``, ``Window.unboundedFollowing``,
        and ``Window.currentRow`` to specify special boundary values, rather than using integral
        values directly.

        :param start: boundary start, inclusive.
                      The frame is unbounded if this is ``Window.unboundedPreceding``, or
                      any value less than or equal to -9223372036854775808.
        :param end: boundary end, inclusive.
                    The frame is unbounded if this is ``Window.unboundedFollowing``, or
                    any value greater than or equal to 9223372036854775807.
        """
        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing
        sc = SparkContext._active_spark_context
        jspec = sc._jvm.org.apache.spark.sql.expressions.Window.rowsBetween(start, end)
        return WindowSpec(jspec)

    @staticmethod
    @since(2.1)
    def rangeBetween(start, end):
        """
        Creates a :class:`WindowSpec` with the frame boundaries defined,
        from `start` (inclusive) to `end` (inclusive).

        Both `start` and `end` are relative from the current row. For example,
        "0" means "current row", while "-1" means one off before the current row,
        and "5" means the five off after the current row.

        We recommend users use ``Window.unboundedPreceding``, ``Window.unboundedFollowing``,
        and ``Window.currentRow`` to specify special boundary values, rather than using integral
        values directly.

        :param start: boundary start, inclusive.
                      The frame is unbounded if this is ``Window.unboundedPreceding``, or
                      any value less than or equal to max(-sys.maxsize, -9223372036854775808).
        :param end: boundary end, inclusive.
                    The frame is unbounded if this is ``Window.unboundedFollowing``, or
                    any value greater than or equal to min(sys.maxsize, 9223372036854775807).
        """
        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing
        sc = SparkContext._active_spark_context
        jspec = sc._jvm.org.apache.spark.sql.expressions.Window.rangeBetween(start, end)
        return WindowSpec(jspec)


class WindowSpec(object):
    """
    A window specification that defines the partitioning, ordering,
    and frame boundaries.

    Use the static methods in :class:`Window` to create a :class:`WindowSpec`.

    .. note:: Experimental

    .. versionadded:: 1.4
    """

    def __init__(self, jspec):
        self._jspec = jspec

    @since(1.4)
    def partitionBy(self, *cols):
        """
        Defines the partitioning columns in a :class:`WindowSpec`.

        :param cols: names of columns or expressions
        """
        return WindowSpec(self._jspec.partitionBy(_to_java_cols(cols)))

    @since(1.4)
    def orderBy(self, *cols):
        """
        Defines the ordering columns in a :class:`WindowSpec`.

        :param cols: names of columns or expressions
        """
        return WindowSpec(self._jspec.orderBy(_to_java_cols(cols)))

    @since(1.4)
    def rowsBetween(self, start, end):
        """
        Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).

        Both `start` and `end` are relative positions from the current row.
        For example, "0" means "current row", while "-1" means the row before
        the current row, and "5" means the fifth row after the current row.

        We recommend users use ``Window.unboundedPreceding``, ``Window.unboundedFollowing``,
        and ``Window.currentRow`` to specify special boundary values, rather than using integral
        values directly.

        :param start: boundary start, inclusive.
                      The frame is unbounded if this is ``Window.unboundedPreceding``, or
                      any value less than or equal to max(-sys.maxsize, -9223372036854775808).
        :param end: boundary end, inclusive.
                    The frame is unbounded if this is ``Window.unboundedFollowing``, or
                    any value greater than or equal to min(sys.maxsize, 9223372036854775807).
        """
        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing
        return WindowSpec(self._jspec.rowsBetween(start, end))

    @since(1.4)
    def rangeBetween(self, start, end):
        """
        Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).

        Both `start` and `end` are relative from the current row. For example,
        "0" means "current row", while "-1" means one off before the current row,
        and "5" means the five off after the current row.

        We recommend users use ``Window.unboundedPreceding``, ``Window.unboundedFollowing``,
        and ``Window.currentRow`` to specify special boundary values, rather than using integral
        values directly.

        :param start: boundary start, inclusive.
                      The frame is unbounded if this is ``Window.unboundedPreceding``, or
                      any value less than or equal to max(-sys.maxsize, -9223372036854775808).
        :param end: boundary end, inclusive.
                    The frame is unbounded if this is ``Window.unboundedFollowing``, or
                    any value greater than or equal to min(sys.maxsize, 9223372036854775807).
        """
        if start <= Window._PRECEDING_THRESHOLD:
            start = Window.unboundedPreceding
        if end >= Window._FOLLOWING_THRESHOLD:
            end = Window.unboundedFollowing
        return WindowSpec(self._jspec.rangeBetween(start, end))


def _test():
    import doctest
    SparkContext('local[4]', 'PythonTest')
    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
