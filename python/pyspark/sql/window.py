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

# mypy: disable-error-code="empty-body"

import sys
from typing import Sequence, TYPE_CHECKING, Union

from pyspark.sql.utils import dispatch_window_method
from pyspark.util import (
    JVM_LONG_MIN,
    JVM_LONG_MAX,
)

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.sql._typing import ColumnOrName

__all__ = ["Window", "WindowSpec"]


class Window:
    """
    Utility functions for defining window in DataFrames.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    When ordering is not defined, an unbounded window frame (rowFrame,
    unboundedPreceding, unboundedFollowing) is used by default. When ordering is defined,
    a growing window frame (rangeFrame, unboundedPreceding, currentRow) is used by default.

    Examples
    --------
    >>> # ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    >>> window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    >>> # PARTITION BY country ORDER BY date RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING
    >>> window = Window.orderBy("date").partitionBy("country").rangeBetween(-3, 3)
    """

    _PRECEDING_THRESHOLD = max(-sys.maxsize, JVM_LONG_MIN)
    _FOLLOWING_THRESHOLD = min(sys.maxsize, JVM_LONG_MAX)

    unboundedPreceding: int = JVM_LONG_MIN

    unboundedFollowing: int = JVM_LONG_MAX

    currentRow: int = 0

    @staticmethod
    @dispatch_window_method
    def partitionBy(*cols: Union["ColumnOrName", Sequence["ColumnOrName"]]) -> "WindowSpec":
        """
        Creates a :class:`WindowSpec` with the partitioning defined.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        cols : str, :class:`Column` or list
            names of columns or expressions

        Returns
        -------
        :class: `WindowSpec`
            A :class:`WindowSpec` with the partitioning defined.

        Examples
        --------
        >>> from pyspark.sql import Window
        >>> from pyspark.sql.functions import row_number
        >>> df = spark.createDataFrame(
        ...      [(1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b")], ["id", "category"])
        >>> df.show()
        +---+--------+
        | id|category|
        +---+--------+
        |  1|       a|
        |  1|       a|
        |  2|       a|
        |  1|       b|
        |  2|       b|
        |  3|       b|
        +---+--------+

        Show row number order by ``id`` in partition ``category``.

        >>> window = Window.partitionBy("category").orderBy("id")
        >>> df.withColumn("row_number", row_number().over(window)).show()
        +---+--------+----------+
        | id|category|row_number|
        +---+--------+----------+
        |  1|       a|         1|
        |  1|       a|         2|
        |  2|       a|         3|
        |  1|       b|         1|
        |  2|       b|         2|
        |  3|       b|         3|
        +---+--------+----------+
        """
        ...

    @staticmethod
    @dispatch_window_method
    def orderBy(*cols: Union["ColumnOrName", Sequence["ColumnOrName"]]) -> "WindowSpec":
        """
        Creates a :class:`WindowSpec` with the ordering defined.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        cols : str, :class:`Column` or list
            names of columns or expressions

        Returns
        -------
        :class: `WindowSpec`
            A :class:`WindowSpec` with the ordering defined.

        Examples
        --------
        >>> from pyspark.sql import Window
        >>> from pyspark.sql.functions import row_number
        >>> df = spark.createDataFrame(
        ...      [(1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b")], ["id", "category"])
        >>> df.show()
        +---+--------+
        | id|category|
        +---+--------+
        |  1|       a|
        |  1|       a|
        |  2|       a|
        |  1|       b|
        |  2|       b|
        |  3|       b|
        +---+--------+

        Show row number order by ``category`` in partition ``id``.

        >>> window = Window.partitionBy("id").orderBy("category")
        >>> df.withColumn("row_number", row_number().over(window)).show()
        +---+--------+----------+
        | id|category|row_number|
        +---+--------+----------+
        |  1|       a|         1|
        |  1|       a|         2|
        |  1|       b|         3|
        |  2|       a|         1|
        |  2|       b|         2|
        |  3|       b|         1|
        +---+--------+----------+
        """
        ...

    @staticmethod
    @dispatch_window_method
    def rowsBetween(start: int, end: int) -> "WindowSpec":
        """
        Creates a :class:`WindowSpec` with the frame boundaries defined,
        from `start` (inclusive) to `end` (inclusive).

        Both `start` and `end` are relative positions from the current row.
        For example, "0" means "current row", while "-1" means the row before
        the current row, and "5" means the fifth row after the current row.

        We recommend users use ``Window.unboundedPreceding``, ``Window.unboundedFollowing``,
        and ``Window.currentRow`` to specify special boundary values, rather than using integral
        values directly.

        A row based boundary is based on the position of the row within the partition.
        An offset indicates the number of rows above or below the current row, the frame for the
        current row starts or ends. For instance, given a row based sliding frame with a lower bound
        offset of -1 and a upper bound offset of +2. The frame for row with index 5 would range from
        index 4 to index 7.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        start : int
            boundary start, inclusive.
            The frame is unbounded if this is ``Window.unboundedPreceding``, or
            any value less than or equal to -9223372036854775808.
        end : int
            boundary end, inclusive.
            The frame is unbounded if this is ``Window.unboundedFollowing``, or
            any value greater than or equal to 9223372036854775807.

        Returns
        -------
        :class: `WindowSpec`
            A :class:`WindowSpec` with the frame boundaries defined,
            from `start` (inclusive) to `end` (inclusive).

        Examples
        --------
        >>> from pyspark.sql import Window
        >>> from pyspark.sql import functions as func
        >>> df = spark.createDataFrame(
        ...      [(1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b")], ["id", "category"])
        >>> df.show()
        +---+--------+
        | id|category|
        +---+--------+
        |  1|       a|
        |  1|       a|
        |  2|       a|
        |  1|       b|
        |  2|       b|
        |  3|       b|
        +---+--------+

        Calculate sum of ``id`` in the range from currentRow to currentRow + 1
        in partition ``category``

        >>> window = Window.partitionBy("category").orderBy("id").rowsBetween(Window.currentRow, 1)
        >>> df.withColumn("sum", func.sum("id").over(window)).sort("id", "category", "sum").show()
        +---+--------+---+
        | id|category|sum|
        +---+--------+---+
        |  1|       a|  2|
        |  1|       a|  3|
        |  1|       b|  3|
        |  2|       a|  2|
        |  2|       b|  5|
        |  3|       b|  3|
        +---+--------+---+

        """
        ...

    @staticmethod
    @dispatch_window_method
    def rangeBetween(start: int, end: int) -> "WindowSpec":
        """
        Creates a :class:`WindowSpec` with the frame boundaries defined,
        from `start` (inclusive) to `end` (inclusive).

        Both `start` and `end` are relative from the current row. For example,
        "0" means "current row", while "-1" means one off before the current row,
        and "5" means the five off after the current row.

        We recommend users use ``Window.unboundedPreceding``, ``Window.unboundedFollowing``,
        and ``Window.currentRow`` to specify special boundary values, rather than using integral
        values directly.

        A range-based boundary is based on the actual value of the ORDER BY
        expression(s). An offset is used to alter the value of the ORDER BY expression, for
        instance if the current ORDER BY expression has a value of 10 and the lower bound offset
        is -3, the resulting lower bound for the current row will be 10 - 3 = 7. This however puts a
        number of constraints on the ORDER BY expressions: there can be only one expression and this
        expression must have a numerical data type. An exception can be made when the offset is
        unbounded, because no value modification is needed, in this case multiple and non-numeric
        ORDER BY expression are allowed.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        start : int
            boundary start, inclusive.
            The frame is unbounded if this is ``Window.unboundedPreceding``, or
            any value less than or equal to max(-sys.maxsize, -9223372036854775808).
        end : int
            boundary end, inclusive.
            The frame is unbounded if this is ``Window.unboundedFollowing``, or
            any value greater than or equal to min(sys.maxsize, 9223372036854775807).

        Returns
        -------
        :class: `WindowSpec`
            A :class:`WindowSpec` with the frame boundaries defined,
            from `start` (inclusive) to `end` (inclusive).

        Examples
        --------
        >>> from pyspark.sql import Window
        >>> from pyspark.sql import functions as func
        >>> df = spark.createDataFrame(
        ...      [(1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b")], ["id", "category"])
        >>> df.show()
        +---+--------+
        | id|category|
        +---+--------+
        |  1|       a|
        |  1|       a|
        |  2|       a|
        |  1|       b|
        |  2|       b|
        |  3|       b|
        +---+--------+

        Calculate sum of ``id`` in the range from ``id`` of currentRow to ``id`` of currentRow + 1
        in partition ``category``

        >>> window = Window.partitionBy("category").orderBy("id").rangeBetween(Window.currentRow, 1)
        >>> df.withColumn("sum", func.sum("id").over(window)).sort("id", "category").show()
        +---+--------+---+
        | id|category|sum|
        +---+--------+---+
        |  1|       a|  4|
        |  1|       a|  4|
        |  1|       b|  3|
        |  2|       a|  2|
        |  2|       b|  5|
        |  3|       b|  3|
        +---+--------+---+

        """
        ...


class WindowSpec:
    """
    A window specification that defines the partitioning, ordering,
    and frame boundaries.

    Use the static methods in :class:`Window` to create a :class:`WindowSpec`.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __new__(cls, jspec: "JavaObject") -> "WindowSpec":
        from pyspark.sql.classic.WindowSpec import WindowSpec  # type: ignore[import-not-found]

        return WindowSpec.__new__(WindowSpec, jspec)

    def partitionBy(self, *cols: Union["ColumnOrName", Sequence["ColumnOrName"]]) -> "WindowSpec":
        """
        Defines the partitioning columns in a :class:`WindowSpec`.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        cols : str, :class:`Column` or list
            names of columns or expressions
        """
        ...

    def orderBy(self, *cols: Union["ColumnOrName", Sequence["ColumnOrName"]]) -> "WindowSpec":
        """
        Defines the ordering columns in a :class:`WindowSpec`.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        cols : str, :class:`Column` or list
            names of columns or expressions
        """
        ...

    def rowsBetween(self, start: int, end: int) -> "WindowSpec":
        """
        Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).

        Both `start` and `end` are relative positions from the current row.
        For example, "0" means "current row", while "-1" means the row before
        the current row, and "5" means the fifth row after the current row.

        We recommend users use ``Window.unboundedPreceding``, ``Window.unboundedFollowing``,
        and ``Window.currentRow`` to specify special boundary values, rather than using integral
        values directly.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        start : int
            boundary start, inclusive.
            The frame is unbounded if this is ``Window.unboundedPreceding``, or
            any value less than or equal to max(-sys.maxsize, -9223372036854775808).
        end : int
            boundary end, inclusive.
            The frame is unbounded if this is ``Window.unboundedFollowing``, or
            any value greater than or equal to min(sys.maxsize, 9223372036854775807).
        """
        ...

    def rangeBetween(self, start: int, end: int) -> "WindowSpec":
        """
        Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).

        Both `start` and `end` are relative from the current row. For example,
        "0" means "current row", while "-1" means one off before the current row,
        and "5" means the five off after the current row.

        We recommend users use ``Window.unboundedPreceding``, ``Window.unboundedFollowing``,
        and ``Window.currentRow`` to specify special boundary values, rather than using integral
        values directly.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        start : int
            boundary start, inclusive.
            The frame is unbounded if this is ``Window.unboundedPreceding``, or
            any value less than or equal to max(-sys.maxsize, -9223372036854775808).
        end : int
            boundary end, inclusive.
            The frame is unbounded if this is ``Window.unboundedFollowing``, or
            any value greater than or equal to min(sys.maxsize, 9223372036854775807).
        """
        ...
