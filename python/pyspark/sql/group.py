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

from typing import Callable, List, Optional, TYPE_CHECKING, overload, Dict, Union, cast, Tuple

from pyspark.sql.column import Column
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.pandas.group_ops import PandasGroupedOpsMixin

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.sql._typing import LiteralType

__all__ = ["GroupedData"]


def dfapi(f: Callable[..., DataFrame]) -> Callable[..., DataFrame]:
    def _api(self: "GroupedData") -> "DataFrame":
        name = f.__name__
        jdf = getattr(self._jgd, name)()
        return DataFrame(jdf, self.session)

    _api.__name__ = f.__name__
    _api.__doc__ = f.__doc__
    return _api


def df_varargs_api(f: Callable[..., DataFrame]) -> Callable[..., DataFrame]:
    def _api(self: "GroupedData", *cols: str) -> "DataFrame":
        from pyspark.sql.classic.column import _to_seq

        name = f.__name__
        jdf = getattr(self._jgd, name)(_to_seq(self.session._sc, cols))
        return DataFrame(jdf, self.session)

    _api.__name__ = f.__name__
    _api.__doc__ = f.__doc__
    return _api


class GroupedData(PandasGroupedOpsMixin):
    """
    A set of methods for aggregations on a :class:`DataFrame`,
    created by :func:`DataFrame.groupBy`.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __init__(self, jgd: "JavaObject", df: DataFrame):
        self._jgd = jgd
        self._df = df
        self.session: SparkSession = df.sparkSession

    def __repr__(self) -> str:
        index = 26  # index to truncate string from the JVM side
        jvm_string = self._jgd.toString()
        if jvm_string is not None and len(jvm_string) > index and jvm_string[index] == "[":
            return f"GroupedData{jvm_string[index:]}"
        else:
            return super().__repr__()

    @overload
    def agg(self, *exprs: Column) -> "DataFrame":
        ...

    @overload
    def agg(self, __exprs: Dict[str, str]) -> "DataFrame":
        ...

    def agg(self, *exprs: Union[Column, Dict[str, str]]) -> "DataFrame":
        """Compute aggregates and returns the result as a :class:`DataFrame`.

        The available aggregate functions can be:

        1. built-in aggregation functions, such as `avg`, `max`, `min`, `sum`, `count`

        2. group aggregate pandas UDFs, created with :func:`pyspark.sql.functions.pandas_udf`

           .. note:: There is no partial aggregation with group aggregate UDFs, i.e.,
               a full shuffle is required. Also, all the data of a group will be loaded into
               memory, so the user should be aware of the potential OOM risk if data is skewed
               and certain groups are too large to fit in memory.

           .. seealso:: :func:`pyspark.sql.functions.pandas_udf`

        If ``exprs`` is a single :class:`dict` mapping from string to string, then the key
        is the column to perform aggregation on, and the value is the aggregate function.

        Alternatively, ``exprs`` can also be a list of aggregate :class:`Column` expressions.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        exprs : dict
            a dict mapping from column name (string) to aggregate functions (string),
            or a list of :class:`Column`.

        Notes
        -----
        Built-in aggregation functions and group aggregate pandas UDFs cannot be mixed
        in a single call to this function.

        Examples
        --------
        >>> from pyspark.sql import functions as sf
        >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (3, "Alice"), (5, "Bob"), (10, "Bob")], ["age", "name"])
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  3|Alice|
        |  5|  Bob|
        | 10|  Bob|
        +---+-----+

        Group-by name, and count each group.

        >>> df.groupBy(df.name)
        GroupedData[grouping...: [name...], value: [age: bigint, name: string], type: GroupBy]

        >>> df.groupBy(df.name).agg({"*": "count"}).sort("name").show()
        +-----+--------+
        | name|count(1)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       2|
        +-----+--------+

        Group-by name, and calculate the minimum age.

        >>> df.groupBy(df.name).agg(sf.min(df.age)).sort("name").show()
        +-----+--------+
        | name|min(age)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       5|
        +-----+--------+

        Same as above but uses pandas UDF.

        >>> @pandas_udf('int', PandasUDFType.GROUPED_AGG)  # doctest: +SKIP
        ... def min_udf(v):
        ...     return v.min()
        ...
        >>> df.groupBy(df.name).agg(min_udf(df.age)).sort("name").show()  # doctest: +SKIP
        +-----+------------+
        | name|min_udf(age)|
        +-----+------------+
        |Alice|           2|
        |  Bob|           5|
        +-----+------------+
        """
        from pyspark.sql.classic.column import _to_seq

        assert exprs, "exprs should not be empty"
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            jdf = self._jgd.agg(exprs[0])
        else:
            # Columns
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
            exprs = cast(Tuple[Column, ...], exprs)
            jdf = self._jgd.agg(exprs[0]._jc, _to_seq(self.session._sc, [c._jc for c in exprs[1:]]))
        return DataFrame(jdf, self.session)

    @dfapi
    def count(self) -> "DataFrame":  # type: ignore[empty-body]
        """Counts the number of records for each group.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (3, "Alice"), (5, "Bob"), (10, "Bob")], ["age", "name"])
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  3|Alice|
        |  5|  Bob|
        | 10|  Bob|
        +---+-----+

        Group-by name, and count each group.

        >>> df.groupBy(df.name).count().sort("name").show()
        +-----+-----+
        | name|count|
        +-----+-----+
        |Alice|    2|
        |  Bob|    2|
        +-----+-----+
        """

    @df_varargs_api
    def mean(self, *cols: str) -> DataFrame:  # type: ignore[empty-body]
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.
        """

    @df_varargs_api
    def avg(self, *cols: str) -> "DataFrame":  # type: ignore[empty-body]
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice", 80), (3, "Alice", 100),
        ...     (5, "Bob", 120), (10, "Bob", 140)], ["age", "name", "height"])
        >>> df.show()
        +---+-----+------+
        |age| name|height|
        +---+-----+------+
        |  2|Alice|    80|
        |  3|Alice|   100|
        |  5|  Bob|   120|
        | 10|  Bob|   140|
        +---+-----+------+

        Group-by name, and calculate the mean of the age in each group.

        >>> df.groupBy("name").avg('age').sort("name").show()
        +-----+--------+
        | name|avg(age)|
        +-----+--------+
        |Alice|     2.5|
        |  Bob|     7.5|
        +-----+--------+

        Calculate the mean of the age and height in all data.

        >>> df.groupBy().avg('age', 'height').show()
        +--------+-----------+
        |avg(age)|avg(height)|
        +--------+-----------+
        |     5.0|      110.0|
        +--------+-----------+
        """

    @df_varargs_api
    def max(self, *cols: str) -> "DataFrame":  # type: ignore[empty-body]
        """Computes the max value for each numeric columns for each group.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice", 80), (3, "Alice", 100),
        ...     (5, "Bob", 120), (10, "Bob", 140)], ["age", "name", "height"])
        >>> df.show()
        +---+-----+------+
        |age| name|height|
        +---+-----+------+
        |  2|Alice|    80|
        |  3|Alice|   100|
        |  5|  Bob|   120|
        | 10|  Bob|   140|
        +---+-----+------+

        Group-by name, and calculate the max of the age in each group.

        >>> df.groupBy("name").max("age").sort("name").show()
        +-----+--------+
        | name|max(age)|
        +-----+--------+
        |Alice|       3|
        |  Bob|      10|
        +-----+--------+

        Calculate the max of the age and height in all data.

        >>> df.groupBy().max("age", "height").show()
        +--------+-----------+
        |max(age)|max(height)|
        +--------+-----------+
        |      10|        140|
        +--------+-----------+
        """

    @df_varargs_api
    def min(self, *cols: str) -> "DataFrame":  # type: ignore[empty-body]
        """Computes the min value for each numeric column for each group.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice", 80), (3, "Alice", 100),
        ...     (5, "Bob", 120), (10, "Bob", 140)], ["age", "name", "height"])
        >>> df.show()
        +---+-----+------+
        |age| name|height|
        +---+-----+------+
        |  2|Alice|    80|
        |  3|Alice|   100|
        |  5|  Bob|   120|
        | 10|  Bob|   140|
        +---+-----+------+

        Group-by name, and calculate the min of the age in each group.

        >>> df.groupBy("name").min("age").sort("name").show()
        +-----+--------+
        | name|min(age)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       5|
        +-----+--------+

        Calculate the min of the age and height in all data.

        >>> df.groupBy().min("age", "height").show()
        +--------+-----------+
        |min(age)|min(height)|
        +--------+-----------+
        |       2|         80|
        +--------+-----------+
        """

    @df_varargs_api
    def sum(self, *cols: str) -> DataFrame:  # type: ignore[empty-body]
        """Computes the sum for each numeric columns for each group.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice", 80), (3, "Alice", 100),
        ...     (5, "Bob", 120), (10, "Bob", 140)], ["age", "name", "height"])
        >>> df.show()
        +---+-----+------+
        |age| name|height|
        +---+-----+------+
        |  2|Alice|    80|
        |  3|Alice|   100|
        |  5|  Bob|   120|
        | 10|  Bob|   140|
        +---+-----+------+

        Group-by name, and calculate the sum of the age in each group.

        >>> df.groupBy("name").sum("age").sort("name").show()
        +-----+--------+
        | name|sum(age)|
        +-----+--------+
        |Alice|       5|
        |  Bob|      15|
        +-----+--------+

        Calculate the sum of the age and height in all data.

        >>> df.groupBy().sum("age", "height").show()
        +--------+-----------+
        |sum(age)|sum(height)|
        +--------+-----------+
        |      20|        440|
        +--------+-----------+
        """

    def pivot(self, pivot_col: str, values: Optional[List["LiteralType"]] = None) -> "GroupedData":
        """
        Pivots a column of the current :class:`DataFrame` and performs the specified aggregation.

        .. versionadded:: 1.6.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        pivot_col : str
            Name of the column to pivot.
        values : list, optional
            List of values that will be translated to columns in the output DataFrame.

            If ``values`` is not provided, Spark will eagerly compute the distinct values in
            ``pivot_col`` so it can determine the resulting schema of the transformation. To avoid
            any eager computations, provide an explicit list of values.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df1 = spark.createDataFrame([
        ...     Row(course="dotNET", year=2012, earnings=10000),
        ...     Row(course="Java", year=2012, earnings=20000),
        ...     Row(course="dotNET", year=2012, earnings=5000),
        ...     Row(course="dotNET", year=2013, earnings=48000),
        ...     Row(course="Java", year=2013, earnings=30000),
        ... ])
        >>> df1.show()
        +------+----+--------+
        |course|year|earnings|
        +------+----+--------+
        |dotNET|2012|   10000|
        |  Java|2012|   20000|
        |dotNET|2012|    5000|
        |dotNET|2013|   48000|
        |  Java|2013|   30000|
        +------+----+--------+
        >>> df2 = spark.createDataFrame([
        ...     Row(training="expert", sales=Row(course="dotNET", year=2012, earnings=10000)),
        ...     Row(training="junior", sales=Row(course="Java", year=2012, earnings=20000)),
        ...     Row(training="expert", sales=Row(course="dotNET", year=2012, earnings=5000)),
        ...     Row(training="junior", sales=Row(course="dotNET", year=2013, earnings=48000)),
        ...     Row(training="expert", sales=Row(course="Java", year=2013, earnings=30000)),
        ... ])  # doctest: +SKIP
        >>> df2.show()  # doctest: +SKIP
        +--------+--------------------+
        |training|               sales|
        +--------+--------------------+
        |  expert|{dotNET, 2012, 10...|
        |  junior| {Java, 2012, 20000}|
        |  expert|{dotNET, 2012, 5000}|
        |  junior|{dotNET, 2013, 48...|
        |  expert| {Java, 2013, 30000}|
        +--------+--------------------+

        Compute the sum of earnings for each year by course with each course as a separate column

        >>> df1.groupBy("year").pivot(
        ...     "course", ["dotNET", "Java"]).sum("earnings").sort("year").show()
        +----+------+-----+
        |year|dotNET| Java|
        +----+------+-----+
        |2012| 15000|20000|
        |2013| 48000|30000|
        +----+------+-----+

        Or without specifying column values (less efficient)

        >>> df1.groupBy("year").pivot("course").sum("earnings").sort("year").show()
        +----+-----+------+
        |year| Java|dotNET|
        +----+-----+------+
        |2012|20000| 15000|
        |2013|30000| 48000|
        +----+-----+------+
        >>> df2.groupBy(
        ...     "sales.year").pivot("sales.course").sum("sales.earnings").sort("year").show()
        ... # doctest: +SKIP
        +----+-----+------+
        |year| Java|dotNET|
        +----+-----+------+
        |2012|20000| 15000|
        |2013|30000| 48000|
        +----+-----+------+
        """
        if values is None:
            jgd = self._jgd.pivot(pivot_col)
        else:
            jgd = self._jgd.pivot(pivot_col, values)
        return GroupedData(jgd, self._df)


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.group

    globs = pyspark.sql.group.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("sql.group tests").getOrCreate()
    globs["spark"] = spark

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.group,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
