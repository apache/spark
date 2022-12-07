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
from pyspark.sql.connect.column import (
    Column,
    Expression,
    LiteralExpression,
    ColumnReference,
    UnresolvedFunction,
    SQLExpression,
)

from typing import Any, TYPE_CHECKING, Union, List, Optional, Tuple

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName


# TODO(SPARK-40538) Add support for the missing PySpark functions.


def _to_col(col: "ColumnOrName") -> Column:
    assert isinstance(col, (Column, str))
    return col if isinstance(col, Column) else column(col)


def _invoke_function(name: str, *args: Union[Column, Expression]) -> Column:
    """
    Simple wrapper function that converts the arguments into the appropriate types.
    Parameters
    ----------
    name Name of the function to be called.
    args The list of arguments.

    Returns
    -------
    :class:`UnresolvedFunction`
    """
    expressions: List[Expression] = []
    for arg in args:
        assert isinstance(arg, (Column, Expression))
        if isinstance(arg, Column):
            expressions.append(arg._expr)
        else:
            expressions.append(arg)
    return Column(UnresolvedFunction(name, expressions))


def _invoke_function_over_columns(name: str, *cols: "ColumnOrName") -> Column:
    """
    Invokes n-ary function identified by name
    and wraps the result with :class:`~pyspark.sql.Column`.
    """
    _cols = [_to_col(c) for c in cols]
    return _invoke_function(name, *_cols)


def _invoke_binary_math_function(name: str, col1: Any, col2: Any) -> Column:
    """
    Invokes binary math function identified by name
    and wraps the result with :class:`~pyspark.sql.Column`.
    """

    # For legacy reasons, the arguments here can be implicitly converted into column
    _cols = [_to_col(c) if isinstance(c, (str, Column)) else lit(c) for c in (col1, col2)]
    return _invoke_function(name, *_cols)


# Normal Functions


def col(col: str) -> Column:
    return Column(ColumnReference(col))


column = col


def lit(col: Any) -> Column:
    return Column(LiteralExpression(col))


# def bitwiseNOT(col: "ColumnOrName") -> Column:
#     """
#     Computes bitwise not.
#
#     .. versionadded:: 1.4.0
#
#     .. deprecated:: 3.2.0
#         Use :func:`bitwise_not` instead.
#     """
#     warnings.warn("Deprecated in 3.2, use bitwise_not instead.", FutureWarning)
#     return bitwise_not(col)


def bitwise_not(col: "ColumnOrName") -> Column:
    """
    Computes bitwise not.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(bitwise_not(lit(0))).show()
    +---+
    | ~0|
    +---+
    | -1|
    +---+
    >>> df.select(bitwise_not(lit(1))).show()
    +---+
    | ~1|
    +---+
    | -2|
    +---+
    """
    return _invoke_function_over_columns("~", col)


# TODO(SPARK-41364): support broadcast
# def broadcast(df: DataFrame) -> DataFrame:
#     """
#     Marks a DataFrame as small enough for use in broadcast joins.
#
#     .. versionadded:: 1.6.0
#
#     Returns
#     -------
#     :class:`~pyspark.sql.DataFrame`
#         DataFrame marked as ready for broadcast join.
#
#     Examples
#     --------
#     >>> from pyspark.sql import types
#     >>> df = spark.createDataFrame([1, 2, 3, 3, 4], types.IntegerType())
#     >>> df_small = spark.range(3)
#     >>> df_b = broadcast(df_small)
#     >>> df.join(df_b, df.value == df_small.id).show()
#     +-----+---+
#     |value| id|
#     +-----+---+
#     |    1|  1|
#     |    2|  2|
#     +-----+---+
#     """
#
#     sc = SparkContext._active_spark_context
#     assert sc is not None and sc._jvm is not None
#     return DataFrame(sc._jvm.functions.broadcast(df._jdf), df.sparkSession)


def coalesce(*cols: "ColumnOrName") -> Column:
    """Returns the first column that is not null.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or str
        list of columns to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        value of the first column that is not null.

    Examples
    --------
    >>> cDf = spark.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
    >>> cDf.show()
    +----+----+
    |   a|   b|
    +----+----+
    |null|null|
    |   1|null|
    |null|   2|
    +----+----+

    >>> cDf.select(coalesce(cDf["a"], cDf["b"])).show()
    +--------------+
    |coalesce(a, b)|
    +--------------+
    |          null|
    |             1|
    |             2|
    +--------------+

    >>> cDf.select('*', coalesce(cDf["a"], lit(0.0))).show()
    +----+----+----------------+
    |   a|   b|coalesce(a, 0.0)|
    +----+----+----------------+
    |null|null|             0.0|
    |   1|null|             1.0|
    |null|   2|             0.0|
    +----+----+----------------+
    """
    return _invoke_function_over_columns("coalesce", *cols)


def expr(str: str) -> Column:
    """Parses the expression string into the column that it represents

    .. versionadded:: 3.4.0

    Parameters
    ----------
    str : str
        expression defined in string.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column representing the expression.

    Examples
    --------
    >>> df = spark.createDataFrame([["Alice"], ["Bob"]], ["name"])
    >>> df.select("name", expr("length(name)")).show()
    +-----+------------+
    | name|length(name)|
    +-----+------------+
    |Alice|           5|
    |  Bob|           3|
    +-----+------------+
    """
    return Column(SQLExpression(str))


def greatest(*cols: "ColumnOrName") -> Column:
    """
    Returns the greatest value of the list of column names, skipping null values.
    This function takes at least 2 parameters. It will return null if all parameters are null.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        columns to check for gratest value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        gratest value.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])
    >>> df.select(greatest(df.a, df.b, df.c).alias("greatest")).collect()
    [Row(greatest=4)]
    """
    if len(cols) < 2:
        raise ValueError("greatest should take at least two columns")
    return _invoke_function_over_columns("greatest", *cols)


def input_file_name() -> Column:
    """
    Creates a string column for the file name of the current Spark task.

    .. versionadded:: 3.4.0

    Returns
    -------
    :class:`~pyspark.sql.Column`
        file names.

    Examples
    --------
    >>> import os
    >>> path = os.path.abspath(__file__)
    >>> df = spark.read.text(path)
    >>> df.select(input_file_name()).first()
    Row(input_file_name()='file:///...')
    """
    return _invoke_function("input_file_name")


def least(*cols: "ColumnOrName") -> Column:
    """
    Returns the least value of the list of column names, skipping null values.
    This function takes at least 2 parameters. It will return null if all parameters are null.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or str
        column names or columns to be compared

    Returns
    -------
    :class:`~pyspark.sql.Column`
        least value.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])
    >>> df.select(least(df.a, df.b, df.c).alias("least")).collect()
    [Row(least=1)]
    """
    if len(cols) < 2:
        raise ValueError("least should take at least two columns")
    return _invoke_function_over_columns("least", *cols)


def isnan(col: "ColumnOrName") -> Column:
    """An expression that returns true if the column is NaN.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        True if value is NaN and False otherwise.

    Examples
    --------
    >>> df = spark.createDataFrame([(1.0, float('nan')), (float('nan'), 2.0)], ("a", "b"))
    >>> df.select("a", "b", isnan("a").alias("r1"), isnan(df.b).alias("r2")).show()
    +---+---+-----+-----+
    |  a|  b|   r1|   r2|
    +---+---+-----+-----+
    |1.0|NaN|false| true|
    |NaN|2.0| true|false|
    +---+---+-----+-----+
    """
    return _invoke_function_over_columns("isnan", col)


def isnull(col: "ColumnOrName") -> Column:
    """An expression that returns true if the column is null.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        True if value is null and False otherwise.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, None), (None, 2)], ("a", "b"))
    >>> df.select("a", "b", isnull("a").alias("r1"), isnull(df.b).alias("r2")).show()
    +----+----+-----+-----+
    |   a|   b|   r1|   r2|
    +----+----+-----+-----+
    |   1|null|false| true|
    |null|   2| true|false|
    +----+----+-----+-----+
    """
    return _invoke_function_over_columns("isnull", col)


def monotonically_increasing_id() -> Column:
    """A column that generates monotonically increasing 64-bit integers.

    The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
    The current implementation puts the partition ID in the upper 31 bits, and the record number
    within each partition in the lower 33 bits. The assumption is that the data frame has
    less than 1 billion partitions, and each partition has less than 8 billion records.

    .. versionadded:: 3.4.0

    Notes
    -----
    The function is non-deterministic because its result depends on partition IDs.

    As an example, consider a :class:`DataFrame` with two partitions, each with 3 records.
    This expression would return the following IDs:
    0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        last value of the group.

    Examples
    --------
    >>> df0 = sc.parallelize(range(2), 2).mapPartitions(lambda x: [(1,), (2,), (3,)]).toDF(['col1'])
    >>> df0.select(monotonically_increasing_id().alias('id')).collect()
    [Row(id=0), Row(id=1), Row(id=2), Row(id=8589934592), Row(id=8589934593), Row(id=8589934594)]
    """
    return _invoke_function("monotonically_increasing_id")


def nanvl(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """Returns col1 if it is not NaN, or col2 if col1 is NaN.

    Both inputs should be floating point columns (:class:`DoubleType` or :class:`FloatType`).

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
        first column to check.
    col2 : :class:`~pyspark.sql.Column` or str
        second column to return if first is NaN.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        value from first column or second if first is NaN .

    Examples
    --------
    >>> df = spark.createDataFrame([(1.0, float('nan')), (float('nan'), 2.0)], ("a", "b"))
    >>> df.select(nanvl("a", "b").alias("r1"), nanvl(df.a, df.b).alias("r2")).collect()
    [Row(r1=1.0, r2=1.0), Row(r1=2.0, r2=2.0)]
    """
    return _invoke_function_over_columns("nanvl", col1, col2)


def rand(seed: Optional[int] = None) -> Column:
    """Generates a random column with independent and identically distributed (i.i.d.) samples
    uniformly distributed in [0.0, 1.0).

    .. versionadded:: 3.4.0

    Notes
    -----
    The function is non-deterministic in general case.

    Parameters
    ----------
    seed : int (default: None)
        seed value for random generator.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        random values.

    Examples
    --------
    >>> df = spark.range(2)
    >>> df.withColumn('rand', rand(seed=42) * 3).show() # doctest: +SKIP
    +---+------------------+
    | id|              rand|
    +---+------------------+
    |  0|1.4385751892400076|
    |  1|1.7082186019706387|
    +---+------------------+
    """
    if seed is not None:
        return _invoke_function("rand", lit(seed))
    else:
        return _invoke_function("rand")


def randn(seed: Optional[int] = None) -> Column:
    """Generates a column with independent and identically distributed (i.i.d.) samples from
    the standard normal distribution.

    .. versionadded:: 3.4.0

    Notes
    -----
    The function is non-deterministic in general case.

    Parameters
    ----------
    seed : int (default: None)
        seed value for random generator.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        random values.

    Examples
    --------
    >>> df = spark.range(2)
    >>> df.withColumn('randn', randn(seed=42)).show() # doctest: +SKIP
    +---+--------------------+
    | id|               randn|
    +---+--------------------+
    |  0|-0.04167221574820542|
    |  1| 0.15241403986452778|
    +---+--------------------+
    """
    if seed is not None:
        return _invoke_function("randn", lit(seed))
    else:
        return _invoke_function("randn")


def spark_partition_id() -> Column:
    """A column for partition ID.

    .. versionadded:: 3.4.0

    Notes
    -----
    This is non deterministic because it depends on data partitioning and task scheduling.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        partition id the record belongs to.

    Examples
    --------
    >>> df = spark.range(2)
    >>> df.repartition(1).select(spark_partition_id().alias("pid")).collect()
    [Row(pid=0), Row(pid=0)]
    """
    return _invoke_function("spark_partition_id")


# TODO(SPARK-41319): Support case-when in Column
# def when(condition: Column, value: Any) -> Column:
#     """Evaluates a list of conditions and returns one of multiple possible result expressions.
#     If :func:`pyspark.sql.Column.otherwise` is not invoked, None is returned for unmatched
#     conditions.
#
#     .. versionadded:: 3.4.0
#
#     Parameters
#     ----------
#     condition : :class:`~pyspark.sql.Column`
#         a boolean :class:`~pyspark.sql.Column` expression.
#     value :
#         a literal value, or a :class:`~pyspark.sql.Column` expression.
#
#     Returns
#     -------
#     :class:`~pyspark.sql.Column`
#         column representing when expression.
#
#     Examples
#     --------
#     >>> df = spark.range(3)
#     >>> df.select(when(df['id'] == 2, 3).otherwise(4).alias("age")).show()
#     +---+
#     |age|
#     +---+
#     |  4|
#     |  4|
#     |  3|
#     +---+
#
#     >>> df.select(when(df.id == 2, df.id + 1).alias("age")).show()
#     +----+
#     | age|
#     +----+
#     |null|
#     |null|
#     |   3|
#     +----+
#     """
#     # Explicitly not using ColumnOrName type here to make reading condition less opaque
#     if not isinstance(condition, Column):
#         raise TypeError("condition should be a Column")
#     v = value._jc if isinstance(value, Column) else value
#
#     return _invoke_function("when", condition._jc, v)


# Sort Functions


def asc(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the ascending order of the given column name.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the ascending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    Sort by the column 'id' in the descending order.

    >>> df = spark.range(5)
    >>> df = df.sort(desc("id"))
    >>> df.show()
    +---+
    | id|
    +---+
    |  4|
    |  3|
    |  2|
    |  1|
    |  0|
    +---+

    Sort by the column 'id' in the ascending order.

    >>> df.orderBy(asc("id")).show()
    +---+
    | id|
    +---+
    |  0|
    |  1|
    |  2|
    |  3|
    |  4|
    +---+
    """
    return _to_col(col).asc()


def asc_nulls_first(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the ascending order of the given
    column name, and null values return before non-null values.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the ascending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(1, "Bob"),
    ...                              (0, None),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(asc_nulls_first(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  0| null|
    |  2|Alice|
    |  1|  Bob|
    +---+-----+

    """
    return _to_col(col).asc_nulls_first()


def asc_nulls_last(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the ascending order of the given
    column name, and null values appear after non-null values.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the ascending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(asc_nulls_last(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  2|Alice|
    |  1|  Bob|
    |  0| null|
    +---+-----+

    """
    return _to_col(col).asc_nulls_last()


def desc(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the descending order of the given column name.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the descending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    Sort by the column 'id' in the descending order.

    >>> spark.range(5).orderBy(desc("id")).show()
    +---+
    | id|
    +---+
    |  4|
    |  3|
    |  2|
    |  1|
    |  0|
    +---+
    """
    return _to_col(col).desc()


def desc_nulls_first(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the descending order of the given
    column name, and null values appear before non-null values.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the descending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(desc_nulls_first(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  0| null|
    |  1|  Bob|
    |  2|Alice|
    +---+-----+

    """
    return _to_col(col).desc_nulls_first()


def desc_nulls_last(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the descending order of the given
    column name, and null values appear after non-null values.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the descending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(desc_nulls_last(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  1|  Bob|
    |  2|Alice|
    |  0| null|
    +---+-----+

    """
    return _to_col(col).desc_nulls_last()


# Math Functions


def abs(col: "ColumnOrName") -> Column:
    """
    Computes the absolute value.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(abs(lit(-1))).show()
    +-------+
    |abs(-1)|
    +-------+
    |      1|
    +-------+
    """
    return _invoke_function_over_columns("abs", col)


def acos(col: "ColumnOrName") -> Column:
    """
    Computes inverse cosine of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        inverse cosine of `col`, as if computed by `java.lang.Math.acos()`

    Examples
    --------
    >>> df = spark.range(1, 3)
    >>> df.select(acos(df.id)).show()
    +--------+
    |ACOS(id)|
    +--------+
    |     0.0|
    |     NaN|
    +--------+
    """
    return _invoke_function_over_columns("acos", col)


def acosh(col: "ColumnOrName") -> Column:
    """
    Computes inverse hyperbolic cosine of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(2)
    >>> df.select(acosh(col("id"))).show()
    +---------+
    |ACOSH(id)|
    +---------+
    |      NaN|
    |      0.0|
    +---------+
    """
    return _invoke_function_over_columns("acosh", col)


def asin(col: "ColumnOrName") -> Column:
    """
    Computes inverse sine of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        inverse sine of `col`, as if computed by `java.lang.Math.asin()`

    Examples
    --------
    >>> df = spark.createDataFrame([(0,), (2,)])
    >>> df.select(asin(df.schema.fieldNames()[0])).show()
    +--------+
    |ASIN(_1)|
    +--------+
    |     0.0|
    |     NaN|
    +--------+
    """
    return _invoke_function_over_columns("asin", col)


def asinh(col: "ColumnOrName") -> Column:
    """
    Computes inverse hyperbolic sine of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(asinh(col("id"))).show()
    +---------+
    |ASINH(id)|
    +---------+
    |      0.0|
    +---------+
    """
    return _invoke_function_over_columns("asinh", col)


def atan(col: "ColumnOrName") -> Column:
    """
    Compute inverse tangent of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        inverse tangent of `col`, as if computed by `java.lang.Math.atan()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(atan(df.id)).show()
    +--------+
    |ATAN(id)|
    +--------+
    |     0.0|
    +--------+
    """
    return _invoke_function_over_columns("atan", col)


def atan2(col1: Union["ColumnOrName", float], col2: Union["ColumnOrName", float]) -> Column:
    """
    .. versionadded:: 3.4.0

    Parameters
    ----------
    col1 : str, :class:`~pyspark.sql.Column` or float
        coordinate on y-axis
    col2 : str, :class:`~pyspark.sql.Column` or float
        coordinate on x-axis

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the `theta` component of the point
        (`r`, `theta`)
        in polar coordinates that corresponds to the point
        (`x`, `y`) in Cartesian coordinates,
        as if computed by `java.lang.Math.atan2()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(atan2(lit(1), lit(2))).first()
    Row(ATAN2(1, 2)=0.46364...)
    """

    return _invoke_binary_math_function("atan2", col1, col2)


def atanh(col: "ColumnOrName") -> Column:
    """
    Computes inverse hyperbolic tangent of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([(0,), (2,)], schema=["numbers"])
    >>> df.select(atanh(df["numbers"])).show()
    +--------------+
    |ATANH(numbers)|
    +--------------+
    |           0.0|
    |           NaN|
    +--------------+
    """
    return _invoke_function_over_columns("atanh", col)


def bin(col: "ColumnOrName") -> Column:
    """Returns the string representation of the binary value of the given column.

    .. versionadded:: 1.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        binary representation of given value as string.

    Examples
    --------
    >>> df = spark.createDataFrame([2,5], "INT")
    >>> df.select(bin(df.value).alias('c')).collect()
    [Row(c='10'), Row(c='101')]
    """
    return _invoke_function_over_columns("bin", col)


def bround(col: "ColumnOrName", scale: int = 0) -> Column:
    """
    Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0
    or at integral part when `scale` < 0.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input column to round.
    scale : int optional default 0
        scale value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        rounded values.

    Examples
    --------
    >>> spark.createDataFrame([(2.5,)], ['a']).select(bround('a', 0).alias('r')).collect()
    [Row(r=2.0)]
    """
    return _invoke_function("bround", _to_col(col), lit(scale))


def cbrt(col: "ColumnOrName") -> Column:
    """
    Computes the cube-root of the given value.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(cbrt(lit(27))).show()
    +--------+
    |CBRT(27)|
    +--------+
    |     3.0|
    +--------+
    """
    return _invoke_function_over_columns("cbrt", col)


def ceil(col: "ColumnOrName") -> Column:
    """
    Computes the ceiling of the given value.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(ceil(lit(-0.1))).show()
    +----------+
    |CEIL(-0.1)|
    +----------+
    |         0|
    +----------+
    """
    return _invoke_function_over_columns("ceil", col)


def conv(col: "ColumnOrName", fromBase: int, toBase: int) -> Column:
    """
    Convert a number in a string column from one base to another.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        a column to convert base for.
    fromBase: int
        from base number.
    toBase: int
        to base number.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        logariphm of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([("010101",)], ['n'])
    >>> df.select(conv(df.n, 2, 16).alias('hex')).collect()
    [Row(hex='15')]
    """
    return _invoke_function("conv", _to_col(col), lit(fromBase), lit(toBase))


def cos(col: "ColumnOrName") -> Column:
    """
    Computes cosine of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        angle in radians

    Returns
    -------
    :class:`~pyspark.sql.Column`
        cosine of the angle, as if computed by `java.lang.Math.cos()`.

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(cos(lit(math.pi))).first()
    Row(COS(3.14159...)=-1.0)
    """
    return _invoke_function_over_columns("cos", col)


def cosh(col: "ColumnOrName") -> Column:
    """
    Computes hyperbolic cosine of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        hyperbolic angle

    Returns
    -------
    :class:`~pyspark.sql.Column`
        hyperbolic cosine of the angle, as if computed by `java.lang.Math.cosh()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(cosh(lit(1))).first()
    Row(COSH(1)=1.54308...)
    """
    return _invoke_function_over_columns("cosh", col)


def cot(col: "ColumnOrName") -> Column:
    """
    Computes cotangent of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        angle in radians.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        cotangent of the angle.

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(cot(lit(math.radians(45)))).first()
    Row(COT(0.78539...)=1.00000...)
    """
    return _invoke_function_over_columns("cot", col)


def csc(col: "ColumnOrName") -> Column:
    """
    Computes cosecant of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        angle in radians.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        cosecant of the angle.

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(csc(lit(math.radians(90)))).first()
    Row(CSC(1.57079...)=1.0)
    """
    return _invoke_function_over_columns("csc", col)


def degrees(col: "ColumnOrName") -> Column:
    """
    Converts an angle measured in radians to an approximately equivalent angle
    measured in degrees.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        angle in radians

    Returns
    -------
    :class:`~pyspark.sql.Column`
        angle in degrees, as if computed by `java.lang.Math.toDegrees()`

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(degrees(lit(math.pi))).first()
    Row(DEGREES(3.14159...)=180.0)
    """
    return _invoke_function_over_columns("degrees", col)


def exp(col: "ColumnOrName") -> Column:
    """
    Computes the exponential of the given value.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to calculate exponential for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        exponential of the given value.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(exp(lit(0))).show()
    +------+
    |EXP(0)|
    +------+
    |   1.0|
    +------+
    """
    return _invoke_function_over_columns("exp", col)


def expm1(col: "ColumnOrName") -> Column:
    """
    Computes the exponential of the given value minus one.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to calculate exponential for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        exponential less one.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(expm1(lit(1))).first()
    Row(EXPM1(1)=1.71828...)
    """
    return _invoke_function_over_columns("expm1", col)


def factorial(col: "ColumnOrName") -> Column:
    """
    Computes the factorial of the given value.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        a column to calculate factorial for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        factorial of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([(5,)], ['n'])
    >>> df.select(factorial(df.n).alias('f')).collect()
    [Row(f=120)]
    """
    return _invoke_function_over_columns("factorial", col)


def floor(col: "ColumnOrName") -> Column:
    """
    Computes the floor of the given value.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to find floor for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        neares integer that is less than or equal to given value.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(floor(lit(2.5))).show()
    +----------+
    |FLOOR(2.5)|
    +----------+
    |         2|
    +----------+
    """
    return _invoke_function_over_columns("floor", col)


def hex(col: "ColumnOrName") -> Column:
    """Computes hex value of the given column, which could be :class:`pyspark.sql.types.StringType`,
    :class:`pyspark.sql.types.BinaryType`, :class:`pyspark.sql.types.IntegerType` or
    :class:`pyspark.sql.types.LongType`.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        hexadecimal representation of given value as string.

    Examples
    --------
    >>> spark.createDataFrame([('ABC', 3)], ['a', 'b']).select(hex('a'), hex('b')).collect()
    [Row(hex(a)='414243', hex(b)='3')]
    """
    return _invoke_function_over_columns("hex", col)


def hypot(col1: Union["ColumnOrName", float], col2: Union["ColumnOrName", float]) -> Column:
    """
    Computes ``sqrt(a^2 + b^2)`` without intermediate overflow or underflow.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col1 : str, :class:`~pyspark.sql.Column` or float
        a leg.
    col2 : str, :class:`~pyspark.sql.Column` or float
        b leg.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        length of the hypotenuse.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(hypot(lit(1), lit(2))).first()
    Row(HYPOT(1, 2)=2.23606...)
    """
    return _invoke_binary_math_function("hypot", col1, col2)


def log(col: "ColumnOrName") -> Column:
    """
    Computes the natural logarithm of the given value.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to calculate natural logarithm for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        natural logarithm of the given value.

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(log(lit(math.e))).first()
    Row(ln(2.71828...)=1.0)
    """
    return _invoke_function_over_columns("ln", col)


def log10(col: "ColumnOrName") -> Column:
    """
    Computes the logarithm of the given value in Base 10.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to calculate logarithm for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        logarithm of the given value in Base 10.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(log10(lit(100))).show()
    +----------+
    |LOG10(100)|
    +----------+
    |       2.0|
    +----------+
    """
    return _invoke_function_over_columns("log10", col)


def log1p(col: "ColumnOrName") -> Column:
    """
    Computes the natural logarithm of the "given value plus one".

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to calculate natural logarithm for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        natural logarithm of the "given value plus one".

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(log1p(lit(math.e))).first()
    Row(LOG1P(2.71828...)=1.31326...)

    Same as:

    >>> df.select(log(lit(math.e+1))).first()
    Row(ln(3.71828...)=1.31326...)
    """
    return _invoke_function_over_columns("log1p", col)


def log2(col: "ColumnOrName") -> Column:
    """Returns the base-2 logarithm of the argument.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        a column to calculate logariphm for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        logariphm of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([(4,)], ['a'])
    >>> df.select(log2('a').alias('log2')).show()
    +----+
    |log2|
    +----+
    | 2.0|
    +----+
    """
    return _invoke_function_over_columns("log2", col)


def pmod(dividend: Union["ColumnOrName", float], divisor: Union["ColumnOrName", float]) -> Column:
    """
    Returns the positive value of dividend mod divisor.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    dividend : str, :class:`~pyspark.sql.Column` or float
        the column that contains dividend, or the specified dividend value
    divisor : str, :class:`~pyspark.sql.Column` or float
        the column that contains divisor, or the specified divisor value

    Returns
    -------
    :class:`~pyspark.sql.Column`
        positive value of dividend mod divisor.

    Examples
    --------
    >>> from pyspark.sql.functions import pmod
    >>> df = spark.createDataFrame([
    ...     (1.0, float('nan')), (float('nan'), 2.0), (10.0, 3.0),
    ...     (float('nan'), float('nan')), (-3.0, 4.0), (-10.0, 3.0),
    ...     (-5.0, -6.0), (7.0, -8.0), (1.0, 2.0)],
    ...     ("a", "b"))
    >>> df.select(pmod("a", "b")).show()
    +----------+
    |pmod(a, b)|
    +----------+
    |       NaN|
    |       NaN|
    |       1.0|
    |       NaN|
    |       1.0|
    |       2.0|
    |      -5.0|
    |       7.0|
    |       1.0|
    +----------+
    """
    return _invoke_binary_math_function("pmod", dividend, divisor)


def pow(col1: Union["ColumnOrName", float], col2: Union["ColumnOrName", float]) -> Column:
    """
    Returns the value of the first argument raised to the power of the second argument.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col1 : str, :class:`~pyspark.sql.Column` or float
        the base number.
    col2 : str, :class:`~pyspark.sql.Column` or float
        the exponent number.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the base rased to the power the argument.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(pow(lit(3), lit(2))).first()
    Row(POWER(3, 2)=9.0)
    """
    return _invoke_binary_math_function("power", col1, col2)


def radians(col: "ColumnOrName") -> Column:
    """
    Converts an angle measured in degrees to an approximately equivalent angle
    measured in radians.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        angle in degrees

    Returns
    -------
    :class:`~pyspark.sql.Column`
        angle in radians, as if computed by `java.lang.Math.toRadians()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(radians(lit(180))).first()
    Row(RADIANS(180)=3.14159...)
    """
    return _invoke_function_over_columns("radians", col)


def rint(col: "ColumnOrName") -> Column:
    """
    Returns the double value that is closest in value to the argument and
    is equal to a mathematical integer.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(rint(lit(10.6))).show()
    +----------+
    |rint(10.6)|
    +----------+
    |      11.0|
    +----------+

    >>> df.select(rint(lit(10.3))).show()
    +----------+
    |rint(10.3)|
    +----------+
    |      10.0|
    +----------+
    """
    return _invoke_function_over_columns("rint", col)


def round(col: "ColumnOrName", scale: int = 0) -> Column:
    """
    Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0
    or at integral part when `scale` < 0.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input column to round.
    scale : int optional default 0
        scale value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        rounded values.

    Examples
    --------
    >>> spark.createDataFrame([(2.5,)], ['a']).select(round('a', 0).alias('r')).collect()
    [Row(r=3.0)]
    """
    return _invoke_function("round", _to_col(col), lit(scale))


def sec(col: "ColumnOrName") -> Column:
    """
    Computes secant of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        Angle in radians

    Returns
    -------
    :class:`~pyspark.sql.Column`
        Secant of the angle.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(sec(lit(1.5))).first()
    Row(SEC(1.5)=14.13683...)
    """
    return _invoke_function_over_columns("sec", col)


# def shiftLeft(col: "ColumnOrName", numBits: int) -> Column:
#     """Shift the given value numBits left.
#
#     .. versionadded:: 1.5.0
#
#     .. deprecated:: 3.2.0
#         Use :func:`shiftleft` instead.
#     """
#     warnings.warn("Deprecated in 3.2, use shiftleft instead.", FutureWarning)
#     return shiftleft(col, numBits)


def shiftleft(col: "ColumnOrName", numBits: int) -> Column:
    """Shift the given value numBits left.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input column of values to shift.
    numBits : int
        number of bits to shift.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        shifted value.

    Examples
    --------
    >>> spark.createDataFrame([(21,)], ['a']).select(shiftleft('a', 1).alias('r')).collect()
    [Row(r=42)]
    """
    return _invoke_function("shiftleft", _to_col(col), lit(numBits))


# def shiftRight(col: "ColumnOrName", numBits: int) -> Column:
#     """(Signed) shift the given value numBits right.
#
#     .. versionadded:: 1.5.0
#
#     .. deprecated:: 3.2.0
#         Use :func:`shiftright` instead.
#     """
#     warnings.warn("Deprecated in 3.2, use shiftright instead.", FutureWarning)
#     return shiftright(col, numBits)


def shiftright(col: "ColumnOrName", numBits: int) -> Column:
    """(Signed) shift the given value numBits right.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input column of values to shift.
    numBits : int
        number of bits to shift.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        shifted values.

    Examples
    --------
    >>> spark.createDataFrame([(42,)], ['a']).select(shiftright('a', 1).alias('r')).collect()
    [Row(r=21)]
    """
    return _invoke_function("shiftright", _to_col(col), lit(numBits))


# def shiftRightUnsigned(col: "ColumnOrName", numBits: int) -> Column:
#     """Unsigned shift the given value numBits right.
#
#     .. versionadded:: 1.5.0
#
#     .. deprecated:: 3.2.0
#         Use :func:`shiftrightunsigned` instead.
#     """
#     warnings.warn("Deprecated in 3.2, use shiftrightunsigned instead.", FutureWarning)
#     return shiftrightunsigned(col, numBits)


def shiftrightunsigned(col: "ColumnOrName", numBits: int) -> Column:
    """Unsigned shift the given value numBits right.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input column of values to shift.
    numBits : int
        number of bits to shift.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        shifted value.

    Examples
    --------
    >>> df = spark.createDataFrame([(-42,)], ['a'])
    >>> df.select(shiftrightunsigned('a', 1).alias('r')).collect()
    [Row(r=9223372036854775787)]
    """
    return _invoke_function("shiftrightunsigned", _to_col(col), lit(numBits))


def signum(col: "ColumnOrName") -> Column:
    """
    Computes the signum of the given value.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(signum(lit(-5))).show()
    +----------+
    |SIGNUM(-5)|
    +----------+
    |      -1.0|
    +----------+

    >>> df.select(signum(lit(6))).show()
    +---------+
    |SIGNUM(6)|
    +---------+
    |      1.0|
    +---------+
    """
    return _invoke_function_over_columns("signum", col)


def sin(col: "ColumnOrName") -> Column:
    """
    Computes sine of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        sine of the angle, as if computed by `java.lang.Math.sin()`

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(sin(lit(math.radians(90)))).first()
    Row(SIN(1.57079...)=1.0)
    """
    return _invoke_function_over_columns("sin", col)


def sinh(col: "ColumnOrName") -> Column:
    """
    Computes hyperbolic sine of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        hyperbolic angle.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        hyperbolic sine of the given value,
        as if computed by `java.lang.Math.sinh()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(sinh(lit(1.1))).first()
    Row(SINH(1.1)=1.33564...)
    """
    return _invoke_function_over_columns("sinh", col)


def sqrt(col: "ColumnOrName") -> Column:
    """
    Computes the square root of the specified float value.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(sqrt(lit(4))).show()
    +-------+
    |SQRT(4)|
    +-------+
    |    2.0|
    +-------+
    """
    return _invoke_function_over_columns("sqrt", col)


def tan(col: "ColumnOrName") -> Column:
    """
    Computes tangent of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        angle in radians

    Returns
    -------
    :class:`~pyspark.sql.Column`
        tangent of the given value, as if computed by `java.lang.Math.tan()`

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(tan(lit(math.radians(45)))).first()
    Row(TAN(0.78539...)=0.99999...)
    """
    return _invoke_function_over_columns("tan", col)


def tanh(col: "ColumnOrName") -> Column:
    """
    Computes hyperbolic tangent of the input column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        hyperbolic angle

    Returns
    -------
    :class:`~pyspark.sql.Column`
        hyperbolic tangent of the given value
        as if computed by `java.lang.Math.tanh()`

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(tanh(lit(math.radians(90)))).first()
    Row(TANH(1.57079...)=0.91715...)
    """
    return _invoke_function_over_columns("tanh", col)


# def toDegrees(col: "ColumnOrName") -> Column:
#     """
#     .. versionadded:: 1.4.0
#
#     .. deprecated:: 2.1.0
#         Use :func:`degrees` instead.
#     """
#     warnings.warn("Deprecated in 2.1, use degrees instead.", FutureWarning)
#     return degrees(col)
#
#
# def toRadians(col: "ColumnOrName") -> Column:
#     """
#     .. versionadded:: 1.4.0
#
#     .. deprecated:: 2.1.0
#         Use :func:`radians` instead.
#     """
#     warnings.warn("Deprecated in 2.1, use radians instead.", FutureWarning)
#     return radians(col)


def unhex(col: "ColumnOrName") -> Column:
    """Inverse of hex. Interprets each pair of characters as a hexadecimal number
    and converts to the byte representation of number.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        string representation of given hexadecimal value.

    Examples
    --------
    >>> spark.createDataFrame([('414243',)], ['a']).select(unhex('a')).collect()
    [Row(unhex(a)=bytearray(b'ABC'))]
    """
    return _invoke_function_over_columns("unhex", col)


# Aggregate Functions


# def approxCountDistinct(col: "ColumnOrName", rsd: Optional[float] = None) -> Column:
#     """
#     .. versionadded:: 1.3.0
#
#     .. deprecated:: 2.1.0
#         Use :func:`approx_count_distinct` instead.
#     """
#     warnings.warn("Deprecated in 2.1, use approx_count_distinct instead.", FutureWarning)
#     return approx_count_distinct(col, rsd)


def approx_count_distinct(col: "ColumnOrName", rsd: Optional[float] = None) -> Column:
    """Aggregate function: returns a new :class:`~pyspark.sql.Column` for approximate distinct count
    of column `col`.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
    rsd : float, optional
        maximum relative standard deviation allowed (default = 0.05).
        For rsd < 0.01, it is more efficient to use :func:`count_distinct`

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column of computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([1,2,2,3], "INT")
    >>> df.agg(approx_count_distinct("value").alias('distinct_values')).show()
    +---------------+
    |distinct_values|
    +---------------+
    |              3|
    +---------------+
    """
    if rsd is None:
        return _invoke_function("approx_count_distinct", _to_col(col))
    else:
        return _invoke_function("approx_count_distinct", _to_col(col), lit(rsd))


def avg(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the average of the values in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(avg(col("id"))).show()
    +-------+
    |avg(id)|
    +-------+
    |    4.5|
    +-------+
    """
    return _invoke_function_over_columns("avg", col)


def collect_list(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns a list of objects with duplicates.

    .. versionadded:: 3.4.0

    Notes
    -----
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        list of objects with duplicates.

    Examples
    --------
    >>> df2 = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
    >>> df2.agg(collect_list('age')).collect()
    [Row(collect_list(age)=[2, 5, 5])]
    """
    return _invoke_function_over_columns("collect_list", col)


def collect_set(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns a set of objects with duplicate elements eliminated.

    .. versionadded:: 3.4.0

    Notes
    -----
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        list of objects with no duplicates.

    Examples
    --------
    >>> df2 = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
    >>> df2.agg(array_sort(collect_set('age')).alias('c')).collect()
    [Row(c=[2, 5])]
    """
    return _invoke_function_over_columns("collect_set", col)


def corr(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """Returns a new :class:`~pyspark.sql.Column` for the Pearson Correlation Coefficient for
    ``col1`` and ``col2``.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
        first column to calculate correlation.
    col1 : :class:`~pyspark.sql.Column` or str
        second column to calculate correlation.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        Pearson Correlation Coefficient of these two column values.

    Examples
    --------
    >>> a = range(20)
    >>> b = [2 * x for x in range(20)]
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(corr("a", "b").alias('c')).collect()
    [Row(c=1.0)]
    """
    return _invoke_function_over_columns("corr", col1, col2)


def count(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the number of items in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    Count by all columns (start), and by a column that does not count ``None``.

    >>> df = spark.createDataFrame([(None,), ("a",), ("b",), ("c",)], schema=["alphabets"])
    >>> df.select(count(expr("*")), count(df.alphabets)).show()
    +--------+----------------+
    |count(1)|count(alphabets)|
    +--------+----------------+
    |       4|               3|
    +--------+----------------+
    """
    return _invoke_function_over_columns("count", col)


# def countDistinct(col: "ColumnOrName", *cols: "ColumnOrName") -> Column:
#     """Returns a new :class:`~pyspark.sql.Column` for distinct count of ``col`` or ``cols``.
#
#     An alias of :func:`count_distinct`, and it is encouraged to use :func:`count_distinct`
#     directly.
#
#     .. versionadded:: 1.3.0
#     """
#     return count_distinct(col, *cols)


# TODO(SPARK-41381): add isDistinct in UnresolvedFunction
# def count_distinct(col: "ColumnOrName", *cols: "ColumnOrName") -> Column:
#     """Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.
#
#     .. versionadded:: 3.4.0
#
#     Parameters
#     ----------
#     col : :class:`~pyspark.sql.Column` or str
#         first column to compute on.
#     cols : :class:`~pyspark.sql.Column` or str
#         other columns to compute on.
#
#     Returns
#     -------
#     :class:`~pyspark.sql.Column`
#         distinct values of these two column values.
#
#     Examples
#     --------
#     >>> from pyspark.sql import types
#     >>> df1 = spark.createDataFrame([1, 1, 3], types.IntegerType())
#     >>> df2 = spark.createDataFrame([1, 2], types.IntegerType())
#     >>> df1.join(df2).show()
#     +-----+-----+
#     |value|value|
#     +-----+-----+
#     |    1|    1|
#     |    1|    2|
#     |    1|    1|
#     |    1|    2|
#     |    3|    1|
#     |    3|    2|
#     +-----+-----+
#     >>> df1.join(df2).select(count_distinct(df1.value, df2.value)).show()
#     +----------------------------+
#     |count(DISTINCT value, value)|
#     +----------------------------+
#     |                           4|
#     +----------------------------+
#     """
#     return _invoke_function(
#         "count_distinct", _to_java_column(col), _to_seq(sc, cols, _to_java_column)
#     )


def covar_pop(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """Returns a new :class:`~pyspark.sql.Column` for the population covariance of ``col1`` and
    ``col2``.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
        first column to calculate covariance.
    col1 : :class:`~pyspark.sql.Column` or str
        second column to calculate covariance.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        covariance of these two column values.

    Examples
    --------
    >>> a = [1] * 10
    >>> b = [1] * 10
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(covar_pop("a", "b").alias('c')).collect()
    [Row(c=0.0)]
    """
    return _invoke_function_over_columns("covar_pop", col1, col2)


def covar_samp(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """Returns a new :class:`~pyspark.sql.Column` for the sample covariance of ``col1`` and
    ``col2``.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
        first column to calculate covariance.
    col1 : :class:`~pyspark.sql.Column` or str
        second column to calculate covariance.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        sample covariance of these two column values.

    Examples
    --------
    >>> a = [1] * 10
    >>> b = [1] * 10
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(covar_samp("a", "b").alias('c')).collect()
    [Row(c=0.0)]
    """
    return _invoke_function_over_columns("covar_samp", col1, col2)


def first(col: "ColumnOrName", ignorenulls: bool = False) -> Column:
    """Aggregate function: returns the first value in a group.

    The function by default returns the first values it sees. It will return the first non-null
    value it sees when ignoreNulls is set to true. If all values are null, then null is returned.

    .. versionadded:: 3.4.0

    Notes
    -----
    The function is non-deterministic because its results depends on the order of the
    rows which may be non-deterministic after a shuffle.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to fetch first value for.
    ignorenulls : :class:`~pyspark.sql.Column` or str
        if first value is null then look for first non-null value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        first value of the group.

    Examples
    --------
    >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5), ("Alice", None)], ("name", "age"))
    >>> df = df.orderBy(df.age)
    >>> df.groupby("name").agg(first("age")).orderBy("name").show()
    +-----+----------+
    | name|first(age)|
    +-----+----------+
    |Alice|      null|
    |  Bob|         5|
    +-----+----------+

    Now, to ignore any nulls we needs to set ``ignorenulls`` to `True`

    >>> df.groupby("name").agg(first("age", ignorenulls=True)).orderBy("name").show()
    +-----+----------+
    | name|first(age)|
    +-----+----------+
    |Alice|         2|
    |  Bob|         5|
    +-----+----------+
    """
    return _invoke_function("first", _to_col(col), lit(ignorenulls))


def grouping(col: "ColumnOrName") -> Column:
    """
    Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated
    or not, returns 1 for aggregated or 0 for not aggregated in the result set.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to check if it's aggregated.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        returns 1 for aggregated or 0 for not aggregated in the result set.

    Examples
    --------
    >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
    >>> df.cube("name").agg(grouping("name"), sum("age")).orderBy("name").show()
    +-----+--------------+--------+
    | name|grouping(name)|sum(age)|
    +-----+--------------+--------+
    | null|             1|       7|
    |Alice|             0|       2|
    |  Bob|             0|       5|
    +-----+--------------+--------+
    """
    return _invoke_function_over_columns("grouping", col)


def grouping_id(*cols: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the level of grouping, equals to

       (grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)

    .. versionadded:: 3.4.0

    Notes
    -----
    The list of columns should match with grouping columns exactly, or empty (means all
    the grouping columns).

    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or str
        columns to check for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        returns level of the grouping it relates to.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, "a", "a"),
    ...                             (3, "a", "a"),
    ...                             (4, "b", "c")], ["c1", "c2", "c3"])
    >>> df.cube("c2", "c3").agg(grouping_id(), sum("c1")).orderBy("c2", "c3").show()
    +----+----+-------------+-------+
    |  c2|  c3|grouping_id()|sum(c1)|
    +----+----+-------------+-------+
    |null|null|            3|      8|
    |null|   a|            2|      4|
    |null|   c|            2|      4|
    |   a|null|            1|      4|
    |   a|   a|            0|      4|
    |   b|null|            1|      4|
    |   b|   c|            0|      4|
    +----+----+-------------+-------+
    """
    return _invoke_function_over_columns("grouping_id", *cols)


def kurtosis(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the kurtosis of the values in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        kurtosis of given column.

    Examples
    --------
    >>> df = spark.createDataFrame([[1],[1],[2]], ["c"])
    >>> df.select(kurtosis(df.c)).show()
    +-----------+
    |kurtosis(c)|
    +-----------+
    |       -1.5|
    +-----------+
    """
    return _invoke_function_over_columns("kurtosis", col)


def last(col: "ColumnOrName", ignorenulls: bool = False) -> Column:
    """Aggregate function: returns the last value in a group.

    The function by default returns the last values it sees. It will return the last non-null
    value it sees when ignoreNulls is set to true. If all values are null, then null is returned.

    .. versionadded:: 3.4.0

    Notes
    -----
    The function is non-deterministic because its results depends on the order of the
    rows which may be non-deterministic after a shuffle.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to fetch last value for.
    ignorenulls : :class:`~pyspark.sql.Column` or str
        if last value is null then look for non-null value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        last value of the group.

    Examples
    --------
    >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5), ("Alice", None)], ("name", "age"))
    >>> df = df.orderBy(df.age.desc())
    >>> df.groupby("name").agg(last("age")).orderBy("name").show()
    +-----+---------+
    | name|last(age)|
    +-----+---------+
    |Alice|     null|
    |  Bob|        5|
    +-----+---------+

    Now, to ignore any nulls we needs to set ``ignorenulls`` to `True`

    >>> df.groupby("name").agg(last("age", ignorenulls=True)).orderBy("name").show()
    +-----+---------+
    | name|last(age)|
    +-----+---------+
    |Alice|        2|
    |  Bob|        5|
    +-----+---------+
    """
    return _invoke_function("last", _to_col(col), lit(ignorenulls))


def max(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the maximum value of the expression in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(max(col("id"))).show()
    +-------+
    |max(id)|
    +-------+
    |      9|
    +-------+
    """
    return _invoke_function_over_columns("max", col)


def max_by(col: "ColumnOrName", ord: "ColumnOrName") -> Column:
    """
    Returns the value associated with the maximum value of ord.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.
    ord : :class:`~pyspark.sql.Column` or str
        column to be maximized

    Returns
    -------
    :class:`~pyspark.sql.Column`
        value associated with the maximum value of ord.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(max_by("year", "earnings")).show()
    +------+----------------------+
    |course|max_by(year, earnings)|
    +------+----------------------+
    |  Java|                  2013|
    |dotNET|                  2013|
    +------+----------------------+
    """
    return _invoke_function_over_columns("max_by", col, ord)


def mean(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the average of the values in a group.
    An alias of :func:`avg`.

    .. versionadded:: 1.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(mean(df.id)).show()
    +-------+
    |avg(id)|
    +-------+
    |    4.5|
    +-------+
    """
    return avg(col)


def median(col: "ColumnOrName") -> Column:
    """
    Returns the median of the values in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the median of the values in a group.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("Java", 2012, 22000), ("dotNET", 2012, 10000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(median("earnings")).show()
    +------+----------------+
    |course|median(earnings)|
    +------+----------------+
    |  Java|         22000.0|
    |dotNET|         10000.0|
    +------+----------------+
    """
    return _invoke_function_over_columns("median", col)


def min(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the minimum value of the expression in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(min(df.id)).show()
    +-------+
    |min(id)|
    +-------+
    |      0|
    +-------+
    """
    return _invoke_function_over_columns("min", col)


def min_by(col: "ColumnOrName", ord: "ColumnOrName") -> Column:
    """
    Returns the value associated with the minimum value of ord.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.
    ord : :class:`~pyspark.sql.Column` or str
        column to be minimized

    Returns
    -------
    :class:`~pyspark.sql.Column`
        value associated with the minimum value of ord.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(min_by("year", "earnings")).show()
    +------+----------------------+
    |course|min_by(year, earnings)|
    +------+----------------------+
    |  Java|                  2012|
    |dotNET|                  2012|
    +------+----------------------+
    """
    return _invoke_function_over_columns("min_by", col, ord)


def mode(col: "ColumnOrName") -> Column:
    """
    Returns the most frequent value in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the most frequent value in a group.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(mode("year")).show()
    +------+----------+
    |course|mode(year)|
    +------+----------+
    |  Java|      2012|
    |dotNET|      2012|
    +------+----------+
    """
    return _invoke_function_over_columns("mode", col)


def percentile_approx(
    col: "ColumnOrName",
    percentage: Union[Column, float, List[float], Tuple[float]],
    accuracy: Union[Column, float] = 10000,
) -> Column:
    """Returns the approximate `percentile` of the numeric column `col` which is the smallest value
    in the ordered `col` values (sorted from least to greatest) such that no more than `percentage`
    of `col` values is less than the value or equal to that value.


    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input column.
    percentage : :class:`~pyspark.sql.Column`, float, list of floats or tuple of floats
        percentage in decimal (must be between 0.0 and 1.0).
        When percentage is an array, each value of the percentage array must be between 0.0 and 1.0.
        In this case, returns the approximate percentile array of column col
        at the given percentage array.
    accuracy : :class:`~pyspark.sql.Column` or float
        is a positive numeric literal which controls approximation accuracy
        at the cost of memory. Higher value of accuracy yields better accuracy,
        1.0/accuracy is the relative error of the approximation. (default: 10000).

    Returns
    -------
    :class:`~pyspark.sql.Column`
        approximate `percentile` of the numeric column.

    Examples
    --------
    >>> key = (col("id") % 3).alias("key")
    >>> value = (randn(42) + key * 10).alias("value")
    >>> df = spark.range(0, 1000, 1, 1).select(key, value)
    >>> df.select(
    ...     percentile_approx("value", [0.25, 0.5, 0.75], 1000000).alias("quantiles")
    ... ).printSchema()
    root
     |-- quantiles: array (nullable = true)
     |    |-- element: double (containsNull = false)

    >>> df.groupBy("key").agg(
    ...     percentile_approx("value", 0.5, lit(1000000)).alias("median")
    ... ).printSchema()
    root
     |-- key: long (nullable = true)
     |-- median: double (nullable = true)
    """

    if isinstance(percentage, Column):
        percentage_col = percentage
    elif isinstance(percentage, (list, tuple)):
        # Convert tuple to list
        percentage_col = lit(list(percentage))
    else:
        # Probably scalar
        percentage_col = lit(percentage)

    return _invoke_function("percentile_approx", _to_col(col), percentage_col, lit(accuracy))


def product(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the product of the values in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : str, :class:`Column`
        column containing values to be multiplied together

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(1, 10).toDF('x').withColumn('mod3', col('x') % 3)
    >>> prods = df.groupBy('mod3').agg(product('x').alias('product'))
    >>> prods.orderBy('mod3').show()
    +----+-------+
    |mod3|product|
    +----+-------+
    |   0|  162.0|
    |   1|   28.0|
    |   2|   80.0|
    +----+-------+
    """
    return _invoke_function_over_columns("product", col)


def skewness(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the skewness of the values in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        skewness of given column.

    Examples
    --------
    >>> df = spark.createDataFrame([[1],[1],[2]], ["c"])
    >>> df.select(skewness(df.c)).first()
    Row(skewness(c)=0.70710...)
    """
    return _invoke_function_over_columns("skewness", col)


def stddev(col: "ColumnOrName") -> Column:
    """
    Aggregate function: alias for stddev_samp.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        standard deviation of given column.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(stddev(df.id)).first()
    Row(stddev_samp(id)=1.87082...)
    """
    return stddev_samp(col)


def stddev_samp(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the unbiased sample standard deviation of
    the expression in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        standard deviation of given column.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(stddev_samp(df.id)).first()
    Row(stddev_samp(id)=1.87082...)
    """
    return _invoke_function_over_columns("stddev_samp", col)


def stddev_pop(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns population standard deviation of
    the expression in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        standard deviation of given column.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(stddev_pop(df.id)).first()
    Row(stddev_pop(id)=1.70782...)
    """
    return _invoke_function_over_columns("stddev_pop", col)


def sum(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the sum of all values in the expression.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(sum(df["id"])).show()
    +-------+
    |sum(id)|
    +-------+
    |     45|
    +-------+
    """
    return _invoke_function_over_columns("sum", col)


# def sumDistinct(col: "ColumnOrName") -> Column:
#     """
#     Aggregate function: returns the sum of distinct values in the expression.
#
#     .. versionadded:: 1.3.0
#
#     .. deprecated:: 3.2.0
#         Use :func:`sum_distinct` instead.
#     """
#     warnings.warn("Deprecated in 3.2, use sum_distinct instead.", FutureWarning)
#     return sum_distinct(col)


# TODO(SPARK-41381): add isDistinct in UnresolvedFunction
# def sum_distinct(col: "ColumnOrName") -> Column:
#     """
#     Aggregate function: returns the sum of distinct values in the expression.
#
#     .. versionadded:: 3.4.0
#
#     Parameters
#     ----------
#     col : :class:`~pyspark.sql.Column` or str
#         target column to compute on.
#
#     Returns
#     -------
#     :class:`~pyspark.sql.Column`
#         the column for computed results.
#
#     Examples
#     --------
#     >>> df = spark.createDataFrame([(None,), (1,), (1,), (2,)], schema=["numbers"])
#     >>> df.select(sum_distinct(col("numbers"))).show()
#     +---------------------+
#     |sum(DISTINCT numbers)|
#     +---------------------+
#     |                    3|
#     +---------------------+
#     """
#     return _invoke_function_over_columns("sum_distinct", col)


def var_pop(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the population variance of the values in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        variance of given column.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(var_pop(df.id)).first()
    Row(var_pop(id)=2.91666...)
    """
    return _invoke_function_over_columns("var_pop", col)


def var_samp(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the unbiased sample variance of
    the values in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        variance of given column.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(var_samp(df.id)).show()
    +------------+
    |var_samp(id)|
    +------------+
    |         3.5|
    +------------+
    """
    return _invoke_function_over_columns("var_samp", col)


def variance(col: "ColumnOrName") -> Column:
    """
    Aggregate function: alias for var_samp

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        variance of given column.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(variance(df.id)).show()
    +------------+
    |var_samp(id)|
    +------------+
    |         3.5|
    +------------+
    """
    return var_samp(col)


# String/Binary functions


def upper(col: "ColumnOrName") -> Column:
    """
    Converts a string expression to upper case.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        upper case values.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(upper("value")).show()
    +------------+
    |upper(value)|
    +------------+
    |       SPARK|
    |     PYSPARK|
    |  PANDAS API|
    +------------+
    """
    return _invoke_function_over_columns("upper", col)


def lower(col: "ColumnOrName") -> Column:
    """
    Converts a string expression to lower case.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        lower case values.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(lower("value")).show()
    +------------+
    |lower(value)|
    +------------+
    |       spark|
    |     pyspark|
    |  pandas api|
    +------------+
    """
    return _invoke_function_over_columns("lower", col)


def ascii(col: "ColumnOrName") -> Column:
    """
    Computes the numeric value of the first character of the string column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        numeric value.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(ascii("value")).show()
    +------------+
    |ascii(value)|
    +------------+
    |          83|
    |          80|
    |          80|
    +------------+
    """
    return _invoke_function_over_columns("ascii", col)


def base64(col: "ColumnOrName") -> Column:
    """
    Computes the BASE64 encoding of a binary column and returns it as a string column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        BASE64 encoding of string value.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(base64("value")).show()
    +----------------+
    |   base64(value)|
    +----------------+
    |        U3Bhcms=|
    |    UHlTcGFyaw==|
    |UGFuZGFzIEFQSQ==|
    +----------------+
    """
    return _invoke_function_over_columns("base64", col)


def unbase64(col: "ColumnOrName") -> Column:
    """
    Decodes a BASE64 encoded string column and returns it as a binary column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        encoded string value.

    Examples
    --------
    >>> df = spark.createDataFrame(["U3Bhcms=",
    ...                             "UHlTcGFyaw==",
    ...                             "UGFuZGFzIEFQSQ=="], "STRING")
    >>> df.select(unbase64("value")).show()
    +--------------------+
    |     unbase64(value)|
    +--------------------+
    |    [53 70 61 72 6B]|
    |[50 79 53 70 61 7...|
    |[50 61 6E 64 61 7...|
    +--------------------+
    """
    return _invoke_function_over_columns("unbase64", col)


def ltrim(col: "ColumnOrName") -> Column:
    """
    Trim the spaces from left end for the specified string value.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        left trimmed values.

    Examples
    --------
    >>> df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    >>> df.select(ltrim("value").alias("r")).withColumn("length", length("r")).show()
    +-------+------+
    |      r|length|
    +-------+------+
    |  Spark|     5|
    |Spark  |     7|
    |  Spark|     5|
    +-------+------+
    """
    return _invoke_function_over_columns("ltrim", col)


def rtrim(col: "ColumnOrName") -> Column:
    """
    Trim the spaces from right end for the specified string value.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        right trimmed values.

    Examples
    --------
    >>> df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    >>> df.select(rtrim("value").alias("r")).withColumn("length", length("r")).show()
    +--------+------+
    |       r|length|
    +--------+------+
    |   Spark|     8|
    |   Spark|     5|
    |   Spark|     6|
    +--------+------+
    """
    return _invoke_function_over_columns("rtrim", col)


def trim(col: "ColumnOrName") -> Column:
    """
    Trim the spaces from both ends for the specified string column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        trimmed values from both sides.

    Examples
    --------
    >>> df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    >>> df.select(trim("value").alias("r")).withColumn("length", length("r")).show()
    +-----+------+
    |    r|length|
    +-----+------+
    |Spark|     5|
    |Spark|     5|
    |Spark|     5|
    +-----+------+
    """
    return _invoke_function_over_columns("trim", col)


def concat_ws(sep: str, *cols: "ColumnOrName") -> Column:
    """
    Concatenates multiple input string columns together into a single string column,
    using the given separator.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    sep : str
        words separator.
    cols : :class:`~pyspark.sql.Column` or str
        list of columns to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        string of concatenated words.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd','123')], ['s', 'd'])
    >>> df.select(concat_ws('-', df.s, df.d).alias('s')).collect()
    [Row(s='abcd-123')]
    """
    return _invoke_function("concat_ws", lit(sep), *[_to_col(c) for c in cols])


# TODO: enable with SPARK-41402
# def decode(col: "ColumnOrName", charset: str) -> Column:
#     """
#     Computes the first argument into a string from a binary using the provided character set
#     (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
#
#     .. versionadded:: 3.4.0
#
#     Parameters
#     ----------
#     col : :class:`~pyspark.sql.Column` or str
#         target column to work on.
#     charset : str
#         charset to use to decode to.
#
#     Returns
#     -------
#     :class:`~pyspark.sql.Column`
#         the column for computed results.
#
#     Examples
#     --------
#     >>> df = spark.createDataFrame([('abcd',)], ['a'])
#     >>> df.select(decode("a", "UTF-8")).show()
#     +----------------------+
#     |stringdecode(a, UTF-8)|
#     +----------------------+
#     |                  abcd|
#     +----------------------+
#     """
#     return _invoke_function("decode", _to_col(col), lit(charset))


def encode(col: "ColumnOrName", charset: str) -> Column:
    """
    Computes the first argument into a binary from a string using the provided character set
    (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.
    charset : str
        charset to use to encode.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['c'])
    >>> df.select(encode("c", "UTF-8")).show()
    +----------------+
    |encode(c, UTF-8)|
    +----------------+
    |   [61 62 63 64]|
    +----------------+
    """
    return _invoke_function("encode", _to_col(col), lit(charset))
