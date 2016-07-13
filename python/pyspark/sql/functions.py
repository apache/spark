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

"""
A collections of builtin functions
"""
import math
import sys

if sys.version < "3":
    from itertools import imap as map

from pyspark import since, SparkContext
from pyspark.rdd import _prepare_for_python_RDD, ignore_unicode_prefix
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql.types import StringType
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.dataframe import DataFrame


def _create_function(name, doc=""):
    """ Create a function for aggregator by name"""
    def _(col):
        sc = SparkContext._active_spark_context
        jc = getattr(sc._jvm.functions, name)(col._jc if isinstance(col, Column) else col)
        return Column(jc)
    _.__name__ = name
    _.__doc__ = doc
    return _


def _create_binary_mathfunction(name, doc=""):
    """ Create a binary mathfunction by name"""
    def _(col1, col2):
        sc = SparkContext._active_spark_context
        # users might write ints for simplicity. This would throw an error on the JVM side.
        jc = getattr(sc._jvm.functions, name)(col1._jc if isinstance(col1, Column) else float(col1),
                                              col2._jc if isinstance(col2, Column) else float(col2))
        return Column(jc)
    _.__name__ = name
    _.__doc__ = doc
    return _


def _create_window_function(name, doc=''):
    """ Create a window function by name """
    def _():
        sc = SparkContext._active_spark_context
        jc = getattr(sc._jvm.functions, name)()
        return Column(jc)
    _.__name__ = name
    _.__doc__ = 'Window function: ' + doc
    return _


_functions = {
    'lit': 'Creates a :class:`Column` of literal value.',
    'col': 'Returns a :class:`Column` based on the given column name.',
    'column': 'Returns a :class:`Column` based on the given column name.',
    'asc': 'Returns a sort expression based on the ascending order of the given column name.',
    'desc': 'Returns a sort expression based on the descending order of the given column name.',

    'upper': 'Converts a string expression to upper case.',
    'lower': 'Converts a string expression to upper case.',
    'sqrt': 'Computes the square root of the specified float value.',
    'abs': 'Computes the absolute value.',

    'max': 'Aggregate function: returns the maximum value of the expression in a group.',
    'min': 'Aggregate function: returns the minimum value of the expression in a group.',
    'count': 'Aggregate function: returns the number of items in a group.',
    'sum': 'Aggregate function: returns the sum of all values in the expression.',
    'avg': 'Aggregate function: returns the average of the values in a group.',
    'mean': 'Aggregate function: returns the average of the values in a group.',
    'sumDistinct': 'Aggregate function: returns the sum of distinct values in the expression.',
}

_functions_1_4 = {
    # unary math functions
    'acos': 'Computes the cosine inverse of the given value; the returned angle is in the range' +
            '0.0 through pi.',
    'asin': 'Computes the sine inverse of the given value; the returned angle is in the range' +
            '-pi/2 through pi/2.',
    'atan': 'Computes the tangent inverse of the given value.',
    'cbrt': 'Computes the cube-root of the given value.',
    'ceil': 'Computes the ceiling of the given value.',
    'cos': 'Computes the cosine of the given value.',
    'cosh': 'Computes the hyperbolic cosine of the given value.',
    'exp': 'Computes the exponential of the given value.',
    'expm1': 'Computes the exponential of the given value minus one.',
    'floor': 'Computes the floor of the given value.',
    'log': 'Computes the natural logarithm of the given value.',
    'log10': 'Computes the logarithm of the given value in Base 10.',
    'log1p': 'Computes the natural logarithm of the given value plus one.',
    'rint': 'Returns the double value that is closest in value to the argument and' +
            ' is equal to a mathematical integer.',
    'signum': 'Computes the signum of the given value.',
    'sin': 'Computes the sine of the given value.',
    'sinh': 'Computes the hyperbolic sine of the given value.',
    'tan': 'Computes the tangent of the given value.',
    'tanh': 'Computes the hyperbolic tangent of the given value.',
    'toDegrees': 'Converts an angle measured in radians to an approximately equivalent angle ' +
                 'measured in degrees.',
    'toRadians': 'Converts an angle measured in degrees to an approximately equivalent angle ' +
                 'measured in radians.',

    'bitwiseNOT': 'Computes bitwise not.',
}

_functions_1_6 = {
    # unary math functions
    'stddev': 'Aggregate function: returns the unbiased sample standard deviation of' +
              ' the expression in a group.',
    'stddev_samp': 'Aggregate function: returns the unbiased sample standard deviation of' +
                   ' the expression in a group.',
    'stddev_pop': 'Aggregate function: returns population standard deviation of' +
                  ' the expression in a group.',
    'variance': 'Aggregate function: returns the population variance of the values in a group.',
    'var_samp': 'Aggregate function: returns the unbiased variance of the values in a group.',
    'var_pop':  'Aggregate function: returns the population variance of the values in a group.',
    'skewness': 'Aggregate function: returns the skewness of the values in a group.',
    'kurtosis': 'Aggregate function: returns the kurtosis of the values in a group.',
    'collect_list': 'Aggregate function: returns a list of objects with duplicates.',
    'collect_set': 'Aggregate function: returns a set of objects with duplicate elements' +
                   ' eliminated.'
}

# math functions that take two arguments as input
_binary_mathfunctions = {
    'atan2': 'Returns the angle theta from the conversion of rectangular coordinates (x, y) to' +
             'polar coordinates (r, theta).',
    'hypot': 'Computes `sqrt(a^2 + b^2)` without intermediate overflow or underflow.',
    'pow': 'Returns the value of the first argument raised to the power of the second argument.',
}

_window_functions = {
    'row_number':
        """returns a sequential number starting at 1 within a window partition.""",
    'dense_rank':
        """returns the rank of rows within a window partition, without any gaps.

        The difference between rank and denseRank is that denseRank leaves no gaps in ranking
        sequence when there are ties. That is, if you were ranking a competition using denseRank
        and had three people tie for second place, you would say that all three were in second
        place and that the next person came in third.""",
    'rank':
        """returns the rank of rows within a window partition.

        The difference between rank and denseRank is that denseRank leaves no gaps in ranking
        sequence when there are ties. That is, if you were ranking a competition using denseRank
        and had three people tie for second place, you would say that all three were in second
        place and that the next person came in third.

        This is equivalent to the RANK function in SQL.""",
    'cume_dist':
        """returns the cumulative distribution of values within a window partition,
        i.e. the fraction of rows that are below the current row.""",
    'percent_rank':
        """returns the relative rank (i.e. percentile) of rows within a window partition.""",
}

for _name, _doc in _functions.items():
    globals()[_name] = since(1.3)(_create_function(_name, _doc))
for _name, _doc in _functions_1_4.items():
    globals()[_name] = since(1.4)(_create_function(_name, _doc))
for _name, _doc in _binary_mathfunctions.items():
    globals()[_name] = since(1.4)(_create_binary_mathfunction(_name, _doc))
for _name, _doc in _window_functions.items():
    globals()[_name] = since(1.6)(_create_window_function(_name, _doc))
for _name, _doc in _functions_1_6.items():
    globals()[_name] = since(1.6)(_create_function(_name, _doc))
del _name, _doc


@since(1.3)
def approxCountDistinct(col, rsd=None):
    """Returns a new :class:`Column` for approximate distinct count of ``col``.

    >>> df.agg(approxCountDistinct(df.age).alias('c')).collect()
    [Row(c=2)]
    """
    sc = SparkContext._active_spark_context
    if rsd is None:
        jc = sc._jvm.functions.approxCountDistinct(_to_java_column(col))
    else:
        jc = sc._jvm.functions.approxCountDistinct(_to_java_column(col), rsd)
    return Column(jc)


@since(1.6)
def broadcast(df):
    """Marks a DataFrame as small enough for use in broadcast joins."""

    sc = SparkContext._active_spark_context
    return DataFrame(sc._jvm.functions.broadcast(df._jdf), df.sql_ctx)


@since(1.4)
def coalesce(*cols):
    """Returns the first column that is not null.

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
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.coalesce(_to_seq(sc, cols, _to_java_column))
    return Column(jc)


@since(1.6)
def corr(col1, col2):
    """Returns a new :class:`Column` for the Pearson Correlation Coefficient for ``col1``
    and ``col2``.

    >>> a = range(20)
    >>> b = [2 * x for x in range(20)]
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(corr("a", "b").alias('c')).collect()
    [Row(c=1.0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.corr(_to_java_column(col1), _to_java_column(col2)))


@since(2.0)
def covar_pop(col1, col2):
    """Returns a new :class:`Column` for the population covariance of ``col1``
    and ``col2``.

    >>> a = [1] * 10
    >>> b = [1] * 10
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(covar_pop("a", "b").alias('c')).collect()
    [Row(c=0.0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.covar_pop(_to_java_column(col1), _to_java_column(col2)))


@since(2.0)
def covar_samp(col1, col2):
    """Returns a new :class:`Column` for the sample covariance of ``col1``
    and ``col2``.

    >>> a = [1] * 10
    >>> b = [1] * 10
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(covar_samp("a", "b").alias('c')).collect()
    [Row(c=0.0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.covar_samp(_to_java_column(col1), _to_java_column(col2)))


@since(1.3)
def countDistinct(col, *cols):
    """Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.

    >>> df.agg(countDistinct(df.age, df.name).alias('c')).collect()
    [Row(c=2)]

    >>> df.agg(countDistinct("age", "name").alias('c')).collect()
    [Row(c=2)]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.countDistinct(_to_java_column(col), _to_seq(sc, cols, _to_java_column))
    return Column(jc)


@since(1.3)
def first(col, ignorenulls=False):
    """Aggregate function: returns the first value in a group.

    The function by default returns the first values it sees. It will return the first non-null
    value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.first(_to_java_column(col), ignorenulls)
    return Column(jc)


@since(2.0)
def grouping(col):
    """
    Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated
    or not, returns 1 for aggregated or 0 for not aggregated in the result set.

    >>> df.cube("name").agg(grouping("name"), sum("age")).orderBy("name").show()
    +-----+--------------+--------+
    | name|grouping(name)|sum(age)|
    +-----+--------------+--------+
    | null|             1|       7|
    |Alice|             0|       2|
    |  Bob|             0|       5|
    +-----+--------------+--------+
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.grouping(_to_java_column(col))
    return Column(jc)


@since(2.0)
def grouping_id(*cols):
    """
    Aggregate function: returns the level of grouping, equals to

       (grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)

    Note: the list of columns should match with grouping columns exactly, or empty (means all the
    grouping columns).

    >>> df.cube("name").agg(grouping_id(), sum("age")).orderBy("name").show()
    +-----+-------------+--------+
    | name|grouping_id()|sum(age)|
    +-----+-------------+--------+
    | null|            1|       7|
    |Alice|            0|       2|
    |  Bob|            0|       5|
    +-----+-------------+--------+
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.grouping_id(_to_seq(sc, cols, _to_java_column))
    return Column(jc)


@since(1.6)
def input_file_name():
    """Creates a string column for the file name of the current Spark task.
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.input_file_name())


@since(1.6)
def isnan(col):
    """An expression that returns true iff the column is NaN.

    >>> df = spark.createDataFrame([(1.0, float('nan')), (float('nan'), 2.0)], ("a", "b"))
    >>> df.select(isnan("a").alias("r1"), isnan(df.a).alias("r2")).collect()
    [Row(r1=False, r2=False), Row(r1=True, r2=True)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.isnan(_to_java_column(col)))


@since(1.6)
def isnull(col):
    """An expression that returns true iff the column is null.

    >>> df = spark.createDataFrame([(1, None), (None, 2)], ("a", "b"))
    >>> df.select(isnull("a").alias("r1"), isnull(df.a).alias("r2")).collect()
    [Row(r1=False, r2=False), Row(r1=True, r2=True)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.isnull(_to_java_column(col)))


@since(1.3)
def last(col, ignorenulls=False):
    """Aggregate function: returns the last value in a group.

    The function by default returns the last values it sees. It will return the last non-null
    value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.last(_to_java_column(col), ignorenulls)
    return Column(jc)


@since(1.6)
def monotonically_increasing_id():
    """A column that generates monotonically increasing 64-bit integers.

    The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
    The current implementation puts the partition ID in the upper 31 bits, and the record number
    within each partition in the lower 33 bits. The assumption is that the data frame has
    less than 1 billion partitions, and each partition has less than 8 billion records.

    As an example, consider a :class:`DataFrame` with two partitions, each with 3 records.
    This expression would return the following IDs:
    0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.

    >>> df0 = sc.parallelize(range(2), 2).mapPartitions(lambda x: [(1,), (2,), (3,)]).toDF(['col1'])
    >>> df0.select(monotonically_increasing_id().alias('id')).collect()
    [Row(id=0), Row(id=1), Row(id=2), Row(id=8589934592), Row(id=8589934593), Row(id=8589934594)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.monotonically_increasing_id())


@since(1.6)
def nanvl(col1, col2):
    """Returns col1 if it is not NaN, or col2 if col1 is NaN.

    Both inputs should be floating point columns (DoubleType or FloatType).

    >>> df = spark.createDataFrame([(1.0, float('nan')), (float('nan'), 2.0)], ("a", "b"))
    >>> df.select(nanvl("a", "b").alias("r1"), nanvl(df.a, df.b).alias("r2")).collect()
    [Row(r1=1.0, r2=1.0), Row(r1=2.0, r2=2.0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.nanvl(_to_java_column(col1), _to_java_column(col2)))


@since(1.4)
def rand(seed=None):
    """Generates a random column with i.i.d. samples from U[0.0, 1.0].
    """
    sc = SparkContext._active_spark_context
    if seed is not None:
        jc = sc._jvm.functions.rand(seed)
    else:
        jc = sc._jvm.functions.rand()
    return Column(jc)


@since(1.4)
def randn(seed=None):
    """Generates a column with i.i.d. samples from the standard normal distribution.
    """
    sc = SparkContext._active_spark_context
    if seed is not None:
        jc = sc._jvm.functions.randn(seed)
    else:
        jc = sc._jvm.functions.randn()
    return Column(jc)


@since(1.5)
def round(col, scale=0):
    """
    Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0
    or at integral part when `scale` < 0.

    >>> spark.createDataFrame([(2.5,)], ['a']).select(round('a', 0).alias('r')).collect()
    [Row(r=3.0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.round(_to_java_column(col), scale))


@since(2.0)
def bround(col, scale=0):
    """
    Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0
    or at integral part when `scale` < 0.

    >>> spark.createDataFrame([(2.5,)], ['a']).select(bround('a', 0).alias('r')).collect()
    [Row(r=2.0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.bround(_to_java_column(col), scale))


@since(1.5)
def shiftLeft(col, numBits):
    """Shift the given value numBits left.

    >>> spark.createDataFrame([(21,)], ['a']).select(shiftLeft('a', 1).alias('r')).collect()
    [Row(r=42)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.shiftLeft(_to_java_column(col), numBits))


@since(1.5)
def shiftRight(col, numBits):
    """Shift the given value numBits right.

    >>> spark.createDataFrame([(42,)], ['a']).select(shiftRight('a', 1).alias('r')).collect()
    [Row(r=21)]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.shiftRight(_to_java_column(col), numBits)
    return Column(jc)


@since(1.5)
def shiftRightUnsigned(col, numBits):
    """Unsigned shift the given value numBits right.

    >>> df = spark.createDataFrame([(-42,)], ['a'])
    >>> df.select(shiftRightUnsigned('a', 1).alias('r')).collect()
    [Row(r=9223372036854775787)]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.shiftRightUnsigned(_to_java_column(col), numBits)
    return Column(jc)


@since(1.6)
def spark_partition_id():
    """A column for partition ID of the Spark task.

    Note that this is indeterministic because it depends on data partitioning and task scheduling.

    >>> df.repartition(1).select(spark_partition_id().alias("pid")).collect()
    [Row(pid=0), Row(pid=0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.spark_partition_id())


@since(1.5)
def expr(str):
    """Parses the expression string into the column that it represents

    >>> df.select(expr("length(name)")).collect()
    [Row(length(name)=5), Row(length(name)=3)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.expr(str))


@ignore_unicode_prefix
@since(1.4)
def struct(*cols):
    """Creates a new struct column.

    :param cols: list of column names (string) or list of :class:`Column` expressions

    >>> df.select(struct('age', 'name').alias("struct")).collect()
    [Row(struct=Row(age=2, name=u'Alice')), Row(struct=Row(age=5, name=u'Bob'))]
    >>> df.select(struct([df.age, df.name]).alias("struct")).collect()
    [Row(struct=Row(age=2, name=u'Alice')), Row(struct=Row(age=5, name=u'Bob'))]
    """
    sc = SparkContext._active_spark_context
    if len(cols) == 1 and isinstance(cols[0], (list, set)):
        cols = cols[0]
    jc = sc._jvm.functions.struct(_to_seq(sc, cols, _to_java_column))
    return Column(jc)


@since(1.5)
def greatest(*cols):
    """
    Returns the greatest value of the list of column names, skipping null values.
    This function takes at least 2 parameters. It will return null iff all parameters are null.

    >>> df = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])
    >>> df.select(greatest(df.a, df.b, df.c).alias("greatest")).collect()
    [Row(greatest=4)]
    """
    if len(cols) < 2:
        raise ValueError("greatest should take at least two columns")
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.greatest(_to_seq(sc, cols, _to_java_column)))


@since(1.5)
def least(*cols):
    """
    Returns the least value of the list of column names, skipping null values.
    This function takes at least 2 parameters. It will return null iff all parameters are null.

    >>> df = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])
    >>> df.select(least(df.a, df.b, df.c).alias("least")).collect()
    [Row(least=1)]
    """
    if len(cols) < 2:
        raise ValueError("least should take at least two columns")
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.least(_to_seq(sc, cols, _to_java_column)))


@since(1.4)
def when(condition, value):
    """Evaluates a list of conditions and returns one of multiple possible result expressions.
    If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

    :param condition: a boolean :class:`Column` expression.
    :param value: a literal value, or a :class:`Column` expression.

    >>> df.select(when(df['age'] == 2, 3).otherwise(4).alias("age")).collect()
    [Row(age=3), Row(age=4)]

    >>> df.select(when(df.age == 2, df.age + 1).alias("age")).collect()
    [Row(age=3), Row(age=None)]
    """
    sc = SparkContext._active_spark_context
    if not isinstance(condition, Column):
        raise TypeError("condition should be a Column")
    v = value._jc if isinstance(value, Column) else value
    jc = sc._jvm.functions.when(condition._jc, v)
    return Column(jc)


@since(1.5)
def log(arg1, arg2=None):
    """Returns the first argument-based logarithm of the second argument.

    If there is only one argument, then this takes the natural logarithm of the argument.

    >>> df.select(log(10.0, df.age).alias('ten')).rdd.map(lambda l: str(l.ten)[:7]).collect()
    ['0.30102', '0.69897']

    >>> df.select(log(df.age).alias('e')).rdd.map(lambda l: str(l.e)[:7]).collect()
    ['0.69314', '1.60943']
    """
    sc = SparkContext._active_spark_context
    if arg2 is None:
        jc = sc._jvm.functions.log(_to_java_column(arg1))
    else:
        jc = sc._jvm.functions.log(arg1, _to_java_column(arg2))
    return Column(jc)


@since(1.5)
def log2(col):
    """Returns the base-2 logarithm of the argument.

    >>> spark.createDataFrame([(4,)], ['a']).select(log2('a').alias('log2')).collect()
    [Row(log2=2.0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.log2(_to_java_column(col)))


@since(1.5)
@ignore_unicode_prefix
def conv(col, fromBase, toBase):
    """
    Convert a number in a string column from one base to another.

    >>> df = spark.createDataFrame([("010101",)], ['n'])
    >>> df.select(conv(df.n, 2, 16).alias('hex')).collect()
    [Row(hex=u'15')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.conv(_to_java_column(col), fromBase, toBase))


@since(1.5)
def factorial(col):
    """
    Computes the factorial of the given value.

    >>> df = spark.createDataFrame([(5,)], ['n'])
    >>> df.select(factorial(df.n).alias('f')).collect()
    [Row(f=120)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.factorial(_to_java_column(col)))


# ---------------  Window functions ------------------------

@since(1.4)
def lag(col, count=1, default=None):
    """
    Window function: returns the value that is `offset` rows before the current row, and
    `defaultValue` if there is less than `offset` rows before the current row. For example,
    an `offset` of one will return the previous row at any given point in the window partition.

    This is equivalent to the LAG function in SQL.

    :param col: name of column or expression
    :param count: number of row to extend
    :param default: default value
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.lag(_to_java_column(col), count, default))


@since(1.4)
def lead(col, count=1, default=None):
    """
    Window function: returns the value that is `offset` rows after the current row, and
    `defaultValue` if there is less than `offset` rows after the current row. For example,
    an `offset` of one will return the next row at any given point in the window partition.

    This is equivalent to the LEAD function in SQL.

    :param col: name of column or expression
    :param count: number of row to extend
    :param default: default value
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.lead(_to_java_column(col), count, default))


@since(1.4)
def ntile(n):
    """
    Window function: returns the ntile group id (from 1 to `n` inclusive)
    in an ordered window partition. For example, if `n` is 4, the first
    quarter of the rows will get value 1, the second quarter will get 2,
    the third quarter will get 3, and the last quarter will get 4.

    This is equivalent to the NTILE function in SQL.

    :param n: an integer
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.ntile(int(n)))


# ---------------------- Date/Timestamp functions ------------------------------

@since(1.5)
def current_date():
    """
    Returns the current date as a date column.
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.current_date())


def current_timestamp():
    """
    Returns the current timestamp as a timestamp column.
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.current_timestamp())


@ignore_unicode_prefix
@since(1.5)
def date_format(date, format):
    """
    Converts a date/timestamp/string to a value of string in the format specified by the date
    format given by the second argument.

    A pattern could be for instance `dd.MM.yyyy` and could return a string like '18.03.1993'. All
    pattern letters of the Java class `java.text.SimpleDateFormat` can be used.

    NOTE: Use when ever possible specialized functions like `year`. These benefit from a
    specialized implementation.

    >>> df = spark.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(date_format('a', 'MM/dd/yyy').alias('date')).collect()
    [Row(date=u'04/08/2015')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.date_format(_to_java_column(date), format))


@since(1.5)
def year(col):
    """
    Extract the year of a given date as integer.

    >>> df = spark.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(year('a').alias('year')).collect()
    [Row(year=2015)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.year(_to_java_column(col)))


@since(1.5)
def quarter(col):
    """
    Extract the quarter of a given date as integer.

    >>> df = spark.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(quarter('a').alias('quarter')).collect()
    [Row(quarter=2)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.quarter(_to_java_column(col)))


@since(1.5)
def month(col):
    """
    Extract the month of a given date as integer.

    >>> df = spark.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(month('a').alias('month')).collect()
    [Row(month=4)]
   """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.month(_to_java_column(col)))


@since(1.5)
def dayofmonth(col):
    """
    Extract the day of the month of a given date as integer.

    >>> df = spark.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(dayofmonth('a').alias('day')).collect()
    [Row(day=8)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.dayofmonth(_to_java_column(col)))


@since(1.5)
def dayofyear(col):
    """
    Extract the day of the year of a given date as integer.

    >>> df = spark.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(dayofyear('a').alias('day')).collect()
    [Row(day=98)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.dayofyear(_to_java_column(col)))


@since(1.5)
def hour(col):
    """
    Extract the hours of a given date as integer.

    >>> df = spark.createDataFrame([('2015-04-08 13:08:15',)], ['a'])
    >>> df.select(hour('a').alias('hour')).collect()
    [Row(hour=13)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.hour(_to_java_column(col)))


@since(1.5)
def minute(col):
    """
    Extract the minutes of a given date as integer.

    >>> df = spark.createDataFrame([('2015-04-08 13:08:15',)], ['a'])
    >>> df.select(minute('a').alias('minute')).collect()
    [Row(minute=8)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.minute(_to_java_column(col)))


@since(1.5)
def second(col):
    """
    Extract the seconds of a given date as integer.

    >>> df = spark.createDataFrame([('2015-04-08 13:08:15',)], ['a'])
    >>> df.select(second('a').alias('second')).collect()
    [Row(second=15)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.second(_to_java_column(col)))


@since(1.5)
def weekofyear(col):
    """
    Extract the week number of a given date as integer.

    >>> df = spark.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(weekofyear(df.a).alias('week')).collect()
    [Row(week=15)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.weekofyear(_to_java_column(col)))


@since(1.5)
def date_add(start, days):
    """
    Returns the date that is `days` days after `start`

    >>> df = spark.createDataFrame([('2015-04-08',)], ['d'])
    >>> df.select(date_add(df.d, 1).alias('d')).collect()
    [Row(d=datetime.date(2015, 4, 9))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.date_add(_to_java_column(start), days))


@since(1.5)
def date_sub(start, days):
    """
    Returns the date that is `days` days before `start`

    >>> df = spark.createDataFrame([('2015-04-08',)], ['d'])
    >>> df.select(date_sub(df.d, 1).alias('d')).collect()
    [Row(d=datetime.date(2015, 4, 7))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.date_sub(_to_java_column(start), days))


@since(1.5)
def datediff(end, start):
    """
    Returns the number of days from `start` to `end`.

    >>> df = spark.createDataFrame([('2015-04-08','2015-05-10')], ['d1', 'd2'])
    >>> df.select(datediff(df.d2, df.d1).alias('diff')).collect()
    [Row(diff=32)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.datediff(_to_java_column(end), _to_java_column(start)))


@since(1.5)
def add_months(start, months):
    """
    Returns the date that is `months` months after `start`

    >>> df = spark.createDataFrame([('2015-04-08',)], ['d'])
    >>> df.select(add_months(df.d, 1).alias('d')).collect()
    [Row(d=datetime.date(2015, 5, 8))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.add_months(_to_java_column(start), months))


@since(1.5)
def months_between(date1, date2):
    """
    Returns the number of months between date1 and date2.

    >>> df = spark.createDataFrame([('1997-02-28 10:30:00', '1996-10-30')], ['t', 'd'])
    >>> df.select(months_between(df.t, df.d).alias('months')).collect()
    [Row(months=3.9495967...)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.months_between(_to_java_column(date1), _to_java_column(date2)))


@since(1.5)
def to_date(col):
    """
    Converts the column of StringType or TimestampType into DateType.

    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(to_date(df.t).alias('date')).collect()
    [Row(date=datetime.date(1997, 2, 28))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.to_date(_to_java_column(col)))


@since(1.5)
def trunc(date, format):
    """
    Returns date truncated to the unit specified by the format.

    :param format: 'year', 'YYYY', 'yy' or 'month', 'mon', 'mm'

    >>> df = spark.createDataFrame([('1997-02-28',)], ['d'])
    >>> df.select(trunc(df.d, 'year').alias('year')).collect()
    [Row(year=datetime.date(1997, 1, 1))]
    >>> df.select(trunc(df.d, 'mon').alias('month')).collect()
    [Row(month=datetime.date(1997, 2, 1))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.trunc(_to_java_column(date), format))


@since(1.5)
def next_day(date, dayOfWeek):
    """
    Returns the first date which is later than the value of the date column.

    Day of the week parameter is case insensitive, and accepts:
        "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".

    >>> df = spark.createDataFrame([('2015-07-27',)], ['d'])
    >>> df.select(next_day(df.d, 'Sun').alias('date')).collect()
    [Row(date=datetime.date(2015, 8, 2))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.next_day(_to_java_column(date), dayOfWeek))


@since(1.5)
def last_day(date):
    """
    Returns the last day of the month which the given date belongs to.

    >>> df = spark.createDataFrame([('1997-02-10',)], ['d'])
    >>> df.select(last_day(df.d).alias('date')).collect()
    [Row(date=datetime.date(1997, 2, 28))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.last_day(_to_java_column(date)))


@since(1.5)
def from_unixtime(timestamp, format="yyyy-MM-dd HH:mm:ss"):
    """
    Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
    representing the timestamp of that moment in the current system time zone in the given
    format.
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.from_unixtime(_to_java_column(timestamp), format))


@since(1.5)
def unix_timestamp(timestamp=None, format='yyyy-MM-dd HH:mm:ss'):
    """
    Convert time string with given pattern ('yyyy-MM-dd HH:mm:ss', by default)
    to Unix time stamp (in seconds), using the default timezone and the default
    locale, return null if fail.

    if `timestamp` is None, then it returns current timestamp.
    """
    sc = SparkContext._active_spark_context
    if timestamp is None:
        return Column(sc._jvm.functions.unix_timestamp())
    return Column(sc._jvm.functions.unix_timestamp(_to_java_column(timestamp), format))


@since(1.5)
def from_utc_timestamp(timestamp, tz):
    """
    Assumes given timestamp is UTC and converts to given timezone.

    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(from_utc_timestamp(df.t, "PST").alias('t')).collect()
    [Row(t=datetime.datetime(1997, 2, 28, 2, 30))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.from_utc_timestamp(_to_java_column(timestamp), tz))


@since(1.5)
def to_utc_timestamp(timestamp, tz):
    """
    Assumes given timestamp is in given timezone and converts to UTC.

    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(to_utc_timestamp(df.t, "PST").alias('t')).collect()
    [Row(t=datetime.datetime(1997, 2, 28, 18, 30))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.to_utc_timestamp(_to_java_column(timestamp), tz))


@since(2.0)
@ignore_unicode_prefix
def window(timeColumn, windowDuration, slideDuration=None, startTime=None):
    """Bucketize rows into one or more time windows given a timestamp specifying column. Window
    starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
    [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
    the order of months are not supported.

    The time column must be of TimestampType.

    Durations are provided as strings, e.g. '1 second', '1 day 12 hours', '2 minutes'. Valid
    interval strings are 'week', 'day', 'hour', 'minute', 'second', 'millisecond', 'microsecond'.
    If the `slideDuration` is not provided, the windows will be tumbling windows.

    The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start
    window intervals. For example, in order to have hourly tumbling windows that start 15 minutes
    past the hour, e.g. 12:15-13:15, 13:15-14:15... provide `startTime` as `15 minutes`.

    The output column will be a struct called 'window' by default with the nested columns 'start'
    and 'end', where 'start' and 'end' will be of `TimestampType`.

    >>> df = spark.createDataFrame([("2016-03-11 09:00:07", 1)]).toDF("date", "val")
    >>> w = df.groupBy(window("date", "5 seconds")).agg(sum("val").alias("sum"))
    >>> w.select(w.window.start.cast("string").alias("start"),
    ...          w.window.end.cast("string").alias("end"), "sum").collect()
    [Row(start=u'2016-03-11 09:00:05', end=u'2016-03-11 09:00:10', sum=1)]
    """
    def check_string_field(field, fieldName):
        if not field or type(field) is not str:
            raise TypeError("%s should be provided as a string" % fieldName)

    sc = SparkContext._active_spark_context
    time_col = _to_java_column(timeColumn)
    check_string_field(windowDuration, "windowDuration")
    if slideDuration and startTime:
        check_string_field(slideDuration, "slideDuration")
        check_string_field(startTime, "startTime")
        res = sc._jvm.functions.window(time_col, windowDuration, slideDuration, startTime)
    elif slideDuration:
        check_string_field(slideDuration, "slideDuration")
        res = sc._jvm.functions.window(time_col, windowDuration, slideDuration)
    elif startTime:
        check_string_field(startTime, "startTime")
        res = sc._jvm.functions.window(time_col, windowDuration, windowDuration, startTime)
    else:
        res = sc._jvm.functions.window(time_col, windowDuration)
    return Column(res)


# ---------------------------- misc functions ----------------------------------

@since(1.5)
@ignore_unicode_prefix
def crc32(col):
    """
    Calculates the cyclic redundancy check value  (CRC32) of a binary column and
    returns the value as a bigint.

    >>> spark.createDataFrame([('ABC',)], ['a']).select(crc32('a').alias('crc32')).collect()
    [Row(crc32=2743272264)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.crc32(_to_java_column(col)))


@ignore_unicode_prefix
@since(1.5)
def md5(col):
    """Calculates the MD5 digest and returns the value as a 32 character hex string.

    >>> spark.createDataFrame([('ABC',)], ['a']).select(md5('a').alias('hash')).collect()
    [Row(hash=u'902fbdd2b1df0c4f70b4a5d23525e932')]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.md5(_to_java_column(col))
    return Column(jc)


@ignore_unicode_prefix
@since(1.5)
def sha1(col):
    """Returns the hex string result of SHA-1.

    >>> spark.createDataFrame([('ABC',)], ['a']).select(sha1('a').alias('hash')).collect()
    [Row(hash=u'3c01bdbb26f358bab27f267924aa2c9a03fcfdb8')]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.sha1(_to_java_column(col))
    return Column(jc)


@ignore_unicode_prefix
@since(1.5)
def sha2(col, numBits):
    """Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384,
    and SHA-512). The numBits indicates the desired bit length of the result, which must have a
    value of 224, 256, 384, 512, or 0 (which is equivalent to 256).

    >>> digests = df.select(sha2(df.name, 256).alias('s')).collect()
    >>> digests[0]
    Row(s=u'3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043')
    >>> digests[1]
    Row(s=u'cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961')
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.sha2(_to_java_column(col), numBits)
    return Column(jc)


@since(2.0)
def hash(*cols):
    """Calculates the hash code of given columns, and returns the result as an int column.

    >>> spark.createDataFrame([('ABC',)], ['a']).select(hash('a').alias('hash')).collect()
    [Row(hash=-757602832)]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.hash(_to_seq(sc, cols, _to_java_column))
    return Column(jc)


# ---------------------- String/Binary functions ------------------------------

_string_functions = {
    'ascii': 'Computes the numeric value of the first character of the string column.',
    'base64': 'Computes the BASE64 encoding of a binary column and returns it as a string column.',
    'unbase64': 'Decodes a BASE64 encoded string column and returns it as a binary column.',
    'initcap': 'Returns a new string column by converting the first letter of each word to ' +
               'uppercase. Words are delimited by whitespace.',
    'lower': 'Converts a string column to lower case.',
    'upper': 'Converts a string column to upper case.',
    'reverse': 'Reverses the string column and returns it as a new string column.',
    'ltrim': 'Trim the spaces from left end for the specified string value.',
    'rtrim': 'Trim the spaces from right end for the specified string value.',
    'trim': 'Trim the spaces from both ends for the specified string column.',
}


for _name, _doc in _string_functions.items():
    globals()[_name] = since(1.5)(_create_function(_name, _doc))
del _name, _doc


@since(1.5)
@ignore_unicode_prefix
def concat(*cols):
    """
    Concatenates multiple input string columns together into a single string column.

    >>> df = spark.createDataFrame([('abcd','123')], ['s', 'd'])
    >>> df.select(concat(df.s, df.d).alias('s')).collect()
    [Row(s=u'abcd123')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.concat(_to_seq(sc, cols, _to_java_column)))


@since(1.5)
@ignore_unicode_prefix
def concat_ws(sep, *cols):
    """
    Concatenates multiple input string columns together into a single string column,
    using the given separator.

    >>> df = spark.createDataFrame([('abcd','123')], ['s', 'd'])
    >>> df.select(concat_ws('-', df.s, df.d).alias('s')).collect()
    [Row(s=u'abcd-123')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.concat_ws(sep, _to_seq(sc, cols, _to_java_column)))


@since(1.5)
def decode(col, charset):
    """
    Computes the first argument into a string from a binary using the provided character set
    (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.decode(_to_java_column(col), charset))


@since(1.5)
def encode(col, charset):
    """
    Computes the first argument into a binary from a string using the provided character set
    (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.encode(_to_java_column(col), charset))


@ignore_unicode_prefix
@since(1.5)
def format_number(col, d):
    """
    Formats the number X to a format like '#,--#,--#.--', rounded to d decimal places,
    and returns the result as a string.

    :param col: the column name of the numeric value to be formatted
    :param d: the N decimal places

    >>> spark.createDataFrame([(5,)], ['a']).select(format_number('a', 4).alias('v')).collect()
    [Row(v=u'5.0000')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.format_number(_to_java_column(col), d))


@ignore_unicode_prefix
@since(1.5)
def format_string(format, *cols):
    """
    Formats the arguments in printf-style and returns the result as a string column.

    :param col: the column name of the numeric value to be formatted
    :param d: the N decimal places

    >>> df = spark.createDataFrame([(5, "hello")], ['a', 'b'])
    >>> df.select(format_string('%d %s', df.a, df.b).alias('v')).collect()
    [Row(v=u'5 hello')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.format_string(format, _to_seq(sc, cols, _to_java_column)))


@since(1.5)
def instr(str, substr):
    """
    Locate the position of the first occurrence of substr column in the given string.
    Returns null if either of the arguments are null.

    NOTE: The position is not zero based, but 1 based index, returns 0 if substr
    could not be found in str.

    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(instr(df.s, 'b').alias('s')).collect()
    [Row(s=2)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.instr(_to_java_column(str), substr))


@since(1.5)
@ignore_unicode_prefix
def substring(str, pos, len):
    """
    Substring starts at `pos` and is of length `len` when str is String type or
    returns the slice of byte array that starts at `pos` in byte and is of length `len`
    when str is Binary type

    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(substring(df.s, 1, 2).alias('s')).collect()
    [Row(s=u'ab')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.substring(_to_java_column(str), pos, len))


@since(1.5)
@ignore_unicode_prefix
def substring_index(str, delim, count):
    """
    Returns the substring from string str before count occurrences of the delimiter delim.
    If count is positive, everything the left of the final delimiter (counting from left) is
    returned. If count is negative, every to the right of the final delimiter (counting from the
    right) is returned. substring_index performs a case-sensitive match when searching for delim.

    >>> df = spark.createDataFrame([('a.b.c.d',)], ['s'])
    >>> df.select(substring_index(df.s, '.', 2).alias('s')).collect()
    [Row(s=u'a.b')]
    >>> df.select(substring_index(df.s, '.', -3).alias('s')).collect()
    [Row(s=u'b.c.d')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.substring_index(_to_java_column(str), delim, count))


@ignore_unicode_prefix
@since(1.5)
def levenshtein(left, right):
    """Computes the Levenshtein distance of the two given strings.

    >>> df0 = spark.createDataFrame([('kitten', 'sitting',)], ['l', 'r'])
    >>> df0.select(levenshtein('l', 'r').alias('d')).collect()
    [Row(d=3)]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.levenshtein(_to_java_column(left), _to_java_column(right))
    return Column(jc)


@since(1.5)
def locate(substr, str, pos=1):
    """
    Locate the position of the first occurrence of substr in a string column, after position pos.

    NOTE: The position is not zero based, but 1 based index. returns 0 if substr
    could not be found in str.

    :param substr: a string
    :param str: a Column of StringType
    :param pos: start position (zero based)

    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(locate('b', df.s, 1).alias('s')).collect()
    [Row(s=2)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.locate(substr, _to_java_column(str), pos))


@since(1.5)
@ignore_unicode_prefix
def lpad(col, len, pad):
    """
    Left-pad the string column to width `len` with `pad`.

    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(lpad(df.s, 6, '#').alias('s')).collect()
    [Row(s=u'##abcd')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.lpad(_to_java_column(col), len, pad))


@since(1.5)
@ignore_unicode_prefix
def rpad(col, len, pad):
    """
    Right-pad the string column to width `len` with `pad`.

    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(rpad(df.s, 6, '#').alias('s')).collect()
    [Row(s=u'abcd##')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.rpad(_to_java_column(col), len, pad))


@since(1.5)
@ignore_unicode_prefix
def repeat(col, n):
    """
    Repeats a string column n times, and returns it as a new string column.

    >>> df = spark.createDataFrame([('ab',)], ['s',])
    >>> df.select(repeat(df.s, 3).alias('s')).collect()
    [Row(s=u'ababab')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.repeat(_to_java_column(col), n))


@since(1.5)
@ignore_unicode_prefix
def split(str, pattern):
    """
    Splits str around pattern (pattern is a regular expression).

    NOTE: pattern is a string represent the regular expression.

    >>> df = spark.createDataFrame([('ab12cd',)], ['s',])
    >>> df.select(split(df.s, '[0-9]+').alias('s')).collect()
    [Row(s=[u'ab', u'cd'])]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.split(_to_java_column(str), pattern))


@ignore_unicode_prefix
@since(1.5)
def regexp_extract(str, pattern, idx):
    """Extract a specific(idx) group identified by a java regex, from the specified string column.

    >>> df = spark.createDataFrame([('100-200',)], ['str'])
    >>> df.select(regexp_extract('str', '(\d+)-(\d+)', 1).alias('d')).collect()
    [Row(d=u'100')]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.regexp_extract(_to_java_column(str), pattern, idx)
    return Column(jc)


@ignore_unicode_prefix
@since(1.5)
def regexp_replace(str, pattern, replacement):
    """Replace all substrings of the specified string value that match regexp with rep.

    >>> df = spark.createDataFrame([('100-200',)], ['str'])
    >>> df.select(regexp_replace('str', '(\\d+)', '--').alias('d')).collect()
    [Row(d=u'-----')]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.regexp_replace(_to_java_column(str), pattern, replacement)
    return Column(jc)


@ignore_unicode_prefix
@since(1.5)
def initcap(col):
    """Translate the first letter of each word to upper case in the sentence.

    >>> spark.createDataFrame([('ab cd',)], ['a']).select(initcap("a").alias('v')).collect()
    [Row(v=u'Ab Cd')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.initcap(_to_java_column(col)))


@since(1.5)
@ignore_unicode_prefix
def soundex(col):
    """
    Returns the SoundEx encoding for a string

    >>> df = spark.createDataFrame([("Peters",),("Uhrbach",)], ['name'])
    >>> df.select(soundex(df.name).alias("soundex")).collect()
    [Row(soundex=u'P362'), Row(soundex=u'U612')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.soundex(_to_java_column(col)))


@ignore_unicode_prefix
@since(1.5)
def bin(col):
    """Returns the string representation of the binary value of the given column.

    >>> df.select(bin(df.age).alias('c')).collect()
    [Row(c=u'10'), Row(c=u'101')]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.bin(_to_java_column(col))
    return Column(jc)


@ignore_unicode_prefix
@since(1.5)
def hex(col):
    """Computes hex value of the given column, which could be StringType,
    BinaryType, IntegerType or LongType.

    >>> spark.createDataFrame([('ABC', 3)], ['a', 'b']).select(hex('a'), hex('b')).collect()
    [Row(hex(a)=u'414243', hex(b)=u'3')]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.hex(_to_java_column(col))
    return Column(jc)


@ignore_unicode_prefix
@since(1.5)
def unhex(col):
    """Inverse of hex. Interprets each pair of characters as a hexadecimal number
    and converts to the byte representation of number.

    >>> spark.createDataFrame([('414243',)], ['a']).select(unhex('a')).collect()
    [Row(unhex(a)=bytearray(b'ABC'))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.unhex(_to_java_column(col)))


@ignore_unicode_prefix
@since(1.5)
def length(col):
    """Calculates the length of a string or binary expression.

    >>> spark.createDataFrame([('ABC',)], ['a']).select(length('a').alias('length')).collect()
    [Row(length=3)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.length(_to_java_column(col)))


@ignore_unicode_prefix
@since(1.5)
def translate(srcCol, matching, replace):
    """A function translate any character in the `srcCol` by a character in `matching`.
    The characters in `replace` is corresponding to the characters in `matching`.
    The translate will happen when any character in the string matching with the character
    in the `matching`.

    >>> spark.createDataFrame([('translate',)], ['a']).select(translate('a', "rnlt", "123") \\
    ...     .alias('r')).collect()
    [Row(r=u'1a2s3ae')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.translate(_to_java_column(srcCol), matching, replace))


# ---------------------- Collection functions ------------------------------

@ignore_unicode_prefix
@since(2.0)
def create_map(*cols):
    """Creates a new map column.

    :param cols: list of column names (string) or list of :class:`Column` expressions that grouped
        as key-value pairs, e.g. (key1, value1, key2, value2, ...).

    >>> df.select(create_map('name', 'age').alias("map")).collect()
    [Row(map={u'Alice': 2}), Row(map={u'Bob': 5})]
    >>> df.select(create_map([df.name, df.age]).alias("map")).collect()
    [Row(map={u'Alice': 2}), Row(map={u'Bob': 5})]
    """
    sc = SparkContext._active_spark_context
    if len(cols) == 1 and isinstance(cols[0], (list, set)):
        cols = cols[0]
    jc = sc._jvm.functions.map(_to_seq(sc, cols, _to_java_column))
    return Column(jc)


@since(1.4)
def array(*cols):
    """Creates a new array column.

    :param cols: list of column names (string) or list of :class:`Column` expressions that have
        the same data type.

    >>> df.select(array('age', 'age').alias("arr")).collect()
    [Row(arr=[2, 2]), Row(arr=[5, 5])]
    >>> df.select(array([df.age, df.age]).alias("arr")).collect()
    [Row(arr=[2, 2]), Row(arr=[5, 5])]
    """
    sc = SparkContext._active_spark_context
    if len(cols) == 1 and isinstance(cols[0], (list, set)):
        cols = cols[0]
    jc = sc._jvm.functions.array(_to_seq(sc, cols, _to_java_column))
    return Column(jc)


@since(1.5)
def array_contains(col, value):
    """
    Collection function: returns True if the array contains the given value. The collection
    elements and value must be of the same type.

    :param col: name of column containing array
    :param value: value to check for in array

    >>> df = spark.createDataFrame([(["a", "b", "c"],), ([],)], ['data'])
    >>> df.select(array_contains(df.data, "a")).collect()
    [Row(array_contains(data, a)=True), Row(array_contains(data, a)=False)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.array_contains(_to_java_column(col), value))


@since(1.4)
def explode(col):
    """Returns a new row for each element in the given array or map.

    >>> from pyspark.sql import Row
    >>> eDF = spark.createDataFrame([Row(a=1, intlist=[1,2,3], mapfield={"a": "b"})])
    >>> eDF.select(explode(eDF.intlist).alias("anInt")).collect()
    [Row(anInt=1), Row(anInt=2), Row(anInt=3)]

    >>> eDF.select(explode(eDF.mapfield).alias("key", "value")).show()
    +---+-----+
    |key|value|
    +---+-----+
    |  a|    b|
    +---+-----+
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.explode(_to_java_column(col))
    return Column(jc)


@since(2.1)
def posexplode(col):
    """Returns a new row for each element with position in the given array or map.

    >>> from pyspark.sql import Row
    >>> eDF = spark.createDataFrame([Row(a=1, intlist=[1,2,3], mapfield={"a": "b"})])
    >>> eDF.select(posexplode(eDF.intlist)).collect()
    [Row(pos=0, col=1), Row(pos=1, col=2), Row(pos=2, col=3)]

    >>> eDF.select(posexplode(eDF.mapfield)).show()
    +---+---+-----+
    |pos|key|value|
    +---+---+-----+
    |  0|  a|    b|
    +---+---+-----+
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.posexplode(_to_java_column(col))
    return Column(jc)


@ignore_unicode_prefix
@since(1.6)
def get_json_object(col, path):
    """
    Extracts json object from a json string based on json path specified, and returns json string
    of the extracted json object. It will return null if the input json string is invalid.

    :param col: string column in json format
    :param path: path to the json object to extract

    >>> data = [("1", '''{"f1": "value1", "f2": "value2"}'''), ("2", '''{"f1": "value12"}''')]
    >>> df = spark.createDataFrame(data, ("key", "jstring"))
    >>> df.select(df.key, get_json_object(df.jstring, '$.f1').alias("c0"), \\
    ...                   get_json_object(df.jstring, '$.f2').alias("c1") ).collect()
    [Row(key=u'1', c0=u'value1', c1=u'value2'), Row(key=u'2', c0=u'value12', c1=None)]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.get_json_object(_to_java_column(col), path)
    return Column(jc)


@ignore_unicode_prefix
@since(1.6)
def json_tuple(col, *fields):
    """Creates a new row for a json column according to the given field names.

    :param col: string column in json format
    :param fields: list of fields to extract

    >>> data = [("1", '''{"f1": "value1", "f2": "value2"}'''), ("2", '''{"f1": "value12"}''')]
    >>> df = spark.createDataFrame(data, ("key", "jstring"))
    >>> df.select(df.key, json_tuple(df.jstring, 'f1', 'f2')).collect()
    [Row(key=u'1', c0=u'value1', c1=u'value2'), Row(key=u'2', c0=u'value12', c1=None)]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.json_tuple(_to_java_column(col), _to_seq(sc, fields))
    return Column(jc)


@since(1.5)
def size(col):
    """
    Collection function: returns the length of the array or map stored in the column.

    :param col: name of column or expression

    >>> df = spark.createDataFrame([([1, 2, 3],),([1],),([],)], ['data'])
    >>> df.select(size(df.data)).collect()
    [Row(size(data)=3), Row(size(data)=1), Row(size(data)=0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.size(_to_java_column(col)))


@since(1.5)
def sort_array(col, asc=True):
    """
    Collection function: sorts the input array for the given column in ascending order.

    :param col: name of column or expression

    >>> df = spark.createDataFrame([([2, 1, 3],),([1],),([],)], ['data'])
    >>> df.select(sort_array(df.data).alias('r')).collect()
    [Row(r=[1, 2, 3]), Row(r=[1]), Row(r=[])]
    >>> df.select(sort_array(df.data, asc=False).alias('r')).collect()
    [Row(r=[3, 2, 1]), Row(r=[1]), Row(r=[])]
     """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.sort_array(_to_java_column(col), asc))


# ---------------------------- User Defined Function ----------------------------------

def _wrap_function(sc, func, returnType):
    command = (func, returnType)
    pickled_command, broadcast_vars, env, includes = _prepare_for_python_RDD(sc, command)
    return sc._jvm.PythonFunction(bytearray(pickled_command), env, includes, sc.pythonExec,
                                  sc.pythonVer, broadcast_vars, sc._javaAccumulator)


class UserDefinedFunction(object):
    """
    User defined function in Python

    .. versionadded:: 1.3
    """
    def __init__(self, func, returnType, name=None):
        self.func = func
        self.returnType = returnType
        self._broadcast = None
        self._judf = self._create_judf(name)

    def _create_judf(self, name):
        from pyspark.sql import SQLContext
        sc = SparkContext.getOrCreate()
        wrapped_func = _wrap_function(sc, self.func, self.returnType)
        ctx = SQLContext.getOrCreate(sc)
        jdt = ctx._ssql_ctx.parseDataType(self.returnType.json())
        if name is None:
            f = self.func
            name = f.__name__ if hasattr(f, '__name__') else f.__class__.__name__
        judf = sc._jvm.org.apache.spark.sql.execution.python.UserDefinedPythonFunction(
            name, wrapped_func, jdt)
        return judf

    def __del__(self):
        if self._broadcast is not None:
            self._broadcast.unpersist()
            self._broadcast = None

    def __call__(self, *cols):
        sc = SparkContext._active_spark_context
        jc = self._judf.apply(_to_seq(sc, cols, _to_java_column))
        return Column(jc)


@since(1.3)
def udf(f, returnType=StringType()):
    """Creates a :class:`Column` expression representing a user defined function (UDF).
    Note that the user-defined functions must be deterministic. Due to optimization,
    duplicate invocations may be eliminated or the function may even be invoked more times than
    it is present in the query.

    >>> from pyspark.sql.types import IntegerType
    >>> slen = udf(lambda s: len(s), IntegerType())
    >>> df.select(slen(df.name).alias('slen')).collect()
    [Row(slen=5), Row(slen=3)]
    """
    return UserDefinedFunction(f, returnType)

blacklist = ['map', 'since', 'ignore_unicode_prefix']
__all__ = [k for k, v in globals().items()
           if not k.startswith('_') and k[0].islower() and callable(v) and k not in blacklist]
__all__.sort()


def _test():
    import doctest
    from pyspark.sql import Row, SparkSession
    import pyspark.sql.functions
    globs = pyspark.sql.functions.__dict__.copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("sql.functions tests")\
        .getOrCreate()
    sc = spark.sparkContext
    globs['sc'] = sc
    globs['spark'] = spark
    globs['df'] = sc.parallelize([Row(name='Alice', age=2), Row(name='Bob', age=5)]).toDF()
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.functions, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    spark.stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
