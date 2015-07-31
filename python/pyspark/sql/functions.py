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

from pyspark import SparkContext
from pyspark.rdd import _prepare_for_python_RDD, ignore_unicode_prefix
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql import since
from pyspark.sql.types import StringType
from pyspark.sql.column import Column, _to_java_column, _to_seq


__all__ = [
    'array',
    'approxCountDistinct',
    'bin',
    'coalesce',
    'countDistinct',
    'explode',
    'format_number',
    'length',
    'log2',
    'md5',
    'monotonicallyIncreasingId',
    'rand',
    'randn',
    'regexp_extract',
    'regexp_replace',
    'sha1',
    'sha2',
    'size',
    'sparkPartitionId',
    'struct',
    'udf',
    'when']

__all__ += ['lag', 'lead', 'ntile']

__all__ += [
    'date_format', 'date_add', 'date_sub', 'add_months', 'months_between',
    'year', 'quarter', 'month', 'hour', 'minute', 'second',
    'dayofmonth', 'dayofyear', 'weekofyear']


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
    'first': 'Aggregate function: returns the first value in a group.',
    'last': 'Aggregate function: returns the last value in a group.',
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

# math functions that take two arguments as input
_binary_mathfunctions = {
    'atan2': 'Returns the angle theta from the conversion of rectangular coordinates (x, y) to' +
             'polar coordinates (r, theta).',
    'hypot': 'Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.',
    'pow': 'Returns the value of the first argument raised to the power of the second argument.',
}

_window_functions = {
    'rowNumber':
        """returns a sequential number starting at 1 within a window partition.

        This is equivalent to the ROW_NUMBER function in SQL.""",
    'denseRank':
        """returns the rank of rows within a window partition, without any gaps.

        The difference between rank and denseRank is that denseRank leaves no gaps in ranking
        sequence when there are ties. That is, if you were ranking a competition using denseRank
        and had three people tie for second place, you would say that all three were in second
        place and that the next person came in third.

        This is equivalent to the DENSE_RANK function in SQL.""",
    'rank':
        """returns the rank of rows within a window partition.

        The difference between rank and denseRank is that denseRank leaves no gaps in ranking
        sequence when there are ties. That is, if you were ranking a competition using denseRank
        and had three people tie for second place, you would say that all three were in second
        place and that the next person came in third.

        This is equivalent to the RANK function in SQL.""",
    'cumeDist':
        """returns the cumulative distribution of values within a window partition,
        i.e. the fraction of rows that are below the current row.

        This is equivalent to the CUME_DIST function in SQL.""",
    'percentRank':
        """returns the relative rank (i.e. percentile) of rows within a window partition.

        This is equivalent to the PERCENT_RANK function in SQL.""",
}

for _name, _doc in _functions.items():
    globals()[_name] = since(1.3)(_create_function(_name, _doc))
for _name, _doc in _functions_1_4.items():
    globals()[_name] = since(1.4)(_create_function(_name, _doc))
for _name, _doc in _binary_mathfunctions.items():
    globals()[_name] = since(1.4)(_create_binary_mathfunction(_name, _doc))
for _name, _doc in _window_functions.items():
    globals()[_name] = since(1.4)(_create_window_function(_name, _doc))
del _name, _doc
__all__ += _functions.keys()
__all__ += _functions_1_4.keys()
__all__ += _binary_mathfunctions.keys()
__all__ += _window_functions.keys()
__all__.sort()


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


@since(1.4)
def coalesce(*cols):
    """Returns the first column that is not null.

    >>> cDf = sqlContext.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
    >>> cDf.show()
    +----+----+
    |   a|   b|
    +----+----+
    |null|null|
    |   1|null|
    |null|   2|
    +----+----+

    >>> cDf.select(coalesce(cDf["a"], cDf["b"])).show()
    +-------------+
    |coalesce(a,b)|
    +-------------+
    |         null|
    |            1|
    |            2|
    +-------------+

    >>> cDf.select('*', coalesce(cDf["a"], lit(0.0))).show()
    +----+----+---------------+
    |   a|   b|coalesce(a,0.0)|
    +----+----+---------------+
    |null|null|            0.0|
    |   1|null|            1.0|
    |null|   2|            0.0|
    +----+----+---------------+
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.coalesce(_to_seq(sc, cols, _to_java_column))
    return Column(jc)


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


@since(1.4)
def explode(col):
    """Returns a new row for each element in the given array or map.

    >>> from pyspark.sql import Row
    >>> eDF = sqlContext.createDataFrame([Row(a=1, intlist=[1,2,3], mapfield={"a": "b"})])
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


@ignore_unicode_prefix
@since(1.5)
def levenshtein(left, right):
    """Computes the Levenshtein distance of the two given strings.

    >>> df0 = sqlContext.createDataFrame([('kitten', 'sitting',)], ['l', 'r'])
    >>> df0.select(levenshtein('l', 'r').alias('d')).collect()
    [Row(d=3)]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.levenshtein(_to_java_column(left), _to_java_column(right))
    return Column(jc)


@ignore_unicode_prefix
@since(1.5)
def regexp_extract(str, pattern, idx):
    """Extract a specific(idx) group identified by a java regex, from the specified string column.

    >>> df = sqlContext.createDataFrame([('100-200',)], ['str'])
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

    >>> df = sqlContext.createDataFrame([('100-200',)], ['str'])
    >>> df.select(regexp_replace('str', '(\\d+)', '##').alias('d')).collect()
    [Row(d=u'##-##')]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.regexp_replace(_to_java_column(str), pattern, replacement)
    return Column(jc)


@ignore_unicode_prefix
@since(1.5)
def md5(col):
    """Calculates the MD5 digest and returns the value as a 32 character hex string.

    >>> sqlContext.createDataFrame([('ABC',)], ['a']).select(md5('a').alias('hash')).collect()
    [Row(hash=u'902fbdd2b1df0c4f70b4a5d23525e932')]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.md5(_to_java_column(col))
    return Column(jc)


@since(1.4)
def monotonicallyIncreasingId():
    """A column that generates monotonically increasing 64-bit integers.

    The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
    The current implementation puts the partition ID in the upper 31 bits, and the record number
    within each partition in the lower 33 bits. The assumption is that the data frame has
    less than 1 billion partitions, and each partition has less than 8 billion records.

    As an example, consider a :class:`DataFrame` with two partitions, each with 3 records.
    This expression would return the following IDs:
    0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.

    >>> df0 = sc.parallelize(range(2), 2).mapPartitions(lambda x: [(1,), (2,), (3,)]).toDF(['col1'])
    >>> df0.select(monotonicallyIncreasingId().alias('id')).collect()
    [Row(id=0), Row(id=1), Row(id=2), Row(id=8589934592), Row(id=8589934593), Row(id=8589934594)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.monotonicallyIncreasingId())


@since(1.4)
def rand(seed=None):
    """Generates a random column with i.i.d. samples from U[0.0, 1.0].
    """
    sc = SparkContext._active_spark_context
    if seed:
        jc = sc._jvm.functions.rand(seed)
    else:
        jc = sc._jvm.functions.rand()
    return Column(jc)


@since(1.4)
def randn(seed=None):
    """Generates a column with i.i.d. samples from the standard normal distribution.
    """
    sc = SparkContext._active_spark_context
    if seed:
        jc = sc._jvm.functions.randn(seed)
    else:
        jc = sc._jvm.functions.randn()
    return Column(jc)


@ignore_unicode_prefix
@since(1.5)
def hex(col):
    """Computes hex value of the given column, which could be StringType,
    BinaryType, IntegerType or LongType.

    >>> sqlContext.createDataFrame([('ABC', 3)], ['a', 'b']).select(hex('a'), hex('b')).collect()
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

    >>> sqlContext.createDataFrame([('414243',)], ['a']).select(unhex('a')).collect()
    [Row(unhex(a)=bytearray(b'ABC'))]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.unhex(_to_java_column(col))
    return Column(jc)


@ignore_unicode_prefix
@since(1.5)
def sha1(col):
    """Returns the hex string result of SHA-1.

    >>> sqlContext.createDataFrame([('ABC',)], ['a']).select(sha1('a').alias('hash')).collect()
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


@since(1.5)
def shiftLeft(col, numBits):
    """Shift the the given value numBits left.

    >>> sqlContext.createDataFrame([(21,)], ['a']).select(shiftLeft('a', 1).alias('r')).collect()
    [Row(r=42)]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.shiftLeft(_to_java_column(col), numBits)
    return Column(jc)


@since(1.5)
def shiftRight(col, numBits):
    """Shift the the given value numBits right.

    >>> sqlContext.createDataFrame([(42,)], ['a']).select(shiftRight('a', 1).alias('r')).collect()
    [Row(r=21)]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.shiftRight(_to_java_column(col), numBits)
    return Column(jc)


@since(1.5)
def shiftRightUnsigned(col, numBits):
    """Unsigned shift the the given value numBits right.

    >>> sqlContext.createDataFrame([(-42,)], ['a']).select(shiftRightUnsigned('a', 1).alias('r'))\
    .collect()
    [Row(r=9223372036854775787)]
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.shiftRightUnsigned(_to_java_column(col), numBits)
    return Column(jc)


@since(1.4)
def sparkPartitionId():
    """A column for partition ID of the Spark task.

    Note that this is indeterministic because it depends on data partitioning and task scheduling.

    >>> df.repartition(1).select(sparkPartitionId().alias("pid")).collect()
    [Row(pid=0), Row(pid=0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.sparkPartitionId())


def expr(str):
    """Parses the expression string into the column that it represents

    >>> df.select(expr("length(name)")).collect()
    [Row('length(name)=5), Row('length(name)=3)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.expr(str))


@ignore_unicode_prefix
@since(1.5)
def length(col):
    """Calculates the length of a string or binary expression.

    >>> sqlContext.createDataFrame([('ABC',)], ['a']).select(length('a').alias('length')).collect()
    [Row(length=3)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.length(_to_java_column(col)))


@ignore_unicode_prefix
@since(1.5)
def format_number(col, d):
    """Formats the number X to a format like '#,###,###.##', rounded to d decimal places,
       and returns the result as a string.
    :param col: the column name of the numeric value to be formatted
    :param d: the N decimal places
    >>> sqlContext.createDataFrame([(5,)], ['a']).select(format_number('a', 4).alias('v')).collect()
    [Row(v=u'5.0000')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.format_number(_to_java_column(col), d))


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

    >>> df.select(log(10.0, df.age).alias('ten')).map(lambda l: str(l.ten)[:7]).collect()
    ['0.30102', '0.69897']

    >>> df.select(log(df.age).alias('e')).map(lambda l: str(l.e)[:7]).collect()
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

    >>> sqlContext.createDataFrame([(4,)], ['a']).select(log2('a').alias('log2')).collect()
    [Row(log2=2.0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.log2(_to_java_column(col)))


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
    Window function: returns a group id from 1 to `n` (inclusive) in a round-robin fashion in
    a window partition. Fow example, if `n` is 3, the first row will get 1, the second row will
    get 2, the third row will get 3, and the fourth row will get 1...

    This is equivalent to the NTILE function in SQL.

    :param n: an integer
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.ntile(int(n)))


@ignore_unicode_prefix
@since(1.5)
def date_format(dateCol, format):
    """
    Converts a date/timestamp/string to a value of string in the format specified by the date
    format given by the second argument.

    A pattern could be for instance `dd.MM.yyyy` and could return a string like '18.03.1993'. All
    pattern letters of the Java class `java.text.SimpleDateFormat` can be used.

    NOTE: Use when ever possible specialized functions like `year`. These benefit from a
    specialized implementation.

    >>> df = sqlContext.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(date_format('a', 'MM/dd/yyy').alias('date')).collect()
    [Row(date=u'04/08/2015')]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.date_format(_to_java_column(dateCol), format))


@since(1.5)
def year(col):
    """
    Extract the year of a given date as integer.

    >>> df = sqlContext.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(year('a').alias('year')).collect()
    [Row(year=2015)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.year(_to_java_column(col)))


@since(1.5)
def quarter(col):
    """
    Extract the quarter of a given date as integer.

    >>> df = sqlContext.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(quarter('a').alias('quarter')).collect()
    [Row(quarter=2)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.quarter(_to_java_column(col)))


@since(1.5)
def month(col):
    """
    Extract the month of a given date as integer.

    >>> df = sqlContext.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(month('a').alias('month')).collect()
    [Row(month=4)]
   """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.month(_to_java_column(col)))


@since(1.5)
def dayofmonth(col):
    """
    Extract the day of the month of a given date as integer.

    >>> df = sqlContext.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(dayofmonth('a').alias('day')).collect()
    [Row(day=8)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.dayofmonth(_to_java_column(col)))


@since(1.5)
def dayofyear(col):
    """
    Extract the day of the year of a given date as integer.

    >>> df = sqlContext.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(dayofyear('a').alias('day')).collect()
    [Row(day=98)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.dayofyear(_to_java_column(col)))


@since(1.5)
def hour(col):
    """
    Extract the hours of a given date as integer.

    >>> df = sqlContext.createDataFrame([('2015-04-08 13:08:15',)], ['a'])
    >>> df.select(hour('a').alias('hour')).collect()
    [Row(hour=13)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.hour(_to_java_column(col)))


@since(1.5)
def minute(col):
    """
    Extract the minutes of a given date as integer.

    >>> df = sqlContext.createDataFrame([('2015-04-08 13:08:15',)], ['a'])
    >>> df.select(minute('a').alias('minute')).collect()
    [Row(minute=8)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.minute(_to_java_column(col)))


@since(1.5)
def second(col):
    """
    Extract the seconds of a given date as integer.

    >>> df = sqlContext.createDataFrame([('2015-04-08 13:08:15',)], ['a'])
    >>> df.select(second('a').alias('second')).collect()
    [Row(second=15)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.second(_to_java_column(col)))


@since(1.5)
def weekofyear(col):
    """
    Extract the week number of a given date as integer.

    >>> df = sqlContext.createDataFrame([('2015-04-08',)], ['a'])
    >>> df.select(weekofyear(df.a).alias('week')).collect()
    [Row(week=15)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.weekofyear(_to_java_column(col)))


@since(1.5)
def date_add(start, days):
    """
    Returns the date that is `days` days after `start`

    >>> df = sqlContext.createDataFrame([('2015-04-08',)], ['d'])
    >>> df.select(date_add(df.d, 1).alias('d')).collect()
    [Row(d=datetime.date(2015, 4, 9))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.date_add(_to_java_column(start), days))


@since(1.5)
def date_sub(start, days):
    """
    Returns the date that is `days` days before `start`

    >>> df = sqlContext.createDataFrame([('2015-04-08',)], ['d'])
    >>> df.select(date_sub(df.d, 1).alias('d')).collect()
    [Row(d=datetime.date(2015, 4, 7))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.date_sub(_to_java_column(start), days))


@since(1.5)
def add_months(start, months):
    """
    Returns the date that is `months` months after `start`

    >>> df = sqlContext.createDataFrame([('2015-04-08',)], ['d'])
    >>> df.select(add_months(df.d, 1).alias('d')).collect()
    [Row(d=datetime.date(2015, 5, 8))]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.add_months(_to_java_column(start), months))


@since(1.5)
def months_between(date1, date2):
    """
    Returns the number of months between date1 and date2.

    >>> df = sqlContext.createDataFrame([('1997-02-28 10:30:00', '1996-10-30')], ['t', 'd'])
    >>> df.select(months_between(df.t, df.d).alias('months')).collect()
    [Row(months=3.9495967...)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.months_between(_to_java_column(date1), _to_java_column(date2)))


@since(1.5)
def size(col):
    """
    Collection function: returns the length of the array or map stored in the column.
    :param col: name of column or expression

    >>> df = sqlContext.createDataFrame([([1, 2, 3],),([1],),([],)], ['data'])
    >>> df.select(size(df.data)).collect()
    [Row(size(data)=3), Row(size(data)=1), Row(size(data)=0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.size(_to_java_column(col)))


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
        f, returnType = self.func, self.returnType  # put them in closure `func`
        func = lambda _, it: map(lambda x: returnType.toInternal(f(*x)), it)
        ser = AutoBatchedSerializer(PickleSerializer())
        command = (func, None, ser, ser)
        sc = SparkContext._active_spark_context
        pickled_command, broadcast_vars, env, includes = _prepare_for_python_RDD(sc, command, self)
        ssql_ctx = sc._jvm.SQLContext(sc._jsc.sc())
        jdt = ssql_ctx.parseDataType(self.returnType.json())
        if name is None:
            name = f.__name__ if hasattr(f, '__name__') else f.__class__.__name__
        judf = sc._jvm.UserDefinedPythonFunction(name, bytearray(pickled_command), env, includes,
                                                 sc.pythonExec, sc.pythonVer, broadcast_vars,
                                                 sc._javaAccumulator, jdt)
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

    >>> from pyspark.sql.types import IntegerType
    >>> slen = udf(lambda s: len(s), IntegerType())
    >>> df.select(slen(df.name).alias('slen')).collect()
    [Row(slen=5), Row(slen=3)]
    """
    return UserDefinedFunction(f, returnType)


def _test():
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext
    import pyspark.sql.functions
    globs = pyspark.sql.functions.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlContext'] = SQLContext(sc)
    globs['df'] = sc.parallelize([Row(name='Alice', age=2), Row(name='Bob', age=5)]).toDF()
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.functions, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
