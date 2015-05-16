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
import sys

if sys.version < "3":
    from itertools import imap as map

from pyspark import SparkContext
from pyspark.rdd import _prepare_for_python_RDD, ignore_unicode_prefix
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql.types import StringType
from pyspark.sql.column import Column, _to_java_column, _to_seq


__all__ = [
    'approxCountDistinct',
    'coalesce',
    'countDistinct',
    'monotonicallyIncreasingId',
    'rand',
    'randn',
    'sparkPartitionId',
    'udf',
    'when']


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

# math functions that take two arguments as input
_binary_mathfunctions = {
    'atan2': 'Returns the angle theta from the conversion of rectangular coordinates (x, y) to' +
             'polar coordinates (r, theta).',
    'hypot': 'Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.',
    'pow': 'Returns the value of the first argument raised to the power of the second argument.'
}

for _name, _doc in _functions.items():
    globals()[_name] = _create_function(_name, _doc)
for _name, _doc in _binary_mathfunctions.items():
    globals()[_name] = _create_binary_mathfunction(_name, _doc)
del _name, _doc
__all__ += _functions.keys()
__all__ += _binary_mathfunctions.keys()
__all__.sort()


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
    |Coalesce(a,b)|
    +-------------+
    |         null|
    |            1|
    |            2|
    +-------------+

    >>> cDf.select('*', coalesce(cDf["a"], lit(0.0))).show()
    +----+----+---------------+
    |   a|   b|Coalesce(a,0.0)|
    +----+----+---------------+
    |null|null|            0.0|
    |   1|null|            1.0|
    |null|   2|            0.0|
    +----+----+---------------+
    """
    sc = SparkContext._active_spark_context
    jc = sc._jvm.functions.coalesce(_to_seq(sc, cols, _to_java_column))
    return Column(jc)


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


def monotonicallyIncreasingId():
    """A column that generates monotonically increasing 64-bit integers.

    The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
    The current implementation puts the partition ID in the upper 31 bits, and the record number
    within each partition in the lower 33 bits. The assumption is that the data frame has
    less than 1 billion partitions, and each partition has less than 8 billion records.

    As an example, consider a [[DataFrame]] with two partitions, each with 3 records.
    This expression would return the following IDs:
    0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.

    >>> df0 = sc.parallelize(range(2), 2).mapPartitions(lambda x: [(1,), (2,), (3,)]).toDF(['col1'])
    >>> df0.select(monotonicallyIncreasingId().alias('id')).collect()
    [Row(id=0), Row(id=1), Row(id=2), Row(id=8589934592), Row(id=8589934593), Row(id=8589934594)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.monotonicallyIncreasingId())


def rand(seed=None):
    """Generates a random column with i.i.d. samples from U[0.0, 1.0].
    """
    sc = SparkContext._active_spark_context
    if seed:
        jc = sc._jvm.functions.rand(seed)
    else:
        jc = sc._jvm.functions.rand()
    return Column(jc)


def randn(seed=None):
    """Generates a column with i.i.d. samples from the standard normal distribution.
    """
    sc = SparkContext._active_spark_context
    if seed:
        jc = sc._jvm.functions.randn(seed)
    else:
        jc = sc._jvm.functions.randn()
    return Column(jc)


def sparkPartitionId():
    """A column for partition ID of the Spark task.

    Note that this is indeterministic because it depends on data partitioning and task scheduling.

    >>> df.repartition(1).select(sparkPartitionId().alias("pid")).collect()
    [Row(pid=0), Row(pid=0)]
    """
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.sparkPartitionId())


@ignore_unicode_prefix
def struct(*cols):
    """Creates a new struct column.

    :param cols: list of column names (string) or list of :class:`Column` expressions
        that are named or aliased.

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


class UserDefinedFunction(object):
    """
    User defined function in Python
    """
    def __init__(self, func, returnType):
        self.func = func
        self.returnType = returnType
        self._broadcast = None
        self._judf = self._create_judf()

    def _create_judf(self):
        f = self.func  # put it in closure `func`
        func = lambda _, it: map(lambda x: f(*x), it)
        ser = AutoBatchedSerializer(PickleSerializer())
        command = (func, None, ser, ser)
        sc = SparkContext._active_spark_context
        pickled_command, broadcast_vars, env, includes = _prepare_for_python_RDD(sc, command, self)
        ssql_ctx = sc._jvm.SQLContext(sc._jsc.sc())
        jdt = ssql_ctx.parseDataType(self.returnType.json())
        fname = f.__name__ if hasattr(f, '__name__') else f.__class__.__name__
        judf = sc._jvm.UserDefinedPythonFunction(fname, bytearray(pickled_command), env,
                                                 includes, sc.pythonExec, broadcast_vars,
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
