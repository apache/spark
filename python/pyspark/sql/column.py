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
import warnings

if sys.version >= '3':
    basestring = str
    long = int

from pyspark import copy_func, since
from pyspark.context import SparkContext
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.types import *

__all__ = ["DataFrame", "Column", "DataFrameNaFunctions", "DataFrameStatFunctions"]


def _create_column_from_literal(literal):
    sc = SparkContext._active_spark_context
    return sc._jvm.functions.lit(literal)


def _create_column_from_name(name):
    sc = SparkContext._active_spark_context
    return sc._jvm.functions.col(name)


def _to_java_column(col):
    if isinstance(col, Column):
        jcol = col._jc
    else:
        jcol = _create_column_from_name(col)
    return jcol


def _to_seq(sc, cols, converter=None):
    """
    Convert a list of Column (or names) into a JVM Seq of Column.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    return sc._jvm.PythonUtils.toSeq(cols)


def _to_list(sc, cols, converter=None):
    """
    Convert a list of Column (or names) into a JVM (Scala) List of Column.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    return sc._jvm.PythonUtils.toList(cols)


def _unary_op(name, doc="unary operator"):
    """ Create a method for given unary operator """
    def _(self):
        jc = getattr(self._jc, name)()
        return Column(jc)
    _.__doc__ = doc
    return _


def _func_op(name, doc=''):
    def _(self):
        sc = SparkContext._active_spark_context
        jc = getattr(sc._jvm.functions, name)(self._jc)
        return Column(jc)
    _.__doc__ = doc
    return _


def _bin_func_op(name, reverse=False, doc="binary function"):
    def _(self, other):
        sc = SparkContext._active_spark_context
        fn = getattr(sc._jvm.functions, name)
        jc = other._jc if isinstance(other, Column) else _create_column_from_literal(other)
        njc = fn(self._jc, jc) if not reverse else fn(jc, self._jc)
        return Column(njc)
    _.__doc__ = doc
    return _


def _bin_op(name, doc="binary operator"):
    """ Create a method for given binary operator
    """
    def _(self, other):
        jc = other._jc if isinstance(other, Column) else other
        njc = getattr(self._jc, name)(jc)
        return Column(njc)
    _.__doc__ = doc
    return _


def _reverse_op(name, doc="binary operator"):
    """ Create a method for binary operator (this object is on right side)
    """
    def _(self, other):
        jother = _create_column_from_literal(other)
        jc = getattr(jother, name)(self._jc)
        return Column(jc)
    _.__doc__ = doc
    return _


class Column(object):

    """
    A column in a DataFrame.

    :class:`Column` instances can be created by::

        # 1. Select a column out of a DataFrame

        df.colName
        df["colName"]

        # 2. Create from an expression
        df.colName + 1
        1 / df.colName

    .. versionadded:: 1.3
    """

    def __init__(self, jc):
        self._jc = jc

    # arithmetic operators
    __neg__ = _func_op("negate")
    __add__ = _bin_op("plus")
    __sub__ = _bin_op("minus")
    __mul__ = _bin_op("multiply")
    __div__ = _bin_op("divide")
    __truediv__ = _bin_op("divide")
    __mod__ = _bin_op("mod")
    __radd__ = _bin_op("plus")
    __rsub__ = _reverse_op("minus")
    __rmul__ = _bin_op("multiply")
    __rdiv__ = _reverse_op("divide")
    __rtruediv__ = _reverse_op("divide")
    __rmod__ = _reverse_op("mod")
    __pow__ = _bin_func_op("pow")
    __rpow__ = _bin_func_op("pow", reverse=True)

    # logistic operators
    __eq__ = _bin_op("equalTo")
    __ne__ = _bin_op("notEqual")
    __lt__ = _bin_op("lt")
    __le__ = _bin_op("leq")
    __ge__ = _bin_op("geq")
    __gt__ = _bin_op("gt")

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    __and__ = _bin_op('and')
    __or__ = _bin_op('or')
    __invert__ = _func_op('not')
    __rand__ = _bin_op("and")
    __ror__ = _bin_op("or")

    # container operators
    __contains__ = _bin_op("contains")
    __getitem__ = _bin_op("apply")

    # bitwise operators
    bitwiseOR = _bin_op("bitwiseOR")
    bitwiseAND = _bin_op("bitwiseAND")
    bitwiseXOR = _bin_op("bitwiseXOR")

    @since(1.3)
    def getItem(self, key):
        """
        An expression that gets an item at position ``ordinal`` out of a list,
        or gets an item by key out of a dict.

        >>> df = sc.parallelize([([1, 2], {"key": "value"})]).toDF(["l", "d"])
        >>> df.select(df.l.getItem(0), df.d.getItem("key")).show()
        +----+------+
        |l[0]|d[key]|
        +----+------+
        |   1| value|
        +----+------+
        >>> df.select(df.l[0], df.d["key"]).show()
        +----+------+
        |l[0]|d[key]|
        +----+------+
        |   1| value|
        +----+------+
        """
        return self[key]

    @since(1.3)
    def getField(self, name):
        """
        An expression that gets a field by name in a StructField.

        >>> from pyspark.sql import Row
        >>> df = sc.parallelize([Row(r=Row(a=1, b="b"))]).toDF()
        >>> df.select(df.r.getField("b")).show()
        +---+
        |r.b|
        +---+
        |  b|
        +---+
        >>> df.select(df.r.a).show()
        +---+
        |r.a|
        +---+
        |  1|
        +---+
        """
        return self[name]

    def __getattr__(self, item):
        if item.startswith("__"):
            raise AttributeError(item)
        return self.getField(item)

    def __iter__(self):
        raise TypeError("Column is not iterable")

    # string methods
    rlike = _bin_op("rlike")
    like = _bin_op("like")
    startswith = _bin_op("startsWith")
    endswith = _bin_op("endsWith")

    @ignore_unicode_prefix
    @since(1.3)
    def substr(self, startPos, length):
        """
        Return a :class:`Column` which is a substring of the column.

        :param startPos: start position (int or Column)
        :param length:  length of the substring (int or Column)

        >>> df.select(df.name.substr(1, 3).alias("col")).collect()
        [Row(col=u'Ali'), Row(col=u'Bob')]
        """
        if type(startPos) != type(length):
            raise TypeError("Can not mix the type")
        if isinstance(startPos, (int, long)):
            jc = self._jc.substr(startPos, length)
        elif isinstance(startPos, Column):
            jc = self._jc.substr(startPos._jc, length._jc)
        else:
            raise TypeError("Unexpected type: %s" % type(startPos))
        return Column(jc)

    __getslice__ = substr

    @ignore_unicode_prefix
    @since(1.5)
    def isin(self, *cols):
        """
        A boolean expression that is evaluated to true if the value of this
        expression is contained by the evaluated values of the arguments.

        >>> df[df.name.isin("Bob", "Mike")].collect()
        [Row(age=5, name=u'Bob')]
        >>> df[df.age.isin([1, 2, 3])].collect()
        [Row(age=2, name=u'Alice')]
        """
        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            cols = cols[0]
        cols = [c._jc if isinstance(c, Column) else _create_column_from_literal(c) for c in cols]
        sc = SparkContext._active_spark_context
        jc = getattr(self._jc, "isin")(_to_seq(sc, cols))
        return Column(jc)

    # order
    asc = _unary_op("asc", "Returns a sort expression based on the"
                           " ascending order of the given column name.")
    desc = _unary_op("desc", "Returns a sort expression based on the"
                             " descending order of the given column name.")

    isNull = _unary_op("isNull", "True if the current expression is null.")
    isNotNull = _unary_op("isNotNull", "True if the current expression is not null.")

    @since(1.3)
    def alias(self, *alias):
        """
        Returns this column aliased with a new name or names (in the case of expressions that
        return more than one column, such as explode).

        >>> df.select(df.age.alias("age2")).collect()
        [Row(age2=2), Row(age2=5)]
        """

        if len(alias) == 1:
            return Column(getattr(self._jc, "as")(alias[0]))
        else:
            sc = SparkContext._active_spark_context
            return Column(getattr(self._jc, "as")(_to_seq(sc, list(alias))))

    name = copy_func(alias, sinceversion=2.0, doc=":func:`name` is an alias for :func:`alias`.")

    @ignore_unicode_prefix
    @since(1.3)
    def cast(self, dataType):
        """ Convert the column into type ``dataType``.

        >>> df.select(df.age.cast("string").alias('ages')).collect()
        [Row(ages=u'2'), Row(ages=u'5')]
        >>> df.select(df.age.cast(StringType()).alias('ages')).collect()
        [Row(ages=u'2'), Row(ages=u'5')]
        """
        if isinstance(dataType, basestring):
            jc = self._jc.cast(dataType)
        elif isinstance(dataType, DataType):
            from pyspark.sql import SQLContext
            sc = SparkContext.getOrCreate()
            ctx = SQLContext.getOrCreate(sc)
            jdt = ctx._ssql_ctx.parseDataType(dataType.json())
            jc = self._jc.cast(jdt)
        else:
            raise TypeError("unexpected type: %s" % type(dataType))
        return Column(jc)

    astype = copy_func(cast, sinceversion=1.4, doc=":func:`astype` is an alias for :func:`cast`.")

    @since(1.3)
    def between(self, lowerBound, upperBound):
        """
        A boolean expression that is evaluated to true if the value of this
        expression is between the given columns.

        >>> df.select(df.name, df.age.between(2, 4)).show()
        +-----+---------------------------+
        | name|((age >= 2) AND (age <= 4))|
        +-----+---------------------------+
        |Alice|                       true|
        |  Bob|                      false|
        +-----+---------------------------+
        """
        return (self >= lowerBound) & (self <= upperBound)

    @since(1.4)
    def when(self, condition, value):
        """
        Evaluates a list of conditions and returns one of multiple possible result expressions.
        If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

        See :func:`pyspark.sql.functions.when` for example usage.

        :param condition: a boolean :class:`Column` expression.
        :param value: a literal value, or a :class:`Column` expression.

        >>> from pyspark.sql import functions as F
        >>> df.select(df.name, F.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)).show()
        +-----+------------------------------------------------------------+
        | name|CASE WHEN (age > 4) THEN 1 WHEN (age < 3) THEN -1 ELSE 0 END|
        +-----+------------------------------------------------------------+
        |Alice|                                                          -1|
        |  Bob|                                                           1|
        +-----+------------------------------------------------------------+
        """
        if not isinstance(condition, Column):
            raise TypeError("condition should be a Column")
        v = value._jc if isinstance(value, Column) else value
        jc = self._jc.when(condition._jc, v)
        return Column(jc)

    @since(1.4)
    def otherwise(self, value):
        """
        Evaluates a list of conditions and returns one of multiple possible result expressions.
        If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

        See :func:`pyspark.sql.functions.when` for example usage.

        :param value: a literal value, or a :class:`Column` expression.

        >>> from pyspark.sql import functions as F
        >>> df.select(df.name, F.when(df.age > 3, 1).otherwise(0)).show()
        +-----+-------------------------------------+
        | name|CASE WHEN (age > 3) THEN 1 ELSE 0 END|
        +-----+-------------------------------------+
        |Alice|                                    0|
        |  Bob|                                    1|
        +-----+-------------------------------------+
        """
        v = value._jc if isinstance(value, Column) else value
        jc = self._jc.otherwise(v)
        return Column(jc)

    @since(1.4)
    def over(self, window):
        """
        Define a windowing column.

        :param window: a :class:`WindowSpec`
        :return: a Column

        >>> from pyspark.sql import Window
        >>> window = Window.partitionBy("name").orderBy("age").rowsBetween(-1, 1)
        >>> from pyspark.sql.functions import rank, min
        >>> # df.select(rank().over(window), min('age').over(window))
        """
        from pyspark.sql.window import WindowSpec
        if not isinstance(window, WindowSpec):
            raise TypeError("window should be WindowSpec")
        jc = self._jc.over(window._jspec)
        return Column(jc)

    def __nonzero__(self):
        raise ValueError("Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
                         "'~' for 'not' when building DataFrame boolean expressions.")
    __bool__ = __nonzero__

    def __repr__(self):
        return 'Column<%s>' % self._jc.toString().encode('utf8')


def _test():
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.column
    globs = pyspark.sql.column.__dict__.copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("sql.column tests")\
        .getOrCreate()
    sc = spark.sparkContext
    globs['sc'] = sc
    globs['df'] = sc.parallelize([(2, 'Alice'), (5, 'Bob')]) \
        .toDF(StructType([StructField('age', IntegerType()),
                          StructField('name', StringType())]))

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.column, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    spark.stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
