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
import json

if sys.version >= '3':
    basestring = str
    long = int

from pyspark import copy_func, since
from pyspark.context import SparkContext
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.types import *

__all__ = ["Column"]


def _create_column_from_literal(literal):
    sc = SparkContext._active_spark_context
    return sc._jvm.functions.lit(literal)


def _create_column_from_name(name):
    sc = SparkContext._active_spark_context
    return sc._jvm.functions.col(name)


def _to_java_column(col):
    if isinstance(col, Column):
        jcol = col._jc
    elif isinstance(col, basestring):
        jcol = _create_column_from_name(col)
    else:
        raise TypeError(
            "Invalid argument, not a string or column: "
            "{0} of type {1}. "
            "For column literals, use 'lit', 'array', 'struct' or 'create_map' "
            "function.".format(col, type(col)))
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

    _eqNullSafe_doc = """
    Equality test that is safe for null values.

    :param other: a value or :class:`Column`

    >>> from pyspark.sql import Row
    >>> df1 = spark.createDataFrame([
    ...     Row(id=1, value='foo'),
    ...     Row(id=2, value=None)
    ... ])
    >>> df1.select(
    ...     df1['value'] == 'foo',
    ...     df1['value'].eqNullSafe('foo'),
    ...     df1['value'].eqNullSafe(None)
    ... ).show()
    +-------------+---------------+----------------+
    |(value = foo)|(value <=> foo)|(value <=> NULL)|
    +-------------+---------------+----------------+
    |         true|           true|           false|
    |         null|          false|            true|
    +-------------+---------------+----------------+
    >>> df2 = spark.createDataFrame([
    ...     Row(value = 'bar'),
    ...     Row(value = None)
    ... ])
    >>> df1.join(df2, df1["value"] == df2["value"]).count()
    0
    >>> df1.join(df2, df1["value"].eqNullSafe(df2["value"])).count()
    1
    >>> df2 = spark.createDataFrame([
    ...     Row(id=1, value=float('NaN')),
    ...     Row(id=2, value=42.0),
    ...     Row(id=3, value=None)
    ... ])
    >>> df2.select(
    ...     df2['value'].eqNullSafe(None),
    ...     df2['value'].eqNullSafe(float('NaN')),
    ...     df2['value'].eqNullSafe(42.0)
    ... ).show()
    +----------------+---------------+----------------+
    |(value <=> NULL)|(value <=> NaN)|(value <=> 42.0)|
    +----------------+---------------+----------------+
    |           false|           true|           false|
    |           false|          false|            true|
    |            true|          false|           false|
    +----------------+---------------+----------------+

    .. note:: Unlike Pandas, PySpark doesn't consider NaN values to be NULL.
       See the `NaN Semantics`_ for details.
    .. _NaN Semantics:
       https://spark.apache.org/docs/latest/sql-programming-guide.html#nan-semantics
    .. versionadded:: 2.3.0
    """
    eqNullSafe = _bin_op("eqNullSafe", _eqNullSafe_doc)

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    __and__ = _bin_op('and')
    __or__ = _bin_op('or')
    __invert__ = _func_op('not')
    __rand__ = _bin_op("and")
    __ror__ = _bin_op("or")

    # container operators
    def __contains__(self, item):
        raise ValueError("Cannot apply 'in' operator against a column: please use 'contains' "
                         "in a string column or 'array_contains' function for an array column.")

    # bitwise operators
    _bitwiseOR_doc = """
    Compute bitwise OR of this expression with another expression.

    :param other: a value or :class:`Column` to calculate bitwise or(|) against
                  this :class:`Column`.

    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(a=170, b=75)])
    >>> df.select(df.a.bitwiseOR(df.b)).collect()
    [Row((a | b)=235)]
    """
    _bitwiseAND_doc = """
    Compute bitwise AND of this expression with another expression.

    :param other: a value or :class:`Column` to calculate bitwise and(&) against
                  this :class:`Column`.

    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(a=170, b=75)])
    >>> df.select(df.a.bitwiseAND(df.b)).collect()
    [Row((a & b)=10)]
    """
    _bitwiseXOR_doc = """
    Compute bitwise XOR of this expression with another expression.

    :param other: a value or :class:`Column` to calculate bitwise xor(^) against
                  this :class:`Column`.

    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(a=170, b=75)])
    >>> df.select(df.a.bitwiseXOR(df.b)).collect()
    [Row((a ^ b)=225)]
    """

    bitwiseOR = _bin_op("bitwiseOR", _bitwiseOR_doc)
    bitwiseAND = _bin_op("bitwiseAND", _bitwiseAND_doc)
    bitwiseXOR = _bin_op("bitwiseXOR", _bitwiseXOR_doc)

    @since(1.3)
    def getItem(self, key):
        """
        An expression that gets an item at position ``ordinal`` out of a list,
        or gets an item by key out of a dict.

        >>> df = spark.createDataFrame([([1, 2], {"key": "value"})], ["l", "d"])
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
        >>> df = spark.createDataFrame([Row(r=Row(a=1, b="b"))])
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

    def __getitem__(self, k):
        if isinstance(k, slice):
            if k.step is not None:
                raise ValueError("slice with step is not supported.")
            return self.substr(k.start, k.stop)
        else:
            return _bin_op("apply")(self, k)

    def __iter__(self):
        raise TypeError("Column is not iterable")

    # string methods
    _contains_doc = """
    Contains the other element. Returns a boolean :class:`Column` based on a string match.

    :param other: string in line

    >>> df.filter(df.name.contains('o')).collect()
    [Row(age=5, name=u'Bob')]
    """
    _rlike_doc = """
    SQL RLIKE expression (LIKE with Regex). Returns a boolean :class:`Column` based on a regex
    match.

    :param other: an extended regex expression

    >>> df.filter(df.name.rlike('ice$')).collect()
    [Row(age=2, name=u'Alice')]
    """
    _like_doc = """
    SQL like expression. Returns a boolean :class:`Column` based on a SQL LIKE match.

    :param other: a SQL LIKE pattern

    See :func:`rlike` for a regex version

    >>> df.filter(df.name.like('Al%')).collect()
    [Row(age=2, name=u'Alice')]
    """
    _startswith_doc = """
    String starts with. Returns a boolean :class:`Column` based on a string match.

    :param other: string at start of line (do not use a regex `^`)

    >>> df.filter(df.name.startswith('Al')).collect()
    [Row(age=2, name=u'Alice')]
    >>> df.filter(df.name.startswith('^Al')).collect()
    []
    """
    _endswith_doc = """
    String ends with. Returns a boolean :class:`Column` based on a string match.

    :param other: string at end of line (do not use a regex `$`)

    >>> df.filter(df.name.endswith('ice')).collect()
    [Row(age=2, name=u'Alice')]
    >>> df.filter(df.name.endswith('ice$')).collect()
    []
    """

    contains = ignore_unicode_prefix(_bin_op("contains", _contains_doc))
    rlike = ignore_unicode_prefix(_bin_op("rlike", _rlike_doc))
    like = ignore_unicode_prefix(_bin_op("like", _like_doc))
    startswith = ignore_unicode_prefix(_bin_op("startsWith", _startswith_doc))
    endswith = ignore_unicode_prefix(_bin_op("endsWith", _endswith_doc))

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
            raise TypeError(
                "startPos and length must be the same type. "
                "Got {startPos_t} and {length_t}, respectively."
                .format(
                    startPos_t=type(startPos),
                    length_t=type(length),
                ))
        if isinstance(startPos, int):
            jc = self._jc.substr(startPos, length)
        elif isinstance(startPos, Column):
            jc = self._jc.substr(startPos._jc, length._jc)
        else:
            raise TypeError("Unexpected type: %s" % type(startPos))
        return Column(jc)

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
    _asc_doc = """
    Returns a sort expression based on ascending order of the column.

    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.asc()).collect()
    [Row(name=u'Alice'), Row(name=u'Tom')]
    """
    _asc_nulls_first_doc = """
    Returns a sort expression based on ascending order of the column, and null values
    return before non-null values.

    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.asc_nulls_first()).collect()
    [Row(name=None), Row(name=u'Alice'), Row(name=u'Tom')]

    .. versionadded:: 2.4
    """
    _asc_nulls_last_doc = """
    Returns a sort expression based on ascending order of the column, and null values
    appear after non-null values.

    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.asc_nulls_last()).collect()
    [Row(name=u'Alice'), Row(name=u'Tom'), Row(name=None)]

    .. versionadded:: 2.4
    """
    _desc_doc = """
    Returns a sort expression based on the descending order of the column.

    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.desc()).collect()
    [Row(name=u'Tom'), Row(name=u'Alice')]
    """
    _desc_nulls_first_doc = """
    Returns a sort expression based on the descending order of the column, and null values
    appear before non-null values.

    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.desc_nulls_first()).collect()
    [Row(name=None), Row(name=u'Tom'), Row(name=u'Alice')]

    .. versionadded:: 2.4
    """
    _desc_nulls_last_doc = """
    Returns a sort expression based on the descending order of the column, and null values
    appear after non-null values.

    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.desc_nulls_last()).collect()
    [Row(name=u'Tom'), Row(name=u'Alice'), Row(name=None)]

    .. versionadded:: 2.4
    """

    asc = ignore_unicode_prefix(_unary_op("asc", _asc_doc))
    asc_nulls_first = ignore_unicode_prefix(_unary_op("asc_nulls_first", _asc_nulls_first_doc))
    asc_nulls_last = ignore_unicode_prefix(_unary_op("asc_nulls_last", _asc_nulls_last_doc))
    desc = ignore_unicode_prefix(_unary_op("desc", _desc_doc))
    desc_nulls_first = ignore_unicode_prefix(_unary_op("desc_nulls_first", _desc_nulls_first_doc))
    desc_nulls_last = ignore_unicode_prefix(_unary_op("desc_nulls_last", _desc_nulls_last_doc))

    _isNull_doc = """
    True if the current expression is null.

    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(name=u'Tom', height=80), Row(name=u'Alice', height=None)])
    >>> df.filter(df.height.isNull()).collect()
    [Row(height=None, name=u'Alice')]
    """
    _isNotNull_doc = """
    True if the current expression is NOT null.

    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(name=u'Tom', height=80), Row(name=u'Alice', height=None)])
    >>> df.filter(df.height.isNotNull()).collect()
    [Row(height=80, name=u'Tom')]
    """

    isNull = ignore_unicode_prefix(_unary_op("isNull", _isNull_doc))
    isNotNull = ignore_unicode_prefix(_unary_op("isNotNull", _isNotNull_doc))

    @since(1.3)
    def alias(self, *alias, **kwargs):
        """
        Returns this column aliased with a new name or names (in the case of expressions that
        return more than one column, such as explode).

        :param alias: strings of desired column names (collects all positional arguments passed)
        :param metadata: a dict of information to be stored in ``metadata`` attribute of the
            corresponding :class: `StructField` (optional, keyword only argument)

        .. versionchanged:: 2.2
           Added optional ``metadata`` argument.

        >>> df.select(df.age.alias("age2")).collect()
        [Row(age2=2), Row(age2=5)]
        >>> df.select(df.age.alias("age3", metadata={'max': 99})).schema['age3'].metadata['max']
        99
        """

        metadata = kwargs.pop('metadata', None)
        assert not kwargs, 'Unexpected kwargs where passed: %s' % kwargs

        sc = SparkContext._active_spark_context
        if len(alias) == 1:
            if metadata:
                jmeta = sc._jvm.org.apache.spark.sql.types.Metadata.fromJson(
                    json.dumps(metadata))
                return Column(getattr(self._jc, "as")(alias[0], jmeta))
            else:
                return Column(getattr(self._jc, "as")(alias[0]))
        else:
            if metadata:
                raise ValueError('metadata can only be provided for a single column')
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
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            jdt = spark._jsparkSession.parseDataType(dataType.json())
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
    globs['spark'] = spark
    globs['df'] = sc.parallelize([(2, 'Alice'), (5, 'Bob')]) \
        .toDF(StructType([StructField('age', IntegerType()),
                          StructField('name', StringType())]))

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.column, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
