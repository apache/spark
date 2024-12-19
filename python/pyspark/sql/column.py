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
from typing import (
    overload,
    Any,
    TYPE_CHECKING,
    Union,
)

from pyspark.sql.tvf_argument import TableValuedFunctionArgument
from pyspark.sql.utils import dispatch_col_method
from pyspark.sql.types import DataType
from pyspark.errors import PySparkValueError

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.sql._typing import LiteralType, DecimalLiteral, DateTimeLiteral
    from pyspark.sql.window import WindowSpec

__all__ = ["Column"]


class Column(TableValuedFunctionArgument):

    """
    A column in a DataFrame.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Examples
    --------
    Column instances can be created by

    >>> df = spark.createDataFrame(
    ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])

    Select a column out of a DataFrame
    >>> df.name
    Column<'name'>
    >>> df["name"]
    Column<'name'>

    Create from an expression

    >>> df.age + 1
    Column<...>
    >>> 1 / df.age
    Column<...>
    """

    # HACK ALERT!! this is to reduce the backward compatibility concern, and returns
    # Spark Classic Column by default. This is NOT an API, and NOT supposed to
    # be directly invoked. DO NOT use this constructor.
    def __new__(
        cls,
        jc: "JavaObject",
    ) -> "Column":
        from pyspark.sql.classic.column import Column

        return Column.__new__(Column, jc)

    def __init__(self, jc: "JavaObject") -> None:
        self._jc = jc

    # arithmetic operators
    @dispatch_col_method
    def __neg__(self) -> "Column":
        ...

    @dispatch_col_method
    def __add__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __sub__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __mul__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __div__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __truediv__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __mod__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __radd__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __rsub__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __rmul__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __rdiv__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __rtruediv__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __rmod__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __pow__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __rpow__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    # logistic operators
    @dispatch_col_method
    def __eq__(  # type: ignore[override]
        self,
        other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
    ) -> "Column":
        """binary function"""
        ...

    @dispatch_col_method
    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> "Column":
        """binary function"""
        ...

    @dispatch_col_method
    def __lt__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __le__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __ge__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __gt__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def eqNullSafe(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        """
        Equality test that is safe for null values.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other
            a value or :class:`Column`

        Examples
        --------
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
        |         NULL|          false|            true|
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

        Notes
        -----
        Unlike Pandas, PySpark doesn't consider NaN values to be NULL. See the
        `NaN Semantics <https://spark.apache.org/docs/latest/sql-ref-datatypes.html#nan-semantics>`_
        for details.
        """
        ...

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    @dispatch_col_method
    def __and__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __or__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __invert__(self) -> "Column":
        ...

    @dispatch_col_method
    def __rand__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    @dispatch_col_method
    def __ror__(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        ...

    # container operators
    @dispatch_col_method
    def __contains__(self, item: Any) -> None:
        raise PySparkValueError(
            errorClass="CANNOT_APPLY_IN_FOR_COLUMN",
            messageParameters={},
        )

    # bitwise operators
    @dispatch_col_method
    def bitwiseOR(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        """ "
        Compute bitwise OR of this expression with another expression.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other
            a value or :class:`Column` to calculate bitwise or(|) with
            this :class:`Column`.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([Row(a=170, b=75)])
        >>> df.select(df.a.bitwiseOR(df.b)).collect()
        [Row((a | b)=235)]
        """
        ...

    @dispatch_col_method
    def bitwiseAND(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        """
        Compute bitwise AND of this expression with another expression.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other
            a value or :class:`Column` to calculate bitwise and(&) with
            this :class:`Column`.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([Row(a=170, b=75)])
        >>> df.select(df.a.bitwiseAND(df.b)).collect()
        [Row((a & b)=10)]
        """
        ...

    @dispatch_col_method
    def bitwiseXOR(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        """
        Compute bitwise XOR of this expression with another expression.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other
            a value or :class:`Column` to calculate bitwise xor(^) with
            this :class:`Column`.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([Row(a=170, b=75)])
        >>> df.select(df.a.bitwiseXOR(df.b)).collect()
        [Row((a ^ b)=225)]
        """
        ...

    @dispatch_col_method
    def getItem(self, key: Any) -> "Column":
        """
        An expression that gets an item at position ``ordinal`` out of a list,
        or gets an item by key out of a dict.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        key
            a literal value, or a :class:`Column` expression.
            The result will only be true at a location if the item matches in the column.

             .. deprecated:: 3.0.0
                 :class:`Column` as a parameter is deprecated.

        Returns
        -------
        :class:`Column`
            Column representing the item(s) got at position out of a list or by key out of a dict.

        Examples
        --------
        >>> df = spark.createDataFrame([([1, 2], {"key": "value"})], ["l", "d"])
        >>> df.select(df.l.getItem(0), df.d.getItem("key")).show()
        +----+------+
        |l[0]|d[key]|
        +----+------+
        |   1| value|
        +----+------+
        """
        ...

    @dispatch_col_method
    def getField(self, name: Any) -> "Column":
        """
        An expression that gets a field by name in a :class:`StructType`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        name
            a literal value, or a :class:`Column` expression.
            The result will only be true at a location if the field matches in the Column.

             .. deprecated:: 3.0.0
                 :class:`Column` as a parameter is deprecated.
        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column got by name.

        Examples
        --------
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
        ...

    @dispatch_col_method
    def withField(self, fieldName: str, col: "Column") -> "Column":
        """
        An expression that adds/replaces a field in :class:`StructType` by name.

        .. versionadded:: 3.1.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        fieldName : str
            a literal value.
            The result will only be true at a location if any field matches in the Column.
        col : :class:`Column`
            A :class:`Column` expression for the column with `fieldName`.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column
            which field was added/replaced by fieldName.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> from pyspark.sql.functions import lit
        >>> df = spark.createDataFrame([Row(a=Row(b=1, c=2))])
        >>> df.withColumn('a', df['a'].withField('b', lit(3))).select('a.b').show()
        +---+
        |  b|
        +---+
        |  3|
        +---+
        >>> df.withColumn('a', df['a'].withField('d', lit(4))).select('a.d').show()
        +---+
        |  d|
        +---+
        |  4|
        +---+
        """
        ...

    @dispatch_col_method
    def dropFields(self, *fieldNames: str) -> "Column":
        """
        An expression that drops fields in :class:`StructType` by name.
        This is a no-op if the schema doesn't contain field name(s).

        .. versionadded:: 3.1.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        fieldNames : str
            Desired field names (collects all positional arguments passed)
            The result will drop at a location if any field matches in the Column.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column with field dropped by fieldName.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> from pyspark.sql.functions import col, lit
        >>> df = spark.createDataFrame([
        ...     Row(a=Row(b=1, c=2, d=3, e=Row(f=4, g=5, h=6)))])
        >>> df.withColumn('a', df['a'].dropFields('b')).show()
        +-----------------+
        |                a|
        +-----------------+
        |{2, 3, {4, 5, 6}}|
        +-----------------+

        >>> df.withColumn('a', df['a'].dropFields('b', 'c')).show()
        +--------------+
        |             a|
        +--------------+
        |{3, {4, 5, 6}}|
        +--------------+

        This method supports dropping multiple nested fields directly e.g.

        >>> df.withColumn("a", col("a").dropFields("e.g", "e.h")).show()
        +--------------+
        |             a|
        +--------------+
        |{1, 2, 3, {4}}|
        +--------------+

        However, if you are going to add/replace multiple nested fields,
        it is preferred to extract out the nested struct before
        adding/replacing multiple fields e.g.

        >>> df.select(col("a").withField(
        ...     "e", col("a.e").dropFields("g", "h")).alias("a")
        ... ).show()
        +--------------+
        |             a|
        +--------------+
        |{1, 2, 3, {4}}|
        +--------------+

        """
        ...

    @dispatch_col_method
    def __getattr__(self, item: Any) -> "Column":
        """
        An expression that gets an item at position ``ordinal`` out of a list,
        or gets an item by key out of a dict.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        item
            a literal value.

        Returns
        -------
        :class:`Column`
            Column representing the item got by key out of a dict.

        Examples
        --------
        >>> df = spark.createDataFrame([('abcedfg', {"key": "value"})], ["l", "d"])
        >>> df.select(df.d.key).show()
        +------+
        |d[key]|
        +------+
        | value|
        +------+
        """
        ...

    @dispatch_col_method
    def __getitem__(self, k: Any) -> "Column":
        """
        An expression that gets an item at position ``ordinal`` out of a list,
        or gets an item by key out of a dict.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        k
            a literal value, or a slice object without step.

        Returns
        -------
        :class:`Column`
            Column representing the item got by key out of a dict, or substrings sliced by
            the given slice object.

        Examples
        --------
        >>> df = spark.createDataFrame([('abcedfg', {"key": "value"})], ["l", "d"])
        >>> df.select(df.l[slice(1, 3)], df.d['key']).show()
        +---------------+------+
        |substr(l, 1, 3)|d[key]|
        +---------------+------+
        |            abc| value|
        +---------------+------+
        """
        ...

    @dispatch_col_method
    def __iter__(self) -> None:
        ...

    # string methods
    @dispatch_col_method
    def contains(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        """
        Contains the other element. Returns a boolean :class:`Column` based on a string match.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other
            string in line. A value as a literal or a :class:`Column`.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.filter(df.name.contains('o')).collect()
        [Row(age=5, name='Bob')]
        """
        ...

    @dispatch_col_method
    def startswith(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        """
        String starts with. Returns a boolean :class:`Column` based on a string match.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`Column` or str
            string at start of line (do not use a regex `^`)

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.filter(df.name.startswith('Al')).collect()
        [Row(age=2, name='Alice')]
        >>> df.filter(df.name.startswith('^Al')).collect()
        []
        """
        ...

    @dispatch_col_method
    def endswith(
        self, other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> "Column":
        """
        String ends with. Returns a boolean :class:`Column` based on a string match.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`Column` or str
            string at end of line (do not use a regex `$`)

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.filter(df.name.endswith('ice')).collect()
        [Row(age=2, name='Alice')]
        >>> df.filter(df.name.endswith('ice$')).collect()
        []
        """
        ...

    @dispatch_col_method
    def like(self: "Column", other: str) -> "Column":
        """
        SQL like expression. Returns a boolean :class:`Column` based on a SQL LIKE match.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : str
            a SQL LIKE pattern

        See Also
        --------
        pyspark.sql.Column.rlike

        Returns
        -------
        :class:`Column`
            Column of booleans showing whether each element
            in the Column is matched by SQL LIKE pattern.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.filter(df.name.like('Al%')).collect()
        [Row(age=2, name='Alice')]
        """
        ...

    @dispatch_col_method
    def rlike(self: "Column", other: str) -> "Column":
        """
        SQL RLIKE expression (LIKE with Regex). Returns a boolean :class:`Column` based on a regex
        match.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : str
            an extended regex expression

        Returns
        -------
        :class:`Column`
            Column of booleans showing whether each element
            in the Column is matched by extended regex expression.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.filter(df.name.rlike('ice$')).collect()
        [Row(age=2, name='Alice')]
        """
        ...

    @dispatch_col_method
    def ilike(self: "Column", other: str) -> "Column":
        """
        SQL ILIKE expression (case insensitive LIKE). Returns a boolean :class:`Column`
        based on a case insensitive match.

        .. versionadded:: 3.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : str
            a SQL LIKE pattern

        See Also
        --------
        pyspark.sql.Column.rlike

        Returns
        -------
        :class:`Column`
            Column of booleans showing whether each element
            in the Column is matched by SQL LIKE pattern.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.filter(df.name.ilike('%Ice')).collect()
        [Row(age=2, name='Alice')]
        """
        ...

    @overload
    def substr(self, startPos: int, length: int) -> "Column":
        ...

    @overload
    def substr(self, startPos: "Column", length: "Column") -> "Column":
        ...

    @dispatch_col_method
    def substr(self, startPos: Union[int, "Column"], length: Union[int, "Column"]) -> "Column":
        """
        Return a :class:`Column` which is a substring of the column.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        startPos : :class:`Column` or int
            start position
        length : :class:`Column` or int
            length of the substring

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is substr of origin Column.

        Examples
        --------

        Example 1. Using integers for the input arguments.

        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.select(df.name.substr(1, 3).alias("col")).collect()
        [Row(col='Ali'), Row(col='Bob')]

        Example 2. Using columns for the input arguments.

        >>> df = spark.createDataFrame(
        ...      [(3, 4, "Alice"), (2, 3, "Bob")], ["sidx", "eidx", "name"])
        >>> df.select(df.name.substr(df.sidx, df.eidx).alias("col")).collect()
        [Row(col='ice'), Row(col='ob')]
        """
        ...

    @dispatch_col_method
    def isin(self, *cols: Any) -> "Column":
        """
        A boolean expression that is evaluated to true if the value of this
        expression is contained by the evaluated values of the arguments.

        .. versionadded:: 1.5.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : Any
            The values to compare with the column values. The result will only be true at a location
            if any value matches in the Column.

        Returns
        -------
        :class:`Column`
            Column of booleans showing whether each element in the Column is contained in cols.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob"), (8, "Mike")], ["age", "name"])

        Example 1: Filter rows with names in the specified values

        >>> df[df.name.isin("Bob", "Mike")].show()
        +---+----+
        |age|name|
        +---+----+
        |  5| Bob|
        |  8|Mike|
        +---+----+

        Example 2: Filter rows with ages in the specified list

        >>> df[df.age.isin([1, 2, 3])].show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        +---+-----+

        Example 3: Filter rows with names not in the specified values

        >>> df[~df.name.isin("Alice", "Bob")].show()
        +---+----+
        |age|name|
        +---+----+
        |  8|Mike|
        +---+----+
        """
        ...

    # order
    @dispatch_col_method
    def asc(self) -> "Column":
        """
        Returns a sort expression based on the ascending order of the column.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([('Tom', 80), ('Alice', None)], ["name", "height"])
        >>> df.select(df.name).orderBy(df.name.asc()).collect()
        [Row(name='Alice'), Row(name='Tom')]
        """
        ...

    @dispatch_col_method
    def asc_nulls_first(self) -> "Column":
        """
        Returns a sort expression based on ascending order of the column, and null values
        return before non-null values.

        .. versionadded:: 2.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame(
        ...     [('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
        >>> df.select(df.name).orderBy(df.name.asc_nulls_first()).collect()
        [Row(name=None), Row(name='Alice'), Row(name='Tom')]

        """
        ...

    @dispatch_col_method
    def asc_nulls_last(self) -> "Column":
        """
        Returns a sort expression based on ascending order of the column, and null values
        appear after non-null values.

        .. versionadded:: 2.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame(
        ...     [('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
        >>> df.select(df.name).orderBy(df.name.asc_nulls_last()).collect()
        [Row(name='Alice'), Row(name='Tom'), Row(name=None)]

        """
        ...

    @dispatch_col_method
    def desc(self) -> "Column":
        """
        Returns a sort expression based on the descending order of the column.

        .. versionadded:: 2.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([('Tom', 80), ('Alice', None)], ["name", "height"])
        >>> df.select(df.name).orderBy(df.name.desc()).collect()
        [Row(name='Tom'), Row(name='Alice')]
        """
        ...

    @dispatch_col_method
    def desc_nulls_first(self) -> "Column":
        """
        Returns a sort expression based on the descending order of the column, and null values
        appear before non-null values.

        .. versionadded:: 2.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame(
        ...     [('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
        >>> df.select(df.name).orderBy(df.name.desc_nulls_first()).collect()
        [Row(name=None), Row(name='Tom'), Row(name='Alice')]

        """
        ...

    @dispatch_col_method
    def desc_nulls_last(self) -> "Column":
        """
        Returns a sort expression based on the descending order of the column, and null values
        appear after non-null values.

        .. versionadded:: 2.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame(
        ...     [('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
        >>> df.select(df.name).orderBy(df.name.desc_nulls_last()).collect()
        [Row(name='Tom'), Row(name='Alice'), Row(name=None)]
        """
        ...

    @dispatch_col_method
    def isNull(self) -> "Column":
        """
        True if the current expression is null.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([Row(name='Tom', height=80), Row(name='Alice', height=None)])
        >>> df.filter(df.height.isNull()).collect()
        [Row(name='Alice', height=None)]
        """
        ...

    @dispatch_col_method
    def isNotNull(self) -> "Column":
        """
        True if the current expression is NOT null.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([Row(name='Tom', height=80), Row(name='Alice', height=None)])
        >>> df.filter(df.height.isNotNull()).collect()
        [Row(name='Tom', height=80)]
        """
        ...

    @dispatch_col_method
    def isNaN(self) -> "Column":
        """
        True if the current expression is NaN.

        .. versionadded:: 4.0.0

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame(
        ...     [Row(name='Tom', height=80.0), Row(name='Alice', height=float('nan'))])
        >>> df.filter(df.height.isNaN()).collect()
        [Row(name='Alice', height=nan)]
        """
        ...

    @dispatch_col_method
    def alias(self, *alias: str, **kwargs: Any) -> "Column":
        """
        Returns this column aliased with a new name or names (in the case of expressions that
        return more than one column, such as explode).

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        alias : str
            desired column names (collects all positional arguments passed)

        Other Parameters
        ----------------
        metadata: dict
            a dict of information to be stored in ``metadata`` attribute of the
            corresponding :class:`StructField <pyspark.sql.types.StructField>` (optional, keyword
            only argument)

            .. versionchanged:: 2.2.0
               Added optional ``metadata`` argument.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is aliased with new name or names.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.select(df.age.alias("age2")).collect()
        [Row(age2=2), Row(age2=5)]
        >>> df.select(df.age.alias("age3", metadata={'max': 99})).schema['age3'].metadata['max']
        99
        """
        ...

    @dispatch_col_method
    def name(self, *alias: str, **kwargs: Any) -> "Column":
        """
        :func:`name` is an alias for :func:`alias`.

        .. versionadded:: 2.0.0
        """
        ...

    @dispatch_col_method
    def cast(self, dataType: Union[DataType, str]) -> "Column":
        """
        Casts the column into type ``dataType``.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        dataType : :class:`DataType` or str
            a DataType or Python string literal with a DDL-formatted string
            to use when parsing the column to the same type.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is cast into new type.

        Examples
        --------
        >>> from pyspark.sql.types import StringType
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.select(df.age.cast("string").alias('ages')).collect()
        [Row(ages='2'), Row(ages='5')]
        >>> df.select(df.age.cast(StringType()).alias('ages')).collect()
        [Row(ages='2'), Row(ages='5')]
        """
        ...

    @dispatch_col_method
    def try_cast(self, dataType: Union[DataType, str]) -> "Column":
        """
        This is a special version of `cast` that performs the same operation, but returns a NULL
        value instead of raising an error if the invoke method throws exception.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        dataType : :class:`DataType` or str
            a DataType or Python string literal with a DDL-formatted string
            to use when parsing the column to the same type.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is cast into new type.

        Examples
        --------
        Example 1: Cast with a Datatype

        >>> from pyspark.sql.types import LongType
        >>> df = spark.createDataFrame(
        ...      [(2, "123"), (5, "Bob"), (3, None)], ["age", "name"])
        >>> df.select(df.name.try_cast(LongType())).show()
        +----+
        |name|
        +----+
        | 123|
        |NULL|
        |NULL|
        +----+

        Example 2: Cast with a DDL string

        >>> df = spark.createDataFrame(
        ...      [(2, "123"), (5, "Bob"), (3, None)], ["age", "name"])
        >>> df.select(df.name.try_cast("double")).show()
        +-----+
        | name|
        +-----+
        |123.0|
        | NULL|
        | NULL|
        +-----+
        """
        ...

    @dispatch_col_method
    def astype(self, dataType: Union[DataType, str]) -> "Column":
        """
        :func:`astype` is an alias for :func:`cast`.

        .. versionadded:: 1.4.0
        """
        ...

    @dispatch_col_method
    def between(
        self,
        lowerBound: Union["Column", "LiteralType", "DateTimeLiteral", "DecimalLiteral"],
        upperBound: Union["Column", "LiteralType", "DateTimeLiteral", "DecimalLiteral"],
    ) -> "Column":
        """
        Check if the current column's values are between the specified lower and upper
        bounds, inclusive.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        lowerBound : :class:`Column`, int, float, string, bool, datetime, date or Decimal
            The lower boundary value, inclusive.
        upperBound : :class:`Column`, int, float, string, bool, datetime, date or Decimal
            The upper boundary value, inclusive.

        Returns
        -------
        :class:`Column`
            A new column of boolean values indicating whether each element in the original
            column is within the specified range (inclusive).

        Examples
        --------
        Using between with integer values.

        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.select(df.name, df.age.between(2, 4)).show()
        +-----+---------------------------+
        | name|((age >= 2) AND (age <= 4))|
        +-----+---------------------------+
        |Alice|                       true|
        |  Bob|                      false|
        +-----+---------------------------+

        Using between with string values.

        >>> df = spark.createDataFrame([("Alice", "A"), ("Bob", "B")], ["name", "initial"])
        >>> df.select(df.name, df.initial.between("A", "B")).show()
        +-----+-----------------------------------+
        | name|((initial >= A) AND (initial <= B))|
        +-----+-----------------------------------+
        |Alice|                               true|
        |  Bob|                               true|
        +-----+-----------------------------------+

        Using between with float values.

        >>> df = spark.createDataFrame(
        ...     [(2.5, "Alice"), (5.5, "Bob")], ["height", "name"])
        >>> df.select(df.name, df.height.between(2.0, 5.0)).show()
        +-----+-------------------------------------+
        | name|((height >= 2.0) AND (height <= 5.0))|
        +-----+-------------------------------------+
        |Alice|                                 true|
        |  Bob|                                false|
        +-----+-------------------------------------+

        Using between with date values.

        >>> import pyspark.sql.functions as sf
        >>> df = spark.createDataFrame(
        ...     [("Alice", "2023-01-01"), ("Bob", "2023-02-01")], ["name", "date"])
        >>> df = df.withColumn("date", sf.to_date(df.date))
        >>> df.select(df.name, df.date.between("2023-01-01", "2023-01-15")).show()
        +-----+-----------------------------------------------+
        | name|((date >= 2023-01-01) AND (date <= 2023-01-15))|
        +-----+-----------------------------------------------+
        |Alice|                                           true|
        |  Bob|                                          false|
        +-----+-----------------------------------------------+
        >>> from datetime import date
        >>> df.select(df.name, df.date.between(date(2023, 1, 1), date(2023, 1, 15))).show()
        +-----+-------------------------------------------------------------+
        | name|((date >= DATE '2023-01-01') AND (date <= DATE '2023-01-15'))|
        +-----+-------------------------------------------------------------+
        |Alice|                                                         true|
        |  Bob|                                                        false|
        +-----+-------------------------------------------------------------+

        Using between with timestamp values.

        >>> import pyspark.sql.functions as sf
        >>> df = spark.createDataFrame(
        ...     [("Alice", "2023-01-01 10:00:00"), ("Bob", "2023-02-01 10:00:00")],
        ...     schema=["name", "timestamp"])
        >>> df = df.withColumn("timestamp", sf.to_timestamp(df.timestamp))
        >>> df.select(df.name, df.timestamp.between("2023-01-01", "2023-02-01")).show()
        +-----+---------------------------------------------------------+
        | name|((timestamp >= 2023-01-01) AND (timestamp <= 2023-02-01))|
        +-----+---------------------------------------------------------+
        |Alice|                                                     true|
        |  Bob|                                                    false|
        +-----+---------------------------------------------------------+
        >>> df.select(df.name, df.timestamp.between("2023-01-01", "2023-02-01 12:00:00")).show()
        +-----+------------------------------------------------------------------+
        | name|((timestamp >= 2023-01-01) AND (timestamp <= 2023-02-01 12:00:00))|
        +-----+------------------------------------------------------------------+
        |Alice|                                                              true|
        |  Bob|                                                              true|
        +-----+------------------------------------------------------------------+
        """
        ...

    @dispatch_col_method
    def when(self, condition: "Column", value: Any) -> "Column":
        """
        Evaluates a list of conditions and returns one of multiple possible result expressions.
        If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        condition : :class:`Column`
            a boolean :class:`Column` expression.
        value
            a literal value, or a :class:`Column` expression.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is in conditions.

        Examples
        --------
        Example 1: Using :func:`when` with conditions and values to create a new Column

        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> result = df.select(df.name, sf.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0))
        >>> result.show()
        +-----+------------------------------------------------------------+
        | name|CASE WHEN (age > 4) THEN 1 WHEN (age < 3) THEN -1 ELSE 0 END|
        +-----+------------------------------------------------------------+
        |Alice|                                                          -1|
        |  Bob|                                                           1|
        +-----+------------------------------------------------------------+

        Example 2: Chaining multiple :func:`when` conditions

        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame([(1, "Alice"), (4, "Bob"), (6, "Charlie")], ["age", "name"])
        >>> result = df.select(
        ...     df.name,
        ...     sf.when(df.age < 3, "Young").when(df.age < 5, "Middle-aged").otherwise("Old")
        ... )
        >>> result.show()
        +-------+---------------------------------------------------------------------------+
        |   name|CASE WHEN (age < 3) THEN Young WHEN (age < 5) THEN Middle-aged ELSE Old END|
        +-------+---------------------------------------------------------------------------+
        |  Alice|                                                                      Young|
        |    Bob|                                                                Middle-aged|
        |Charlie|                                                                        Old|
        +-------+---------------------------------------------------------------------------+

        Example 3: Using literal values as conditions

        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> result = df.select(
        ...     df.name, sf.when(sf.lit(True), 1).otherwise(
        ...         sf.raise_error("unreachable")).alias("when"))
        >>> result.show()
        +-----+----+
        | name|when|
        +-----+----+
        |Alice|   1|
        |  Bob|   1|
        +-----+----+

        See Also
        --------
        pyspark.sql.functions.when
        """
        ...

    @dispatch_col_method
    def otherwise(self, value: Any) -> "Column":
        """
        Evaluates a list of conditions and returns one of multiple possible result expressions.
        If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        value
            a literal value, or a :class:`Column` expression.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is unmatched conditions.

        Examples
        --------
        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.select(df.name, sf.when(df.age > 3, 1).otherwise(0)).show()
        +-----+-------------------------------------+
        | name|CASE WHEN (age > 3) THEN 1 ELSE 0 END|
        +-----+-------------------------------------+
        |Alice|                                    0|
        |  Bob|                                    1|
        +-----+-------------------------------------+

        See Also
        --------
        pyspark.sql.functions.when
        """
        ...

    @dispatch_col_method
    def over(self, window: "WindowSpec") -> "Column":
        """
        Define a windowing column.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        window : :class:`WindowSpec`

        Returns
        -------
        :class:`Column`

        Examples
        --------
        >>> from pyspark.sql import Window
        >>> window = (
        ...     Window.partitionBy("name")
        ...     .orderBy("age")
        ...     .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ... )
        >>> from pyspark.sql.functions import rank, min, desc
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.withColumn(
        ...      "rank", rank().over(window)
        ... ).withColumn(
        ...      "min", min('age').over(window)
        ... ).sort(desc("age")).show()
        +---+-----+----+---+
        |age| name|rank|min|
        +---+-----+----+---+
        |  5|  Bob|   1|  5|
        |  2|Alice|   1|  2|
        +---+-----+----+---+
        """
        ...

    @dispatch_col_method
    def outer(self) -> "Column":
        """
        Mark this column as an outer column if its expression refers to columns from an outer query.

        This is used to trigger lazy analysis of Spark Classic DataFrame, so that we can use it
        to build subquery expressions. Spark Connect DataFrame is always lazily analyzed and
        does not need to use this function.

        .. versionadded:: 4.0.0

        See Also
        --------
        pyspark.sql.dataframe.DataFrame.scalar
        pyspark.sql.dataframe.DataFrame.exists
        """
        ...

    @dispatch_col_method
    def __nonzero__(self) -> None:
        ...

    @dispatch_col_method
    def __bool__(self) -> None:
        ...

    @dispatch_col_method
    def __repr__(self) -> str:
        ...


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.column

    globs = pyspark.sql.column.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("sql.column tests").getOrCreate()
    globs["spark"] = spark

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.column,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
