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

from typing import get_args, TYPE_CHECKING, Callable, Any, Union, overload, cast, Sequence

import json
import decimal
import datetime

from pyspark.sql.types import (
    DateType,
    NullType,
    BooleanType,
    BinaryType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    DataType,
    TimestampType,
    TimestampNTZType,
    DayTimeIntervalType,
)

import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.types import pyspark_types_to_proto_types

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName, PrimitiveType
    from pyspark.sql.connect.client import SparkConnectClient
    import pyspark.sql.connect.proto as proto

JVM_BYTE_MIN = -(1 << 7)
JVM_BYTE_MAX = (1 << 7) - 1
JVM_SHORT_MIN = -(1 << 15)
JVM_SHORT_MAX = (1 << 15) - 1
JVM_INT_MIN = -(1 << 31)
JVM_INT_MAX = (1 << 31) - 1
JVM_LONG_MIN = -(1 << 63)
JVM_LONG_MAX = (1 << 63) - 1


def _func_op(name: str, doc: str = "") -> Callable[["Column"], "Column"]:
    def _(self: "Column") -> "Column":
        return scalar_function(name, self)

    return _


def _bin_op(
    name: str, doc: str = "binary function", reverse: bool = False
) -> Callable[["Column", Any], "Column"]:
    def _(self: "Column", other: Any) -> "Column":
        from pyspark.sql.connect._typing import PrimitiveType
        from pyspark.sql.connect.functions import lit

        if isinstance(other, get_args(PrimitiveType)):
            other = lit(other)
        if not reverse:
            return scalar_function(name, self, other)
        else:
            return scalar_function(name, other, self)

    return _


def _unary_op(name: str, doc: str = "unary function") -> Callable[["Column"], "Column"]:
    def _(self: "Column") -> "Column":
        return scalar_function(name, self)

    return _


def scalar_function(op: str, *args: "Column") -> "Column":
    return Column(UnresolvedFunction(op, [arg._expr for arg in args]))


def sql_expression(expr: str) -> "Column":
    return Column(SQLExpression(expr))


class Expression:
    """
    Expression base class.
    """

    def __init__(self) -> None:
        pass

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        ...

    def __repr__(self) -> str:
        ...

    def alias(self, *alias: str, **kwargs: Any) -> "ColumnAlias":
        """
        Returns this column aliased with a new name or names (in the case of expressions that
        return more than one column, such as explode).

        .. versionadded:: 3.4.0

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
        metadata = kwargs.pop("metadata", None)
        assert not kwargs, "Unexpected kwargs where passed: %s" % kwargs
        return ColumnAlias(self, list(alias), metadata)

    def name(self) -> str:
        ...


class ColumnAlias(Expression):
    def __init__(self, parent: Expression, alias: list[str], metadata: Any):

        self._alias = alias
        self._metadata = metadata
        self._parent = parent

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        if len(self._alias) == 1:
            exp = proto.Expression()
            exp.alias.name.append(self._alias[0])
            exp.alias.expr.CopyFrom(self._parent.to_plan(session))

            if self._metadata:
                exp.alias.metadata = json.dumps(self._metadata)
            return exp
        else:
            if self._metadata:
                raise ValueError("metadata can only be provided for a single column")
            exp = proto.Expression()
            exp.alias.name.extend(self._alias)
            exp.alias.expr.CopyFrom(self._parent.to_plan(session))
            return exp

    def __repr__(self) -> str:
        return f"Alias({self._parent}, ({','.join(self._alias)}))"


class LiteralExpression(Expression):
    """A literal expression.

    The Python types are converted best effort into the relevant proto types. On the Spark Connect
    server side, the proto types are converted to the Catalyst equivalents."""

    def __init__(self, value: Any, dataType: DataType) -> None:
        super().__init__()

        assert isinstance(
            dataType,
            (
                NullType,
                BinaryType,
                BooleanType,
                ByteType,
                ShortType,
                IntegerType,
                LongType,
                FloatType,
                DoubleType,
                DecimalType,
                StringType,
                DateType,
                TimestampType,
                TimestampNTZType,
                DayTimeIntervalType,
            ),
        )

        if isinstance(dataType, NullType):
            assert value is None

        if value is not None:
            if isinstance(dataType, BinaryType):
                assert isinstance(value, (bytes, bytearray))
            elif isinstance(dataType, BooleanType):
                assert isinstance(value, bool)
            elif isinstance(dataType, ByteType):
                assert isinstance(value, int) and JVM_BYTE_MIN <= int(value) <= JVM_BYTE_MAX
            elif isinstance(dataType, ShortType):
                assert isinstance(value, int) and JVM_SHORT_MIN <= int(value) <= JVM_SHORT_MAX
            elif isinstance(dataType, IntegerType):
                assert isinstance(value, int) and JVM_INT_MIN <= int(value) <= JVM_INT_MAX
            elif isinstance(dataType, LongType):
                assert isinstance(value, int) and JVM_LONG_MIN <= int(value) <= JVM_LONG_MAX
            elif isinstance(dataType, FloatType):
                assert isinstance(value, float)
            elif isinstance(dataType, DoubleType):
                assert isinstance(value, float)
            elif isinstance(dataType, DecimalType):
                assert isinstance(value, decimal.Decimal)
            elif isinstance(dataType, StringType):
                assert isinstance(value, str)
            elif isinstance(dataType, DateType):
                assert isinstance(value, (datetime.date, datetime.datetime))
                if isinstance(value, datetime.date):
                    value = DateType().toInternal(value)
                else:
                    value = DateType().toInternal(value.date())
            elif isinstance(dataType, TimestampType):
                assert isinstance(value, datetime.datetime)
                value = TimestampType().toInternal(value)
            elif isinstance(dataType, TimestampNTZType):
                assert isinstance(value, datetime.datetime)
                value = TimestampNTZType().toInternal(value)
            elif isinstance(dataType, DayTimeIntervalType):
                assert isinstance(value, datetime.timedelta)
                value = DayTimeIntervalType().toInternal(value)
                assert value is not None
            else:
                raise ValueError(f"Unsupported Data Type {dataType}")

        self._value = value
        self._dataType = dataType

    @classmethod
    def _infer_type(cls, value: Any) -> DataType:
        if value is None:
            return NullType()
        elif isinstance(value, (bytes, bytearray)):
            return BinaryType()
        elif isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            if JVM_INT_MIN <= value <= JVM_INT_MAX:
                return IntegerType()
            elif JVM_LONG_MIN <= value <= JVM_LONG_MAX:
                return LongType()
            else:
                raise ValueError(f"integer {value} out of bounds")
        elif isinstance(value, float):
            return DoubleType()
        elif isinstance(value, str):
            return StringType()
        elif isinstance(value, decimal.Decimal):
            return DecimalType()
        elif isinstance(value, datetime.datetime):
            return TimestampType()
        elif isinstance(value, datetime.date):
            return DateType()
        elif isinstance(value, datetime.timedelta):
            return DayTimeIntervalType()
        else:
            raise ValueError(f"Unsupported Data Type {type(value).__name__}")

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        """Converts the literal expression to the literal in proto."""

        expr = proto.Expression()

        if isinstance(self._dataType, NullType):
            expr.literal.null = True
        elif self._value is None:
            expr.typed_null.CopyFrom(pyspark_types_to_proto_types(self._dataType))
        elif isinstance(self._dataType, BinaryType):
            expr.literal.binary = bytes(self._value)
        elif isinstance(self._dataType, BooleanType):
            expr.literal.boolean = bool(self._value)
        elif isinstance(self._dataType, ByteType):
            expr.literal.byte = int(self._value)
        elif isinstance(self._dataType, ShortType):
            expr.literal.short = int(self._value)
        elif isinstance(self._dataType, IntegerType):
            expr.literal.integer = int(self._value)
        elif isinstance(self._dataType, LongType):
            expr.literal.long = int(self._value)
        elif isinstance(self._dataType, FloatType):
            expr.literal.float = float(self._value)
        elif isinstance(self._dataType, DoubleType):
            expr.literal.double = float(self._value)
        elif isinstance(self._dataType, DecimalType):
            expr.literal.decimal.value = str(self._value)
            expr.literal.decimal.precision = self._dataType.precision
            expr.literal.decimal.scale = self._dataType.scale
        elif isinstance(self._dataType, StringType):
            expr.literal.string = str(self._value)
        elif isinstance(self._dataType, DateType):
            expr.literal.date = int(self._value)
        elif isinstance(self._dataType, TimestampType):
            expr.literal.timestamp = int(self._value)
        elif isinstance(self._dataType, TimestampNTZType):
            expr.literal.timestamp_ntz = int(self._value)
        elif isinstance(self._dataType, DayTimeIntervalType):
            expr.literal.day_time_interval = int(self._value)
        else:
            raise ValueError(f"Unsupported Data Type {self._dataType}")

        return expr

    def __repr__(self) -> str:
        return f"Literal({self._value})"


class ColumnReference(Expression):
    """Represents a column reference. There is no guarantee that this column
    actually exists. In the context of this project, we refer by its name and
    treat it as an unresolved attribute. Attributes that have the same fully
    qualified name are identical"""

    @classmethod
    def from_qualified_name(cls, name: str) -> "ColumnReference":
        return ColumnReference(name)

    def __init__(self, name: "ColumnOrName") -> None:
        super().__init__()
        if isinstance(name, str):
            self._unparsed_identifier = name
        else:
            self._unparsed_identifier = name.name()

    def name(self) -> str:
        """Returns the qualified name of the column reference."""
        return self._unparsed_identifier

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        """Returns the Proto representation of the expression."""
        expr = proto.Expression()
        expr.unresolved_attribute.unparsed_identifier = self._unparsed_identifier
        return expr

    def desc(self) -> "SortOrder":
        return SortOrder(self, ascending=False, nullsLast=True)

    def asc(self) -> "SortOrder":
        return SortOrder(self, ascending=True, nullsLast=False)

    def __repr__(self) -> str:
        return f"ColumnReference({self._unparsed_identifier})"


class SQLExpression(Expression):
    """Returns Expression which contains a string which is a SQL expression
    and server side will parse it by Catalyst
    """

    def __init__(self, expr: str) -> None:
        super().__init__()
        self._expr: str = expr

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        """Returns the Proto representation of the SQL expression."""
        expr = proto.Expression()
        expr.expression_string.expression = self._expr
        return expr


class SortOrder(Expression):
    def __init__(self, child: Expression, ascending: bool = True, nullsLast: bool = False) -> None:
        super().__init__()
        self._child = child
        self._ascending = ascending
        self._nullsLast = nullsLast

    def __repr__(self) -> str:
        return (
            str(self._child)
            + (" ASC" if self._ascending else " DESC")
            + (" NULLS LAST" if self._nullsLast else " NULLS FIRST")
        )

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        # TODO(SPARK-41334): move SortField from relations.proto to expressions.proto
        sort = proto.Sort.SortField()
        sort.expression.CopyFrom(self._child.to_plan(session))

        if self._ascending:
            sort.direction = proto.Sort.SortDirection.SORT_DIRECTION_ASCENDING
        else:
            sort.direction = proto.Sort.SortDirection.SORT_DIRECTION_DESCENDING

        if self._nullsLast:
            sort.nulls = proto.Sort.SortNulls.SORT_NULLS_LAST
        else:
            sort.nulls = proto.Sort.SortNulls.SORT_NULLS_FIRST

        return cast(proto.Expression, sort)


class UnresolvedFunction(Expression):
    def __init__(
        self,
        name: str,
        args: Sequence["Expression"],
        is_distinct: bool = False,
    ) -> None:
        super().__init__()

        assert isinstance(name, str)
        self._name = name

        assert isinstance(args, list) and all(isinstance(arg, Expression) for arg in args)
        self._args = args

        assert isinstance(is_distinct, bool)
        self._is_distinct = is_distinct

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        fun = proto.Expression()
        fun.unresolved_function.function_name = self._name
        if len(self._args) > 0:
            fun.unresolved_function.arguments.extend([arg.to_plan(session) for arg in self._args])
        fun.unresolved_function.is_distinct = self._is_distinct
        return fun

    def __repr__(self) -> str:
        if self._is_distinct:
            return f"{self._name}(distinct {', '.join([str(arg) for arg in self._args])})"
        else:
            return f"{self._name}({', '.join([str(arg) for arg in self._args])})"


class CastExpression(Expression):
    def __init__(
        self,
        col: "Column",
        data_type: Union[DataType, str],
    ) -> None:
        super().__init__()
        self._col = col
        self._data_type = data_type

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        fun = proto.Expression()
        fun.cast.expr.CopyFrom(self._col.to_plan(session))
        if isinstance(self._data_type, str):
            fun.cast.type_str = self._data_type
        else:
            fun.cast.type.CopyFrom(pyspark_types_to_proto_types(self._data_type))
        return fun

    def __repr__(self) -> str:
        return f"({self._col} ({self._data_type}))"


class Column:
    """
    A column in a DataFrame. Column can refer to different things based on the
    wrapped expression. Some common examples include attribute references, functions,
    literals, etc.

    .. versionadded:: 3.4.0
    """

    def __init__(self, expr: Expression) -> None:
        if not isinstance(expr, Expression):
            raise TypeError(
                f"Cannot construct column expected Expression, got {expr} ({type(expr)})"
            )
        self._expr = expr

    __gt__ = _bin_op(">")
    __lt__ = _bin_op("<")
    __add__ = _bin_op("+")
    __sub__ = _bin_op("-")
    __mul__ = _bin_op("*")
    __div__ = _bin_op("/")
    __truediv__ = _bin_op("/")
    __mod__ = _bin_op("%")
    __radd__ = _bin_op("+", reverse=True)
    __rsub__ = _bin_op("-", reverse=True)
    __rmul__ = _bin_op("*", reverse=True)
    __rdiv__ = _bin_op("/", reverse=True)
    __rtruediv__ = _bin_op("/", reverse=True)
    __pow__ = _bin_op("pow")
    __rpow__ = _bin_op("pow", reverse=True)
    __ge__ = _bin_op(">=")
    __le__ = _bin_op("<=")

    _eqNullSafe_doc = """
        Equality test that is safe for null values.

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
        Notes
        -----
        Unlike Pandas, PySpark doesn't consider NaN values to be NULL. See the
        `NaN Semantics <https://spark.apache.org/docs/latest/sql-ref-datatypes.html#nan-semantics>`_
        for details.
        """
    eqNullSafe = _bin_op("eqNullSafe", _eqNullSafe_doc)

    __neg__ = _func_op("negate")

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    __and__ = _bin_op("and")
    __or__ = _bin_op("or")
    __invert__ = _func_op("not")
    __rand__ = _bin_op("and")
    __ror__ = _bin_op("or")

    # bitwise operators
    _bitwiseOR_doc = """
        Compute bitwise OR of this expression with another expression.

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
    _bitwiseAND_doc = """
        Compute bitwise AND of this expression with another expression.

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
    _bitwiseXOR_doc = """
        Compute bitwise XOR of this expression with another expression.

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

    bitwiseOR = _bin_op("bitwiseOR", _bitwiseOR_doc)
    bitwiseAND = _bin_op("bitwiseAND", _bitwiseAND_doc)
    bitwiseXOR = _bin_op("bitwiseXOR", _bitwiseXOR_doc)

    _isNull_doc = """
    True if the current expression is null.
    """
    _isNotNull_doc = """
    True if the current expression is NOT null.
    """

    isNull = _unary_op("isNull", _isNull_doc)
    isNotNull = _unary_op("isNotNull", _isNotNull_doc)

    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> "Column":
        """binary function"""
        return _func_op("not")(_bin_op("==")(self, other))

    # string methods
    def contains(self, other: Union["PrimitiveType", "Column"]) -> "Column":
        """
        Contains the other element. Returns a boolean :class:`Column` based on a string match.

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
        return _bin_op("contains")(self, other)

    _startswith_doc = """
    String starts with. Returns a boolean :class:`Column` based on a string match.

    Parameters
    ----------
    other : :class:`Column` or str
        string at start of line (do not use a regex `^`)
    """
    _endswith_doc = """
    String ends with. Returns a boolean :class:`Column` based on a string match.

    Parameters
    ----------
    other : :class:`Column` or str
        string at end of line (do not use a regex `$`)
    """
    startswith = _bin_op("startsWith", _startswith_doc)
    endswith = _bin_op("endsWith", _endswith_doc)

    def like(self: "Column", other: str) -> "Column":
        """
        SQL like expression. Returns a boolean :class:`Column` based on a SQL LIKE match.

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
        return _bin_op("like")(self, other)

    def rlike(self: "Column", other: str) -> "Column":
        """
        SQL RLIKE expression (LIKE with Regex). Returns a boolean :class:`Column` based on a regex
        match.

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
        return _bin_op("like")(self, other)

    def ilike(self: "Column", other: str) -> "Column":
        """
        SQL ILIKE expression (case insensitive LIKE). Returns a boolean :class:`Column`
        based on a case insensitive match.

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
        return _bin_op("ilike")(self, other)

    @overload
    def substr(self, startPos: int, length: int) -> "Column":
        ...

    @overload
    def substr(self, startPos: "Column", length: "Column") -> "Column":
        ...

    def substr(self, startPos: Union[int, "Column"], length: Union[int, "Column"]) -> "Column":
        """
        Return a :class:`Column` which is a substring of the column.

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
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.select(df.name.substr(1, 3).alias("col")).collect()
        [Row(col='Ali'), Row(col='Bob')]
        """
        from pyspark.sql.connect.function_builder import functions as F
        from pyspark.sql.connect.functions import lit

        if type(startPos) != type(length):
            raise TypeError(
                "startPos and length must be the same type. "
                "Got {startPos_t} and {length_t}, respectively.".format(
                    startPos_t=type(startPos),
                    length_t=type(length),
                )
            )

        if isinstance(length, int):
            length_exp = lit(length)
        elif isinstance(length, Column):
            length_exp = length
        else:
            raise TypeError("Unsupported type for substr().")

        if isinstance(startPos, int):
            start_exp = lit(startPos)
        else:
            start_exp = startPos

        return F.substr(self, start_exp, length_exp)

    def __eq__(self, other: Any) -> "Column":  # type: ignore[override]
        """Returns a binary expression with the current column as the left
        side and the other expression as the right side.
        """
        from pyspark.sql.connect._typing import PrimitiveType
        from pyspark.sql.connect.functions import lit

        if isinstance(other, get_args(PrimitiveType)):
            other = lit(other)
        return scalar_function("==", self, other)

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        return self._expr.to_plan(session)

    def alias(self, *alias: str, **kwargs: Any) -> "Column":
        return Column(self._expr.alias(*alias, **kwargs))

    def asc(self) -> "Column":
        return self.asc_nulls_first()

    def asc_nulls_first(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=True, nullsLast=False))

    def asc_nulls_last(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=True, nullsLast=True))

    def desc(self) -> "Column":
        return self.desc_nulls_last()

    def desc_nulls_first(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=False, nullsLast=False))

    def desc_nulls_last(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=False, nullsLast=True))

    def name(self) -> str:
        return self._expr.name()

    def cast(self, dataType: Union[DataType, str]) -> "Column":
        """
        Casts the column into type ``dataType``.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        dataType : :class:`DataType` or str
            a DataType or Python string literal with a DDL-formatted string
            to use when parsing the column to the same type.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is cast into new type.
        """
        if isinstance(dataType, (DataType, str)):
            return Column(CastExpression(col=self, data_type=dataType))
        else:
            raise TypeError("unexpected type: %s" % type(dataType))

    def __repr__(self) -> str:
        return "Column<'%s'>" % self._expr.__repr__()

    def otherwise(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("otherwise() is not yet implemented.")

    def over(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("over() is not yet implemented.")

    def isin(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("isin() is not yet implemented.")

    def when(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("when() is not yet implemented.")

    def getItem(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("getItem() is not yet implemented.")

    def astype(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("astype() is not yet implemented.")

    def between(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("between() is not yet implemented.")

    def getField(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("getField() is not yet implemented.")

    def withField(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("withField() is not yet implemented.")

    def dropFields(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("dropFields() is not yet implemented.")

    def __getitem__(self, k: Any) -> None:
        raise NotImplementedError("apply() - __getitem__ is not yet implemented.")

    def __iter__(self) -> None:
        raise TypeError("Column is not iterable")
