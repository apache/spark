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

from typing import (
    TYPE_CHECKING,
    Callable,
    Any,
    Union,
    overload,
    Sequence,
    Tuple,
    Optional,
)

import json
import decimal
import datetime
import warnings

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

from pyspark.sql.column import Column as PySparkColumn
import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.types import pyspark_types_to_proto_types

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.window import WindowSpec

JVM_BYTE_MIN: int = -(1 << 7)
JVM_BYTE_MAX: int = (1 << 7) - 1
JVM_SHORT_MIN: int = -(1 << 15)
JVM_SHORT_MAX: int = (1 << 15) - 1
JVM_INT_MIN: int = -(1 << 31)
JVM_INT_MAX: int = (1 << 31) - 1
JVM_LONG_MIN: int = -(1 << 63)
JVM_LONG_MAX: int = (1 << 63) - 1


def _func_op(name: str, doc: Optional[str] = "") -> Callable[["Column"], "Column"]:
    def wrapped(self: "Column") -> "Column":
        return scalar_function(name, self)

    wrapped.__doc__ = doc
    return wrapped


def _bin_op(
    name: str, doc: Optional[str] = "binary function", reverse: bool = False
) -> Callable[["Column", Any], "Column"]:
    def wrapped(self: "Column", other: Any) -> "Column":
        from pyspark.sql.connect.functions import lit

        if isinstance(other, (bool, float, int, str, datetime.datetime, datetime.date)):
            other = lit(other)
        if not reverse:
            return scalar_function(name, self, other)
        else:
            return scalar_function(name, other, self)

    wrapped.__doc__ = doc
    return wrapped


def _unary_op(name: str, doc: Optional[str] = "unary function") -> Callable[["Column"], "Column"]:
    def wrapped(self: "Column") -> "Column":
        return scalar_function(name, self)

    wrapped.__doc__ = doc
    return wrapped


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
        metadata = kwargs.pop("metadata", None)
        assert not kwargs, "Unexpected kwargs where passed: %s" % kwargs
        return ColumnAlias(self, list(alias), metadata)

    alias.__doc__ = PySparkColumn.alias.__doc__

    def name(self) -> str:
        ...


class CaseWhen(Expression):
    def __init__(
        self, branches: Sequence[Tuple[Expression, Expression]], else_value: Optional[Expression]
    ):

        assert isinstance(branches, list)
        for branch in branches:
            assert (
                isinstance(branch, tuple)
                and len(branch) == 2
                and all(isinstance(expr, Expression) for expr in branch)
            )
        self._branches = branches

        if else_value is not None:
            assert isinstance(else_value, Expression)

        self._else_value = else_value

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        args = []
        for condition, value in self._branches:
            args.append(condition)
            args.append(value)

        if self._else_value is not None:
            args.append(self._else_value)

        unresolved_function = UnresolvedFunction(name="when", args=args)

        return unresolved_function.to_plan(session)

    def __repr__(self) -> str:
        _cases = "".join([f" WHEN {c} THEN {v}" for c, v in self._branches])
        _else = f" ELSE {self._else_value}" if self._else_value is not None else ""
        return "CASE" + _cases + _else + " END"


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
            expr.literal.typed_null.CopyFrom(pyspark_types_to_proto_types(self._dataType))
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
    def __init__(self, child: Expression, ascending: bool = True, nullsFirst: bool = True) -> None:
        super().__init__()
        self._child = child
        self._ascending = ascending
        self._nullsFirst = nullsFirst

    def __repr__(self) -> str:
        return (
            str(self._child)
            + (" ASC" if self._ascending else " DESC")
            + (" NULLS FIRST" if self._nullsFirst else " NULLS LAST")
        )

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        sort = proto.Expression()
        sort.sort_order.child.CopyFrom(self._child.to_plan(session))

        if self._ascending:
            sort.sort_order.direction = (
                proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
            )
        else:
            sort.sort_order.direction = (
                proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
            )

        if self._nullsFirst:
            sort.sort_order.null_ordering = proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
        else:
            sort.sort_order.null_ordering = proto.Expression.SortOrder.NullOrdering.SORT_NULLS_LAST

        return sort


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


class UnresolvedRegex(Expression):
    def __init__(
        self,
        col_name: str,
    ) -> None:
        super().__init__()

        assert isinstance(col_name, str)
        self.col_name = col_name

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = proto.Expression()
        expr.unresolved_regex.col_name = self.col_name
        return expr

    def __repr__(self) -> str:
        return f"UnresolvedRegex({self.col_name})"


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


class LambdaFunction(Expression):
    def __init__(
        self,
        function: Expression,
        arguments: Sequence[str],
    ) -> None:
        super().__init__()

        assert isinstance(function, Expression)

        assert (
            isinstance(arguments, list)
            and len(arguments) > 0
            and all(isinstance(arg, str) for arg in arguments)
        )

        self._function = function
        self._arguments = arguments

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        fun = proto.Expression()
        fun.lambda_function.function.CopyFrom(self._function.to_plan(session))
        fun.lambda_function.arguments.extend(self._arguments)
        return fun

    def __repr__(self) -> str:
        return f"(LambdaFunction({str(self._function)}, {', '.join(self._arguments)})"


class WindowExpression(Expression):
    def __init__(
        self,
        windowFunction: Expression,
        windowSpec: "WindowSpec",
    ) -> None:
        super().__init__()

        from pyspark.sql.connect.window import WindowSpec

        assert windowFunction is not None and isinstance(windowFunction, Expression)

        assert windowSpec is not None and isinstance(windowSpec, WindowSpec)

        self._windowFunction = windowFunction

        self._windowSpec = windowSpec

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = proto.Expression()

        expr.window.window_function.CopyFrom(self._windowFunction.to_plan(session))

        if len(self._windowSpec._partitionSpec) > 0:
            expr.window.partition_spec.extend(
                [p.to_plan(session) for p in self._windowSpec._partitionSpec]
            )
        else:
            warnings.warn(
                "WARN WindowExpression: No Partition Defined for Window operation! "
                "Moving all data to a single partition, this can cause serious "
                "performance degradation."
            )

        if len(self._windowSpec._orderSpec) > 0:
            expr.window.order_spec.extend(
                [s.to_plan(session).sort_order for s in self._windowSpec._orderSpec]
            )

        if self._windowSpec._frame is not None:
            if self._windowSpec._frame._isRowFrame:
                expr.window.frame_spec.frame_type = (
                    proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW
                )

                start = self._windowSpec._frame._start
                if start == 0:
                    expr.window.frame_spec.lower.current_row = True
                elif start == JVM_LONG_MIN:
                    expr.window.frame_spec.lower.unbounded = True
                elif JVM_INT_MIN <= start <= JVM_INT_MAX:
                    expr.window.frame_spec.lower.value.literal.integer = start
                else:
                    raise ValueError(f"start is out of bound: {start}")

                end = self._windowSpec._frame._end
                if end == 0:
                    expr.window.frame_spec.upper.current_row = True
                elif end == JVM_LONG_MAX:
                    expr.window.frame_spec.upper.unbounded = True
                elif JVM_INT_MIN <= end <= JVM_INT_MAX:
                    expr.window.frame_spec.upper.value.literal.integer = end
                else:
                    raise ValueError(f"end is out of bound: {end}")

            else:
                expr.window.frame_spec.frame_type = (
                    proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE
                )

                start = self._windowSpec._frame._start
                if start == 0:
                    expr.window.frame_spec.lower.current_row = True
                elif start == JVM_LONG_MIN:
                    expr.window.frame_spec.lower.unbounded = True
                else:
                    expr.window.frame_spec.lower.value.literal.long = start

                end = self._windowSpec._frame._end
                if end == 0:
                    expr.window.frame_spec.upper.current_row = True
                elif end == JVM_LONG_MAX:
                    expr.window.frame_spec.upper.unbounded = True
                else:
                    expr.window.frame_spec.upper.value.literal.long = end

        return expr

    def __repr__(self) -> str:
        return f"WindowExpression({str(self._windowFunction)}, ({str(self._windowSpec)}))"


class Column:
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

    eqNullSafe = _bin_op("eqNullSafe", PySparkColumn.eqNullSafe.__doc__)

    __neg__ = _func_op("negate")

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    __and__ = _bin_op("and")
    __or__ = _bin_op("or")
    __invert__ = _func_op("not")
    __rand__ = _bin_op("and")
    __ror__ = _bin_op("or")

    # bitwise operators
    bitwiseOR = _bin_op("bitwiseOR", PySparkColumn.bitwiseOR.__doc__)
    bitwiseAND = _bin_op("bitwiseAND", PySparkColumn.bitwiseAND.__doc__)
    bitwiseXOR = _bin_op("bitwiseXOR", PySparkColumn.bitwiseXOR.__doc__)

    isNull = _unary_op("isNull", PySparkColumn.isNull.__doc__)
    isNotNull = _unary_op("isNotNull", PySparkColumn.isNotNull.__doc__)

    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> "Column":
        """binary function"""
        return _func_op("not")(_bin_op("==")(self, other))

    # string methods
    contains = _bin_op("contains", PySparkColumn.contains.__doc__)
    startswith = _bin_op("startsWith", PySparkColumn.startswith.__doc__)
    endswith = _bin_op("endsWith", PySparkColumn.endswith.__doc__)

    def when(self, condition: "Column", value: Any) -> "Column":
        if not isinstance(condition, Column):
            raise TypeError("condition should be a Column")

        if not isinstance(self._expr, CaseWhen):
            raise TypeError(
                "when() can only be applied on a Column previously generated by when() function"
            )

        if self._expr._else_value is not None:
            raise TypeError("when() cannot be applied once otherwise() is applied")

        if isinstance(value, Column):
            _value = value._expr
        else:
            _value = LiteralExpression(value, LiteralExpression._infer_type(value))

        _branches = self._expr._branches + [(condition._expr, _value)]

        return Column(CaseWhen(branches=_branches, else_value=None))

    when.__doc__ = PySparkColumn.when.__doc__

    def otherwise(self, value: Any) -> "Column":
        if not isinstance(self._expr, CaseWhen):
            raise TypeError(
                "otherwise() can only be applied on a Column previously generated by when()"
            )

        if self._expr._else_value is not None:
            raise TypeError(
                "otherwise() can only be applied once on a Column previously generated by when()"
            )

        if isinstance(value, Column):
            _value = value._expr
        else:
            _value = LiteralExpression(value, LiteralExpression._infer_type(value))

        return Column(CaseWhen(branches=self._expr._branches, else_value=_value))

    otherwise.__doc__ = PySparkColumn.otherwise.__doc__

    like = _bin_op("like", PySparkColumn.like.__doc__)
    rlike = _bin_op("rlike", PySparkColumn.rlike.__doc__)
    ilike = _bin_op("ilike", PySparkColumn.ilike.__doc__)

    @overload
    def substr(self, startPos: int, length: int) -> "Column":
        ...

    @overload
    def substr(self, startPos: "Column", length: "Column") -> "Column":
        ...

    def substr(self, startPos: Union[int, "Column"], length: Union[int, "Column"]) -> "Column":
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

    substr.__doc__ = PySparkColumn.substr.__doc__

    def __eq__(self, other: Any) -> "Column":  # type: ignore[override]
        """Returns a binary expression with the current column as the left
        side and the other expression as the right side.
        """
        from pyspark.sql.connect.functions import lit

        if isinstance(other, (bool, float, int, str, datetime.datetime, datetime.date)):
            other = lit(other)
        return scalar_function("==", self, other)

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        return self._expr.to_plan(session)

    def alias(self, *alias: str, **kwargs: Any) -> "Column":
        return Column(self._expr.alias(*alias, **kwargs))

    def asc(self) -> "Column":
        return self.asc_nulls_first()

    def asc_nulls_first(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=True, nullsFirst=True))

    def asc_nulls_last(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=True, nullsFirst=False))

    def desc(self) -> "Column":
        return self.desc_nulls_last()

    def desc_nulls_first(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=False, nullsFirst=True))

    def desc_nulls_last(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=False, nullsFirst=False))

    def name(self) -> str:
        return self._expr.name()

    def cast(self, dataType: Union[DataType, str]) -> "Column":
        if isinstance(dataType, (DataType, str)):
            return Column(CastExpression(col=self, data_type=dataType))
        else:
            raise TypeError("unexpected type: %s" % type(dataType))

    cast.__doc__ = PySparkColumn.cast.__doc__

    astype = cast

    def __repr__(self) -> str:
        return "Column<'%s'>" % self._expr.__repr__()

    def over(self, window: "WindowSpec") -> "Column":
        """
        Define a windowing column.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        window : :class:`WindowSpec`

        Returns
        -------
        :class:`Column`

        Examples
        --------
        >>> from pyspark.sql import Window
        >>> window = Window.partitionBy("name").orderBy("age") \
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        >>> from pyspark.sql.functions import rank, min
        >>> from pyspark.sql.functions import desc
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.withColumn("rank", rank().over(window)) \
                .withColumn("min", min('age').over(window)).sort(desc("age")).show()
        +---+-----+----+---+
        |age| name|rank|min|
        +---+-----+----+---+
        |  5|  Bob|   1|  5|
        |  2|Alice|   1|  2|
        +---+-----+----+---+
        """
        from pyspark.sql.connect.window import WindowSpec

        if not isinstance(window, WindowSpec):
            raise TypeError(
                f"window should be WindowSpec, but got {type(window).__name__} {window}"
            )

        return Column(WindowExpression(windowFunction=self._expr, windowSpec=window))

    def isin(self, *cols: Any) -> "Column":
        from pyspark.sql.connect.functions import lit

        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            _cols = list(cols[0])
        else:
            _cols = list(cols)

        return Column(UnresolvedFunction("in", [self._expr] + [lit(c)._expr for c in _cols]))

    isin.__doc__ = PySparkColumn.isin.__doc__

    def getItem(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("getItem() is not yet implemented.")

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


Column.__doc__ = PySparkColumn.__doc__
