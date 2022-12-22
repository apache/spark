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
    Any,
    Union,
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

import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.types import (
    JVM_BYTE_MIN,
    JVM_BYTE_MAX,
    JVM_SHORT_MIN,
    JVM_SHORT_MAX,
    JVM_INT_MIN,
    JVM_INT_MAX,
    JVM_LONG_MIN,
    JVM_LONG_MAX,
    pyspark_types_to_proto_types,
)

if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.window import WindowSpec


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

    def __init__(self, name: str) -> None:
        super().__init__()
        assert isinstance(name, str)
        self._unparsed_identifier = name

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
        expr: Expression,
        data_type: Union[DataType, str],
    ) -> None:
        super().__init__()
        self._expr = expr
        self._data_type = data_type

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        fun = proto.Expression()
        fun.cast.expr.CopyFrom(self._expr.to_plan(session))
        if isinstance(self._data_type, str):
            fun.cast.type_str = self._data_type
        else:
            fun.cast.type.CopyFrom(pyspark_types_to_proto_types(self._data_type))
        return fun

    def __repr__(self) -> str:
        return f"({self._expr} ({self._data_type}))"


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
