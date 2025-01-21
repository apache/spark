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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import (
    cast,
    TYPE_CHECKING,
    Any,
    Callable,
    Union,
    Sequence,
    Tuple,
    Optional,
)

import json
import decimal
import datetime
import warnings
from threading import Lock

import numpy as np

from pyspark.serializers import CloudPickleSerializer
from pyspark.sql.types import (
    _from_numpy_type,
    DateType,
    ArrayType,
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
from pyspark.util import (
    JVM_BYTE_MIN,
    JVM_BYTE_MAX,
    JVM_SHORT_MIN,
    JVM_SHORT_MAX,
    JVM_INT_MIN,
    JVM_INT_MAX,
    JVM_LONG_MIN,
    JVM_LONG_MAX,
)
from pyspark.sql.connect.types import (
    UnparsedDataType,
    pyspark_types_to_proto_types,
    proto_schema_to_pyspark_data_type,
)
from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.errors.utils import current_origin
from pyspark.sql.utils import is_timestamp_ntz_preferred, enum_to_value

if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.window import WindowSpec
    from pyspark.sql.connect.plan import LogicalPlan


class Expression:
    """
    Expression base class.
    """

    def __init__(self) -> None:
        origin = current_origin()
        fragment = origin.fragment
        call_site = origin.call_site
        self.origin = None
        if fragment is not None and call_site is not None:
            self.origin = proto.Origin(
                python_origin=proto.PythonOrigin(
                    fragment=origin.fragment, call_site=origin.call_site
                )
            )

    def to_plan(  # type: ignore[empty-body]
        self, session: "SparkConnectClient"
    ) -> "proto.Expression":
        ...

    def __repr__(self) -> str:  # type: ignore[empty-body]
        ...

    def alias(self, *alias: str, **kwargs: Any) -> "ColumnAlias":
        metadata = kwargs.pop("metadata", None)
        if len(alias) > 1 and metadata is not None:
            raise PySparkValueError(
                errorClass="ONLY_ALLOWED_FOR_SINGLE_COLUMN",
                messageParameters={"arg_name": "metadata"},
            )
        assert not kwargs, "Unexpected kwargs where passed: %s" % kwargs
        return ColumnAlias(self, list(alias), metadata)

    def name(self) -> str:  # type: ignore[empty-body]
        ...

    def _create_proto_expression(self) -> proto.Expression:
        plan = proto.Expression()
        if self.origin is not None:
            plan.common.origin.CopyFrom(self.origin)
        return plan

    @property
    def children(self) -> Sequence["Expression"]:
        return []

    def foreach(self, f: Callable[["Expression"], None]) -> None:
        f(self)
        for c in self.children:
            c.foreach(f)


class CaseWhen(Expression):
    def __init__(
        self, branches: Sequence[Tuple[Expression, Expression]], else_value: Optional[Expression]
    ):
        super().__init__()

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

    @property
    def children(self) -> Sequence["Expression"]:
        children = []
        for branch in self._branches:
            children.append(branch[0])
            children.append(branch[1])
        if self._else_value is not None:
            children.append(self._else_value)
        return children

    def __repr__(self) -> str:
        _cases = "".join([f" WHEN {c} THEN {v}" for c, v in self._branches])
        _else = f" ELSE {self._else_value}" if self._else_value is not None else ""
        return "CASE" + _cases + _else + " END"


class ColumnAlias(Expression):
    def __init__(self, child: Expression, alias: Sequence[str], metadata: Any):
        super().__init__()

        self._alias = alias
        self._metadata = metadata
        self._child = child

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        if len(self._alias) == 1:
            exp = self._create_proto_expression()
            exp.alias.name.append(self._alias[0])
            exp.alias.expr.CopyFrom(self._child.to_plan(session))

            if self._metadata:
                exp.alias.metadata = json.dumps(self._metadata)
            return exp
        else:
            if self._metadata:
                raise PySparkValueError(
                    errorClass="CANNOT_PROVIDE_METADATA",
                    messageParameters={},
                )
            exp = self._create_proto_expression()
            exp.alias.name.extend(self._alias)
            exp.alias.expr.CopyFrom(self._child.to_plan(session))
            return exp

    @property
    def children(self) -> Sequence["Expression"]:
        return [self._child]

    def __repr__(self) -> str:
        return f"{self._child} AS {','.join(self._alias)}"


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
                ArrayType,
            ),
        )

        value = enum_to_value(value)
        if isinstance(dataType, NullType):
            assert value is None

        if value is not None:
            if isinstance(dataType, BinaryType):
                assert isinstance(value, (bytes, bytearray))
            elif isinstance(dataType, BooleanType):
                assert isinstance(value, (bool, np.bool_))
                value = bool(value)
            elif isinstance(dataType, ByteType):
                assert isinstance(value, (int, np.int8))
                assert JVM_BYTE_MIN <= int(value) <= JVM_BYTE_MAX
                value = int(value)
            elif isinstance(dataType, ShortType):
                assert isinstance(value, (int, np.int8, np.int16))
                assert JVM_SHORT_MIN <= int(value) <= JVM_SHORT_MAX
                value = int(value)
            elif isinstance(dataType, IntegerType):
                assert isinstance(value, (int, np.int8, np.int16, np.int32))
                assert JVM_INT_MIN <= int(value) <= JVM_INT_MAX
                value = int(value)
            elif isinstance(dataType, LongType):
                assert isinstance(value, (int, np.int8, np.int16, np.int32, np.int64))
                assert JVM_LONG_MIN <= int(value) <= JVM_LONG_MAX
                value = int(value)
            elif isinstance(dataType, FloatType):
                assert isinstance(value, (float, np.float32))
                value = float(value)
            elif isinstance(dataType, DoubleType):
                assert isinstance(value, (float, np.float32, np.float64))
                value = float(value)
            elif isinstance(dataType, DecimalType):
                assert isinstance(value, decimal.Decimal)
            elif isinstance(dataType, StringType):
                assert isinstance(value, (str, np.str_))
                value = str(value)
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
            elif isinstance(dataType, ArrayType):
                assert isinstance(value, list)
            else:
                raise PySparkTypeError(
                    errorClass="UNSUPPORTED_DATA_TYPE",
                    messageParameters={"data_type": str(dataType)},
                )

        self._value = value
        self._dataType = dataType

    @classmethod
    def _infer_type(cls, value: Any) -> DataType:
        value = enum_to_value(value)
        if value is None:
            return NullType()
        elif isinstance(value, (bytes, bytearray)):
            return BinaryType()
        elif isinstance(value, (bool, np.bool_)):
            return BooleanType()
        elif isinstance(value, int):
            if JVM_INT_MIN <= value <= JVM_INT_MAX:
                return IntegerType()
            elif JVM_LONG_MIN <= value <= JVM_LONG_MAX:
                return LongType()
            else:
                raise PySparkValueError(
                    errorClass="VALUE_NOT_BETWEEN",
                    messageParameters={
                        "arg_name": "value",
                        "min": str(JVM_LONG_MIN),
                        "max": str(JVM_SHORT_MAX),
                    },
                )
        elif isinstance(value, float):
            return DoubleType()
        elif isinstance(value, (str, np.str_)):
            return StringType()
        elif isinstance(value, decimal.Decimal):
            return DecimalType()
        elif isinstance(value, datetime.datetime):
            return TimestampNTZType() if is_timestamp_ntz_preferred() else TimestampType()
        elif isinstance(value, datetime.date):
            return DateType()
        elif isinstance(value, datetime.timedelta):
            return DayTimeIntervalType()
        elif isinstance(value, np.generic):
            dt = _from_numpy_type(value.dtype)
            if dt is not None:
                return dt
        elif isinstance(value, list):
            # follow the 'infer_array_from_first_element' strategy in 'sql.types._infer_type'
            # right now, it's dedicated for pyspark.ml params like array<...>, array<array<...>>
            if len(value) == 0 or value[0] is None:
                raise PySparkTypeError(
                    errorClass="CANNOT_INFER_ARRAY_ELEMENT_TYPE",
                    messageParameters={},
                )
            return ArrayType(LiteralExpression._infer_type(value[0]), True)

        raise PySparkTypeError(
            errorClass="UNSUPPORTED_DATA_TYPE",
            messageParameters={"data_type": type(value).__name__},
        )

    @classmethod
    def _from_value(cls, value: Any) -> "LiteralExpression":
        return LiteralExpression(value=value, dataType=LiteralExpression._infer_type(value))

    @classmethod
    def _to_value(
        cls, literal: "proto.Expression.Literal", dataType: Optional[DataType] = None
    ) -> Any:
        if literal.HasField("null"):
            return None
        elif literal.HasField("binary"):
            assert dataType is None or isinstance(dataType, BinaryType)
            return literal.binary
        elif literal.HasField("boolean"):
            assert dataType is None or isinstance(dataType, BooleanType)
            return literal.boolean
        elif literal.HasField("byte"):
            assert dataType is None or isinstance(dataType, ByteType)
            return literal.byte
        elif literal.HasField("short"):
            assert dataType is None or isinstance(dataType, ShortType)
            return literal.short
        elif literal.HasField("integer"):
            assert dataType is None or isinstance(dataType, IntegerType)
            return literal.integer
        elif literal.HasField("long"):
            assert dataType is None or isinstance(dataType, LongType)
            return literal.long
        elif literal.HasField("float"):
            assert dataType is None or isinstance(dataType, FloatType)
            return literal.float
        elif literal.HasField("double"):
            assert dataType is None or isinstance(dataType, DoubleType)
            return literal.double
        elif literal.HasField("decimal"):
            assert dataType is None or isinstance(dataType, DecimalType)
            return decimal.Decimal(literal.decimal.value)
        elif literal.HasField("string"):
            assert dataType is None or isinstance(dataType, StringType)
            return literal.string
        elif literal.HasField("date"):
            assert dataType is None or isinstance(dataType, DataType)
            return DateType().fromInternal(literal.date)
        elif literal.HasField("timestamp"):
            assert dataType is None or isinstance(dataType, TimestampType)
            return TimestampType().fromInternal(literal.timestamp)
        elif literal.HasField("timestamp_ntz"):
            assert dataType is None or isinstance(dataType, TimestampNTZType)
            return TimestampNTZType().fromInternal(literal.timestamp_ntz)
        elif literal.HasField("day_time_interval"):
            assert dataType is None or isinstance(dataType, DayTimeIntervalType)
            return DayTimeIntervalType().fromInternal(literal.day_time_interval)
        elif literal.HasField("array"):
            elementType = proto_schema_to_pyspark_data_type(literal.array.element_type)
            if dataType is not None:
                assert isinstance(dataType, ArrayType)
                assert elementType == dataType.elementType
            return [LiteralExpression._to_value(v, elementType) for v in literal.array.elements]

        raise PySparkTypeError(
            errorClass="UNSUPPORTED_LITERAL",
            messageParameters={"literal": str(literal)},
        )

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        """Converts the literal expression to the literal in proto."""

        expr = self._create_proto_expression()

        if self._value is None:
            expr.literal.null.CopyFrom(pyspark_types_to_proto_types(self._dataType))
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
        elif isinstance(self._dataType, ArrayType):
            element_type = self._dataType.elementType
            expr.literal.array.element_type.CopyFrom(pyspark_types_to_proto_types(element_type))
            for v in self._value:
                expr.literal.array.elements.append(
                    LiteralExpression(v, element_type).to_plan(session).literal
                )
        else:
            raise PySparkTypeError(
                errorClass="UNSUPPORTED_DATA_TYPE",
                messageParameters={"data_type": str(self._dataType)},
            )

        return expr

    def __repr__(self) -> str:
        if self._value is None:
            return "NULL"
        elif isinstance(self._dataType, DateType):
            dt = DateType().fromInternal(self._value)
            if dt is not None and isinstance(dt, datetime.date):
                return dt.strftime("%Y-%m-%d")
        elif isinstance(self._dataType, TimestampType):
            ts = TimestampType().fromInternal(self._value)
            if ts is not None and isinstance(ts, datetime.datetime):
                return ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif isinstance(self._dataType, TimestampNTZType):
            ts = TimestampNTZType().fromInternal(self._value)
            if ts is not None and isinstance(ts, datetime.datetime):
                return ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif isinstance(self._dataType, DayTimeIntervalType):
            delta = DayTimeIntervalType().fromInternal(self._value)
            if delta is not None and isinstance(delta, datetime.timedelta):
                import pandas as pd

                # Note: timedelta itself does not provide isoformat method.
                # Both Pandas and java.time.Duration provide it, but the format
                # is sightly different:
                # java.time.Duration only applies HOURS, MINUTES, SECONDS units,
                # while Pandas applies all supported units.
                return pd.Timedelta(delta).isoformat()
        return f"{self._value}"


class ColumnReference(Expression):
    """Represents a column reference. There is no guarantee that this column
    actually exists. In the context of this project, we refer by its name and
    treat it as an unresolved attribute. Attributes that have the same fully
    qualified name are identical"""

    def __init__(
        self,
        unparsed_identifier: str,
        plan_id: Optional[int] = None,
        is_metadata_column: bool = False,
    ) -> None:
        super().__init__()
        assert isinstance(unparsed_identifier, str)
        self._unparsed_identifier = unparsed_identifier

        assert plan_id is None or isinstance(plan_id, int)
        self._plan_id = plan_id

        self._is_metadata_column = is_metadata_column

    def name(self) -> str:
        """Returns the qualified name of the column reference."""
        return self._unparsed_identifier

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        """Returns the Proto representation of the expression."""
        expr = self._create_proto_expression()
        expr.unresolved_attribute.unparsed_identifier = self._unparsed_identifier
        if self._plan_id is not None:
            expr.unresolved_attribute.plan_id = self._plan_id
        expr.unresolved_attribute.is_metadata_column = self._is_metadata_column
        return expr

    def __repr__(self) -> str:
        return f"{self._unparsed_identifier}"

    def __eq__(self, other: Any) -> bool:
        return (
            other is not None
            and isinstance(other, ColumnReference)
            and other._unparsed_identifier == self._unparsed_identifier
        )


class UnresolvedStar(Expression):
    def __init__(self, unparsed_target: Optional[str], plan_id: Optional[int] = None):
        super().__init__()

        if unparsed_target is not None:
            assert isinstance(unparsed_target, str) and unparsed_target.endswith(".*")
        self._unparsed_target = unparsed_target

        assert plan_id is None or isinstance(plan_id, int)
        self._plan_id = plan_id

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        expr = self._create_proto_expression()
        expr.unresolved_star.SetInParent()
        if self._unparsed_target is not None:
            expr.unresolved_star.unparsed_target = self._unparsed_target
        if self._plan_id is not None:
            expr.unresolved_star.plan_id = self._plan_id
        return expr

    def __repr__(self) -> str:
        if self._unparsed_target is not None:
            return f"unresolvedstar({self._unparsed_target})"
        else:
            return "unresolvedstar()"

    def __eq__(self, other: Any) -> bool:
        return (
            other is not None
            and isinstance(other, UnresolvedStar)
            and other._unparsed_target == self._unparsed_target
        )


class SQLExpression(Expression):
    """Returns Expression which contains a string which is a SQL expression
    and server side will parse it by Catalyst
    """

    def __init__(self, expr: str) -> None:
        super().__init__()
        assert isinstance(expr, str)
        self._expr: str = expr

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        """Returns the Proto representation of the SQL expression."""
        expr = self._create_proto_expression()
        expr.expression_string.expression = self._expr
        return expr

    def __eq__(self, other: Any) -> bool:
        return other is not None and isinstance(other, SQLExpression) and other._expr == self._expr

    def __repr__(self) -> str:
        return self._expr


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
        sort = self._create_proto_expression()
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

    @property
    def children(self) -> Sequence["Expression"]:
        return [self._child]


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
        fun = self._create_proto_expression()
        fun.unresolved_function.function_name = self._name
        if len(self._args) > 0:
            fun.unresolved_function.arguments.extend([arg.to_plan(session) for arg in self._args])
        fun.unresolved_function.is_distinct = self._is_distinct
        return fun

    @property
    def children(self) -> Sequence["Expression"]:
        return self._args

    def __repr__(self) -> str:
        # Default print handling:
        if self._is_distinct:
            return f"{self._name}(distinct {', '.join([str(arg) for arg in self._args])})"
        else:
            return f"{self._name}({', '.join([str(arg) for arg in self._args])})"


class PythonUDF:
    """Represents a Python user-defined function."""

    def __init__(
        self,
        output_type: Union[DataType, str],
        eval_type: int,
        func: Callable[..., Any],
        python_ver: str,
    ) -> None:
        self._output_type: DataType = (
            UnparsedDataType(output_type) if isinstance(output_type, str) else output_type
        )
        self._eval_type = eval_type
        self._func = func
        self._python_ver = python_ver

    def to_plan(self, session: "SparkConnectClient") -> proto.PythonUDF:
        if isinstance(self._output_type, UnparsedDataType):
            parsed = session._analyze(
                method="ddl_parse", ddl_string=self._output_type.data_type_string
            ).parsed
            assert isinstance(parsed, DataType)
            output_type = parsed
        else:
            output_type = self._output_type
        expr = proto.PythonUDF()
        expr.output_type.CopyFrom(pyspark_types_to_proto_types(output_type))
        expr.eval_type = self._eval_type
        expr.command = CloudPickleSerializer().dumps((self._func, output_type))
        expr.python_ver = self._python_ver
        return expr

    def __repr__(self) -> str:
        return f"{self._output_type}, {self._eval_type}, {self._func}, f{self._python_ver}"


class JavaUDF:
    """Represents a Java (aggregate) user-defined function."""

    def __init__(
        self,
        class_name: str,
        output_type: Optional[Union[DataType, str]] = None,
        aggregate: bool = False,
    ) -> None:
        self._class_name = class_name
        self._output_type: Optional[DataType] = (
            UnparsedDataType(output_type) if isinstance(output_type, str) else output_type
        )
        self._aggregate = aggregate

    def to_plan(self, session: "SparkConnectClient") -> proto.JavaUDF:
        expr = proto.JavaUDF()
        expr.class_name = self._class_name
        if self._output_type is not None:
            expr.output_type.CopyFrom(pyspark_types_to_proto_types(self._output_type))
        expr.aggregate = self._aggregate
        return expr

    def __repr__(self) -> str:
        return f"{self._class_name}, {self._output_type}"


class CommonInlineUserDefinedFunction(Expression):
    """Represents a user-defined function with an inlined defined function body of any programming
    languages."""

    def __init__(
        self,
        function_name: str,
        function: Union[PythonUDF, JavaUDF],
        deterministic: bool = False,
        arguments: Optional[Sequence[Expression]] = None,
    ):
        super().__init__()
        self._function_name = function_name
        self._deterministic = deterministic
        self._arguments: Sequence[Expression] = arguments or []
        self._function = function

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        expr = self._create_proto_expression()
        expr.common_inline_user_defined_function.function_name = self._function_name
        expr.common_inline_user_defined_function.deterministic = self._deterministic
        if len(self._arguments) > 0:
            expr.common_inline_user_defined_function.arguments.extend(
                [arg.to_plan(session) for arg in self._arguments]
            )
        expr.common_inline_user_defined_function.python_udf.CopyFrom(
            cast(proto.PythonUDF, self._function.to_plan(session))
        )
        return expr

    def to_plan_udf(self, session: "SparkConnectClient") -> "proto.CommonInlineUserDefinedFunction":
        """Compared to `to_plan`, it returns a CommonInlineUserDefinedFunction instead of an
        Expression."""
        expr = proto.CommonInlineUserDefinedFunction()
        expr.function_name = self._function_name
        expr.deterministic = self._deterministic
        if len(self._arguments) > 0:
            expr.arguments.extend([arg.to_plan(session) for arg in self._arguments])
        expr.python_udf.CopyFrom(cast(proto.PythonUDF, self._function.to_plan(session)))
        return expr

    def to_plan_judf(
        self, session: "SparkConnectClient"
    ) -> "proto.CommonInlineUserDefinedFunction":
        expr = proto.CommonInlineUserDefinedFunction()
        expr.function_name = self._function_name
        expr.java_udf.CopyFrom(cast(proto.JavaUDF, self._function.to_plan(session)))
        return expr

    @property
    def children(self) -> Sequence["Expression"]:
        return self._arguments

    def __repr__(self) -> str:
        return f"{self._function_name}({', '.join([str(arg) for arg in self._arguments])})"


class WithField(Expression):
    def __init__(
        self,
        structExpr: Expression,
        fieldName: str,
        valueExpr: Expression,
    ) -> None:
        super().__init__()

        assert isinstance(structExpr, Expression)
        self._structExpr = structExpr

        assert isinstance(fieldName, str)
        self._fieldName = fieldName

        assert isinstance(valueExpr, Expression)
        self._valueExpr = valueExpr

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = self._create_proto_expression()
        expr.update_fields.struct_expression.CopyFrom(self._structExpr.to_plan(session))
        expr.update_fields.field_name = self._fieldName
        expr.update_fields.value_expression.CopyFrom(self._valueExpr.to_plan(session))
        return expr

    @property
    def children(self) -> Sequence["Expression"]:
        return [self._structExpr, self._valueExpr]

    def __repr__(self) -> str:
        return f"update_field({self._structExpr}, {self._fieldName}, {self._valueExpr})"


class DropField(Expression):
    def __init__(
        self,
        structExpr: Expression,
        fieldName: str,
    ) -> None:
        super().__init__()

        assert isinstance(structExpr, Expression)
        self._structExpr = structExpr

        assert isinstance(fieldName, str)
        self._fieldName = fieldName

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = self._create_proto_expression()
        expr.update_fields.struct_expression.CopyFrom(self._structExpr.to_plan(session))
        expr.update_fields.field_name = self._fieldName
        return expr

    @property
    def children(self) -> Sequence["Expression"]:
        return [self._structExpr]

    def __repr__(self) -> str:
        return f"drop_field({self._structExpr}, {self._fieldName})"


class UnresolvedExtractValue(Expression):
    def __init__(
        self,
        child: Expression,
        extraction: Expression,
    ) -> None:
        super().__init__()

        assert isinstance(child, Expression)
        self._child = child

        assert isinstance(extraction, Expression)
        self._extraction = extraction

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = self._create_proto_expression()
        expr.unresolved_extract_value.child.CopyFrom(self._child.to_plan(session))
        expr.unresolved_extract_value.extraction.CopyFrom(self._extraction.to_plan(session))
        return expr

    @property
    def children(self) -> Sequence["Expression"]:
        return [self._child, self._extraction]

    def __repr__(self) -> str:
        return f"{self._child}['{self._extraction}']"


class UnresolvedRegex(Expression):
    def __init__(self, col_name: str, plan_id: Optional[int] = None) -> None:
        super().__init__()

        assert isinstance(col_name, str)
        self.col_name = col_name

        assert plan_id is None or isinstance(plan_id, int)
        self._plan_id = plan_id

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = self._create_proto_expression()
        expr.unresolved_regex.col_name = self.col_name
        if self._plan_id is not None:
            expr.unresolved_regex.plan_id = self._plan_id
        return expr

    def __repr__(self) -> str:
        return f"UnresolvedRegex({self.col_name})"


class CastExpression(Expression):
    def __init__(
        self,
        expr: Expression,
        data_type: Union[DataType, str],
        eval_mode: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._expr = expr
        assert isinstance(data_type, (DataType, str))
        self._data_type = data_type
        if eval_mode is not None:
            assert isinstance(eval_mode, str)
            assert eval_mode in ["legacy", "ansi", "try"]
        self._eval_mode = eval_mode

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        fun = self._create_proto_expression()
        fun.cast.expr.CopyFrom(self._expr.to_plan(session))
        if isinstance(self._data_type, str):
            fun.cast.type_str = self._data_type
        else:
            fun.cast.type.CopyFrom(pyspark_types_to_proto_types(self._data_type))

        if self._eval_mode is not None:
            if self._eval_mode == "legacy":
                fun.cast.eval_mode = proto.Expression.Cast.EvalMode.EVAL_MODE_LEGACY
            elif self._eval_mode == "ansi":
                fun.cast.eval_mode = proto.Expression.Cast.EvalMode.EVAL_MODE_ANSI
            elif self._eval_mode == "try":
                fun.cast.eval_mode = proto.Expression.Cast.EvalMode.EVAL_MODE_TRY

        return fun

    @property
    def children(self) -> Sequence["Expression"]:
        return [self._expr]

    def __repr__(self) -> str:
        # We cannot guarantee the string representations be exactly the same, e.g.
        # str(sf.col("a").cast("long")):
        #   Column<'CAST(a AS BIGINT)'>     <- Spark Classic
        #   Column<'CAST(a AS LONG)'>       <- Spark Connect
        if isinstance(self._data_type, DataType):
            str_data_type = self._data_type.simpleString().upper()
        else:
            str_data_type = str(self._data_type).upper()
        if self._eval_mode is not None and self._eval_mode == "try":
            return f"TRY_CAST({self._expr} AS {str_data_type})"
        else:
            return f"CAST({self._expr} AS {str_data_type})"


class UnresolvedNamedLambdaVariable(Expression):
    _lock: Lock = Lock()
    _nextVarNameId: int = 0

    def __init__(
        self,
        name_parts: Sequence[str],
    ) -> None:
        super().__init__()

        assert (
            isinstance(name_parts, list)
            and len(name_parts) > 0
            and all(isinstance(p, str) for p in name_parts)
        )

        self._name_parts = name_parts

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = self._create_proto_expression()
        expr.unresolved_named_lambda_variable.name_parts.extend(self._name_parts)
        return expr

    def __repr__(self) -> str:
        return ", ".join(self._name_parts)

    @staticmethod
    def fresh_var_name(name: str) -> str:
        assert isinstance(name, str) and str != ""

        _id: Optional[int] = None

        with UnresolvedNamedLambdaVariable._lock:
            _id = UnresolvedNamedLambdaVariable._nextVarNameId
            UnresolvedNamedLambdaVariable._nextVarNameId += 1

        assert _id is not None

        return f"{name}_{_id}"


class LambdaFunction(Expression):
    def __init__(
        self,
        function: Expression,
        arguments: Sequence[UnresolvedNamedLambdaVariable],
    ) -> None:
        super().__init__()

        assert isinstance(function, Expression)

        assert (
            isinstance(arguments, list)
            and len(arguments) > 0
            and all(isinstance(arg, UnresolvedNamedLambdaVariable) for arg in arguments)
        )

        self._function = function
        self._arguments = arguments

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = self._create_proto_expression()
        expr.lambda_function.function.CopyFrom(self._function.to_plan(session))
        expr.lambda_function.arguments.extend(
            [arg.to_plan(session).unresolved_named_lambda_variable for arg in self._arguments]
        )
        return expr

    @property
    def children(self) -> Sequence["Expression"]:
        return [self._function] + self._arguments

    def __repr__(self) -> str:
        return (
            f"LambdaFunction({str(self._function)}, "
            + f"{', '.join([str(arg) for arg in self._arguments])})"
        )


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
        expr = self._create_proto_expression()

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
                    raise PySparkValueError(
                        errorClass="VALUE_NOT_BETWEEN",
                        messageParameters={
                            "arg_name": "start",
                            "min": str(JVM_INT_MIN),
                            "max": str(JVM_INT_MAX),
                        },
                    )

                end = self._windowSpec._frame._end
                if end == 0:
                    expr.window.frame_spec.upper.current_row = True
                elif end == JVM_LONG_MAX:
                    expr.window.frame_spec.upper.unbounded = True
                elif JVM_INT_MIN <= end <= JVM_INT_MAX:
                    expr.window.frame_spec.upper.value.literal.integer = end
                else:
                    raise PySparkValueError(
                        errorClass="VALUE_NOT_BETWEEN",
                        messageParameters={
                            "arg_name": "end",
                            "min": str(JVM_INT_MIN),
                            "max": str(JVM_INT_MAX),
                        },
                    )

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

    @property
    def children(self) -> Sequence["Expression"]:
        return (
            [self._windowFunction] + self._windowSpec._partitionSpec + self._windowSpec._orderSpec
        )

    def __repr__(self) -> str:
        return f"WindowExpression({str(self._windowFunction)}, ({str(self._windowSpec)}))"


class DistributedSequenceID(Expression):
    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        unresolved_function = UnresolvedFunction(name="distributed_sequence_id", args=[])
        return unresolved_function.to_plan(session)

    def __repr__(self) -> str:
        return "DistributedSequenceID()"


class CallFunction(Expression):
    def __init__(self, name: str, args: Sequence["Expression"]):
        super().__init__()

        assert isinstance(name, str)
        self._name = name

        assert isinstance(args, list) and all(isinstance(arg, Expression) for arg in args)
        self._args = args

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        expr = self._create_proto_expression()
        expr.call_function.function_name = self._name
        if len(self._args) > 0:
            expr.call_function.arguments.extend([arg.to_plan(session) for arg in self._args])
        return expr

    @property
    def children(self) -> Sequence["Expression"]:
        return self._args

    def __repr__(self) -> str:
        if len(self._args) > 0:
            return f"CallFunction('{self._name}', {', '.join([str(arg) for arg in self._args])})"
        else:
            return f"CallFunction('{self._name}')"


class NamedArgumentExpression(Expression):
    def __init__(self, key: str, value: Expression):
        super().__init__()

        assert isinstance(key, str)
        self._key = key

        assert isinstance(value, Expression)
        self._value = value

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        expr = self._create_proto_expression()
        expr.named_argument_expression.key = self._key
        expr.named_argument_expression.value.CopyFrom(self._value.to_plan(session))
        return expr

    @property
    def children(self) -> Sequence["Expression"]:
        return [self._value]

    def __repr__(self) -> str:
        return f"{self._key} => {self._value}"


class LazyExpression(Expression):
    def __init__(self, expr: Expression):
        assert isinstance(expr, Expression)
        super().__init__()
        self._expr = expr

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = self._create_proto_expression()
        expr.lazy_expression.child.CopyFrom(self._expr.to_plan(session))
        return expr

    @property
    def children(self) -> Sequence["Expression"]:
        return [self._expr]

    def __repr__(self) -> str:
        return f"lazy({self._expr})"


class SubqueryExpression(Expression):
    def __init__(self, plan: "LogicalPlan", subquery_type: str) -> None:
        assert isinstance(subquery_type, str)
        assert subquery_type in ("scalar", "exists")

        super().__init__()
        self._plan = plan
        self._subquery_type = subquery_type

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = self._create_proto_expression()
        expr.subquery_expression.plan_id = self._plan._plan_id
        if self._subquery_type == "scalar":
            expr.subquery_expression.subquery_type = proto.SubqueryExpression.SUBQUERY_TYPE_SCALAR
        elif self._subquery_type == "exists":
            expr.subquery_expression.subquery_type = proto.SubqueryExpression.SUBQUERY_TYPE_EXISTS
        return expr

    def __repr__(self) -> str:
        return f"SubqueryExpression({self._plan}, {self._subquery_type})"
