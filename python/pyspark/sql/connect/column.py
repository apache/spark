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

import datetime
import decimal
import warnings

from typing import (
    TYPE_CHECKING,
    Any,
    Union,
    Optional,
    Tuple,
)

from pyspark.sql.column import Column as ParentColumn
from pyspark.errors import (
    PySparkTypeError,
    PySparkAttributeError,
    PySparkValueError,
)
from pyspark.sql.types import DataType
from pyspark.sql.utils import enum_to_value

import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.expressions import (
    Expression,
    UnresolvedFunction,
    UnresolvedExtractValue,
    LiteralExpression,
    CaseWhen,
    SortOrder,
    SubqueryExpression,
    CastExpression,
    WindowExpression,
    WithField,
    DropField,
)
from pyspark.errors.utils import with_origin_to_class


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import (
        LiteralType,
        DateTimeLiteral,
        DecimalLiteral,
    )
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.window import WindowSpec


def _func_op(name: str, self: ParentColumn) -> ParentColumn:
    return Column(UnresolvedFunction(name, [self._expr]))  # type: ignore[list-item]


def _bin_op(
    name: str,
    self: ParentColumn,
    other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
    reverse: bool = False,
) -> ParentColumn:
    other = enum_to_value(other)
    if other is None or isinstance(
        other,
        (
            bool,
            float,
            int,
            str,
            datetime.datetime,
            datetime.date,
            decimal.Decimal,
            datetime.timedelta,
        ),
    ):
        other_expr = LiteralExpression._from_value(other)
    else:
        other_expr = other._expr  # type: ignore[assignment]

    if not reverse:
        return Column(UnresolvedFunction(name, [self._expr, other_expr]))  # type: ignore[list-item]
    else:
        return Column(UnresolvedFunction(name, [other_expr, self._expr]))  # type: ignore[list-item]


def _unary_op(name: str, self: ParentColumn) -> ParentColumn:
    return Column(UnresolvedFunction(name, [self._expr]))  # type: ignore[list-item]


def _to_expr(v: Any) -> Expression:
    return v._expr if isinstance(v, Column) else LiteralExpression._from_value(v)


@with_origin_to_class(["to_plan"])
class Column(ParentColumn):
    def __new__(cls, *args: Any, **kwargs: Any) -> "Column":
        return object.__new__(cls)

    def __getnewargs__(self) -> Tuple[Any, ...]:
        return (self._expr,)

    def __init__(self, expr: "Expression") -> None:
        if not isinstance(expr, Expression):
            raise PySparkTypeError(
                errorClass="NOT_EXPRESSION",
                messageParameters={"arg_name": "expr", "arg_type": type(expr).__name__},
            )
        self._expr = expr

    def __gt__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op(">", self, other)

    def __lt__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("<", self, other)

    def __add__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("+", self, other)

    def __sub__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("-", self, other)

    def __mul__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("*", self, other)

    def __div__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("/", self, other)

    def __truediv__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("/", self, other)

    def __mod__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("%", self, other)

    def __radd__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("+", self, other, reverse=True)

    def __rsub__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("-", self, other, reverse=True)

    def __rmul__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("*", self, other, reverse=True)

    def __rdiv__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("/", self, other, reverse=True)

    def __rtruediv__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("/", self, other, reverse=True)

    def __rmod__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("%", self, other, reverse=True)

    def __pow__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("power", self, other)

    def __rpow__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("power", self, other, reverse=True)

    def __ge__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op(">=", self, other)

    def __le__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("<=", self, other)

    def eqNullSafe(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("<=>", self, other)

    def __neg__(self) -> ParentColumn:
        return _func_op("negative", self)

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    def __and__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("and", self, other)

    def __or__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("or", self, other)

    def __invert__(self) -> ParentColumn:
        return _func_op("not", self)

    def __rand__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("and", self, other)

    def __ror__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("or", self, other)

    # container operators
    def __contains__(self, item: Any) -> None:
        raise PySparkValueError(
            errorClass="CANNOT_APPLY_IN_FOR_COLUMN",
            messageParameters={},
        )

    # bitwise operators
    def bitwiseOR(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("|", self, other)

    def bitwiseAND(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("&", self, other)

    def bitwiseXOR(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("^", self, other)

    def isNull(self) -> ParentColumn:
        return _unary_op("isNull", self)

    def isNotNull(self) -> ParentColumn:
        return _unary_op("isNotNull", self)

    def isNaN(self) -> ParentColumn:
        return _unary_op("isNaN", self)

    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> ParentColumn:
        return _func_op("not", _bin_op("==", self, other))

    # string methods
    def contains(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("contains", self, other)

    def startswith(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("startsWith", self, other)

    def endswith(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("endsWith", self, other)

    def when(self, condition: ParentColumn, value: Any) -> ParentColumn:
        if not isinstance(condition, Column):
            raise PySparkTypeError(
                errorClass="NOT_COLUMN",
                messageParameters={"arg_name": "condition", "arg_type": type(condition).__name__},
            )

        if not isinstance(self._expr, CaseWhen):
            raise PySparkTypeError(
                errorClass="INVALID_WHEN_USAGE",
                messageParameters={},
            )

        if self._expr._else_value is not None:
            raise PySparkTypeError(
                errorClass="INVALID_WHEN_USAGE",
                messageParameters={},
            )

        return Column(
            CaseWhen(
                branches=self._expr._branches + [(condition._expr, _to_expr(value))],
                else_value=None,
            )
        )

    def otherwise(self, value: Any) -> ParentColumn:
        if not isinstance(self._expr, CaseWhen):
            raise PySparkTypeError(
                "otherwise() can only be applied on a Column previously generated by when()"
            )

        if self._expr._else_value is not None:
            raise PySparkTypeError(
                "otherwise() can only be applied once on a Column previously generated by when()"
            )

        return Column(
            CaseWhen(
                branches=self._expr._branches,
                else_value=_to_expr(value),
            )
        )

    def like(self: ParentColumn, other: str) -> ParentColumn:
        return _bin_op("like", self, other)

    def rlike(self: ParentColumn, other: str) -> ParentColumn:
        return _bin_op("rlike", self, other)

    def ilike(self: ParentColumn, other: str) -> ParentColumn:
        return _bin_op("ilike", self, other)

    def substr(
        self, startPos: Union[int, ParentColumn], length: Union[int, ParentColumn]
    ) -> ParentColumn:
        startPos = enum_to_value(startPos)
        length = enum_to_value(length)

        if type(startPos) != type(length):
            raise PySparkTypeError(
                errorClass="NOT_SAME_TYPE",
                messageParameters={
                    "arg_name1": "startPos",
                    "arg_name2": "length",
                    "arg_type1": type(startPos).__name__,
                    "arg_type2": type(length).__name__,
                },
            )

        if isinstance(length, (Column, int)):
            length_expr = _to_expr(length)
            start_expr = _to_expr(startPos)
        else:
            raise PySparkTypeError(
                errorClass="NOT_COLUMN_OR_INT",
                messageParameters={"arg_name": "startPos", "arg_type": type(length).__name__},
            )
        return Column(UnresolvedFunction("substr", [self._expr, start_expr, length_expr]))

    def __eq__(self, other: Any) -> ParentColumn:  # type: ignore[override]
        other = enum_to_value(other)
        if other is None or isinstance(
            other, (bool, float, int, str, datetime.datetime, datetime.date, decimal.Decimal)
        ):
            other_expr = LiteralExpression._from_value(other)
        else:
            other_expr = other._expr

        return Column(UnresolvedFunction("==", [self._expr, other_expr]))

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        return self._expr.to_plan(session)

    def alias(self, *alias: str, **kwargs: Any) -> ParentColumn:
        return Column(self._expr.alias(*alias, **kwargs))

    name = alias

    def asc(self) -> ParentColumn:
        return self.asc_nulls_first()

    def asc_nulls_first(self) -> ParentColumn:
        return Column(SortOrder(self._expr, ascending=True, nullsFirst=True))

    def asc_nulls_last(self) -> ParentColumn:
        return Column(SortOrder(self._expr, ascending=True, nullsFirst=False))

    def desc(self) -> ParentColumn:
        return self.desc_nulls_last()

    def desc_nulls_first(self) -> ParentColumn:
        return Column(SortOrder(self._expr, ascending=False, nullsFirst=True))

    def desc_nulls_last(self) -> ParentColumn:
        return Column(SortOrder(self._expr, ascending=False, nullsFirst=False))

    def cast(self, dataType: Union[DataType, str]) -> ParentColumn:
        if isinstance(dataType, (DataType, str)):
            return Column(CastExpression(expr=self._expr, data_type=dataType))
        else:
            raise PySparkTypeError(
                errorClass="NOT_DATATYPE_OR_STR",
                messageParameters={"arg_name": "dataType", "arg_type": type(dataType).__name__},
            )

    astype = cast

    def try_cast(self, dataType: Union[DataType, str]) -> ParentColumn:
        if isinstance(dataType, (DataType, str)):
            return Column(
                CastExpression(
                    expr=self._expr,
                    data_type=dataType,
                    eval_mode="try",
                )
            )
        else:
            raise PySparkTypeError(
                errorClass="NOT_DATATYPE_OR_STR",
                messageParameters={"arg_name": "dataType", "arg_type": type(dataType).__name__},
            )

    def __repr__(self) -> str:
        return "Column<'%s'>" % self._expr.__repr__()

    def over(self, window: "WindowSpec") -> ParentColumn:  # type: ignore[override]
        from pyspark.sql.connect.window import WindowSpec

        if not isinstance(window, WindowSpec):
            raise PySparkTypeError(
                errorClass="NOT_WINDOWSPEC",
                messageParameters={"arg_name": "window", "arg_type": type(window).__name__},
            )

        return Column(WindowExpression(windowFunction=self._expr, windowSpec=window))

    def outer(self) -> ParentColumn:
        return Column(self._expr)

    def isin(self, *cols: Any) -> ParentColumn:
        from pyspark.sql.connect.dataframe import DataFrame

        if len(cols) == 1 and isinstance(cols[0], DataFrame):
            if isinstance(self._expr, UnresolvedFunction) and self._expr._name == "struct":
                values = self._expr.children
            else:
                values = [self._expr]

            return Column(
                SubqueryExpression(cols[0]._plan, subquery_type="in", in_subquery_values=values)
            )

        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            _cols = list(cols[0])
        else:
            _cols = list(cols)

        return Column(UnresolvedFunction("in", [self._expr] + [_to_expr(c) for c in _cols]))

    def between(
        self,
        lowerBound: Union[ParentColumn, "LiteralType", "DateTimeLiteral", "DecimalLiteral"],
        upperBound: Union[ParentColumn, "LiteralType", "DateTimeLiteral", "DecimalLiteral"],
    ) -> ParentColumn:
        return (self >= lowerBound) & (self <= upperBound)

    def getItem(self, key: Any) -> ParentColumn:
        if isinstance(key, Column):
            warnings.warn(
                "A column as 'key' in getItem is deprecated as of Spark 3.0, and will not "
                "be supported in the future release. Use `column[key]` or `column.key` syntax "
                "instead.",
                FutureWarning,
            )
        return self[key]

    def getField(self, name: Any) -> ParentColumn:
        if isinstance(name, Column):
            warnings.warn(
                "A column as 'name' in getField is deprecated as of Spark 3.0, and will not "
                "be supported in the future release. Use `column[name]` or `column.name` syntax "
                "instead.",
                FutureWarning,
            )
        return self[name]

    def withField(self, fieldName: str, col: ParentColumn) -> ParentColumn:
        if not isinstance(fieldName, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "fieldName", "arg_type": type(fieldName).__name__},
            )

        if not isinstance(col, Column):
            raise PySparkTypeError(
                errorClass="NOT_COLUMN",
                messageParameters={"arg_name": "col", "arg_type": type(col).__name__},
            )

        return Column(WithField(self._expr, fieldName, col._expr))

    def dropFields(self, *fieldNames: str) -> ParentColumn:
        dropField: Optional[DropField] = None
        for fieldName in fieldNames:
            if not isinstance(fieldName, str):
                raise PySparkTypeError(
                    errorClass="NOT_STR",
                    messageParameters={
                        "arg_name": "fieldName",
                        "arg_type": type(fieldName).__name__,
                    },
                )

            if dropField is None:
                dropField = DropField(self._expr, fieldName)
            else:
                dropField = DropField(dropField, fieldName)

        if dropField is None:
            raise PySparkValueError(
                errorClass="CANNOT_BE_EMPTY",
                messageParameters={
                    "item": "dropFields",
                },
            )

        return Column(dropField)

    def __getattr__(self, item: Any) -> ParentColumn:
        if item == "_jc":
            raise PySparkAttributeError(
                errorClass="JVM_ATTRIBUTE_NOT_SUPPORTED", messageParameters={"attr_name": "_jc"}
            )
        if item.startswith("__"):
            raise PySparkAttributeError(
                errorClass="ATTRIBUTE_NOT_SUPPORTED", messageParameters={"attr_name": item}
            )
        return self[item]

    def __getitem__(self, k: Any) -> ParentColumn:
        if isinstance(k, slice):
            if k.step is not None:
                raise PySparkValueError(
                    errorClass="SLICE_WITH_STEP",
                    messageParameters={},
                )
            return self.substr(k.start, k.stop)
        else:
            return Column(UnresolvedExtractValue(self._expr, _to_expr(k)))

    def __iter__(self) -> None:
        raise PySparkTypeError(
            errorClass="NOT_ITERABLE",
            messageParameters={"objectName": "Column"},
        )

    def __nonzero__(self) -> None:
        raise PySparkValueError(
            errorClass="CANNOT_CONVERT_COLUMN_INTO_BOOL",
            messageParameters={},
        )

    __bool__ = __nonzero__


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.column

    globs = pyspark.sql.column.__dict__.copy()
    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.column tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.column,
        globs=globs,
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL,
    )

    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
