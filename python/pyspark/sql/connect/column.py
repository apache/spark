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
    Callable,
    Any,
    Union,
    overload,
    Optional,
)

from pyspark.errors import PySparkTypeError, PySparkAttributeError, PySparkValueError
from pyspark.sql.types import DataType
from pyspark.sql.column import Column as PySparkColumn

import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.expressions import (
    Expression,
    UnresolvedFunction,
    UnresolvedExtractValue,
    LiteralExpression,
    CaseWhen,
    SortOrder,
    CastExpression,
    WindowExpression,
    WithField,
    DropField,
)


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import (
        LiteralType,
        DateTimeLiteral,
        DecimalLiteral,
    )
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.window import WindowSpec


def _func_op(name: str, doc: Optional[str] = "") -> Callable[["Column"], "Column"]:
    def wrapped(self: "Column") -> "Column":
        return Column(UnresolvedFunction(name, [self._expr]))

    wrapped.__doc__ = doc
    return wrapped


def _bin_op(
    name: str, doc: Optional[str] = "binary function", reverse: bool = False
) -> Callable[["Column", Any], "Column"]:
    def wrapped(self: "Column", other: Any) -> "Column":
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
            other_expr = other._expr

        if not reverse:
            return Column(UnresolvedFunction(name, [self._expr, other_expr]))
        else:
            return Column(UnresolvedFunction(name, [other_expr, self._expr]))

    wrapped.__doc__ = doc
    return wrapped


def _unary_op(name: str, doc: Optional[str] = "unary function") -> Callable[["Column"], "Column"]:
    def wrapped(self: "Column") -> "Column":
        return Column(UnresolvedFunction(name, [self._expr]))

    wrapped.__doc__ = doc
    return wrapped


class Column:
    def __init__(self, expr: "Expression") -> None:
        if not isinstance(expr, Expression):
            raise PySparkTypeError(
                error_class="NOT_EXPRESSION",
                message_parameters={"arg_name": "expr", "arg_type": type(expr).__name__},
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
    __rmod__ = _bin_op("%", reverse=True)
    __pow__ = _bin_op("power")
    __rpow__ = _bin_op("power", reverse=True)
    __ge__ = _bin_op(">=")
    __le__ = _bin_op("<=")

    eqNullSafe = _bin_op("<=>", PySparkColumn.eqNullSafe.__doc__)

    __neg__ = _func_op("negative")

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    __and__ = _bin_op("and")
    __or__ = _bin_op("or")
    __invert__ = _func_op("not")
    __rand__ = _bin_op("and")
    __ror__ = _bin_op("or")

    # container operators
    def __contains__(self, item: Any) -> None:
        raise PySparkValueError(
            error_class="CANNOT_APPLY_IN_FOR_COLUMN",
            message_parameters={},
        )

    # bitwise operators
    bitwiseOR = _bin_op("|", PySparkColumn.bitwiseOR.__doc__)
    bitwiseAND = _bin_op("&", PySparkColumn.bitwiseAND.__doc__)
    bitwiseXOR = _bin_op("^", PySparkColumn.bitwiseXOR.__doc__)

    isNull = _unary_op("isnull", PySparkColumn.isNull.__doc__)
    isNotNull = _unary_op("isnotnull", PySparkColumn.isNotNull.__doc__)

    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> "Column":
        """binary function"""
        return _func_op("not")(_bin_op("==")(self, other))

    # string methods
    contains = _bin_op("contains", PySparkColumn.contains.__doc__)
    startswith = _bin_op("startswith", PySparkColumn.startswith.__doc__)
    endswith = _bin_op("endswith", PySparkColumn.endswith.__doc__)

    def when(self, condition: "Column", value: Any) -> "Column":
        if not isinstance(condition, Column):
            raise PySparkTypeError(
                error_class="NOT_COLUMN",
                message_parameters={"arg_name": "condition", "arg_type": type(condition).__name__},
            )

        if not isinstance(self._expr, CaseWhen):
            raise PySparkTypeError(
                error_class="INVALID_WHEN_USAGE",
                message_parameters={},
            )

        if self._expr._else_value is not None:
            raise PySparkTypeError(
                error_class="INVALID_WHEN_USAGE",
                message_parameters={},
            )

        if isinstance(value, Column):
            _value = value._expr
        else:
            _value = LiteralExpression._from_value(value)

        _branches = self._expr._branches + [(condition._expr, _value)]

        return Column(CaseWhen(branches=_branches, else_value=None))

    when.__doc__ = PySparkColumn.when.__doc__

    def otherwise(self, value: Any) -> "Column":
        if not isinstance(self._expr, CaseWhen):
            raise PySparkTypeError(
                "otherwise() can only be applied on a Column previously generated by when()"
            )

        if self._expr._else_value is not None:
            raise PySparkTypeError(
                "otherwise() can only be applied once on a Column previously generated by when()"
            )

        if isinstance(value, Column):
            _value = value._expr
        else:
            _value = LiteralExpression._from_value(value)

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
        if type(startPos) != type(length):
            raise PySparkTypeError(
                error_class="NOT_SAME_TYPE",
                message_parameters={
                    "arg_name1": "startPos",
                    "arg_name2": "length",
                    "arg_type1": type(startPos).__name__,
                    "arg_type2": type(length).__name__,
                },
            )

        if isinstance(length, Column):
            length_expr = length._expr
            start_expr = startPos._expr  # type: ignore[union-attr]
        elif isinstance(length, int):
            length_expr = LiteralExpression._from_value(length)
            start_expr = LiteralExpression._from_value(startPos)
        else:
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_INT",
                message_parameters={"arg_name": "length", "arg_type": type(length).__name__},
            )
        return Column(UnresolvedFunction("substring", [self._expr, start_expr, length_expr]))

    substr.__doc__ = PySparkColumn.substr.__doc__

    def __eq__(self, other: Any) -> "Column":  # type: ignore[override]
        """Returns a binary expression with the current column as the left
        side and the other expression as the right side.
        """
        if other is None or isinstance(
            other, (bool, float, int, str, datetime.datetime, datetime.date, decimal.Decimal)
        ):
            other_expr = LiteralExpression._from_value(other)
        else:
            other_expr = other._expr

        return Column(UnresolvedFunction("==", [self._expr, other_expr]))

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        return self._expr.to_plan(session)

    def alias(self, *alias: str, **kwargs: Any) -> "Column":
        return Column(self._expr.alias(*alias, **kwargs))

    alias.__doc__ = PySparkColumn.alias.__doc__

    name = alias

    name.__doc__ = PySparkColumn.name.__doc__

    def asc(self) -> "Column":
        return self.asc_nulls_first()

    asc.__doc__ = PySparkColumn.asc.__doc__

    def asc_nulls_first(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=True, nullsFirst=True))

    asc_nulls_first.__doc__ = PySparkColumn.asc_nulls_first.__doc__

    def asc_nulls_last(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=True, nullsFirst=False))

    asc_nulls_last.__doc__ = PySparkColumn.asc_nulls_last.__doc__

    def desc(self) -> "Column":
        return self.desc_nulls_last()

    desc.__doc__ = PySparkColumn.desc.__doc__

    def desc_nulls_first(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=False, nullsFirst=True))

    desc_nulls_first.__doc__ = PySparkColumn.desc_nulls_first.__doc__

    def desc_nulls_last(self) -> "Column":
        return Column(SortOrder(self._expr, ascending=False, nullsFirst=False))

    desc_nulls_last.__doc__ = PySparkColumn.desc_nulls_last.__doc__

    def cast(self, dataType: Union[DataType, str]) -> "Column":
        if isinstance(dataType, (DataType, str)):
            return Column(CastExpression(expr=self._expr, data_type=dataType))
        else:
            raise PySparkTypeError(
                error_class="NOT_DATATYPE_OR_STR",
                message_parameters={"arg_name": "dataType", "arg_type": type(dataType).__name__},
            )

    cast.__doc__ = PySparkColumn.cast.__doc__

    astype = cast

    def __repr__(self) -> str:
        return "Column<'%s'>" % self._expr.__repr__()

    def over(self, window: "WindowSpec") -> "Column":
        from pyspark.sql.connect.window import WindowSpec

        if not isinstance(window, WindowSpec):
            raise PySparkTypeError(
                error_class="NOT_WINDOWSPEC",
                message_parameters={"arg_name": "window", "arg_type": type(window).__name__},
            )

        return Column(WindowExpression(windowFunction=self._expr, windowSpec=window))

    over.__doc__ = PySparkColumn.over.__doc__

    def isin(self, *cols: Any) -> "Column":
        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            _cols = list(cols[0])
        else:
            _cols = list(cols)

        _exprs = [self._expr]
        for c in _cols:
            if isinstance(c, Column):
                _exprs.append(c._expr)
            else:
                _exprs.append(LiteralExpression._from_value(c))

        return Column(UnresolvedFunction("in", _exprs))

    isin.__doc__ = PySparkColumn.isin.__doc__

    def between(
        self,
        lowerBound: Union["Column", "LiteralType", "DateTimeLiteral", "DecimalLiteral"],
        upperBound: Union["Column", "LiteralType", "DateTimeLiteral", "DecimalLiteral"],
    ) -> "Column":
        return (self >= lowerBound) & (self <= upperBound)

    between.__doc__ = PySparkColumn.between.__doc__

    def getItem(self, key: Any) -> "Column":
        if isinstance(key, Column):
            warnings.warn(
                "A column as 'key' in getItem is deprecated as of Spark 3.0, and will not "
                "be supported in the future release. Use `column[key]` or `column.key` syntax "
                "instead.",
                FutureWarning,
            )
        return self[key]

    getItem.__doc__ = PySparkColumn.getItem.__doc__

    def getField(self, name: Any) -> "Column":
        if isinstance(name, Column):
            warnings.warn(
                "A column as 'name' in getField is deprecated as of Spark 3.0, and will not "
                "be supported in the future release. Use `column[name]` or `column.name` syntax "
                "instead.",
                FutureWarning,
            )
        return self[name]

    getField.__doc__ = PySparkColumn.getField.__doc__

    def withField(self, fieldName: str, col: "Column") -> "Column":
        if not isinstance(fieldName, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "fieldName", "arg_type": type(fieldName).__name__},
            )

        if not isinstance(col, Column):
            raise PySparkTypeError(
                error_class="NOT_COLUMN",
                message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
            )

        return Column(WithField(self._expr, fieldName, col._expr))

    withField.__doc__ = PySparkColumn.withField.__doc__

    def dropFields(self, *fieldNames: str) -> "Column":
        dropField: Optional[DropField] = None
        for fieldName in fieldNames:
            if not isinstance(fieldName, str):
                raise PySparkTypeError(
                    error_class="NOT_STR",
                    message_parameters={
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
                error_class="CANNOT_BE_EMPTY",
                message_parameters={
                    "item": "dropFields",
                },
            )

        return Column(dropField)

    dropFields.__doc__ = PySparkColumn.dropFields.__doc__

    def __getattr__(self, item: Any) -> "Column":
        if item == "_jc":
            raise PySparkAttributeError(
                error_class="JVM_ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": "_jc"}
            )
        if item.startswith("__"):
            raise PySparkAttributeError(
                error_class="ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": item}
            )
        return self[item]

    def __getitem__(self, k: Any) -> "Column":
        if isinstance(k, slice):
            if k.step is not None:
                raise PySparkValueError(
                    error_class="SLICE_WITH_STEP",
                    message_parameters={},
                )
            return self.substr(k.start, k.stop)
        else:
            return Column(UnresolvedExtractValue(self._expr, LiteralExpression._from_value(k)))

    def __iter__(self) -> None:
        raise PySparkTypeError(
            error_class="NOT_ITERABLE",
            message_parameters={"objectName": "Column"},
        )

    def __nonzero__(self) -> None:
        raise PySparkValueError(
            error_class="CANNOT_CONVERT_COLUMN_INTO_BOOL",
            message_parameters={},
        )

    __bool__ = __nonzero__


Column.__doc__ = PySparkColumn.__doc__


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.column

    globs = pyspark.sql.connect.column.__dict__.copy()
    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.column tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.column,
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
