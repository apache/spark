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

import datetime
import decimal

from typing import (
    TYPE_CHECKING,
    Callable,
    Any,
    Union,
    overload,
    Optional,
)

from pyspark.sql.types import DataType
from pyspark.sql.column import Column as PySparkColumn

import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.expressions import (
    Expression,
    UnresolvedFunction,
    SQLExpression,
    LiteralExpression,
    CaseWhen,
    SortOrder,
    CastExpression,
    WindowExpression,
)


if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.window import WindowSpec


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

        if isinstance(
            other, (bool, float, int, str, datetime.datetime, datetime.date, decimal.Decimal)
        ):
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


class Column:
    def __init__(self, expr: "Expression") -> None:
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

    # container operators
    def __contains__(self, item: Any) -> None:
        raise ValueError(
            "Cannot apply 'in' operator against a column: please use 'contains' "
            "in a string column or 'array_contains' function for an array column."
        )

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
        if type(startPos) != type(length):
            raise TypeError(
                "startPos and length must be the same type. "
                "Got {startPos_t} and {length_t}, respectively.".format(
                    startPos_t=type(startPos),
                    length_t=type(length),
                )
            )

        if isinstance(length, Column):
            length_expr = length._expr
        elif isinstance(length, int):
            length_expr = LiteralExpression._from_value(length)
        else:
            raise TypeError("Unsupported type for substr().")

        if isinstance(startPos, Column):
            start_expr = startPos._expr
        elif isinstance(startPos, int):
            start_expr = LiteralExpression._from_value(startPos)
        else:
            raise TypeError("Unsupported type for substr().")

        return Column(UnresolvedFunction("substring", [self._expr, start_expr, length_expr]))

    substr.__doc__ = PySparkColumn.substr.__doc__

    def __eq__(self, other: Any) -> "Column":  # type: ignore[override]
        """Returns a binary expression with the current column as the left
        side and the other expression as the right side.
        """
        from pyspark.sql.connect.functions import lit

        if isinstance(
            other, (bool, float, int, str, datetime.datetime, datetime.date, decimal.Decimal)
        ):
            other = lit(other)
        return scalar_function("==", self, other)

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        return self._expr.to_plan(session)

    def alias(self, *alias: str, **kwargs: Any) -> "Column":
        return Column(self._expr.alias(*alias, **kwargs))

    alias.__doc__ = PySparkColumn.alias.__doc__

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
            return Column(CastExpression(expr=self._expr, data_type=dataType))
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

    def __nonzero__(self) -> None:
        raise ValueError(
            "Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
            "'~' for 'not' when building DataFrame boolean expressions."
        )

    __bool__ = __nonzero__


Column.__doc__ = PySparkColumn.__doc__
