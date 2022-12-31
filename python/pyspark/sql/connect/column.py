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
import warnings

from typing import (
    TYPE_CHECKING,
    Callable,
    Any,
    Union,
    overload,
    Optional,
)

from pyspark import SparkContext, SparkConf
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
            other, (bool, float, int, str, datetime.datetime, datetime.date, decimal.Decimal)
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
        raise ValueError(
            "Cannot apply 'in' operator against a column: please use 'contains' "
            "in a string column or 'array_contains' function for an array column."
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
            _value = LiteralExpression._from_value(value)

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
        from pyspark.sql.connect.window import WindowSpec

        if not isinstance(window, WindowSpec):
            raise TypeError(
                f"window should be WindowSpec, but got {type(window).__name__} {window}"
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
            raise TypeError(
                f"fieldName should be a string, but got {type(fieldName).__name__} {fieldName}"
            )

        if not isinstance(col, Column):
            raise TypeError(f"col should be a Column, but got {type(col).__name__} {col}")

        return Column(WithField(self._expr, fieldName, col._expr))

    withField.__doc__ = PySparkColumn.withField.__doc__

    def dropFields(self, *fieldNames: str) -> "Column":
        dropField: Optional[DropField] = None
        for fieldName in fieldNames:
            if not isinstance(fieldName, str):
                raise TypeError(
                    f"fieldName should be a string, but got {type(fieldName).__name__} {fieldName}"
                )

            if dropField is None:
                dropField = DropField(self._expr, fieldName)
            else:
                dropField = DropField(dropField, fieldName)

        if dropField is None:
            raise ValueError("dropFields requires at least 1 field")

        return Column(dropField)

    dropFields.__doc__ = PySparkColumn.dropFields.__doc__

    def __getattr__(self, item: Any) -> "Column":
        if item.startswith("__"):
            raise AttributeError(item)
        return self[item]

    def __getitem__(self, k: Any) -> "Column":
        if isinstance(k, slice):
            if k.step is not None:
                raise ValueError("slice with step is not supported.")
            return self.substr(k.start, k.stop)
        else:
            return Column(UnresolvedExtractValue(self._expr, LiteralExpression._from_value(k)))

    def __iter__(self) -> None:
        raise TypeError("Column is not iterable")

    def __nonzero__(self) -> None:
        raise ValueError(
            "Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
            "'~' for 'not' when building DataFrame boolean expressions."
        )

    __bool__ = __nonzero__


Column.__doc__ = PySparkColumn.__doc__


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

    os.chdir(os.environ["SPARK_HOME"])

    if should_test_connect:
        import pyspark.sql.connect.column

        globs = pyspark.sql.connect.column.__dict__.copy()
        # Works around to create a regular Spark session
        sc = SparkContext("local[4]", "sql.connect.column tests", conf=SparkConf())
        globs["_spark"] = PySparkSession(sc, options={"spark.app.name": "sql.connect.column tests"})

        # Creates a remote Spark session.
        os.environ["SPARK_REMOTE"] = "sc://localhost"
        globs["spark"] = PySparkSession.builder.remote("sc://localhost").getOrCreate()

        # TODO(SPARK-41746): SparkSession.createDataFrame does not support nested datatypes
        del pyspark.sql.connect.column.Column.dropFields.__doc__
        # TODO(SPARK-41772): Enable pyspark.sql.connect.column.Column.withField doctest
        del pyspark.sql.connect.column.Column.withField.__doc__
        # TODO(SPARK-41745): SparkSession.createDataFrame does not respect the column names in
        #  the row
        del pyspark.sql.connect.column.Column.bitwiseAND.__doc__
        del pyspark.sql.connect.column.Column.bitwiseOR.__doc__
        del pyspark.sql.connect.column.Column.bitwiseXOR.__doc__
        # TODO(SPARK-41745): SparkSession.createDataFrame does not respect the column names in
        #  the row
        del pyspark.sql.connect.column.Column.eqNullSafe.__doc__
        # TODO(SPARK-41745): SparkSession.createDataFrame does not respect the column names in
        #  the row
        del pyspark.sql.connect.column.Column.isNotNull.__doc__
        del pyspark.sql.connect.column.Column.isNull.__doc__
        del pyspark.sql.connect.column.Column.getField.__doc__
        del pyspark.sql.connect.column.Column.getItem.__doc__

        (failure_count, test_count) = doctest.testmod(
            pyspark.sql.connect.column,
            globs=globs,
            optionflags=doctest.ELLIPSIS
            | doctest.NORMALIZE_WHITESPACE
            | doctest.IGNORE_EXCEPTION_DETAIL,
        )

        globs["spark"].stop()
        globs["_spark"].stop()
        if failure_count:
            sys.exit(-1)
    else:
        print(
            f"Skipping pyspark.sql.connect.column doctests: {connect_requirement_message}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    _test()
