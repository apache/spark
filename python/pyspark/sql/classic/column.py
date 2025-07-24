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
import warnings
from typing import (
    cast,
    overload,
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union,
)

from pyspark.sql.column import Column as ParentColumn
from pyspark.errors import PySparkAttributeError, PySparkTypeError, PySparkValueError
from pyspark.errors.utils import with_origin_to_class
from pyspark.sql.types import DataType
from pyspark.sql.utils import get_active_spark_context, enum_to_value

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.core.context import SparkContext
    from pyspark.sql._typing import ColumnOrName, LiteralType, DecimalLiteral, DateTimeLiteral
    from pyspark.sql.window import WindowSpec

__all__ = ["Column"]


def _create_column_from_literal(
    literal: Union["LiteralType", "DecimalLiteral", "DateTimeLiteral", "ParentColumn"]
) -> "JavaObject":
    from py4j.java_gateway import JVMView

    sc = get_active_spark_context()
    return cast(JVMView, sc._jvm).functions.lit(enum_to_value(literal))


def _create_column_from_name(name: str) -> "JavaObject":
    from py4j.java_gateway import JVMView

    sc = get_active_spark_context()
    return cast(JVMView, sc._jvm).functions.col(name)


def _to_java_column(col: "ColumnOrName") -> "JavaObject":
    if isinstance(col, Column):
        jcol = col._jc
    elif isinstance(col, str):
        jcol = _create_column_from_name(col)
    else:
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "col", "arg_type": type(col).__name__},
        )
    return jcol


@overload
def _to_seq(sc: "SparkContext", cols: Iterable["JavaObject"]) -> "JavaObject":
    ...


@overload
def _to_seq(
    sc: "SparkContext",
    cols: Iterable["ColumnOrName"],
    converter: Optional[Callable[["ColumnOrName"], "JavaObject"]],
) -> "JavaObject":
    ...


def _to_seq(
    sc: "SparkContext",
    cols: Union[Iterable["ColumnOrName"], Iterable["JavaObject"]],
    converter: Optional[Callable[["ColumnOrName"], "JavaObject"]] = None,
) -> "JavaObject":
    """
    Convert a list of Columns (or names) into a JVM Seq of Column.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    assert sc._jvm is not None
    return sc._jvm.PythonUtils.toSeq(cols)


def _to_list(
    sc: "SparkContext",
    cols: List["ColumnOrName"],
    converter: Optional[Callable[["ColumnOrName"], "JavaObject"]] = None,
) -> "JavaObject":
    """
    Convert a list of Columns (or names) into a JVM (Scala) List of Columns.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    assert sc._jvm is not None
    return sc._jvm.PythonUtils.toList(cols)


def _unary_op(name: str, self: ParentColumn) -> ParentColumn:
    """Create a method for given unary operator"""

    jc = getattr(self._jc, name)()
    return Column(jc)


def _func_op(name: str, self: ParentColumn) -> ParentColumn:
    from py4j.java_gateway import JVMView

    sc = get_active_spark_context()
    jc = getattr(cast(JVMView, sc._jvm).functions, name)(self._jc)
    return Column(jc)


def _bin_func_op(
    name: str,
    self: ParentColumn,
    other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
    reverse: bool = False,
) -> ParentColumn:
    from py4j.java_gateway import JVMView

    sc = get_active_spark_context()
    fn = getattr(cast(JVMView, sc._jvm).functions, name)
    jc = other._jc if isinstance(other, ParentColumn) else _create_column_from_literal(other)
    njc = fn(self._jc, jc) if not reverse else fn(jc, self._jc)
    return Column(njc)


def _bin_op(
    name: str,
    self: ParentColumn,
    other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
) -> ParentColumn:
    """Create a method for given binary operator"""
    jc = other._jc if isinstance(other, ParentColumn) else enum_to_value(other)
    njc = getattr(self._jc, name)(jc)
    return Column(njc)


def _reverse_op(
    name: str,
    self: ParentColumn,
    other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
) -> ParentColumn:
    """Create a method for binary operator (this object is on right side)"""
    jother = _create_column_from_literal(other)
    jc = getattr(jother, name)(self._jc)
    return Column(jc)


@with_origin_to_class
class Column(ParentColumn):
    def __new__(cls, *args: Any, **kwargs: Any) -> "Column":
        return object.__new__(cls)

    def __getnewargs__(self) -> Tuple[Any, ...]:
        return (self._jc,)

    def __init__(self, jc: "JavaObject") -> None:
        self._jc = jc

    # arithmetic operators
    def __neg__(self) -> ParentColumn:
        return _func_op("negate", self)

    def __add__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("plus", self, other)

    def __sub__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("minus", self, other)

    def __mul__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("multiply", self, other)

    def __div__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("divide", self, other)

    def __truediv__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("divide", self, other)

    def __mod__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("mod", self, other)

    def __radd__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("plus", self, other)

    def __rsub__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _reverse_op("minus", self, other)

    def __rmul__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("multiply", self, other)

    def __rdiv__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _reverse_op("divide", self, other)

    def __rtruediv__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _reverse_op("divide", self, other)

    def __rmod__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _reverse_op("mod", self, other)

    def __pow__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_func_op("pow", self, other)

    def __rpow__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_func_op("pow", self, other, reverse=True)

    # logistic operators
    def __eq__(  # type: ignore[override]
        self,
        other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
    ) -> ParentColumn:
        return _bin_op("equalTo", self, other)

    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> ParentColumn:
        return _bin_op("notEqual", self, other)

    def __lt__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("lt", self, other)

    def __le__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("leq", self, other)

    def __ge__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("geq", self, other)

    def __gt__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("gt", self, other)

    def eqNullSafe(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("eqNullSafe", self, other)

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    def __and__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        from pyspark.sql.functions import lit

        return _bin_op("and", self, lit(other))

    def __or__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        from pyspark.sql.functions import lit

        return _bin_op("or", self, lit(other))

    def __invert__(self) -> ParentColumn:
        return _func_op("not", self)

    def __rand__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        from pyspark.sql.functions import lit

        return _bin_op("and", self, lit(other))

    def __ror__(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        from pyspark.sql.functions import lit

        return _bin_op("or", self, lit(other))

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
        return _bin_op("bitwiseOR", self, other)

    def bitwiseAND(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("bitwiseAND", self, other)

    def bitwiseXOR(
        self, other: Union[ParentColumn, "LiteralType", "DecimalLiteral", "DateTimeLiteral"]
    ) -> ParentColumn:
        return _bin_op("bitwiseXOR", self, other)

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

        return Column(self._jc.withField(fieldName, col._jc))

    def dropFields(self, *fieldNames: str) -> ParentColumn:
        sc = get_active_spark_context()
        jc = self._jc.dropFields(_to_seq(sc, fieldNames))
        return Column(jc)

    def __getattr__(self, item: Any) -> ParentColumn:
        if item.startswith("__"):
            raise PySparkAttributeError(
                errorClass="CANNOT_ACCESS_TO_DUNDER",
                messageParameters={},
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
            return _bin_op("apply", self, k)

    def __iter__(self) -> None:
        raise PySparkTypeError(
            errorClass="NOT_ITERABLE", messageParameters={"objectName": "Column"}
        )

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

    def like(self: ParentColumn, other: str) -> ParentColumn:
        njc = getattr(self._jc, "like")(enum_to_value(other))
        return Column(njc)

    def rlike(self: ParentColumn, other: str) -> ParentColumn:
        njc = getattr(self._jc, "rlike")(enum_to_value(other))
        return Column(njc)

    def ilike(self: ParentColumn, other: str) -> ParentColumn:
        njc = getattr(self._jc, "ilike")(enum_to_value(other))
        return Column(njc)

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
        if isinstance(startPos, int):
            jc = self._jc.substr(startPos, length)
        elif isinstance(startPos, Column):
            jc = self._jc.substr(startPos._jc, cast(ParentColumn, length)._jc)
        else:
            raise PySparkTypeError(
                errorClass="NOT_COLUMN_OR_INT",
                messageParameters={"arg_name": "startPos", "arg_type": type(startPos).__name__},
            )
        return Column(jc)

    def isin(self, *cols: Any) -> ParentColumn:
        from pyspark.sql.classic.dataframe import DataFrame

        if len(cols) == 1 and isinstance(cols[0], DataFrame):
            df: DataFrame = cols[0]
            jc = self._jc.isin(df._jdf)
            return Column(jc)

        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            cols = cast(Tuple, cols[0])
        cols = cast(
            Tuple,
            [c._jc if isinstance(c, Column) else _create_column_from_literal(c) for c in cols],
        )
        sc = get_active_spark_context()
        jc = getattr(self._jc, "isin")(_to_seq(sc, cols))
        return Column(jc)

    # order
    def asc(self) -> ParentColumn:
        return _unary_op("asc", self)

    def asc_nulls_first(self) -> ParentColumn:
        return _unary_op("asc_nulls_first", self)

    def asc_nulls_last(self) -> ParentColumn:
        return _unary_op("asc_nulls_last", self)

    def desc(self) -> ParentColumn:
        return _unary_op("desc", self)

    def desc_nulls_first(self) -> ParentColumn:
        return _unary_op("desc_nulls_first", self)

    def desc_nulls_last(self) -> ParentColumn:
        return _unary_op("desc_nulls_last", self)

    def isNull(self) -> ParentColumn:
        return _unary_op("isNull", self)

    def isNotNull(self) -> ParentColumn:
        return _unary_op("isNotNull", self)

    def isNaN(self) -> ParentColumn:
        return _unary_op("isNaN", self)

    def alias(self, *alias: str, **kwargs: Any) -> ParentColumn:
        metadata = kwargs.pop("metadata", None)
        assert not kwargs, "Unexpected kwargs where passed: %s" % kwargs

        sc = get_active_spark_context()
        if len(alias) == 1:
            if metadata:
                assert sc._jvm is not None
                jmeta = getattr(sc._jvm, "org.apache.spark.sql.types.Metadata").fromJson(
                    json.dumps(metadata)
                )
                return Column(getattr(self._jc, "as")(alias[0], jmeta))
            else:
                return Column(getattr(self._jc, "as")(alias[0]))
        else:
            if metadata is not None:
                raise PySparkValueError(
                    errorClass="ONLY_ALLOWED_FOR_SINGLE_COLUMN",
                    messageParameters={"arg_name": "metadata"},
                )
            return Column(getattr(self._jc, "as")(_to_seq(sc, list(alias))))

    def name(self, *alias: str, **kwargs: Any) -> ParentColumn:
        return self.alias(*alias, **kwargs)

    def cast(self, dataType: Union[DataType, str]) -> ParentColumn:
        if isinstance(dataType, str):
            jc = self._jc.cast(dataType)
        elif isinstance(dataType, DataType):
            from pyspark.sql import SparkSession

            spark = SparkSession._getActiveSessionOrCreate()
            jdt = spark._jsparkSession.parseDataType(dataType.json())
            jc = self._jc.cast(jdt)
        else:
            raise PySparkTypeError(
                errorClass="NOT_DATATYPE_OR_STR",
                messageParameters={"arg_name": "dataType", "arg_type": type(dataType).__name__},
            )
        return Column(jc)

    def try_cast(self, dataType: Union[DataType, str]) -> ParentColumn:
        if isinstance(dataType, str):
            jc = self._jc.try_cast(dataType)
        elif isinstance(dataType, DataType):
            from pyspark.sql import SparkSession

            spark = SparkSession._getActiveSessionOrCreate()
            jdt = spark._jsparkSession.parseDataType(dataType.json())
            jc = self._jc.try_cast(jdt)
        else:
            raise PySparkTypeError(
                errorClass="NOT_DATATYPE_OR_STR",
                messageParameters={"arg_name": "dataType", "arg_type": type(dataType).__name__},
            )
        return Column(jc)

    def astype(self, dataType: Union[DataType, str]) -> ParentColumn:
        return self.cast(dataType)

    def between(
        self,
        lowerBound: Union[ParentColumn, "LiteralType", "DateTimeLiteral", "DecimalLiteral"],
        upperBound: Union[ParentColumn, "LiteralType", "DateTimeLiteral", "DecimalLiteral"],
    ) -> ParentColumn:
        return (self >= lowerBound) & (self <= upperBound)

    def when(self, condition: ParentColumn, value: Any) -> ParentColumn:
        if not isinstance(condition, Column):
            raise PySparkTypeError(
                errorClass="NOT_COLUMN",
                messageParameters={"arg_name": "condition", "arg_type": type(condition).__name__},
            )
        v = value._jc if isinstance(value, Column) else enum_to_value(value)
        jc = self._jc.when(condition._jc, v)
        return Column(jc)

    def otherwise(self, value: Any) -> ParentColumn:
        v = value._jc if isinstance(value, Column) else enum_to_value(value)
        jc = self._jc.otherwise(v)
        return Column(jc)

    def over(self, window: "WindowSpec") -> ParentColumn:
        from pyspark.sql.classic.window import WindowSpec

        if not isinstance(window, WindowSpec):
            raise PySparkTypeError(
                errorClass="NOT_WINDOWSPEC",
                messageParameters={"arg_name": "window", "arg_type": type(window).__name__},
            )
        jc = self._jc.over(window._jspec)
        return Column(jc)

    def outer(self) -> ParentColumn:
        jc = self._jc.outer()
        return Column(jc)

    def __nonzero__(self) -> None:
        raise PySparkValueError(
            errorClass="CANNOT_CONVERT_COLUMN_INTO_BOOL",
            messageParameters={},
        )

    __bool__ = __nonzero__

    def __repr__(self) -> str:
        return "Column<'%s'>" % self._jc.toString()


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.column

    # It inherits docstrings but doctests cannot detect them so we run
    # the parent classe's doctests here directly.
    globs = pyspark.sql.column.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[4]").appName("sql.classic.column tests").getOrCreate()
    )
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
