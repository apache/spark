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
from typing import get_args, TYPE_CHECKING, Callable, Any

import json
import decimal
import datetime

from pyspark.sql.types import TimestampType, DayTimeIntervalType, DateType

import pyspark.sql.connect.proto as proto

if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient
    import pyspark.sql.connect.proto as proto


def _bin_op(
    name: str, doc: str = "binary function", reverse: bool = False
) -> Callable[["Column", Any], "Expression"]:
    def _(self: "Column", other: Any) -> "Expression":
        from pyspark.sql.connect._typing import PrimitiveType

        if isinstance(other, get_args(PrimitiveType)):
            other = LiteralExpression(other)
        if not reverse:
            return ScalarFunctionExpression(name, self, other)
        else:
            return ScalarFunctionExpression(name, other, self)

    return _


class Expression(object):
    """
    Expression base class.
    """

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

    def __eq__(self, other: Any) -> "Expression":  # type: ignore[override]
        """Returns a binary expression with the current column as the left
        side and the other expression as the right side.
        """
        from pyspark.sql.connect._typing import PrimitiveType

        if isinstance(other, get_args(PrimitiveType)):
            other = LiteralExpression(other)
        return ScalarFunctionExpression("==", self, other)

    def __init__(self) -> None:
        pass

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        ...

    def __str__(self) -> str:
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

    def __str__(self) -> str:
        return f"Alias({self._parent}, ({','.join(self._alias)}))"


class LiteralExpression(Expression):
    """A literal expression.

    The Python types are converted best effort into the relevant proto types. On the Spark Connect
    server side, the proto types are converted to the Catalyst equivalents."""

    def __init__(self, value: Any) -> None:
        super().__init__()
        self._value = value

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        """Converts the literal expression to the literal in proto.

        TODO(SPARK-40533) This method always assumes the largest type and can thus
             create weird interpretations of the literal."""
        expr = proto.Expression()
        if self._value is None:
            expr.literal.null = True
        elif isinstance(self._value, (bytes, bytearray)):
            expr.literal.binary = bytes(self._value)
        elif isinstance(self._value, bool):
            expr.literal.boolean = bool(self._value)
        elif isinstance(self._value, int):
            expr.literal.long = int(self._value)
        elif isinstance(self._value, float):
            expr.literal.double = float(self._value)
        elif isinstance(self._value, str):
            expr.literal.string = str(self._value)
        elif isinstance(self._value, decimal.Decimal):
            expr.literal.decimal.value = str(self._value)
            expr.literal.decimal.precision = int(decimal.getcontext().prec)
        elif isinstance(self._value, datetime.datetime):
            expr.literal.timestamp = TimestampType().toInternal(self._value)
        elif isinstance(self._value, datetime.date):
            expr.literal.date = DateType().toInternal(self._value)
        elif isinstance(self._value, datetime.timedelta):
            interval = DayTimeIntervalType().toInternal(self._value)
            assert interval is not None
            expr.literal.day_time_interval = int(interval)
        elif isinstance(self._value, list):
            expr.literal.array.SetInParent()
            for item in list(self._value):
                if isinstance(item, LiteralExpression):
                    expr.literal.array.values.append(item.to_plan(session).literal)
                else:
                    expr.literal.array.values.append(
                        LiteralExpression(item).to_plan(session).literal
                    )
        elif isinstance(self._value, tuple):
            expr.literal.struct.SetInParent()
            for item in list(self._value):
                if isinstance(item, LiteralExpression):
                    expr.literal.struct.fields.append(item.to_plan(session).literal)
                else:
                    expr.literal.struct.fields.append(
                        LiteralExpression(item).to_plan(session).literal
                    )
        elif isinstance(self._value, dict):
            expr.literal.map.SetInParent()
            for key, value in dict(self._value).items():
                pair = proto.Expression.Literal.Map.Pair()
                if isinstance(key, LiteralExpression):
                    pair.key.CopyFrom(key.to_plan(session).literal)
                else:
                    pair.key.CopyFrom(LiteralExpression(key).to_plan(session).literal)
                if isinstance(value, LiteralExpression):
                    pair.value.CopyFrom(value.to_plan(session).literal)
                else:
                    pair.value.CopyFrom(LiteralExpression(value).to_plan(session).literal)
                expr.literal.map.pairs.append(pair)
        else:
            raise ValueError(f"Could not convert literal for type {type(self._value)}")

        return expr

    def __str__(self) -> str:
        return f"Literal({self._value})"


class Column(Expression):
    """Represents a column reference. There is no guarantee that this column
    actually exists. In the context of this project, we refer by its name and
    treat it as an unresolved attribute. Attributes that have the same fully
    qualified name are identical"""

    @classmethod
    def from_qualified_name(cls, name: str) -> "Column":
        return Column(name)

    def __init__(self, name: str) -> None:
        super().__init__()
        self._unparsed_identifier: str = name

    def name(self) -> str:
        """Returns the qualified name of the column reference."""
        return self._unparsed_identifier

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        """Returns the Proto representation of the expression."""
        expr = proto.Expression()
        expr.unresolved_attribute.unparsed_identifier = self._unparsed_identifier
        return expr

    def desc(self) -> "SortOrder":
        return SortOrder(self, ascending=False)

    def asc(self) -> "SortOrder":
        return SortOrder(self, ascending=True)

    def __str__(self) -> str:
        return f"Column({self._unparsed_identifier})"


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
    def __init__(self, col: Column, ascending: bool = True, nullsLast: bool = True) -> None:
        super().__init__()
        self.ref = col
        self.ascending = ascending
        self.nullsLast = nullsLast

    def __str__(self) -> str:
        return str(self.ref) + " ASC" if self.ascending else " DESC"

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        return self.ref.to_plan(session)


class ScalarFunctionExpression(Expression):
    def __init__(
        self,
        op: str,
        *args: Expression,
    ) -> None:
        super().__init__()
        self._args = args
        self._op = op

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        fun = proto.Expression()
        fun.unresolved_function.parts.append(self._op)
        fun.unresolved_function.arguments.extend([x.to_plan(session) for x in self._args])
        return fun

    def __str__(self) -> str:
        return f"({self._op} ({', '.join([str(x) for x in self._args])}))"
