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

from typing import List, Union, cast, get_args, TYPE_CHECKING

import pyspark.sql.connect.proto as proto

PrimitiveType = Union[str, int, bool, float]
ExpressionOrString = Union[str, "Expression"]
ColumnOrString = Union[str, "ColumnRef"]

if TYPE_CHECKING:
    from pyspark.sql.connect.client import RemoteSparkSession
    import pyspark.sql.connect.proto as proto


class Expression(object):
    """
    Expression base class.
    """

    def __init__(self) -> None:  # type: ignore[name-defined]
        pass

    def to_plan(self, session: "RemoteSparkSession") -> "proto.Expression":  # type: ignore
        ...

    def __str__(self) -> str:
        ...


class LiteralExpression(Expression):
    """A literal expression.

    The Python types are converted best effort into the relevant proto types. On the Spark Connect
    server side, the proto types are converted to the Catalyst equivalents."""

    def __init__(self, value: PrimitiveType) -> None:  # type: ignore[name-defined]
        super().__init__()
        self._value = value

    def to_plan(self, session: "RemoteSparkSession") -> "proto.Expression":
        """Converts the literal expression to the literal in proto.

        TODO(SPARK-40533) This method always assumes the largest type and can thus
             create weird interpretations of the literal."""
        value_type = type(self._value)
        exp = proto.Expression()
        if value_type is int:
            exp.literal.i32 = cast(int, self._value)
        elif value_type is str:
            exp.literal.string = cast(str, self._value)
        elif value_type is float:
            exp.literal.fp64 = cast(float, self._value)
        else:
            raise ValueError(f"Could not convert literal for type {type(self._value)}")

        return exp

    def __str__(self) -> str:
        return f"Literal({self._value})"


def _bin_op(name: str, doc: str = "binary function", reverse=False):
    def _(self: "ColumnRef", other) -> Expression:
        if isinstance(other, get_args(PrimitiveType)):
            other = LiteralExpression(other)
        if not reverse:
            return ScalarFunctionExpression(name, self, other)
        else:
            return ScalarFunctionExpression(name, other, self)

    return _


class ColumnRef(Expression):
    """Represents a column reference. There is no guarantee that this column
    actually exists. In the context of this project, we refer by its name and
    treat it as an unresolved attribute. Attributes that have the same fully
    qualified name are identical"""

    @classmethod
    def from_qualified_name(cls, name) -> "ColumnRef":
        return ColumnRef(*name.split("."))

    def __init__(self, *parts: str) -> None:  # type: ignore[name-defined]
        super().__init__()
        self._parts: List[str] = list(filter(lambda x: x is not None, list(parts)))

    def name(self) -> str:
        """Returns the qualified name of the column reference."""
        return ".".join(self._parts)

    __gt__ = _bin_op("gt")
    __lt__ = _bin_op("lt")
    __add__ = _bin_op("plus")
    __sub__ = _bin_op("minus")
    __mul__ = _bin_op("multiply")
    __div__ = _bin_op("divide")
    __truediv__ = _bin_op("divide")
    __mod__ = _bin_op("modulo")
    __radd__ = _bin_op("plus", reverse=True)
    __rsub__ = _bin_op("minus", reverse=True)
    __rmul__ = _bin_op("multiply", reverse=True)
    __rdiv__ = _bin_op("divide", reverse=True)
    __rtruediv__ = _bin_op("divide", reverse=True)
    __pow__ = _bin_op("pow")
    __rpow__ = _bin_op("pow", reverse=True)
    __ge__ = _bin_op("greterEquals")
    __le__ = _bin_op("lessEquals")

    def __eq__(self, other) -> Expression:  # type: ignore[override]
        """Returns a binary expression with the current column as the left
        side and the other expression as the right side.
        """
        if isinstance(other, get_args(PrimitiveType)):
            other = LiteralExpression(other)
        return ScalarFunctionExpression("eq", self, other)

    def to_plan(self, session: "RemoteSparkSession") -> proto.Expression:
        """Returns the Proto representation of the expression."""
        expr = proto.Expression()
        expr.unresolved_attribute.parts.extend(self._parts)
        return expr

    def desc(self):
        return SortOrder(self, ascending=False)

    def asc(self):
        return SortOrder(self, ascending=True)

    def __str__(self) -> str:
        return f"Column({'.'.join(self._parts)})"


class SortOrder(Expression):
    def __init__(self, col: ColumnRef, ascending=True, nullsLast=True) -> None:
        super().__init__()
        self.ref = col
        self.ascending = ascending
        self.nullsLast = nullsLast

    def __str__(self) -> str:
        return str(self.ref) + " ASC" if self.ascending else " DESC"

    def to_plan(self, session: "RemoteSparkSession") -> proto.Expression:
        return self.ref.to_plan()


class ScalarFunctionExpression(Expression):
    def __init__(
        self,
        op: str,
        *args: Expression,
    ) -> None:
        super().__init__()
        self._args = args
        self._op = op

    def to_plan(self, session: "RemoteSparkSession") -> proto.Expression:
        fun = proto.Expression()
        fun.unresolved_function.parts.append(self._op)
        fun.unresolved_function.arguments.extend([x.to_plan(session) for x in self._args])
        return fun

    def __str__(self) -> str:
        return f"({self._op} ({', '.join([str(x) for x in self._args])}))"
