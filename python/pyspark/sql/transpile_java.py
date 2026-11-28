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
"""
The ``java`` UDF transpilation target: a Python UDF lowered to the body of a Java method.

Opt in after ``catalyst``::

    spark.conf.set("spark.sql.experimental.optimizer.pyTranspilers", "catalyst,java")

The Catalyst target lowers a body to one expression. A Java method has statements, so this
target also lowers local variables, several ``if`` statements and early returns. Catalyst
is tried first because its expressions stay visible to the optimizer; a Java body is opaque.

What is lowered (anything else declines, falling back to the next transpiler or to Python):

* parameters and results of type ``int`` (any integral column, as ``Long``) or ``bool``;
* ``return``, assignment to a local name, ``if`` / ``else``;
* int, bool and None literals, ``+ - * // %``, unary ``-``, ``not``, a single comparison,
  ``is None`` / ``is not None`` and ``a if test else b``.

Python's semantics are kept by ``TranspiledJavaUDFHelpers``: ``//`` and ``%`` floor, ``==``
treats None as Python does, and arithmetic or ordering on None raises. Truthiness of an int
and ``and`` / ``or`` (which would need short-circuiting) are not lowered.
"""

import ast
import itertools
from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional, Tuple

from pyspark.errors import UnsupportedOperationException
from pyspark.sql.column import Column
from pyspark.sql.functions import col
from pyspark.sql.transpile import AbstractTranspiler
from pyspark.sql.types import BooleanType, DataType, IntegralType, LongType

if TYPE_CHECKING:
    from pyspark.sql._typing import DataTypeOrString

# Keyed by the categories the Catalyst target's input-type variants use.
_TYPES: Dict[str, Tuple[DataType, str]] = {
    "integer": (LongType(), "Long"),
    "bool": (BooleanType(), "Boolean"),
}

_HELPERS = "org.apache.spark.sql.catalyst.expressions.TranspiledJavaUDFHelpers"

_ARITHMETIC = {
    ast.Add: "add",
    ast.Sub: "subtract",
    ast.Mult: "multiply",
    ast.FloorDiv: "floorDivide",
    ast.Mod: "mod",
}

_ORDERING = {
    ast.Lt: "lessThan",
    ast.LtE: "lessThanOrEqual",
    ast.Gt: "greaterThan",
    ast.GtE: "greaterThanOrEqual",
}


class _Value(NamedTuple):
    """Java source for an expression and its category: ``integer``, ``bool`` or ``none``."""

    code: str
    category: str


def _decline(reason: str) -> UnsupportedOperationException:
    return UnsupportedOperationException(f"the java transpiler does not lower {reason}")


def _always_returns(body: List[ast.stmt]) -> bool:
    """Whether every path through ``body`` returns. Java rejects unreachable statements."""
    for stmt in body:
        if isinstance(stmt, ast.Return):
            return True
        if isinstance(stmt, ast.If) and stmt.orelse:
            if _always_returns(stmt.body) and _always_returns(stmt.orelse):
                return True
    return False


class JavaTranspiler(AbstractTranspiler):
    """Transpiles a Python UDF into a Java method body. See the module docstring."""

    variety = "java"

    def _transpile_from_ast(
        self,
        src: Optional[str],
        ast_info: ast.AST,
        function_ast: ast.FunctionDef,
        params: List[str],
        returnType: "DataTypeOrString",
        param_categories: Optional[dict] = None,
    ) -> Optional[Column]:
        if not src or ast_info is None:
            return None
        categories = param_categories or {}
        for index, param in enumerate(params):
            if categories.get(index) not in _TYPES:
                raise _decline(f"parameter {param!r} as {categories.get(index)!r}")
        if isinstance(returnType, IntegralType):
            result_category = "integer"
        elif isinstance(returnType, BooleanType):
            result_category = "bool"
        else:
            raise _decline(f"return type {returnType!r}")
        body = self._method_body(function_ast, params, categories, result_category)

        from pyspark.sql.classic.column import Column as ClassicColumn
        from pyspark.sql.classic.column import _to_java_column
        from pyspark.sql.utils import get_active_spark_context

        sc = get_active_spark_context()
        assert sc._jvm is not None
        children = [
            _to_java_column(col(f"_udf_param_{index}").cast(_TYPES[categories[index]][0]))
            for index in range(len(params))
        ]
        result_type = _TYPES[result_category][0]
        jcol = getattr(
            sc._jvm, "org.apache.spark.sql.execution.python.TranspiledJavaUDFBuilder"
        ).create(function_ast.name, body, children, result_type.json())
        column = ClassicColumn(jcol)
        return column if returnType == result_type else column.cast(returnType)

    # ---------------------------------------------------------------------------------------
    # Statements
    # ---------------------------------------------------------------------------------------

    def _method_body(
        self,
        function_ast: ast.FunctionDef,
        params: List[str],
        categories: dict,
        result_category: str,
    ) -> str:
        """The Java method body for ``function_ast``. Needs no JVM."""
        self._params = {
            param: _Value(f"_udf_arg_{index}", categories[index])
            for index, param in enumerate(params)
        }
        # Python name -> (Java name, category). Java names are numbered, not derived from the
        # Python name, which may not be a valid Java identifier.
        self._locals: Dict[str, Tuple[str, str]] = {}
        self._local_ids = itertools.count()
        statements = self._block(function_ast.body, result_category)
        if not _always_returns(function_ast.body):
            statements.append("return null;")  # Falling off the end returns None.
        return "\n".join(statements)

    def _block(self, body: List[ast.stmt], result_category: str) -> List[str]:
        out: List[str] = []
        for stmt in body:
            out.extend(self._statement(stmt, result_category))
            if _always_returns([stmt]):
                break  # Anything after is unreachable, which Java rejects.
        return out

    def _statement(self, stmt: ast.stmt, result_category: str) -> List[str]:
        match stmt:
            case ast.Pass() | ast.Expr(value=ast.Constant(value=str())):
                return []
            case ast.Return(value=value):
                # A bare `return` is `return None`.
                lowered = _Value("null", "none") if value is None else self._expr(value)
                return [f"return {self._as(lowered, result_category)};"]
            case ast.Assign(targets=[ast.Name(id=name)], value=value):
                return [self._assign(name, self._expr(value))]
            case ast.If(test=test, body=body, orelse=orelse):
                condition = self._as(self._expr(test), "bool")
                # A local first bound inside a branch is out of scope after it in Java, so
                # forget it, which makes a later read decline.
                outer = dict(self._locals)
                out = [f"if ({_HELPERS}.isTrue({condition})) {{"]
                out += _indent(self._block(body, result_category))
                self._locals = dict(outer)
                if orelse:
                    out += ["} else {"] + _indent(self._block(orelse, result_category))
                    self._locals = dict(outer)
                return out + ["}"]
        raise _decline(f"{type(stmt).__name__} statements")

    def _assign(self, name: str, value: _Value) -> str:
        if name in self._params:
            raise _decline(f"assignment to the parameter {name!r}")
        if value.category == "none":
            raise _decline(f"binding the local {name!r} to None, which has no Java type")
        if name in self._locals:
            java_name, category = self._locals[name]
            if category != value.category:
                raise _decline(f"the local {name!r} holding both {category} and {value.category}")
            return f"{java_name} = {value.code};"
        java_name = f"_udf_local_{next(self._local_ids)}"
        self._locals[name] = (java_name, value.category)
        return f"{_TYPES[value.category][1]} {java_name} = {value.code};"

    # ---------------------------------------------------------------------------------------
    # Expressions
    # ---------------------------------------------------------------------------------------

    def _expr(self, node: ast.expr) -> _Value:
        match node:
            case ast.Constant(value=None):
                return _Value("null", "none")
            case ast.Constant(value=bool() as value):  # Before int: bool is an int subclass.
                return _Value(f"Boolean.valueOf({str(value).lower()})", "bool")
            case ast.Constant(value=int() as value) if -(2**63) <= value < 2**63:
                return _Value(f"Long.valueOf({value}L)", "integer")
            case ast.Name(id=name) if name in self._locals:
                return _Value(*self._locals[name])
            case ast.Name(id=name) if name in self._params:
                return self._params[name]
            case ast.UnaryOp(op=ast.USub(), operand=operand):
                return _Value(self._call("negate", operand), "integer")
            case ast.UnaryOp(op=ast.Not(), operand=operand):
                return _Value(f"{_HELPERS}.not({self._as(self._expr(operand), 'bool')})", "bool")
            case ast.BinOp(left=left, op=op, right=right) if type(op) in _ARITHMETIC:
                return _Value(self._call(_ARITHMETIC[type(op)], left, right), "integer")
            case ast.Compare(left=left, ops=[op], comparators=[right]):
                return self._compare(left, op, right)
            case ast.IfExp(test=test, body=body, orelse=orelse):
                condition = self._as(self._expr(test), "bool")
                when_true, when_false = self._expr(body), self._expr(orelse)
                category = _common(when_true, when_false)
                return _Value(
                    f"({_HELPERS}.isTrue({condition}) ? {self._as(when_true, category)} : "
                    f"{self._as(when_false, category)})",
                    category,
                )
        raise _decline(f"the expression {ast.unparse(node)!r}")

    def _call(self, helper: str, *operands: ast.expr) -> str:
        """A helper call over integer operands. Java, like Python, evaluates left to right."""
        args = ", ".join(self._as(self._expr(operand), "integer") for operand in operands)
        return f"{_HELPERS}.{helper}({args})"

    def _compare(self, left: ast.expr, op: ast.cmpop, right: ast.expr) -> _Value:
        if isinstance(op, (ast.Is, ast.IsNot)):
            if not (isinstance(right, ast.Constant) and right.value is None):
                raise _decline("`is` against anything but None")
            test = "==" if isinstance(op, ast.Is) else "!="
            return _Value(f"Boolean.valueOf({self._expr(left).code} {test} null)", "bool")
        lowered_left, lowered_right = self._expr(left), self._expr(right)
        category = _common(lowered_left, lowered_right)
        left_code = self._as(lowered_left, category)
        right_code = self._as(lowered_right, category)
        if isinstance(op, (ast.Eq, ast.NotEq)) and category in _TYPES:
            helper = "equalsLong" if category == "integer" else "equalsBoolean"
            call = f"{_HELPERS}.{helper}({left_code}, {right_code})"
            return _Value(call if isinstance(op, ast.Eq) else f"{_HELPERS}.not({call})", "bool")
        if type(op) in _ORDERING and category == "integer":
            return _Value(f"{_HELPERS}.{_ORDERING[type(op)]}({left_code}, {right_code})", "bool")
        raise _decline(f"`{type(op).__name__}` over {category} operands")

    def _as(self, value: _Value, category: str) -> str:
        """``value``'s code where a ``category`` value is needed. None fits any category."""
        if value.category == category:
            return value.code
        if value.category == "none" and category in _TYPES:
            return f"(({_TYPES[category][1]}) {value.code})"
        raise _decline(f"a {value.category} value where a {category} one is needed")


def _common(left: _Value, right: _Value) -> str:
    """The category two operands share, treating None as fitting either."""
    return right.category if left.category == "none" else left.category


def _indent(lines: List[str]) -> List[str]:
    return [f"  {line}" for line in lines]


JavaTranspiler.register()
