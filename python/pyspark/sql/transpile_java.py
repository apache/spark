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
The ``java`` UDF transpilation target: Python UDF to generated Java source.

Opt in after ``catalyst``::

    spark.conf.set("spark.sql.experimental.optimizer.pyTranspilers", "catalyst,java")

Tried left to right; first surviving option wins. ``catalyst`` first is the
recommendation: a Catalyst lowering is ordinary expressions the optimizer can
push, prune and fold, while a generated body is opaque. This target buys reach,
not speed relative to Catalyst -- speed relative to a Python worker.

A Catalyst option is an expression, so it refuses more than one top-level
statement. A Java method has statements, so this target lowers locals, several
statements, early returns, and ``/`` / ``//``.

Each argument is a method parameter, so a body that reads it N times still
evaluates it once (the thing SPARK-58626 is about for Catalyst). The flip side:
unread arguments are still evaluated, matching the interpreted UDF and not a
Catalyst lowering.

Arithmetic propagates null (``None + 1`` is NULL, same as Catalyst). Ordering
raises on null (same as Catalyst). ``NaN`` uses raw IEEE, which is CPython's
and not Spark's normalised ``NaN == NaN``.

``"numeric"`` is split into ``"integral"`` (Long) and ``"fractional"``
(Double). Annotate parameters to keep the option count down.

Overflow, ``//`` / ``%`` signs, and every other Python/Java/ANSI fight live in
``TranspiledJavaUDFHelpers``.

What falls back (``UnsupportedOperationException`` -> ordinary fallback):

* ``and`` / ``or`` -- a helper call cannot short-circuit
  (``b != 0 and a // b > 1``). Catalyst's ``And`` can, so ``catalyst,java``
  keeps them.
* ``== None`` / ``!= None`` -- use ``is None`` / ``is not None``.
* ``not`` over anything but a real boolean.
* An annotated local whose annotation disagrees with its value.
* Loops, ``try``/``except``, string methods, bitwise ops, and any type
  outside integral/fractional/str/bool/bytes.
"""

import ast
import itertools
from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Tuple

from pyspark.errors import UnsupportedOperationException
from pyspark.sql.column import Column
from pyspark.sql.functions import col
from pyspark.sql.transpile import AbstractTranspiler, _is_definitely_boolean
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DataType,
    DecimalType,
    DoubleType,
    FractionalType,
    IntegralType,
    LongType,
    StringType,
)

if TYPE_CHECKING:
    from pyspark.sql._typing import DataTypeOrString

# One table: Catalyst type and Java type are the same ABI decision. Split tables let a
# new category pass the entry-point check and fail later inside the lowering.
_CATEGORY_ABI: Dict[str, Tuple[DataType, str]] = {
    "integral": (LongType(), "Long"),
    "fractional": (DoubleType(), "Double"),
    "string": (StringType(), "UTF8String"),
    "bool": (BooleanType(), "Boolean"),
    "binary": (BinaryType(), "byte[]"),
}

# FQCN: this is off by default, so the helpers stay off codegen's default-import list.
_HELPERS = "org.apache.spark.sql.catalyst.expressions.TranspiledJavaUDFHelpers"

# Per-category operator helper names. Keyed by (ast op class name, category).
_BINARY_HELPERS: Dict[Tuple[str, str], str] = {
    ("Add", "integral"): "addLong",
    ("Add", "fractional"): "addDouble",
    ("Add", "string"): "concat",
    ("Sub", "integral"): "subtractLong",
    ("Sub", "fractional"): "subtractDouble",
    ("Mult", "integral"): "multiplyLong",
    ("Mult", "fractional"): "multiplyDouble",
    ("FloorDiv", "integral"): "floorDivideLong",
    ("FloorDiv", "fractional"): "floorDivideDouble",
    ("Mod", "integral"): "modLong",
    ("Mod", "fractional"): "modDouble",
    # Python's `/` on two ints is a float, so this one changes category (see `_lower_binop`).
    ("Div", "integral"): "divideLong",
    ("Div", "fractional"): "divideDouble",
}

_ORDER_OPS = {ast.Lt: "Lt", ast.LtE: "LtE", ast.Gt: "Gt", ast.GtE: "GtE"}

_COMPARE_HELPERS: Dict[Tuple[str, str], str] = {
    ("Eq", "integral"): "equalsLong",
    ("Eq", "fractional"): "equalsDouble",
    ("Eq", "string"): "equalsString",
    ("Eq", "bool"): "equalsBoolean",
    ("Lt", "integral"): "lessThanLong",
    ("Lt", "fractional"): "lessThanDouble",
    ("Lt", "string"): "lessThanString",
    ("LtE", "integral"): "lessThanOrEqualLong",
    ("LtE", "fractional"): "lessThanOrEqualDouble",
    ("LtE", "string"): "lessThanOrEqualString",
    ("Gt", "integral"): "greaterThanLong",
    ("Gt", "fractional"): "greaterThanDouble",
    ("Gt", "string"): "greaterThanString",
    ("GtE", "integral"): "greaterThanOrEqualLong",
    ("GtE", "fractional"): "greaterThanOrEqualDouble",
    ("GtE", "string"): "greaterThanOrEqualString",
}


class _JavaValue(NamedTuple):
    """A lowered Java expression and the category it produces. Boxed, so ``null``
    is both SQL NULL and Python ``None``."""

    code: str
    category: str


def _java_string_literal(value: str) -> str:
    """A Java ``UTF8String`` literal. Non-ASCII is ``\\uXXXX`` so the generated
    source stays ASCII regardless of how the compiler decodes it."""
    out = []
    for ch in value:
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\\"')
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif " " <= ch <= "~":
            out.append(ch)
        else:
            # Java source is UTF-16, so a character outside the BMP needs both of its
            # surrogates written out rather than one escape.
            try:
                encoded = ch.encode("utf-16-be")
            except UnicodeEncodeError:
                # A lone surrogate is legal in a Python str but has no UTF-16 encoding, so it
                # cannot be written as a Java escape. Decline like every other unsupported
                # construct rather than surfacing a codec error as the reason.
                raise UnsupportedOperationException(
                    "a string literal containing an unpaired surrogate is not lowered by the "
                    "java transpiler"
                )
            for i in range(0, len(encoded), 2):
                out.append(f"\\u{encoded[i] << 8 | encoded[i + 1]:04x}")
    return f'UTF8String.fromString("{"".join(out)}")'


def _definitely_returns(body: List[ast.stmt]) -> bool:
    """Whether every path through ``body`` hits a ``return``.

    Java rejects an unreachable statement outright, so the trailing ``return null;`` that
    stands for Python falling off the end of a function can only be emitted when the body
    might actually reach it. Verified: a `return` after an if/else whose branches both
    return does not compile.
    """
    for stmt in body:
        match stmt:
            case ast.Return():
                return True
            case ast.If(body=if_body, orelse=if_orelse):
                # An `if` with no `else` always has a path that falls through.
                if if_orelse and _definitely_returns(if_body) and _definitely_returns(if_orelse):
                    return True
            case _:
                pass
    return False


class JavaTranspiler(AbstractTranspiler):
    """Transpiles a Python UDF into Java source. See the module docstring."""

    variety = "java"

    def __init__(self) -> None:
        self._param_categories: Dict[int, str] = {}
        self._params: List[str] = []
        self._arg_names: List[str] = []
        self._locals: Dict[str, str] = {}
        # Held, not computed: sanitising to Java's character set is not injective
        # (``cafe`` + acute and ``caf_`` both become ``caf_``), and two locals
        # sharing storage would be a silently wrong answer.
        self._local_java_names: Dict[str, str] = {}
        self._cast_columns: Dict[Tuple[int, str], Any] = {}

    def _bind(self, params: List[str], param_categories: dict) -> None:
        self._param_categories = dict(param_categories)
        self._params = list(params)
        self._arg_names = [f"_udf_arg_{i}" for i in range(len(params))]
        self._locals = {}
        self._local_java_names = {}

    # ---------------------------------------------------------------------------------
    # Input-type variants
    # ---------------------------------------------------------------------------------

    def _param_category_combos(
        self, function_ast: ast.FunctionDef, public_params: List[str]
    ) -> List[dict]:
        """Like the Catalyst target's, but ``"numeric"`` split into integral/fractional.

        Cap is two untyped parameters, not three: 3**n options, and pruning keeps at
        most one.
        """
        n = len(public_params)
        if n == 0:
            return [{}]
        public_args = function_ast.args.args[len(function_ast.args.args) - n :]
        candidates: List[List[str]] = []
        untyped = 0
        for arg in public_args:
            pinned = _java_annotation_category(arg.annotation)
            if pinned is None:
                candidates.append(["integral", "fractional", "string"])
                untyped += 1
            else:
                candidates.append([pinned])
        if untyped > 2:
            return [
                {i: c[0] if len(c) == 1 else fill for i, c in enumerate(candidates)}
                for fill in ("integral", "fractional", "string")
            ]
        return [{i: choice[i] for i in range(n)} for choice in itertools.product(*candidates)]

    # ---------------------------------------------------------------------------------
    # Entry point
    # ---------------------------------------------------------------------------------

    def _transpile_from_ast(
        self,
        src: Optional[str],
        ast_info: ast.AST,
        function_ast: ast.FunctionDef,
        params: List[str],
        returnType: "DataTypeOrString",
        param_categories: Optional[dict] = None,
    ) -> Optional[Column]:
        if src == "" or ast_info is None:
            return None
        self._bind(params, param_categories or {})

        if not isinstance(returnType, DataType):
            # ``_transpile_func`` parses a string return type before we are called.
            raise UnsupportedOperationException("the java transpiler needs a resolved return type")
        return_category = _return_category(returnType)

        for index in range(len(params)):
            if self._param_categories.get(index) not in _CATEGORY_ABI:
                raise UnsupportedOperationException(
                    f"parameter {params[index]!r} has category "
                    f"{self._param_categories.get(index)!r}, which the java transpiler "
                    "does not lower"
                )

        statements = self._lower_body(function_ast.body, return_category)
        body = "\n".join(statements)
        return self._build_column(
            name=function_ast.name,
            body=body,
            arg_names=self._arg_names,
            params=params,
            return_type=_catalyst_type(return_category),
            declared_return_type=returnType,
        )

    # ---------------------------------------------------------------------------------
    # Statements
    # ---------------------------------------------------------------------------------

    def _lower_body(self, body: List[ast.stmt], return_category: str) -> List[str]:
        """Lower a function body to Java statements, ending with a return on every path."""
        statements = self._lower_statements(body, return_category)
        # Java rejects an unreachable statement (JLS 14.21), so only emit the
        # fall-off-the-end ``return null`` when a path can actually reach it.
        if not _definitely_returns(body):
            statements.append(f"return (({_java_type(return_category)}) null);")
        return statements

    def _lower_statements(self, body: List[ast.stmt], return_category: str) -> List[str]:
        out: List[str] = []
        for stmt in body:
            out.extend(self._lower_statement(stmt, return_category))
            if _definitely_returns([stmt]):
                break
        return out

    def _lower_statement(self, stmt: ast.stmt, return_category: str) -> List[str]:
        match stmt:
            case ast.Expr(value=ast.Constant(value=str())) | ast.Pass():
                return []
            case ast.Return(value=value):
                if value is None:
                    return ["return null;"]
                lowered = self._lower_expr(value)
                return [f"return {self._coerce(lowered, return_category).code};"]
            case ast.Assign(targets=targets, value=value):
                if len(targets) != 1 or not isinstance(targets[0], ast.Name):
                    raise UnsupportedOperationException(
                        "only assignment to a single plain name is lowered by the java transpiler"
                    )
                return self._assign_lowered(targets[0].id, self._lower_expr(value))
            case ast.AnnAssign(target=ast.Name(id=target), value=value, annotation=annotation):
                if value is None:
                    raise UnsupportedOperationException(
                        "a bare annotation with no value is not lowered"
                    )
                lowered = self._lower_expr(value)
                pinned = _java_annotation_category(annotation)
                if pinned is not None and pinned != lowered.category:
                    # An annotation is not a cast. Converting ``y: float = x`` over a
                    # bigint would lose precision past 2**53, with no fallback left.
                    raise UnsupportedOperationException(
                        f"the annotation on local {target!r} says {pinned} but the value is "
                        f"{lowered.category}; a Python annotation does not convert the value, so "
                        "the java transpiler declines rather than inserting a conversion"
                    )
                return self._assign_lowered(target, lowered)
            case ast.If(test=test, body=if_body, orelse=if_orelse):
                condition = self._lower_expr(test)
                if condition.category != "bool":
                    raise UnsupportedOperationException(
                        "an `if` condition must be statically boolean; Python truthiness "
                        "over numbers, strings and None is not lowered by the java "
                        "transpiler"
                    )
                out = [f"if ({_HELPERS}.isTrue({condition.code})) {{"]
                # A Java local dies with its block; Python's would still be bound after
                # the `if`. Restore the outer set so a name first assigned in a branch
                # is unknown afterwards. Fail closed: declining costs a lowering.
                outer_locals = dict(self._locals)
                out.extend(_indent(self._lower_statements(if_body, return_category)))
                self._locals = dict(outer_locals)
                if if_orelse:
                    out.append("} else {")
                    out.extend(_indent(self._lower_statements(if_orelse, return_category)))
                    self._locals = dict(outer_locals)
                out.append("}")
                return out
            case _:
                raise UnsupportedOperationException(
                    f"{type(stmt).__name__} statements are not lowered by the java transpiler"
                )

    def _assign_lowered(self, target: str, lowered: _JavaValue) -> List[str]:
        if target in self._params:
            raise UnsupportedOperationException(
                f"assigning to the parameter {target!r} is not lowered"
            )
        if lowered.category == "none":
            # Ordinary Python, but a bare null has no Java type to declare.
            raise UnsupportedOperationException(
                f"assigning a bare None to the local {target!r} is not lowered: the local has no "
                "type to declare, since None carries no category"
            )
        if target in self._locals and self._locals[target] != lowered.category:
            raise UnsupportedOperationException(
                f"local {target!r} is assigned both a {self._locals[target]} and a "
                f"{lowered.category} value, which the java transpiler does not lower"
            )
        declaration = "" if target in self._locals else f"{_java_type(lowered.category)} "
        self._locals[target] = lowered.category
        return [f"{declaration}{self._java_local(target)} = {lowered.code};"]

    # ---------------------------------------------------------------------------------
    # Expressions
    # ---------------------------------------------------------------------------------

    def _lower_expr(self, node: ast.expr) -> _JavaValue:
        match node:
            case ast.Constant(value=None):
                return _JavaValue("null", "none")
            case ast.Constant(value=bool() as value):
                # Before int: `bool` is a subclass of `int`.
                return _JavaValue("Boolean.valueOf(" + ("true" if value else "false") + ")", "bool")
            case ast.Constant(value=int() as value):
                if not (-(2**63) <= value < 2**63):
                    raise UnsupportedOperationException(
                        f"the integer literal {value} does not fit in a Java long, which is "
                        "what the java transpiler lowers Python ints to"
                    )
                return _JavaValue(f"Long.valueOf({value}L)", "integral")
            case ast.Constant(value=float() as value):
                if value != value or value in (float("inf"), float("-inf")):
                    raise UnsupportedOperationException("non-finite float literals are not lowered")
                return _JavaValue(f"Double.valueOf({value!r}d)", "fractional")
            case ast.Constant(value=str() as value):
                return _JavaValue(_java_string_literal(value), "string")
            case ast.Constant(value=bytes()):
                raise UnsupportedOperationException(
                    "bytes literals are not lowered by the java transpiler"
                )
            case ast.Name(id=name):
                return self._lower_name(name)
            case ast.UnaryOp(op=ast.Not(), operand=operand):
                if not _is_definitely_boolean(operand):
                    raise UnsupportedOperationException(
                        "`not` is only lowered for a statically boolean operand; Python "
                        "truthiness over other types is not reproduced"
                    )
                lowered = self._lower_expr(operand)
                if lowered.category != "bool":
                    # `_is_definitely_boolean` admits `None`; we have no coalesce back
                    # to Python's `not None is True`, so require a real boolean.
                    raise UnsupportedOperationException(
                        f"`not` on a {lowered.category} operand is not lowered; Python's `not "
                        "None` is True where SQL's `NOT NULL` is NULL"
                    )
                return _JavaValue(f"{_HELPERS}.not({lowered.code})", "bool")
            case ast.UnaryOp(op=(ast.USub() | ast.UAdd()) as op, operand=operand):
                lowered = self._lower_expr(operand)
                if lowered.category not in ("integral", "fractional"):
                    raise UnsupportedOperationException(
                        "unary `+`/`-` is only lowered for numbers; Python raises TypeError "
                        f"for a {lowered.category} operand"
                    )
                if isinstance(op, ast.UAdd):
                    return lowered
                helper = "negateLong" if lowered.category == "integral" else "negateDouble"
                return _JavaValue(f"{_HELPERS}.{helper}({lowered.code})", lowered.category)
            case ast.BinOp(left=left, op=op, right=right):
                return self._lower_binop(left, op, right)
            case ast.BoolOp():
                # TODO (SPARK-55209 follow-up): hoist the right operand behind an `if`.
                # A helper call cannot short-circuit, so `b != 0 and a // b > 1` would
                # raise on the rows the guard exists to exclude. Catalyst's `And` can,
                # so `catalyst,java` keeps these.
                raise UnsupportedOperationException(
                    "`and`/`or` are not lowered by the java transpiler: Python short-circuits "
                    "them and a Java helper call would evaluate both operands, which turns a "
                    "guard like `b != 0 and a // b > 1` into a divide-by-zero"
                )
            case ast.Compare(left=left, ops=ops, comparators=comparators):
                return self._lower_compare(left, ops, comparators)
            case ast.IfExp(test=test, body=body, orelse=orelse):
                condition = self._lower_expr(test)
                if condition.category != "bool":
                    raise UnsupportedOperationException(
                        "a conditional expression's test must be statically boolean"
                    )
                when_true = self._lower_expr(body)
                when_false = self._lower_expr(orelse)
                category = self._unify(when_true.category, when_false.category, "conditional")
                true_code = self._coerce(when_true, category).code
                false_code = self._coerce(when_false, category).code
                return _JavaValue(
                    f"({_HELPERS}.isTrue({condition.code}) ? {true_code} : {false_code})",
                    category,
                )
            case _:
                raise UnsupportedOperationException(
                    f"{type(node).__name__} expressions are not lowered by the java transpiler"
                )

    def _java_local(self, name: str) -> str:
        """The Java name for a Python local. Numbered so the mapping cannot collide."""
        existing = self._local_java_names.get(name)
        if existing is not None:
            return existing
        cleaned = "".join(
            ch if (ch.isalnum() and ch.isascii()) or ch == "_" else "_" for ch in name
        )
        assigned = f"_udf_local_{len(self._local_java_names)}_{cleaned}"
        self._local_java_names[name] = assigned
        return assigned

    def _lower_name(self, name: str) -> _JavaValue:
        if name in self._locals:
            return _JavaValue(self._java_local(name), self._locals[name])
        if name in self._params:
            index = self._params.index(name)
            return _JavaValue(self._arg_names[index], self._param_categories[index])
        # Free variable. SPARK-55207 is the shared fix; until then neither target reads them.
        raise UnsupportedOperationException(
            f"{name!r} is neither a parameter nor a local assigned in the body, so the "
            "java transpiler cannot lower it"
        )

    def _lower_binop(self, left: ast.expr, op: ast.operator, right: ast.expr) -> _JavaValue:
        op_name = type(op).__name__
        lowered_left = self._lower_expr(left)
        lowered_right = self._lower_expr(right)

        # `str * int`: refuse a fractional count rather than truncate the way Catalyst does.
        if op_name == "Mult" and {lowered_left.category, lowered_right.category} == {
            "string",
            "integral",
        }:
            text, count = (
                (lowered_left, lowered_right)
                if lowered_left.category == "string"
                else (lowered_right, lowered_left)
            )
            return _JavaValue(f"{_HELPERS}.repeat({text.code}, {count.code})", "string")

        category = self._unify(lowered_left.category, lowered_right.category, op_name)
        if category == "string" and op_name != "Add":
            raise UnsupportedOperationException(
                f"`{op_name}` is not lowered for text (Python raises TypeError)"
            )
        if category == "bool":
            # `True + 1` is legal Python; ANSI Spark rejects it. Both targets refuse.
            raise UnsupportedOperationException(f"`{op_name}` is not lowered for booleans")
        helper = _BINARY_HELPERS.get((op_name, category))
        if helper is None:
            raise UnsupportedOperationException(
                f"`{op_name}` on {category} operands is not lowered by the java transpiler"
            )
        left_code = self._coerce(lowered_left, category).code
        right_code = self._coerce(lowered_right, category).code
        result_category = "fractional" if op_name == "Div" else category
        return _JavaValue(f"{_HELPERS}.{helper}({left_code}, {right_code})", result_category)

    def _lower_compare(
        self, left: ast.expr, ops: List[ast.cmpop], comparators: List[ast.expr]
    ) -> _JavaValue:
        if len(ops) != 1:
            raise UnsupportedOperationException(
                "chained comparisons are not lowered by the java transpiler"
            )
        op = ops[0]
        right = comparators[0]

        if isinstance(op, (ast.Is, ast.IsNot)):
            if not isinstance(right, ast.Constant) or right.value is not None:
                raise UnsupportedOperationException("`is` is only lowered against None")
            lowered = self._lower_expr(left)
            test = "!=" if isinstance(op, ast.IsNot) else "=="
            return _JavaValue(f"Boolean.valueOf({lowered.code} {test} null)", "bool")

        # Either side: `None == x` is as much a None comparison as `x == None`.
        # Catalyst lowers this specially and is tried first; `is None` is the idiom.
        if any(
            isinstance(operand, ast.Constant) and operand.value is None for operand in (left, right)
        ):
            raise UnsupportedOperationException(
                "`==`/`!=` against None are not lowered by the java transpiler; use "
                "`is None` / `is not None`"
            )

        lowered_left = self._lower_expr(left)
        lowered_right = self._lower_expr(right)
        category = self._unify(lowered_left.category, lowered_right.category, "comparison")
        left_code = self._coerce(lowered_left, category).code
        right_code = self._coerce(lowered_right, category).code

        # Own helper per operator, not swapped `<`: Java evaluates args left to
        # right, so `x // 0 > x * x` would raise the multiply first.
        if isinstance(op, (ast.Eq, ast.NotEq)):
            helper = _COMPARE_HELPERS.get(("Eq", category))
            if helper is None:
                sym = "==" if isinstance(op, ast.Eq) else "!="
                raise UnsupportedOperationException(
                    f"`{sym}` on {category} operands is not lowered"
                )
            call = f"{_HELPERS}.{helper}({left_code}, {right_code})"
            if isinstance(op, ast.NotEq):
                call = f"{_HELPERS}.not({call})"
            return _JavaValue(call, "bool")
        order = _ORDER_OPS.get(type(op))
        if order is None:
            raise UnsupportedOperationException(
                f"{type(op).__name__} comparisons are not lowered by the java transpiler"
            )
        return self._ordered(category, order, left_code, right_code)

    def _ordered(self, category: str, op_name: str, left: str, right: str) -> _JavaValue:
        helper = _COMPARE_HELPERS.get((op_name, category))
        if helper is None:
            raise UnsupportedOperationException(
                f"ordering comparisons on {category} operands are not lowered "
                "(Python has no ordering for booleans against each other here)"
            )
        return _JavaValue(f"{_HELPERS}.{helper}({left}, {right})", "bool")

    # ---------------------------------------------------------------------------------
    # Categories
    # ---------------------------------------------------------------------------------

    def _unify(self, left: str, right: str, what: str) -> str:
        """The category an operation over ``left`` and ``right`` produces.

        ``none`` unifies with anything. Integral promotes to fractional. Anything
        else mixed is a Python ``TypeError``, so it is not lowered.
        """
        if left == right:
            return left
        if left == "none":
            return right
        if right == "none":
            return left
        if {left, right} == {"integral", "fractional"}:
            return "fractional"
        raise UnsupportedOperationException(
            f"{what} mixes {left} and {right} operands, which Python does not allow and the "
            "java transpiler does not lower"
        )

    def _coerce(self, value: _JavaValue, category: str) -> _JavaValue:
        """Bring ``value`` into ``category``, or refuse."""
        if value.category == category:
            return value
        if value.category == "none":
            # Keep the code. Replacing a "none" value with a bare `null` dropped
            # `None if a // b > 0 else None`'s divide, returning NULL where
            # CPython raises.
            return _JavaValue(f"(({_java_type(category)}) {value.code})", category)
        if value.category == "integral" and category == "fractional":
            return _JavaValue(f"{_HELPERS}.toDouble({value.code})", "fractional")
        raise UnsupportedOperationException(
            f"a {value.category} value cannot be used where a {category} one is needed; "
            "the java transpiler falls back rather than converting"
        )

    # ---------------------------------------------------------------------------------
    # Handing the source to the JVM
    # ---------------------------------------------------------------------------------

    def _build_column(
        self,
        name: str,
        body: str,
        arg_names: List[str],
        params: List[str],
        return_type: DataType,
        declared_return_type: DataType,
    ) -> Column:
        from pyspark.sql.classic.column import Column as ClassicColumn
        from pyspark.sql.classic.column import _to_java_column
        from pyspark.sql.utils import get_active_spark_context

        sc = get_active_spark_context()
        assert sc._jvm is not None

        # Widen into the category type (int/smallint/bigint all arrive as Long).
        # Memoized: this runs once per option and a Column is immutable.
        children = []
        input_type_json = []
        for index in range(len(params)):
            category = self._param_categories[index]
            target = _catalyst_type(category)
            cached = self._cast_columns.get((index, category))
            if cached is None:
                cached = _to_java_column(col(f"_udf_param_{index}").cast(target))
                self._cast_columns[(index, category)] = cached
            children.append(cached)
            input_type_json.append(target.json())

        jcol = getattr(
            sc._jvm,
            "org.apache.spark.sql.execution.python.TranspiledJavaUDFBuilder",
        ).create(
            name,
            body,
            arg_names,
            children,
            input_type_json,
            return_type.json(),
        )
        result = ClassicColumn(jcol)
        if return_type != declared_return_type:
            return result.cast(declared_return_type)
        return result


def _abi(category: str) -> Tuple[DataType, str]:
    abi = _CATEGORY_ABI.get(category)
    if abi is None:
        raise UnsupportedOperationException(f"no ABI for category {category!r}")
    return abi


def _java_type(category: str) -> str:
    return _abi(category)[1]


def _catalyst_type(category: str) -> DataType:
    return _abi(category)[0]


def _indent(lines: List[str]) -> List[str]:
    return [f"  {line}" for line in lines]


def _java_annotation_category(annotation: Optional[ast.AST]) -> Optional[str]:
    """Like ``_annotation_category`` but ``int`` and ``float`` stay split."""
    name: Optional[str] = None
    if isinstance(annotation, ast.Name):
        name = annotation.id
    elif isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        name = annotation.value  # a stringized annotation, e.g. def f(a: "int")
    match name:
        case "int":
            return "integral"
        case "float":
            return "fractional"
        case "str":
            return "string"
        case "bool":
            return "bool"
        case "bytes":
            return "binary"
        case _:
            return None


def _return_category(returnType: DataType) -> str:
    """The category a declared return type is produced in.

    Only types that map straight onto a category: the interpreted converter
    nulls a mismatched result, so converting here would silently diverge.
    """
    if isinstance(returnType, IntegralType):
        return "integral"
    if isinstance(returnType, FractionalType) and not isinstance(returnType, DecimalType):
        return "fractional"
    if isinstance(returnType, StringType):
        if not returnType.isUTF8BinaryCollation():
            raise UnsupportedOperationException(
                "a non-binary collation compares by collation rules where Python compares "
                "codepoints, so the java transpiler does not lower it"
            )
        return "string"
    if isinstance(returnType, BooleanType):
        return "bool"
    if isinstance(returnType, BinaryType):
        return "binary"
    raise UnsupportedOperationException(
        f"return type {returnType.simpleString()} is not lowered by the java transpiler"
    )


JavaTranspiler.register()
