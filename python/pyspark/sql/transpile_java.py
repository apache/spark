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

Opt in by naming it in ``spark.sql.experimental.optimizer.pyTranspilers``, after
``catalyst``::

    spark.conf.set("spark.sql.experimental.optimizer.pyTranspilers", "catalyst,java")

Order matters and that order is the recommended one. Transpilers are tried left to
right and the first option that survives type pruning is the one used, so with
``catalyst`` first a UDF it can lower is lowered by it, and this target only sees
what it declined. That is the right way round: a Catalyst lowering is made of ordinary
expressions the optimizer can push into a data source, prune partitions with, and fold
constants inside, while a generated body is opaque to all of it. What this target buys
is reach, not speed relative to Catalyst -- speed relative to a Python worker.

Where it reaches further
------------------------

A Catalyst option is an expression, and a Python body is statements, so
:class:`~pyspark.sql.transpile.CatalystTranspiler` refuses any body with more than one
top-level statement. A Java method has statements, so this target lowers local
variables, several statements, and early returns::

    def clamp(x):
        doubled = x * 2
        if doubled > 10:
            return 10
        return doubled + 1

It also lowers ``/`` and ``//``, which the Catalyst target does not handle at all.

Each parameter is read once
---------------------------

The generated method takes each argument as a parameter, so however many times the
body reads it, the plan holds one child expression and evaluates it once. Nothing has
to be hoisted into a projection to make that true, which is what the Catalyst target
needs a plan rewrite for (SPARK-58626).

The flip side: every parameter is a child, so every argument is evaluated, before the
call and whether or not the body reads it. Under ANSI ``f(x, y / 0)`` therefore raises
even for ``def f(a, b): return a``. That is what the interpreted Python UDF does too,
evaluating its arguments in a projection feeding the worker -- so this target agrees
with interpreted Python here, where a Catalyst lowering, which inlines each parameter
into just the branches that read it, does not.

None
----

Arithmetic propagates null: ``None + 1`` gives NULL rather than raising the way Python's
``TypeError`` would. That is a divergence, and it is the same one the Catalyst target
has. Ordering is the other way round -- ``<``, ``<=``, ``>``, ``>=`` **raise** on a null
operand, because returning NULL would make ``if x > 0`` quietly take its false branch
and hand back a confident wrong answer; the Catalyst target raises there too. Either
way, guarding explicitly (which is the idiom anyway) behaves exactly as written::

    def add_one(x):
        if x is not None:
            return x + 1

The two targets are close but not interchangeable, and this one does not claim
otherwise. ``NaN`` is the standing example: Spark normalises it, so under ``catalyst``
``NaN == NaN`` is true and NaN sorts greatest, while this target uses raw IEEE
semantics, which is what CPython does. A body comparing doubles that can be NaN
therefore answers differently under the two -- with this one the closer to interpreted
Python.

Numbers
-------

A Java method has to name one type per parameter and cannot be polymorphic over the
numeric types the way a Catalyst ``Add`` is. So where the Catalyst target has one
``"numeric"`` input category, this one splits it into ``"integral"`` (lowered to
``Long``) and ``"fractional"`` (lowered to ``Double``), and the JVM picks by the bound
column's type. Both halves are lossless -- every integral type fits in a long, both
fractional types in a double -- so the split costs extra options, never precision.

Annotating parameters pins each category and keeps the option count down; an untyped
parameter is tried as integral, fractional and string. Prefer annotating.

Overflow, division by zero, and the sign of ``//`` and ``%`` are handled in
``TranspiledJavaUDFHelpers``, which is where every point Python, Java and ANSI
disagree about an operator is written down.

What falls back
---------------

Anything not listed above raises :class:`UnsupportedOperationException`, which
``_transpile_func`` turns into an ordinary fallback -- to the Catalyst option if it
produced one, otherwise to interpreted Python. A body that somehow lowers but does not
compile is caught the same way: the generated source is compiled once while the option
is still being built, so a bug here costs a fallback rather than a failed query.

Notably absent for now:

* ``and`` / ``or``. Python short-circuits them and a Java helper call cannot, which
  would turn ``b != 0 and a // b > 1`` into a divide-by-zero on exactly the rows the
  guard exists to exclude. The Catalyst target's ``And`` does short-circuit, so it keeps
  lowering these under the recommended ``catalyst,java``.
* ``== None`` / ``!= None`` on either side -- use ``is None`` / ``is not None``.
* ``not`` over anything but a real boolean, since Python's ``not None`` is ``True``
  where SQL's ``NOT NULL`` is NULL.
* An annotated local whose annotation disagrees with its value (``y: float = x`` over an
  int), because a Python annotation does not convert anything.
* Loops, ``try``/``except``, string methods, bitwise operators, and any parameter or
  return type outside integral/fractional/str/bool/bytes.
"""

import ast
import itertools
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

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

# The categories this target uses, and the Catalyst type each one is lowered over. Every
# argument is cast to its category's type before the call, so the generated method's
# parameter types are exactly these and never depend on which width of column was bound.
# One table, not two: the Catalyst type and the Java type for a category are two halves of the
# same ABI decision, and splitting them meant a new category could satisfy the entry-point check
# that exists to catch exactly that and then fail much later inside the lowering.
_CATEGORY_ABI: Dict[str, Tuple[DataType, str]] = {
    "integral": (LongType(), "Long"),
    "fractional": (DoubleType(), "Double"),
    "string": (StringType(), "UTF8String"),
    "bool": (BooleanType(), "Boolean"),
    "binary": (BinaryType(), "byte[]"),
}

# The helper class every emitted operator call goes through, by fully-qualified name: it is
# deliberately not on codegen's default-import list, since this feature is off by default and
# every generated class in Spark would otherwise carry the import.
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


class _JavaValue:
    """A lowered Java expression and the category it produces.

    Everything is a boxed Java value, so ``null`` is the one representation of both SQL
    NULL and Python ``None`` and no separate null flag has to be threaded around.
    """

    def __init__(self, code: str, category: str) -> None:
        self.code = code
        self.category = category

    def __repr__(self) -> str:
        return f"_JavaValue({self.code!r}, {self.category!r})"


def _java_string_literal(value: str) -> str:
    """A Java ``UTF8String`` literal for ``value``.

    Escapes for Java source, then relies on ``UTF8String.fromString``. Non-ASCII goes out
    as ``\\uXXXX`` rather than raw bytes: the generated source is handed to a compiler as a
    Java string, and keeping it ASCII removes any question of how that compiler decodes it.
    """
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
        # The generated method's parameter names, positional. The one place the naming is
        # decided: both the method signature and every read in the body come from here.
        self._arg_names: List[str] = []
        # Locals the body has assigned, name -> category, so a later read knows its type.
        self._locals: Dict[str, str] = {}
        # Python name -> Java name for locals. Held rather than computed so the mapping is
        # injective: sanitising a name to Java's character set can map two distinct Python
        # locals onto one Java local -- an accented name and the same name with the
        # accent already written as an underscore both reduce to the underscore form --
        # and two locals sharing storage would be a silently wrong answer, not a failure.
        self._local_java_names: Dict[str, str] = {}
        # (param index, category) -> the JVM cast column for it, shared across this UDF's options.
        self._cast_columns: Dict[Tuple[int, str], Any] = {}

    # ---------------------------------------------------------------------------------
    # Input-type variants
    # ---------------------------------------------------------------------------------

    def _param_category_combos(
        self, function_ast: ast.FunctionDef, public_params: List[str]
    ) -> List[dict]:
        """Like the Catalyst target's, but with ``"numeric"`` split in two.

        ``int`` pins integral and ``float`` pins fractional; an untyped parameter is tried
        as integral, fractional and string. Past TWO untyped parameters the product is
        collapsed to the all-of-each variants, with every annotated parameter still pinned --
        a tighter cap than the base target's three, because three categories grow as 3**n
        rather than 2**n. See the comment on the guard itself.
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
        # Capped at two untyped parameters, not the base target's three: with three categories the
        # product is 3**n rather than 2**n, so three untyped parameters would be 27 options on one
        # node -- each carrying a body string through the analyzer -- where at most ONE can survive
        # pruning, since `optionMatchesTypes` maps any argument type into exactly one category.
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
        self._param_categories = dict(param_categories or {})
        self._params = list(params)
        self._arg_names = [f"_udf_arg_{i}" for i in range(len(params))]
        self._locals = {}
        self._local_java_names = {}

        if not isinstance(returnType, DataType):
            # A string return type is parsed by ``_transpile_func`` before we are called;
            # anything else is not something we can name a Java type for.
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

        # The result is produced in the body's own category and returned in the declared
        # one. Only a widening within the numbers is allowed, matching the Catalyst
        # target's rule that a lowering's category has to match the declared return type
        # -- otherwise the interpreted path, whose converter nulls a mismatched result,
        # and this one disagree.
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
        # Python falls off the end of a function by returning None, and a Java method must
        # return on every path -- but only where that path exists. Java rejects a statement
        # it can prove unreachable, so a body that already returns everywhere must not get
        # this appended.
        if not _definitely_returns(body):
            statements.append(f"return (({_java_type(return_category)}) null);")
        return statements

    def _lower_statements(self, body: List[ast.stmt], return_category: str) -> List[str]:
        out: List[str] = []
        for stmt in body:
            out.extend(self._lower_statement(stmt, return_category))
            # Stop at the first statement that returns on every path. Python simply never runs
            # what follows, but Java rejects it outright (JLS 14.21), so emitting it would produce
            # source that does not compile -- and a body that does not compile is not a fallback,
            # it is a failed query, since by execution time the interpreted UDF is gone. A
            # defensive trailing `return` after an if/else, or dead code after a `return`, both
            # land here.
            if _definitely_returns([stmt]):
                break
        return out

    def _lower_statement(self, stmt: ast.stmt, return_category: str) -> List[str]:
        match stmt:
            case ast.Expr(value=ast.Constant(value=str())):
                # A docstring. Nothing to emit.
                return []
            case ast.Pass():
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
                    # `x: int` on its own binds nothing; there is no value to lower.
                    raise UnsupportedOperationException(
                        "a bare annotation with no value is not lowered"
                    )
                lowered = self._lower_expr(value)
                pinned = _java_annotation_category(annotation)
                if pinned is not None and pinned != lowered.category:
                    # An annotation is not a cast. `y: float = x` over a bigint column leaves `y`
                    # an int in Python, so converting here would silently lose precision past 2**53
                    # -- and there is no fallback once the option is in the plan. Refuse instead and
                    # let the interpreted UDF, which does what the annotation says it does
                    # (nothing), run.
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
                # A Java local declared inside a block dies with it, while Python's would
                # still be bound after the `if`. Rather than hoist declarations to match
                # Python -- which would also have to answer what an unbound one reads as --
                # restore the outer set so a name first assigned inside a branch is simply
                # not known afterwards, and reading it declines and falls back. Fail closed:
                # declining costs a lowering, guessing costs a wrong answer.
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
            # `y = None` is ordinary Python -- a local a later branch rebinds -- and a bare null
            # has no category to declare a Java local with. Named here rather than left to
            # `_java_type`, whose "no Java type for category 'none'" reads like an internal error.
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
                # Untyped null. The consumer coerces it to whatever it needs.
                return _JavaValue("null", "none")
            case ast.Constant(value=bool() as value):
                # Before int: in Python `bool` is a subclass of `int`.
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
                    # `_is_definitely_boolean` admits a literal `None`, which the Catalyst target
                    # wants because it coalesces `~NULL` back to Python's `not None is True`. This
                    # target has no such coalesce, so `not None` would give NULL where Python gives
                    # True. Require a real boolean and let the Catalyst target have the rest.
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
                # TODO (SPARK-55209 follow-up): lower these with the right operand hoisted behind
                # an `if`, which this target can do because it emits statements, and which is what
                # short-circuiting requires.
                #
                # Not lowered at all for now. Python's `and`/`or` short-circuit, and a helper call
                # cannot: Java evaluates both arguments first. That is not a cosmetic difference --
                # it turns the standard guard idiom into a failure. `b != 0 and a // b > 1` would
                # evaluate `a // b` on the very rows the guard exists to exclude and raise
                # DIVIDE_BY_ZERO where Python returns False, and `x < 2**62 and x * 2 > 0` would
                # raise on overflow where Python short-circuits. The Catalyst target's `And` does
                # short-circuit, so it lowers these correctly and, being tried first, keeps them
                # working under the recommended `catalyst,java`.
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
        """The Java name for a Python local, assigned once and reused.

        Numbered so the mapping cannot collide, with the sanitised Python name kept on the
        end only to make the generated source readable.
        """
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
        # A free variable. The Catalyst target can bake one in once SPARK-55207 lands, in
        # shared code this target will inherit; until then neither reads them.
        raise UnsupportedOperationException(
            f"{name!r} is neither a parameter nor a local assigned in the body, so the "
            "java transpiler cannot lower it"
        )

    def _lower_binop(self, left: ast.expr, op: ast.operator, right: ast.expr) -> _JavaValue:
        op_name = type(op).__name__
        lowered_left = self._lower_expr(left)
        lowered_right = self._lower_expr(right)

        # `str * int` and `int * str`: Python repeats, and the count has to be integral.
        # This is where the java target avoids a divergence the Catalyst target documents:
        # there a fractional count arriving from a column is truncated by the cast it
        # inserts, where Python raises, and an `int` annotation does not prevent it. Here
        # the count's category is known before any code is emitted, so a fractional one
        # simply is not lowered.
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
            # `True + 1` is legal Python, and ANSI Spark rejects it, so both targets refuse.
            raise UnsupportedOperationException(f"`{op_name}` is not lowered for booleans")
        helper = _BINARY_HELPERS.get((op_name, category))
        if helper is None:
            raise UnsupportedOperationException(
                f"`{op_name}` on {category} operands is not lowered by the java transpiler"
            )
        left_code = self._coerce(lowered_left, category).code
        right_code = self._coerce(lowered_right, category).code
        # Python's `/` always produces a float, including for two ints.
        result_category = "fractional" if op_name == "Div" else category
        return _JavaValue(f"{_HELPERS}.{helper}({left_code}, {right_code})", result_category)

    def _lower_compare(
        self, left: ast.expr, ops: List[ast.cmpop], comparators: List[ast.expr]
    ) -> _JavaValue:
        if len(ops) != 1:
            # `a < b < c` binds as a chain with its own short-circuiting; not lowered, the
            # same way the Catalyst target refuses it.
            raise UnsupportedOperationException(
                "chained comparisons are not lowered by the java transpiler"
            )
        op = ops[0]
        right = comparators[0]

        # `is None` / `is not None`: a plain null check, and the one None comparison that
        # means the same thing in Python and in SQL.
        if isinstance(op, (ast.Is, ast.IsNot)) and isinstance(right, ast.Constant):
            if right.value is not None:
                raise UnsupportedOperationException("`is` is only lowered against None")
            lowered = self._lower_expr(left)
            test = "!=" if isinstance(op, ast.IsNot) else "=="
            return _JavaValue(f"Boolean.valueOf({lowered.code} {test} null)", "bool")
        if isinstance(op, (ast.Is, ast.IsNot)):
            raise UnsupportedOperationException("`is` is only lowered against None")

        # Either side, not just the right: `None == x` is as much a None comparison as `x == None`,
        # and checking only one of them let the mirrored form through to a NULL-propagating
        # equality that returns NULL where Python returns a definite False.
        if any(
            isinstance(operand, ast.Constant) and operand.value is None for operand in (left, right)
        ):
            # `x == None` is False in Python for a non-None x but NULL in SQL. The Catalyst
            # target lowers this specially; here we decline and let it, since it is tried
            # first. `is None` above covers the idiom.
            raise UnsupportedOperationException(
                "`==`/`!=` against None are not lowered by the java transpiler; use "
                "`is None` / `is not None`"
            )

        lowered_left = self._lower_expr(left)
        lowered_right = self._lower_expr(right)
        category = self._unify(lowered_left.category, lowered_right.category, "comparison")
        left_code = self._coerce(lowered_left, category).code
        right_code = self._coerce(lowered_right, category).code

        # Each ordering operator gets its own helper rather than being reduced to `<`/`<=` with the
        # operands swapped. Swapping gives the right answer but the wrong evaluation order: Java
        # evaluates arguments left to right, so `x // 0 > x * x` would raise the multiplication's
        # overflow where Python -- and a Catalyst `GreaterThan` -- raises the division's error
        # first. `!=` still negates `==`, which is safe because both operands are evaluated either
        # way and the negation happens after.
        match op:
            case ast.Eq():
                helper = _COMPARE_HELPERS.get(("Eq", category))
                if helper is None:
                    raise UnsupportedOperationException(
                        f"`==` on {category} operands is not lowered"
                    )
                return _JavaValue(f"{_HELPERS}.{helper}({left_code}, {right_code})", "bool")
            case ast.NotEq():
                helper = _COMPARE_HELPERS.get(("Eq", category))
                if helper is None:
                    raise UnsupportedOperationException(
                        f"`!=` on {category} operands is not lowered"
                    )
                inner = f"{_HELPERS}.{helper}({left_code}, {right_code})"
                return _JavaValue(f"{_HELPERS}.not({inner})", "bool")
            case ast.Lt():
                return self._ordered(category, "Lt", left_code, right_code)
            case ast.LtE():
                return self._ordered(category, "LtE", left_code, right_code)
            case ast.Gt():
                return self._ordered(category, "Gt", left_code, right_code)
            case ast.GtE():
                return self._ordered(category, "GtE", left_code, right_code)
            case _:
                raise UnsupportedOperationException(
                    f"{type(op).__name__} comparisons are not lowered by the java transpiler"
                )

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

        ``none`` unifies with anything, since an untyped null is representable in every
        category. Integral promotes to fractional, which is Python's rule. Anything else
        mixed is a Python ``TypeError``, so it is not lowered.
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
            # A null, typed by where it is used, and its code KEPT rather than replaced by the
            # literal `null`. Replacing it deleted whole subexpressions: the category of
            # `None if a // b > 0 else None` is "none", so emitting a bare null dropped the
            # condition and with it the divide-by-zero that CPython raises on b = 0 -- the UDF
            # quietly returned NULL instead. Casting keeps the evaluation and still gives Java
            # overload resolution and the assignment target a type to see.
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

        # Cast each argument into its category's type. This is what lets the generated
        # method name one concrete parameter type: an int, smallint or bigint column all
        # arrive as a Long. Every cast is a widening one within the category the option is
        # pruned to, so none of them can lose a value.
        #
        # Memoized on (index, category) because this runs once per option and the distinct
        # columns number only (params x categories): building them fresh each time made
        # `udf()` several times slower on a multi-parameter UDF for nothing. Sharing is safe --
        # a Column is immutable, and each option's copy of the placeholder is substituted
        # independently by `resolveUDFParams`, which rebuilds nodes rather than mutating them.
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
        # The node reports its body's type; the declared return type is what the plan has to
        # see. Within the numbers that is a real conversion (a long body under a declared
        # double), everywhere else it is the identity, and `_return_category` has already
        # refused any pair that would need more than that.
        if return_type != declared_return_type:
            return result.cast(declared_return_type)
        return result


def _java_type(category: str) -> str:
    """The boxed Java type a category is lowered over.

    The message names the category because the reachable caller is a value with the pseudo-category
    ``"none"`` -- a bare ``None`` -- which the statement lowerings refuse by name before getting
    here.
    """
    abi = _CATEGORY_ABI.get(category)
    if abi is None:
        raise UnsupportedOperationException(f"no Java type for category {category!r}")
    return abi[1]


def _catalyst_type(category: str) -> DataType:
    """The Catalyst type a category's arguments are cast to before the call."""
    abi = _CATEGORY_ABI.get(category)
    if abi is None:
        raise UnsupportedOperationException(f"no Catalyst type for category {category!r}")
    return abi[0]


def _indent(lines: List[str]) -> List[str]:
    return [f"  {line}" for line in lines]


def _java_annotation_category(annotation: Optional[ast.AST]) -> Optional[str]:
    """The category a parameter annotation pins, or ``None`` when it pins nothing.

    Like :func:`pyspark.sql.transpile._annotation_category` but splitting ``int`` and
    ``float``, which this target has to lower over different Java types.
    """
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

    Mirrors the Catalyst target's rule that a lowering's category has to match the declared
    return type. A conversion the interpreted path would not perform -- its converter nulls
    a result whose type is not the declared one -- would be a silent divergence, so only
    types that map straight onto a category are lowered, and DecimalType is excluded for
    the same reason it is excluded as an input.
    """
    if isinstance(returnType, IntegralType):
        return "integral"
    if isinstance(returnType, FractionalType) and not isinstance(returnType, DecimalType):
        # FloatType included: the body computes in double and the result is cast down, which
        # is exactly what the interpreted path does with a Python float declared as float.
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
