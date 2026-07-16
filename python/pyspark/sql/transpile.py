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
Experimental tools for transpiling UDFS.

Transpilation is only attempted when both
``spark.sql.experimental.optimizer.transpilePyUDFs=true`` and
``spark.sql.ansi.enabled=true``. The generated Catalyst expressions
target ANSI-mode SQL semantics (overflow raises, divide-by-zero raises,
etc.); running them under non-ANSI mode would silently diverge from the
Python interpretation in ways we don't currently track. If you flip
transpilation on with ANSI off the UDF will fall back to interpreted
Python execution and a warning is logged at UDF construction time.

Python's ``+`` and ``*`` are overloaded for text (concat / repeat), so an
untyped parameter is transpiled into one option per input-type category
(numeric and string) and the JVM picks the one matching the bound column
types -- falling back to interpreted Python when none fit. Annotating the
UDF's parameters (e.g. ``def f(a: int, b: str)``) pins each category and
keeps the option matrix small; prefer doing so. To bound plan growth,
functions with more than three untyped parameters only emit the
all-numeric and all-string variants.
"""

import ast
from typing import Any, Callable, List, Optional, Tuple, TYPE_CHECKING
import inspect
import itertools
import textwrap
from pyspark.errors import UnsupportedOperationException
from pyspark.sql.column import Column
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DataType,
    DecimalType,
    NumericType,
    StringType,
)
from pyspark.sql.functions import (
    abs as _abs,
    coalesce,
    col,
    concat,
    lit,
    pmod,
    raise_error,
    repeat,
    when,
)


if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql._typing import DataTypeOrString


class AbstractTranspiler(object):
    """Base class for transpilers. All experimental."""

    varieties: dict[str, type["AbstractTranspiler"]] = {}
    # Specify the "friendly" name a user can add to spark.sql.experimental.optimizer.pyTranspilers
    # to enable this transpiler.
    variety: str = ""

    @classmethod
    def register(cls) -> None:
        AbstractTranspiler.varieties[cls.variety] = cls

    def _transpile_from_ast(
        self,
        src: Optional[str],
        ast_info: ast.AST,
        function_ast: ast.FunctionDef,
        params: List[str],
        returnType: "DataTypeOrString",
        param_categories: Optional[dict] = None,
    ) -> Optional[Column]:
        pass


def _is_definitely_basic_type(node: ast.AST) -> bool:
    """
    Return True when ``node`` is statically guaranteed to produce a Python
    basic/builtin type (int, float, str, bool, None, lists, etc.).
    All ast.Name's are treated as basic types for now this will need to be updated
    if/when we add free variables / closures to transpilation.
    """
    match node:
        case ast.Constant():
            return True
        case ast.BinOp(left=left, right=right):
            return _is_definitely_basic_type(left) and _is_definitely_basic_type(right)
        case ast.UnaryOp(operand=operand):
            return _is_definitely_basic_type(operand)
        case ast.Name():
            return True
        case _:
            return False


def _is_definitely_boolean(node: ast.AST) -> bool:
    """Return True when ``node`` is statically guaranteed to produce a Python
    ``bool`` (or ``None``, which round-trips through ``coalesce``).

    Used to gate ``if``/ternary lowering: we only allow the test expression
    into Catalyst's ``when(coalesce(test, false), ...)`` form when it provably
    produces a boolean. Everything else (bare Name, arithmetic, function calls,
    subscript, ...) must force a fallback to interpreted Python instead of
    silently diverging.
    """
    match node:
        case ast.Constant(value=v):
            return v is None or isinstance(v, bool)
        case ast.Compare(left=left, comparators=comparators):
            # All comparison operators of simple types bool
            return all(_is_definitely_basic_type(v) for v in comparators + [left])
        case ast.BoolOp(values=values):
            return all(_is_definitely_boolean(v) for v in values)
        case ast.UnaryOp(op=ast.Not()):
            # `not x` always produces bool.
            return True
        case ast.IfExp(body=body, orelse=orelse):
            # Ternary is boolean only if both branches are.
            return _is_definitely_boolean(body) and _is_definitely_boolean(orelse)
        case _:
            return False


class CatalystTranspiler(AbstractTranspiler):
    """Transpiler that attempts to convert a Python UDF into native Spark SQL expressions."""

    variety = "catalyst"

    # TODO (SPARK-55218): handle implicit-None return bodies like
    # ``def f(x): x + x`` -- no return statement means return None;
    # we should lower to lit(None) and optionally warn since it's
    # likely a mistake.
    def _convert_branch(self, params: List[str], statements: List[ast.stmt], slot: str) -> Column:
        """Lower a single-statement if-body / if-else block.

        ``slot`` is just used to disambiguate the multi-statement error
        message between the body and the else arm.
        """
        if len(statements) > 1:
            raise UnsupportedOperationException(
                f"if statements with more than one expression in the {slot} "
                "are not currently supported by the transpiler"
            )
        if len(statements) == 0:
            return lit(None)
        return self._convert_chunk(params, statements[0])

    def _safe_category(self, params: List[str], node: Optional[ast.AST]) -> Optional[str]:
        """Best-effort input-type category for an if/else branch, or ``None`` when
        it can't be pinned down statically.

        Used only to compare the two branches of an if/ternary. A ``None`` result
        means "treat as compatible" (don't force a fallback): the node is absent,
        is a bare ``None`` literal (which unifies with any branch type via
        ``coalesce``/``Cast``), or its category can't be determined.
        """
        if node is None:
            return None
        # If-statement branches arrive as ``Return`` statements; classify the
        # returned value, not the statement wrapper (``_is_definitely_boolean``
        # has no ``Return`` case, so without this a boolean-returning branch
        # would fall through to ``_category``'s numeric catch-all).
        if isinstance(node, ast.Return):
            return self._safe_category(params, node.value)
        # An if-statement's category is its branches' common category (the
        # ``_category`` catch-all would mislabel every ``ast.If`` "numeric").
        # Mismatched branches return None ("can't be pinned down"); the
        # branch-compatibility check in ``_convert_if_like`` raises for them.
        if isinstance(node, ast.If):
            body_c = self._safe_category(params, node.body[0]) if node.body else None
            else_c = self._safe_category(params, node.orelse[0]) if node.orelse else None
            if body_c is not None and else_c is not None and body_c != else_c:
                return None
            return body_c if body_c is not None else else_c
        if isinstance(node, ast.Constant) and node.value is None:
            return None
        # Comparisons / ``not`` / boolean ops produce a boolean column; classify
        # them as "bool" (``_category``'s catch-all would mislabel them numeric).
        if _is_definitely_boolean(node):
            return "bool"
        try:
            return self._category(params, node)
        except UnsupportedOperationException:
            return None

    def _convert_if_like(
        self,
        params: List[str],
        test_col: Column,
        body_col: Column,
        else_col: Column,
        test_node: ast.AST,
        body_node: Optional[ast.AST],
        else_node: Optional[ast.AST],
    ) -> Column:
        # We cannot soundly lower a generic Python truthiness test here.
        # Python truthiness depends on the runtime input type and value:
        # for example, 0, 0.0, "", empty collections, and None are all
        # falsy, while most other values are truthy. The transpiler does
        # not have enough input type information at this point to decide
        # whether ``test_col`` is a boolean expression or a bare value
        # whose truthiness would need Python-specific handling. Emitting
        # ``when(coalesce(test_col, false), ...)`` is therefore unsound:
        # it can either fail Spark analysis for non-boolean columns or
        # silently diverge from Python semantics. Fail closed so the UDF
        # falls back to interpreted Python execution instead.
        if not _is_definitely_boolean(test_node):
            raise UnsupportedOperationException(
                f"bare truthiness tests ({ast.dump(test_node)}) in if-expressions are "
                " not currently supported by the transpiler"
            )
        # When the two branches resolve to concrete but different categories
        # (e.g. numeric vs string), the lowered ``when(...).otherwise(...)`` is a
        # CASE WHEN whose branch values share no common type under ANSI. That node
        # is carried as a child of the TranspiledPythonUDF and is type-checked by
        # CheckAnalysis *before* ConvertToCatalyst can drop it, so it would fail
        # the whole query rather than fall back. Refuse here so the UDF runs as
        # interpreted Python instead. Branches whose category we can't pin down
        # (e.g. a bare ``None``) are treated as compatible and don't force this.
        body_cat = self._safe_category(params, body_node)
        else_cat = self._safe_category(params, else_node)
        if body_cat is not None and else_cat is not None and body_cat != else_cat:
            raise UnsupportedOperationException(
                f"if/else branches have incompatible categories ({body_cat} vs "
                f"{else_cat}); the lowered CASE WHEN has no common type under ANSI, "
                "so the transpiler falls back to interpreted Python"
            )
        safe_test = coalesce(test_col, lit(False))
        return when(safe_test, body_col).otherwise(else_col)

    def _lower_eq(
        self,
        params: List[str],
        left_node: ast.AST,
        right_node: ast.AST,
        equal: bool,
    ) -> Column:
        """Lower ``==`` / ``!=`` with Python's None-equality semantics.

        Unlike ordering operators, Python doesn't raise on ``None == x`` /
        ``None != x``: ``None == None`` is True, ``None == 0`` is False,
        and ``!=`` is the negation. Spark's ``==`` returns NULL on NULL
        operands (three-valued logic), which would round-trip through
        the UDF as ``None`` rather than the bool Python would have
        produced. Hand-roll the four cases via ``when`` branches.

        When the two operands resolve to concrete but DIFFERENT categories
        (e.g. ``x == True`` on a numeric column, or ``x == "5"`` under the
        numeric variant), the lowered ``=`` either fails analysis under ANSI
        (bool vs bigint) -- which would break a working UDF since the option
        is type-checked before ConvertToCatalyst can drop it -- or coerces
        where Python's ``==`` is simply False. Refuse those so the UDF falls
        back to interpreted Python. A ``None`` literal operand stays allowed
        (the four-branch NULL handling above reproduces Python exactly).

        One value-level difference remains (needs runtime values, so it is
        documented, not guarded): Spark treats ``NaN = NaN`` as true, while
        Python's ``nan == nan`` is False.
        """
        lc = self._safe_category(params, left_node)
        rc = self._safe_category(params, right_node)
        if lc is not None and rc is not None and lc != rc:
            raise UnsupportedOperationException(
                f"`==`/`!=` operands have incompatible categories ({lc} vs {rc}); "
                "Python compares across types as unequal while Spark would coerce "
                "or fail analysis, so the transpiler falls back to interpreted Python"
            )
        left_col = self._convert_chunk(params, left_node)
        right_col = self._convert_chunk(params, right_node)
        left_null = left_col.isNull()
        right_null = right_col.isNull()
        if equal:
            both_null_val: Column = lit(True)
            one_null_val: Column = lit(False)
            value_cmp = left_col == right_col
        else:
            both_null_val = lit(False)
            one_null_val = lit(True)
            value_cmp = left_col != right_col
        return (
            when(left_null & right_null, both_null_val)
            .when(left_null | right_null, one_null_val)
            .otherwise(value_cmp)
        )

    def _lower_value_compare(
        self,
        params: List[str],
        left_node: ast.AST,
        right_node: ast.AST,
        op: Callable[[Column, Column], Column],
        op_repr: str,
    ) -> Column:
        """Lower a value comparison (``<``, ``<=``, ``>``, ``>=``).

        Python raises ``TypeError`` when an operand of these operators is
        ``None`` (e.g. ``None > 0``), whereas Spark's three-valued logic
        returns ``NULL``. To stay faithful to the source UDF we guard the
        comparison: if either operand is ``NULL`` we raise via
        ``raise_error``, otherwise we evaluate ``left op right`` as usual.
        Callers that have already proven the operand non-null (``if x is
        not None: x > 0``) take the otherwise branch, so they never trip
        the raise.

        Python also forbids ordering across types (``1 < "a"`` -> TypeError),
        whereas Spark would coerce the operands and return a (wrong) boolean.
        We therefore only lower when both operands share a category; a
        mismatch raises so this variant is dropped and the UDF falls back to
        interpreted Python rather than silently diverging.

        One value-level difference from Python remains (it needs runtime
        value info, so it is documented, not guarded): Spark orders ``NaN``
        as greater than every value, whereas Python's ``NaN`` comparisons
        are all ``False``.
        """
        lc = self._category(params, left_node)
        rc = self._category(params, right_node)
        if lc != rc:
            raise UnsupportedOperationException(
                f"`{op_repr}` compares operands of different categories "
                f"({lc} vs {rc}); Python would raise TypeError, so the "
                "transpiler falls back to interpreted Python"
            )
        left_col = self._convert_chunk(params, left_node)
        right_col = self._convert_chunk(params, right_node)
        null_guard = left_col.isNull() | right_col.isNull()
        err = lit(
            "Python UDF transpiler: cannot compare NULL with operator "
            f"`{op_repr}`; Python would raise TypeError here. Add an "
            "`is not None` guard or filter NULLs upstream."
        )
        return when(null_guard, raise_error(err)).otherwise(op(left_col, right_col))

    def _category(self, params: List[str], node: ast.AST) -> str:
        """Infer ``"numeric"`` or ``"string"`` for ``node`` under the current
        ``self._param_categories`` assumption (set per input-type variant).

        Drives operator selection (``+`` -> add vs concat, ``*`` -> multiply vs
        repeat) and raises ``UnsupportedOperationException`` when an operator's
        operands are type-incompatible, so the caller drops that variant and the
        JVM picks another option / falls back to the Python UDF.
        """
        match node:
            case ast.Constant(value=v):
                # bool subclasses int, so classify it first: int/float -> numeric,
                # str -> string, bool -> bool, bytes -> binary. None/complex/
                # Ellipsis have no usable Spark column type, so raise to drop this
                # variant and fall back rather than emit an option that fails
                # CheckAnalysis or silently diverges (e.g. `x + None` -> NULL where
                # Python raises TypeError).
                if isinstance(v, bool):
                    return "bool"
                if isinstance(v, bytes):
                    return "binary"
                if isinstance(v, (int, float)):
                    return "numeric"
                if isinstance(v, str):
                    return "string"
                raise UnsupportedOperationException(
                    f"constant {v!r} ({type(v).__name__}) has no usable column "
                    "category; falling back to interpreted Python"
                )
            case ast.Name(id=name) if name in params:
                index = params.index(name)
                if params and params[0] == "self":
                    index -= 1
                return self._param_categories.get(index, "numeric")
            case ast.BinOp(left=left, op=op, right=right):
                lc = self._category(params, left)
                rc = self._category(params, right)
                if isinstance(op, ast.Add) and lc == rc:
                    return lc  # str + str -> str, num + num -> num
                if isinstance(op, ast.Mult):
                    if lc == "numeric" and rc == "numeric":
                        return "numeric"
                    if {lc, rc} == {"numeric", "string"}:
                        return "string"  # str * int / int * str -> repeat
                if isinstance(op, (ast.Sub, ast.Mod)) and lc == rc == "numeric":
                    return "numeric"
                raise UnsupportedOperationException(
                    f"operands of `{type(op).__name__}` are not type-compatible "
                    "for this input-type variant"
                )
            case ast.Return(value=value) if value is not None:
                return self._category(params, value)
            case ast.IfExp(body=if_body, orelse=if_orelse):
                # A ternary's category is its branches' common category. Without
                # this arm the catch-all labeled every IfExp "numeric", so e.g.
                # `("5" if c else "6") == 5` passed the equality guard as
                # numeric-vs-numeric and Spark's string-number coercion silently
                # diverged from Python's cross-type `==` (always False). A
                # None-literal branch adopts the other branch's category (NULL
                # unifies with any type in the lowered CASE WHEN); mismatched or
                # all-None branches raise so the variant is dropped.
                def branch_category(b: ast.AST) -> Optional[str]:
                    if isinstance(b, ast.Constant) and b.value is None:
                        return None
                    return self._category(params, b)

                body_cat = branch_category(if_body)
                else_cat = branch_category(if_orelse)
                if body_cat is not None and else_cat is not None and body_cat != else_cat:
                    raise UnsupportedOperationException(
                        f"ternary branches have mismatched categories ({body_cat} "
                        f"vs {else_cat}) and cannot drive operator selection"
                    )
                result_cat = body_cat if body_cat is not None else else_cat
                if result_cat is None:
                    raise UnsupportedOperationException(
                        "ternary with all-None branches has no usable column category"
                    )
                return result_cat
            case _ if _is_definitely_boolean(node):
                # Comparisons, `not`, and boolean ops produce a boolean column.
                # Labeling them "numeric" (the old catch-all) let booleans into
                # arithmetic/equality lowerings where ANSI analysis fails (e.g.
                # `(x > 0) + 1`, valid Python) instead of falling back.
                return "bool"
            case _:
                # Remaining nodes (unsupported calls, subscripts, ...) don't
                # drive concat/repeat selection and are rejected later by
                # `_convert_chunk`; treat as numeric for category purposes.
                return "numeric"

    def _convert_chunk(self, params: List[str], body: ast.AST | None) -> Column:
        match body:
            case None:
                # Special case literal None, the implicit return None
                return lit(None)
            case ast.UnaryOp(op=ast.Not(), operand=operand):
                # Python's `not None` is `True` (None is falsy), but Spark's
                # `~NULL` is `NULL`. Coalesce against `lit(True)` so a NULL
                # operand mirrors Python's "None is falsy" rule. We only
                # accept operands that are statically known to be boolean;
                # for non-boolean operands (e.g. `not 0`, `not x` where x is
                # a bare parameter name) Spark's `~` is bitwise, not Python
                # truthiness, so we bail and let the caller fall back to
                # interpreted Python rather than silently diverge.
                if not _is_definitely_boolean(operand):
                    raise UnsupportedOperationException(
                        "`not` operand type is not statically known to be "
                        "boolean; Spark's `~` is bitwise, not Python "
                        "truthiness, so the transpiler refuses to lower this "
                        "and the UDF falls back to interpreted Python"
                    )
                return coalesce(self._convert_chunk(params, operand).__invert__(), lit(True))
            case ast.UnaryOp(op=(ast.USub() | ast.UAdd()) as op, operand=operand):
                # `-x` / `+x` -- like the binary arithmetic operators, only
                # lower for numeric operands. Python raises TypeError for
                # unary +/- on strings, but Spark's ANSI string promotion
                # would silently coerce the string to double (`-'5'` ->
                # -5.0), and a boolean operand emits UnaryMinus(bool), which
                # fails CheckAnalysis outright -- breaking the query instead
                # of falling back, since the option is type-checked as a
                # child of TranspiledPythonUDF before ConvertToCatalyst can
                # drop it. Fail closed for every non-numeric category.
                if self._category(params, operand) != "numeric":
                    raise UnsupportedOperationException(
                        "unary `+`/`-` is only supported for numeric operands "
                        "(Python raises TypeError on strings, and Spark would "
                        "coerce or fail analysis); the transpiler falls back "
                        "to interpreted Python"
                    )
                if isinstance(op, ast.USub):
                    # Handles both literal negative ints (USub on a Constant)
                    # and runtime negation of a column.
                    return self._convert_chunk(params, operand).__neg__()
                # `+x` -- identity, kept for symmetry with USub.
                return self._convert_chunk(params, operand)
            case ast.BoolOp(op=op, values=values):
                # Python `and` / `or` short-circuit and return one of the
                # operands rather than a strict boolean. For the booleans
                # produced by Compare / UnaryOp(Not) / nested BoolOps this
                # maps cleanly onto Spark Column `&` / `|`. For
                # non-boolean operands (including bare parameter names whose
                # runtime type is unknown) the right semantics would require
                # Python's truthiness rules (0 / "" / None / [] all
                # falsy), which we can't faithfully reproduce without the
                # input column types -- Spark's `&` / `|` would silently
                # do bitwise instead. Require all operands to be statically
                # known boolean so the caller falls back to interpreted
                # Python rather than producing a plan whose results diverge.
                if not all(_is_definitely_boolean(v) for v in values):
                    raise UnsupportedOperationException(
                        "`and` / `or` operand type is not statically known "
                        "to be boolean; Spark's `&` / `|` are bitwise, not "
                        "Python truthiness, so the transpiler refuses to "
                        "lower this and the UDF falls back to interpreted "
                        "Python"
                    )
                # A literal None operand short-circuits differently: Python's
                # `None and (x > 0)` returns None regardless of x, but Spark's
                # three-valued `null AND false` is false (and `null OR true` is
                # true), so the lowered form diverges. `_is_definitely_boolean`
                # accepts None for `not`/if-test contexts where coalesce handles
                # it; here it must force a fallback instead.
                if any(isinstance(v, ast.Constant) and v.value is None for v in values):
                    raise UnsupportedOperationException(
                        "literal None operand in `and` / `or` cannot be lowered: "
                        "Spark's three-valued logic diverges from Python's "
                        "short-circuit-return-operand semantics, so the UDF "
                        "falls back to interpreted Python"
                    )
                cols = [self._convert_chunk(params, v) for v in values]
                if isinstance(op, ast.And):
                    result = cols[0]
                    for c in cols[1:]:
                        result = result & c
                    return result
                if isinstance(op, ast.Or):
                    result = cols[0]
                    for c in cols[1:]:
                        result = result | c
                    return result
                raise UnsupportedOperationException(f"BoolOp operator {op} is not supported")
            case ast.IfExp(test=test, body=body_expr, orelse=orelse_expr):
                # Ternary `body if test else orelse` -- shares the
                # NULL-as-falsy lowering with the if-statement case.
                return self._convert_if_like(
                    params,
                    self._convert_chunk(params, test),
                    self._convert_chunk(params, body_expr),
                    self._convert_chunk(params, orelse_expr),
                    test,
                    body_expr,
                    orelse_expr,
                )
            case ast.If(test, success, orelse):
                return self._convert_if_like(
                    params,
                    self._convert_chunk(params, test),
                    self._convert_branch(params, success, "body"),
                    self._convert_branch(params, orelse, "else body"),
                    test,
                    success[0] if success else None,
                    orelse[0] if orelse else None,
                )
            case ast.Compare(left, ops, comps):
                if len(ops) != 1 or len(comps) != 1:
                    raise UnsupportedOperationException(
                        "chained comparisons (e.g. `a < b < c`) are not supported by the transpiler"
                    )
                comp = comps[0]
                match ops[0]:
                    case ast.Is() | ast.IsNot():
                        # Only lower `x is None` / `None is x` (and their
                        # `is not` variants) to isNull/isNotNull. For any
                        # other comparator (e.g. `x is 0`, `x is y`) Python
                        # performs an object-identity check that has no SQL
                        # equivalent, so we must fall back to interpreted
                        # Python rather than silently emitting a null check.
                        is_none_left = isinstance(left, ast.Constant) and left.value is None
                        is_none_right = isinstance(comp, ast.Constant) and comp.value is None
                        if not (is_none_left or is_none_right):
                            raise UnsupportedOperationException(
                                "`is`/`is not` is only supported when one "
                                "operand is the literal None; other identity "
                                "checks (e.g. `x is 0`, `x is y`) cannot be "
                                "lowered to SQL and the UDF falls back to "
                                "interpreted Python"
                            )
                        subject_node = comp if is_none_left else left
                        subject_col = self._convert_chunk(params, subject_node)
                        if isinstance(ops[0], ast.Is):
                            return subject_col.isNull()
                        else:
                            return subject_col.isNotNull()
                    case ast.Eq():
                        return self._lower_eq(params, left, comp, equal=True)
                    case ast.NotEq():
                        return self._lower_eq(params, left, comp, equal=False)
                    case ast.Lt():
                        return self._lower_value_compare(
                            params, left, comp, lambda l, r: l < r, "<"
                        )
                    case ast.LtE():
                        return self._lower_value_compare(
                            params, left, comp, lambda l, r: l <= r, "<="
                        )
                    case ast.Gt():
                        return self._lower_value_compare(
                            params, left, comp, lambda l, r: l > r, ">"
                        )
                    case ast.GtE():
                        return self._lower_value_compare(
                            params, left, comp, lambda l, r: l >= r, ">="
                        )
                    case _:
                        raise UnsupportedOperationException(
                            f"comparison operator {type(ops[0]).__name__} "
                            "is not supported by the transpiler"
                        )
            case ast.BinOp(left=left, op=op, right=right):
                # Operator selection is driven by the operand *categories* under
                # the current input-type variant (see ``_category``): Python's
                # `+` / `*` are overloaded for text. `+` -> add (num,num) or
                # concat (str,str); `*` -> multiply (num,num) or repeat (str,int
                # / int,str); `-` / `%` are numeric-only. Combos that don't fit
                # (str+int, str-str, ...) raise so this variant is dropped and
                # the JVM picks another option or falls back to the Python UDF.
                #
                # `**` is intentionally NOT lowered: Spark's `pow` is DOUBLE and
                # loses precision for large integers, so it would silently return
                # wrong results. TODO (SPARK-55210): add an exact integer-power
                # lowering and re-enable it.
                #
                # Value-level divergences remain documented (need runtime value
                # info, not type): overflow raises ARITHMETIC_OVERFLOW under ANSI
                # where Python promotes to a big int; arithmetic is not
                # NULL-guarded (`x + 1` on NULL -> NULL vs Python TypeError).
                # TODO (SPARK-55210): map overflow / divide-by-zero precisely.
                lc = self._category(params, left)
                rc = self._category(params, right)
                left_col = self._convert_chunk(params, left)
                right_col = self._convert_chunk(params, right)
                match op:
                    case ast.Add():
                        if lc == rc == "string":
                            return concat(left_col, right_col)
                        if lc == rc == "numeric":
                            return left_col.__add__(right_col)
                    case ast.Sub():
                        if lc == rc == "numeric":
                            return left_col.__sub__(right_col)
                    case ast.Mult():
                        if lc == "numeric" and rc == "numeric":
                            return left_col.__mul__(right_col)
                        if lc == "string" and rc == "numeric":
                            return repeat(left_col, right_col.cast("int"))
                        if lc == "numeric" and rc == "string":
                            return repeat(right_col, left_col.cast("int"))
                    case ast.Mod():
                        if lc == rc == "numeric":
                            # Python's `%` takes the sign of the divisor; Spark's
                            # takes the dividend's. `sign(b) * pmod(sign(b) * a,
                            # abs(b))` reproduces Python for every non-zero divisor
                            # except at the LongType overflow boundaries -- `a =
                            # Long.MinValue` with `b < 0` (the `sign(b) * a` negate
                            # overflows) and `b = Long.MinValue` (the `abs(b)`
                            # overflows) -- where this raises ARITHMETIC_OVERFLOW
                            # under ANSI while Python returns a value. That matches
                            # the documented overflow caveat for `+`/`-`/`*` above.
                            # Use a CASE-based integer sign rather than sign() to
                            # avoid promoting operands to DoubleType, which loses
                            # precision near LongType boundaries.
                            sb = (
                                when(right_col > 0, lit(1))
                                .when(right_col < 0, lit(-1))
                                .otherwise(lit(0))
                            )
                            return sb * pmod(sb * left_col, _abs(right_col))
                    case _:
                        raise UnsupportedOperationException(
                            f"binary operator {type(op).__name__} is not "
                            "supported by the transpiler"
                        )
                raise UnsupportedOperationException(
                    f"`{type(op).__name__}` operands are not type-compatible for "
                    "this input-type variant"
                )
            case ast.Return(value=value):
                return self._convert_chunk(params, value)
            case ast.Constant(value=value):
                # Avoid circular import issue.
                return lit(value)
            case ast.Name(id=name, ctx=ast.Load()):
                # Insert columns referencing the param indexes for children
                if name in params:
                    param_index = params.index(name)
                    # Special hack for self on callables
                    if params[0] == "self":
                        # A body that references the receiver itself (e.g.
                        # ``return self``) has no column equivalent: ``self``
                        # is not an argument at the call site, and offsetting
                        # would emit ``_udf_param_-1``, which the JVM builder
                        # rejects with an AnalysisException at call
                        # construction instead of falling back. Refuse so the
                        # UDF stays interpreted.
                        if name == "self":
                            raise UnsupportedOperationException(
                                "references to `self` in a callable's body "
                                "are not supported by the transpiler; falling "
                                "back to interpreted Python"
                            )
                        param_index -= 1
                    return col(f"_udf_param_{param_index}")
                else:
                    # TODO (SPARK-55207): Handle assignments, class vars, and closures
                    # via scope evaluation.
                    raise UnsupportedOperationException(
                        f"name {name!r} is not in the UDF's parameter list "
                        "and free variables / closures are not supported"
                    )
            case _:
                raise UnsupportedOperationException(
                    f"AST node {type(body).__name__} is not supported by the "
                    f"transpiler ({ast.dump(body)[:120]})"
                )

    def _transpile_from_ast(
        self,
        src: Optional[str],
        ast_info: ast.AST,
        function_ast: ast.FunctionDef,
        params: List[str],
        returnType: "DataTypeOrString",
        param_categories: Optional[dict] = None,
    ) -> Optional[Column]:
        # Short circuit on nothing to transpile.
        if src == "" or ast_info is None:
            return None
        # Per-variant input-type assumption ({public_param_index -> category}),
        # read by ``_category`` to choose str vs numeric operators.
        self._param_categories = param_categories or {}
        function_body = function_ast.body
        if len(function_body) != 1:
            raise UnsupportedOperationException(
                "functions with more than one top-level statement are not "
                "supported by the transpiler"
            )
        # Refuse variants whose body category does not MATCH the declared
        # return type's category. Two distinct failure modes hide here:
        #
        # * A cast that can never resolve (binary -> numeric, bool -> binary):
        #   the options are type-checked by CheckAnalysis as children of
        #   TranspiledPythonUDF before ConvertToCatalyst could drop them, so
        #   the whole query fails instead of falling back.
        # * A cast that IS analysis-valid but that the interpreted
        #   SQL_BATCHED_UDF path never performs: EvaluatePython.makeFromJava
        #   accepts only the expected JVM types for the declared return type
        #   and nulls everything else. E.g. `def f(s: str): return s` declared
        #   LongType() returns NULL interpreted, but a lowered
        #   cast(string as bigint) would return 123 for '123' (or raise
        #   CAST_INVALID_INPUT for 'abc') -- a silent divergence.
        #
        # So require the strict match: numeric -> non-decimal NumericType
        # (DecimalType is excluded like it is for inputs: the interpreted
        # converter accepts only decimal.Decimal results there and nulls the
        # ints/floats these lowerings produce), string -> StringType, bool ->
        # BooleanType, binary -> BinaryType. An unknown category (e.g. a bare
        # None body) lowers to NULL, which every return type accepts as NULL
        # on both paths. Within-numeric conversions (e.g. a bigint body cast
        # to a double return type) are intentionally still allowed and
        # documented as the transpiled-cast behavior pinned by
        # test_udf_transpile_casts_to_return_type.
        if isinstance(returnType, DataType):
            body_cat = self._safe_category(params, function_body[0])
            cast_ok = (
                body_cat is None
                or (
                    body_cat == "numeric"
                    and isinstance(returnType, NumericType)
                    and not isinstance(returnType, DecimalType)
                )
                or (body_cat == "string" and isinstance(returnType, StringType))
                or (body_cat == "bool" and isinstance(returnType, BooleanType))
                or (body_cat == "binary" and isinstance(returnType, BinaryType))
            )
            if not cast_ok:
                raise UnsupportedOperationException(
                    f"a {body_cat}-typed lowering does not match the declared "
                    f"return type {returnType.simpleString()}; the interpreted "
                    "path would return NULL where the lowered cast would "
                    "convert (or fail), so the transpiler falls back to "
                    "interpreted Python"
                )
        converted = self._convert_chunk(params, function_body[0])
        # Cast to the declared return type so the rewritten plan reports a
        # known data type to the optimizer's plan validator (otherwise it
        # sees an UnresolvedFunction tree and reports VOID, which fails
        # the schema-stability check on this rule).
        return converted.cast(returnType)


CatalystTranspiler.register()


def _get_transpilers(session: "SparkSession") -> List[AbstractTranspiler]:
    """Get the transpilers we should try."""
    configured_transpilers = session.conf.get("spark.sql.experimental.optimizer.pyTranspilers")
    if not configured_transpilers:
        return []
    transpiler_names = configured_transpilers.split(",")
    return [
        AbstractTranspiler.varieties[name]()
        for name in transpiler_names
        if name in AbstractTranspiler.varieties
    ]


def _annotation_category(annotation: Optional[ast.AST]) -> Optional[str]:
    """Map a parameter's type annotation to a category
    (``"numeric"``/``"string"``/``"bool"``/``"binary"``), or ``None`` when it's
    absent or unrecognised (the caller then tries both numeric and string)."""
    name: Optional[str] = None
    if isinstance(annotation, ast.Name):
        name = annotation.id
    elif isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        name = annotation.value  # stringized annotation, e.g. def f(a: "int")
    # str -> "string", int/float -> "numeric", bool -> "bool", bytes -> "binary"
    # (matching the constant handling in ``_category``). complex and anything
    # unrecognised return None so the caller tries both numeric and string.
    if name == "str":
        return "string"
    if name in ("int", "float"):
        return "numeric"
    if name == "bool":
        return "bool"
    if name == "bytes":
        return "binary"
    return None


def _param_category_combos(function_ast: ast.FunctionDef, public_params: List[str]) -> List[dict]:
    """Per-variant maps ``{public_param_index -> category}`` where category is
    one of ``"numeric"``/``"string"``/``"bool"``/``"binary"``.

    A typed param (``def f(a: str, b: int)``) is pinned to its category; an
    untyped param is tried as both numeric and string. To cap plan growth, when
    more than three params are untyped we collapse the untyped ones to the
    all-numeric and all-string variants (encourage typing inputs to keep the
    matrix small) while keeping every typed param pinned.
    """
    n = len(public_params)
    public_args = function_ast.args.args[len(function_ast.args.args) - n :]
    candidates: List[List[str]] = []
    untyped = 0
    for arg in public_args:
        cat = _annotation_category(arg.annotation)
        if cat is None:
            candidates.append(["numeric", "string"])
            untyped += 1
        else:
            candidates.append([cat])
    if untyped > 3:
        # Cap the 2**untyped blow-up, but keep each typed param pinned to its
        # category (a single-element ``candidates`` entry); only the untyped
        # params collapse to the all-numeric / all-string pair.
        return [
            {i: c[0] if len(c) == 1 else fill for i, c in enumerate(candidates)}
            for fill in ("numeric", "string")
        ]
    return [{i: choice[i] for i in range(n)} for choice in itertools.product(*candidates)] or [{}]


def _get_src_ast_from_func(func: Callable) -> Tuple[Optional[str], Optional[ast.AST]]:
    """Try and get the AST from a given callable"""
    # Note: consider maybe dill? (see the JYTHON PR)
    # inspect getsource does not work for functions defined in vanilla
    # repl, but does for those in files or in ipython.
    # It also fails when we give it an instance of a callable class.
    try:
        src = inspect.getsource(func)
        src = textwrap.dedent(src).strip()
        ast_info = ast.parse(src)
    except Exception:
        try:
            # getattr keeps mypy happy: `__call__` on a bare Callable is
            # not attribute-accessible in the type system.
            src = inspect.getsource(getattr(func, "__call__"))
            src = textwrap.dedent(src).strip()
            ast_info = ast.parse(src)
        except Exception:
            # No usable source (REPL/stdin definition, builtin, ...) --
            # return cleanly so the caller reports "cannot transpile"
            # instead of surfacing an UnboundLocalError as the reason.
            return None, None
    return src, ast_info


def _get_parameter_list(node: ast.FunctionDef) -> list[str]:
    """Return the positional argument names in order."""
    return [arg.arg for arg in node.args.args]


def _get_function_from_ast(body: ast.AST) -> ast.FunctionDef | None:
    """
    Extract a :class:`ast.FunctionDef` node from an AST produced by
    ``ast.parse(inspect.getsource(udf_func))``.

    Handles the following source patterns (in order):

    * ``f = lambda x: x + 1`` -- lambda bound directly to a name
    * ``lambda x: x + 1`` -- bare expression (getsource on a raw lambda)
    * ``def f(x): ... return x + 1``
    * a class with a ``__call__`` method

    Returns ``None`` when no single unambiguous function can be identified --
    notably, a lambda wrapped in a call such as
    ``f = some_wrapper(lambda x: x + 1)`` parses as ``Assign(value=Call(...))``,
    which is not unwrapped here and so falls back to interpreted Python. Local
    class variables are likewise unsupported.
    """
    if not hasattr(body, "body") or not body.body:
        return None

    stmt = body.body[0]

    # Grab the value side of a top level assign (e.g. x = lambda ...)
    if isinstance(stmt, ast.Assign):
        stmt = stmt.value

    # Bare ``lambda x: ...`` (when ``inspect.getsource`` returns a raw
    # lambda expression at module top level) parses as ``Expr(Lambda)``.
    if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Lambda):
        stmt = stmt.value

    if isinstance(stmt, ast.Lambda):
        # Synthesize a one-statement FunctionDef wrapping the lambda body so
        # the rest of the transpiler can treat lambdas and ``def`` uniformly.
        # ``ast.FunctionDef``'s overloads in mypy's typeshed require
        # keyword-only ``type_params`` on 3.12+, which doesn't exist at
        # runtime on every Python we support (the field was added in
        # 3.12 -- before that, passing it raises). Drop to ``Any`` so we
        # avoid the overload resolution entirely; constructing the node
        # via keyword args is well-defined at runtime even when the typed
        # overloads disagree.
        fn_ctor: Any = ast.FunctionDef
        return fn_ctor(
            name="<lambda>",
            args=stmt.args,
            body=[ast.Return(value=stmt.body)],
            decorator_list=[],
        )

    if isinstance(stmt, ast.FunctionDef):
        return stmt
    return None


def _transpile_func(
    session: "SparkSession",
    func: Callable[..., Any],
    returnType: "DataTypeOrString",
) -> Tuple[List[Column], List[str], List[str], List[List[str]]]:
    """
    An experimental internal function that attempts to transpile a callable function.

    Returns
    -------
    list of transpiled options (one per backend x input-type variant)
    list of errors as strings
    list of positional parameter names (excluding ``self`` for callable
    instances) -- needed so the caller can resolve named-argument
    invocations to positional order at call time, since the ``_udf_param_N``
    substitution in :class:`UserDefinedPythonFunction` is positional.
    list of per-option input-type categories (``"numeric"`` / ``"string"`` per
    public param) -- the JVM picks the option whose categories match the bound
    column types, or falls back to the Python UDF when none match.
    """
    try:
        # The transpiler lowers to atomic (numeric/string/boolean/binary)
        # expressions and casts the result to the declared return type. For a
        # return type no lowering can even category-match (arrays, maps,
        # structs, datetimes, ...), that Cast either never resolves -- and
        # because the options ride along as children of TranspiledPythonUDF,
        # an unresolvable Cast fails the WHOLE query at CheckAnalysis instead
        # of falling back -- or diverges from the interpreted converter, which
        # nulls type-mismatched results. Restrict transpilation to return
        # types some lowering can match (the strict per-variant body-category
        # check lives in ``_transpile_from_ast``); everything else falls back
        # to interpreted Python.
        if isinstance(returnType, str):
            from pyspark.sql.types import _parse_datatype_string

            returnType = _parse_datatype_string(returnType)
        if not isinstance(returnType, (NumericType, StringType, BooleanType, BinaryType)):
            return (
                [],
                [
                    f"return type {returnType.simpleString()} is not supported by "
                    "the transpiler (no lowered expression can be cast to it "
                    "under ANSI rules); falling back to interpreted Python"
                ],
                [],
                [],
            )
        # A functools.wraps-style decorator makes ``inspect.getsource`` return
        # the WRAPPED function's source (getsource follows ``__wrapped__``),
        # while the UDF actually executes the wrapper. Transpiling would
        # silently reproduce the wrong behavior, so refuse and fall back.
        if (
            getattr(func, "__wrapped__", None) is not None
            or getattr(getattr(func, "__call__", None), "__wrapped__", None) is not None
        ):
            return (
                [],
                [
                    "decorated callables (functools.wraps) are not supported: "
                    "the visible source is the wrapped function's, not the "
                    "wrapper's, so transpilation would change behavior"
                ],
                [],
                [],
            )
        src, ast = _get_src_ast_from_func(func)
        if ast is None:
            return ([], ["Error getting ast for function, cannot transpile"], [], [])
        # Get the lambda body and parameters
        function_ast = _get_function_from_ast(ast)
        if function_ast is None:
            return ([], ["Error extracting function body from ast, cannot transpile"], [], [])
        # Default, variadic (``*args`` / ``**kwargs``), keyword-only, and
        # positional-only parameters can't be represented by the positional
        # ``_udf_param_N`` placeholder scheme: a call site may omit a
        # defaulted argument, leaving the placeholder referencing a position
        # the call never bound, and ``_get_parameter_list`` only reads
        # ``args``. Fall back to interpreted Python rather than emit an
        # invalid plan.
        fn_args = function_ast.args
        if (
            fn_args.defaults
            or any(d is not None for d in fn_args.kw_defaults)
            or fn_args.kwonlyargs
            or fn_args.vararg is not None
            or fn_args.kwarg is not None
            or getattr(fn_args, "posonlyargs", [])
        ):
            return (
                [],
                [
                    "functions with default, variadic, keyword-only, or "
                    "positional-only arguments are not supported by the transpiler"
                ],
                [],
                [],
            )
        params = _get_parameter_list(function_ast)
        # The transpiler strips a leading ``self`` on the assumption that the
        # source came from a bound ``__call__`` / method whose receiver is not
        # supplied at the call site. A PLAIN function whose first parameter
        # happens to be named ``self`` breaks that assumption: every arg IS
        # supplied at the call site, and stripping would misnumber the
        # ``_udf_param_N`` placeholders (emitting ``_udf_param_-1``). Refuse
        # and fall back rather than guess.
        if params and params[0] == "self" and inspect.isfunction(func):
            return (
                [],
                [
                    "plain function with first parameter named 'self' is "
                    "ambiguous to the transpiler's self-stripping; falling "
                    "back to interpreted Python"
                ],
                [],
                [],
            )
        # Strip ``self`` for the caller-facing param list -- callers will
        # match user-supplied kwargs against this, and the user doesn't
        # name ``self`` at the call site.
        public_params = params[1:] if params and params[0] == "self" else list(params)
        transpiled: list[Column] = []
        input_categories: list[list[str]] = []
        errors = []
        # One transpiled option per (backend x input-type variant). Untyped
        # params are tried as both numeric and string so the JVM can pick the
        # option matching the actual column types (or fall back if none match).
        combos = _param_category_combos(function_ast, public_params)
        # Maybe multiple transpilers (think CUDA, etc.).
        transpilers = _get_transpilers(session)
        for transpiler in transpilers:
            for combo in combos:
                try:
                    transpiled_column = transpiler._transpile_from_ast(
                        src, ast, function_ast, params, returnType, combo
                    )
                    if transpiled_column is not None:
                        transpiled.append(transpiled_column)
                        input_categories.append(
                            [combo.get(i, "numeric") for i in range(len(public_params))]
                        )
                except Exception as e:
                    errors.append(str(e))
        return (transpiled, errors, public_params, input_categories)
    except Exception as e:
        # Don't re-raise: an inability to transpile must never break a
        # working UDF. The caller treats an empty ``transpiled`` list as a
        # silent fall-back to interpreted Python.
        return ([], [str(e)], [], [])
