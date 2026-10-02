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

``len(s)`` lowers to Catalyst ``length`` for a string operand (SPARK-55214).
Other ``len`` arguments (numbers, lists, ...) stay interpreted Python.
``len(None)`` raises (via ``raise_error`` on NULL), matching CPython.

A lambda is lowered only when its source names it directly and alone: bind it
to a name (``f = lambda x: x + 1``, annotated if you like) and give it a line
of its own. Passed straight to ``udf(...)``, wrapped in another call, returned
by another lambda, or sharing a line with a second lambda, nothing in the
source read back says which lambda is the UDF, so it falls back to interpreted
Python rather than risk the wrong body.

A lowered UDF computes each argument it uses once per row (SPARK-58626): the
argument becomes a column on the operator's input, and however many times the
body reads that parameter it reads that one column. Two parameters bound to the
same deterministic argument share the column, so ``f(a + 1, a + 1)`` computes
``a + 1`` once.

A call inside a higher-order function's lambda is never lowered: Spark already
applies a Python UDF over the whole array there, and that path is left to it.
Otherwise a few positions have nowhere to put the column -- under a ``groupBy``,
in a join condition, in a command such as ``DELETE FROM``, or a draw anywhere
above a join, where the column would cost the join its condition -- and there a
body reading the parameter more than once stays an interpreted Python UDF, which
computes its inputs once, rather than being lowered into an argument evaluated
per read. One evaluation per parameter per row either way; ``f(rand(), rand())``
is two parameters and so still two draws::

    body = lambda x: x if x > 0.5 else 0.0
    clamp = udf(body, "double")
    df.select(clamp(rand()))  # never a value the body's own condition rejects

Where the column stands it is computed for every row reaching the operator, which
makes the argument eager even where the call sits in a branch that may not run:
under ANSI ``when(cond, f(a / b))`` can raise on a row with ``b = 0`` and ``cond``
false. That is what the interpreted Python UDF does too, evaluating its inputs in
a projection feeding the worker, below the conditional -- unlike a bare Catalyst
``when``, which is lazy. Whether such an error surfaces is not a guarantee in
either direction: an argument left inline, or inlined again by a later rule, is
lazy once more.

Two arguments no projection can hold count as those positions too: one that is
itself an aggregate (``f(sum(a))``), and one reading an outer query's column when
correlated-subquery decorrelation is disabled.

An argument read once gets no column, since one read is one evaluation anyway,
and neither does one as cheap to repeat as to read -- a bare column or a literal.
Anything more, arithmetic included, is either computed once or left to
interpreted Python; a repeated ``a + 1`` gets a column where one fits and stops
the UDF being lowered where one does not.

That column is what we emit, not what necessarily runs: later optimizer rules may
inline a deterministic one again where that is faster, as they may for any other
expression. A draw is never inlined, because those rules check determinism.

An argument the body never reads is not computed at all.


Scalar pandas UDFs
------------------
A scalar pandas UDF receives one ``pandas.Series`` per argument and must return a
Series, so most of the lowerings above -- which were written against Python
scalars -- do NOT carry over. Only the subset whose Python-scalar meaning
coincides with pandas' element-wise meaning is reused, selected by the strict
allowlist in :func:`_check_series_semantics`: parameter references, ``int`` /
``float`` / ``str`` constants, ``+`` ``-`` ``*`` ``%``, unary ``+`` / ``-``, and
``<param>.isnull()`` (with the ``.isna()`` / ``.notnull()`` / ``.notna()``
aliases). Everything else falls back to interpreted Python. The exclusions are
not conservatism for its own sake -- each one is a construct whose scalar
lowering would be actively wrong on a Series:

* ``if x:``, ``x and y``, ``not x`` -- pandas raises
  ``ValueError: The truth value of a Series is ambiguous``. Lowering them to
  ``CASE WHEN`` / ``AND`` / ``NOT`` would turn a UDF that raises today into one
  that returns values.
* ``x is None`` -- always ``False``, because the *Series object* is not None.
  ``isnull(a)`` would answer a different question; ``x.isnull()`` is the
  construct that means what ``x is None`` means for a scalar.
* ``x < y`` and friends -- a missing value compares ``False`` under numpy
  (never ``NULL``, and never a ``TypeError``, so the ``raise_error`` guard the
  scalar lowering emits is wrong), and NaN compares ``False`` where Spark orders
  it above every value.
* a body that never references a parameter (``return 1``) -- the result is a
  scalar, which the interpreted path rejects with ``UDF_RETURN_TYPE``.

Two pandas-specific value rules are then reproduced explicitly:

* NaN in the result is serialized as NULL -- but only in the default dtype regime,
  where the result Series is numpy-backed and the return path masks it with
  ``series.isnull()``. A pandas masked extension array (which is what
  ``spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype=true`` produces,
  including for any float arithmetic promoted off an ``Int64`` column) is handed
  to Arrow with ``mask=None`` instead, and its ``isnull()`` is False for NaN in
  any case, so there the same NaN arrives as NaN. Catalyst keeps NaN, so a numeric
  body is wrapped in an ``isnan -> NULL`` normalization that reproduces the
  default regime -- and transpilation is refused outright when that conf is on,
  since no single expression covers both.
* ``Series.isnull()`` is True for NaN on a numpy-backed Series -- by the time the
  UDF runs, the Arrow-to-pandas conversion has already collapsed a NULL in a
  fractional column into NaN and pandas cannot tell them apart -- so it lowers to
  ``isnull(a) OR isnan(a)`` rather than to ``isnull(a)`` alone. (On a masked
  extension array ``isnull()`` is the mask alone, but that regime is refused per
  the previous point.)

What remains divergent is numpy's fixed-width arithmetic, and it is documented
rather than guarded because it cannot be detected without the runtime values.
Every case is one where the transpiled expression raises or is exact and the
interpreted one is lossy, silent, or inconsistent:

* integer overflow wraps under numpy where ANSI raises ARITHMETIC_OVERFLOW. This
  also covers the ``%`` boundaries the ``Mod`` lowering notes below
  (``Long.MinValue % -1``), and a literal too wide for the column's dtype, where
  numpy raises ``OverflowError`` and Catalyst widens exactly.
* ``a % 0`` has no single interpreted answer to reproduce: pandas promotes to
  ``float64`` and yields NaN (so, NULL), unless
  ``spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype`` is on, in which
  case it yields ``0``. ANSI raises REMAINDER_BY_ZERO, and that is what the
  transpiled expression does.
* a ``float32`` column keeps ``float32`` through a ``float`` literal (NEP 50)
  where Catalyst promotes to double.
* an integral column is converted to ``float64`` as soon as the batch contains a
  NULL, losing precision above 2**53 -- which makes the interpreted answer depend
  on how rows happen to be batched, and is avoided by
  ``preferIntExtensionDtype=true``.

One case runs the other way: a fractional body (e.g. ``s * 1.5``) declared with an
integral return type. Interpreted, the float64 result fails the Arrow conversion and
raises; transpiled, the final ``Cast`` to the return type truncates instead. This is
the general within-numeric return-type cast allowed below, not a special case for
this UDF type -- see ``test_fractional_result_for_an_integral_return_type``.
"""

import ast
import contextlib
import inspect
import itertools
import sys
import textwrap
import threading
import warnings
from typing import TYPE_CHECKING, Any, Callable, Iterator, List, Optional, Tuple, Union

from pyspark.errors import UnsupportedOperationException
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    abs as _abs,
)
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    isnan,
    length,
    lit,
    pmod,
    raise_error,
    repeat,
    when,
)
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DataType,
    DecimalType,
    NumericType,
    StringType,
)
from pyspark.util import PythonEvalType

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
        param_categories: Optional[dict],
        series_semantics: bool,
    ) -> Optional[Column]:
        """Lower ``function_ast`` to a :class:`Column`, or return ``None`` to decline.

        The override point for ``spark.sql.experimental.optimizer.pyTranspilers``.

        ``params`` is the CALLER-FACING parameter list: a receiver already bound, as
        on a method or callable instance, has been removed, so ``params[i]`` is the
        name bound to placeholder ``_udf_param_i`` with no offsetting needed. It is
        also the list ``param_categories`` is keyed by.

        ``series_semantics`` is True when the arguments are ``pandas.Series`` rather than
        Python scalars (a scalar pandas UDF), which admits only the element-wise subset in
        the module docstring. It has no default on purpose: a transpiler that silently
        received False for a pandas UDF would apply the Python-scalar lowerings to a Series,
        turning one that raises ValueError into one that returns values. The trade-off is
        that a third-party transpiler (registered via
        ``spark.sql.experimental.optimizer.pyTranspilers``) written to the older
        signature raises TypeError on the new keyword, which ``_transpile_func`` records
        as a per-variant failure -- so it now falls back to interpreted Python for EVERY
        eval type, including the pickled one where it previously worked. That is a
        fallback, not a wrong answer, and this is an experimental extension point, so
        requiring the argument (correctness) is preferred over a default
        (bug-compatibility for an unknown subclass).
        """
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


def _truthiness_col(cat: Optional[str], c: Column) -> Optional[Column]:
    """Return a boolean Column expressing Python's ``bool(c)`` for the given category.

    Returns ``None`` for unsupported or unknown categories (caller falls back).

    Semantics (NULL-as-False throughout, matching Python's ``None`` is falsy):
      "bool"    -> coalesce(c, False)
      "string"  -> coalesce(length(c) > 0, False)  -- empty string is falsy
      "numeric" -> coalesce(c != 0, False)           -- zero is falsy
                   NaN != 0 is True in Spark so float NaN is truthy, matching Python.
    """
    if cat == "bool":
        return coalesce(c, lit(False))
    if cat == "string":
        return coalesce(length(c) > lit(0), lit(False))
    if cat == "numeric":
        return coalesce(c != lit(0), lit(False))
    return None


# ----------------------------------------------------------------------------
# Scalar pandas UDF (Series in / Series out) support. See the module docstring
# for why the subset below is an allowlist rather than a set of exclusions.
# ----------------------------------------------------------------------------

# ``pandas.Series`` methods meaning "is this element missing", mapped to whether the method
# is the positive form. All four are the same lowering up to a negation.
_SERIES_NULL_METHODS = {"isnull": True, "isna": True, "notnull": False, "notna": False}

# Binary and unary operators pandas applies element-wise with the same result Catalyst
# produces, given operands of a matching category (`_category` still decides add-vs-concat
# and multiply-vs-repeat). `**` and `/` are absent because the scalar transpiler does not
# lower them either.
#
# `Mult` is in the allowlist for numeric multiply and for string-repeat by an integer
# LITERAL only (`s * 2`). Repeat by a numeric COLUMN is refused under series semantics in
# `_convert_chunk`, because a Series count is unsound in every batch shape: pandas raises
# TypeError on any genuine float value (so `_masked_arith_op` only succeeds when the whole
# batch is missing, yielding NULL), while the lowered `repeat(s, cast(n as int))` returns a
# repeated string for a real value and raises CAST_OVERFLOW for a NaN -- so an all-missing
# batch turns a NULL-returning query into an error. See `_reject_series_string_repeat`.
_SERIES_SAFE_BINOPS = (ast.Add, ast.Sub, ast.Mult, ast.Mod)
_SERIES_SAFE_UNARYOPS = (ast.UAdd, ast.USub)


def _is_int_literal(node: ast.AST) -> bool:
    """True for an ``int`` constant that is not a ``bool`` (``bool`` subclasses ``int``).

    This is the only repeat count that is safe under series semantics: it cannot be NaN or
    NULL, and it is fixed at transpile time so no column value can make it fractional.
    """
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, int)
        and not isinstance(node.value, bool)
    )


# Raised from both the up-front ``_check_series_semantics`` call and ``_transpile_from_ast``
# itself; a single constant keeps the two paths reporting the same reason.
_MULTI_STATEMENT_ERROR = (
    "functions with more than one top-level statement are not supported by the transpiler"
)


def _match_series_null_check(node: ast.AST, params: List[str]) -> Optional[Tuple[ast.Name, bool]]:
    """Match ``<param>.isnull()`` and its aliases, returning ``(receiver, positive)``.

    ``None`` when ``node`` is any other call. The receiver must be a bare parameter: on an
    intermediate expression the ``isnan`` arm of the lowering would have to reason about
    which sub-expression can produce NaN, so those fall back to interpreted Python instead.
    Arguments are refused too (``isna()`` takes none, but a future keyword should not be
    silently ignored).
    """
    match node:
        case ast.Call(
            func=ast.Attribute(value=ast.Name(id=receiver_name) as receiver, attr=attr),
            args=[],
            keywords=[],
        ) if receiver_name in params and attr in _SERIES_NULL_METHODS:
            return receiver, _SERIES_NULL_METHODS[attr]
        case _:
            return None


def _describe_unsupported_series_node(node: ast.AST) -> str:
    """A short user-facing label for a node refused by :func:`_check_series_semantics`.

    These end up in the ``Unable to transpile UDF ...`` warning, which is the only place a
    user learns why the rewrite did not happen, so name the pandas-specific reason rather
    than just the AST class.
    """
    match node:
        case ast.Compare(ops=[ast.Is() | ast.IsNot(), *_]):
            return (
                "an `is` / `is not` test (a Series object is never None, so this is always "
                "False at runtime -- write `x.isnull()` for a per-element check)"
            )
        case ast.Compare():
            return (
                "a comparison (on a Series a missing value compares False rather than "
                "raising or yielding NULL, and NaN compares False where Spark orders it "
                "above every value)"
            )
        case ast.BoolOp() | ast.UnaryOp(op=ast.Not()):
            return "`and` / `or` / `not` (these raise ValueError on a Series)"
        case ast.If() | ast.IfExp():
            return "an `if` (testing a Series for truth raises ValueError)"
        case ast.Name(id=name):
            return f"the free variable {name!r}"
        case ast.Call():
            return "a function call other than the `.isnull()` family"
        case ast.BinOp(op=op):
            return f"the binary operator `{type(op).__name__}`"
        case ast.UnaryOp(op=op):
            return f"the unary operator `{type(op).__name__}`"
        case ast.Constant(value=value):
            return f"the constant {value!r} ({type(value).__name__})"
        case _:
            return f"an unsupported `{type(node).__name__}` node"


def _check_series_semantics(function_ast: ast.FunctionDef, params: List[str]) -> None:
    """Raise unless the body is element-wise-equivalent for a scalar pandas UDF.

    Two properties are checked together because both are about the body as a whole:

    * every node is in the allowlist, so no lowering written for Python scalars is reused
      where pandas would raise or answer differently;
    * the body references at least one parameter. With only the allowlisted operators, an
      expression containing a parameter is a Series and one without is a Python scalar --
      and returning a scalar makes the interpreted UDF fail with ``UDF_RETURN_TYPE``, so a
      rewrite that returned a value per row would not be a faithful reproduction.
    """
    # Match the statement-count guard in ``_transpile_from_ast`` first, and with the same
    # message (shared constant): this walk only visits ``body[0]``, so without this a
    # multi-statement body would be rejected as "an unsupported `Assign` node" (its first
    # statement) rather than for the real reason, and the warning is the only place the user
    # learns why.
    if len(function_ast.body) != 1:
        raise UnsupportedOperationException(_MULTI_STATEMENT_ERROR)
    references_param = False

    def visit(node: Optional[ast.AST]) -> None:
        nonlocal references_param
        if node is None:
            # A bare `return` (or, via `_convert_chunk`'s None case, an absent one).
            raise UnsupportedOperationException(
                "a scalar pandas UDF must return a pandas.Series, but this body returns "
                "None; the interpreted path fails with UDF_RETURN_TYPE, so the transpiler "
                "falls back to interpreted Python"
            )
        match node:
            case ast.Return(value=value):
                visit(value)
            case ast.Name(id=name) if name in params:
                references_param = True
            # bool is a subclass of int, and bytes has no element-wise operator here, so
            # neither is admitted by the numeric/string constant arm.
            case ast.Constant(value=value) if isinstance(value, (int, float, str)) and not (
                isinstance(value, bool)
            ):
                pass
            case ast.BinOp(left=left, op=op, right=right) if isinstance(op, _SERIES_SAFE_BINOPS):
                visit(left)
                visit(right)
            case ast.UnaryOp(op=op, operand=operand) if isinstance(op, _SERIES_SAFE_UNARYOPS):
                visit(operand)
            case ast.Call() if _match_series_null_check(node, params) is not None:
                references_param = True
            case _:
                raise UnsupportedOperationException(
                    f"{_describe_unsupported_series_node(node)} has no element-wise "
                    "pandas.Series equivalent the transpiler can reuse, so this scalar "
                    "pandas UDF falls back to interpreted Python. The supported subset is "
                    "parameter references, int/float/str constants, `+` `-` `*` `%`, unary "
                    "`+`/`-`, and `<param>.isnull()` / `.isna()` / `.notnull()` / `.notna()`"
                )

    visit(function_ast.body[0] if function_ast.body else None)
    if not references_param:
        raise UnsupportedOperationException(
            "the body of this scalar pandas UDF never references a parameter, so it "
            "produces a Python scalar rather than a pandas.Series; the interpreted path "
            "fails with UDF_RETURN_TYPE, so the transpiler falls back to interpreted Python"
        )


class CatalystTranspiler(AbstractTranspiler):
    """Transpiler that attempts to convert a Python UDF into native Spark SQL expressions."""

    variety = "catalyst"

    def __init__(self) -> None:
        self._param_categories: dict[int, str] = {}
        self._category_cache: dict[int, str] = {}
        # True while lowering a scalar pandas UDF, whose arguments are ``pandas.Series``
        # rather than Python scalars. Set per call by ``_transpile_from_ast``; the default
        # keeps the scalar behavior for anything that reaches ``_convert_chunk`` directly.
        self._series_semantics = False

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
        # Determine the boolean guard for the CASE WHEN.
        # Two paths:
        # 1. The test is statically known to be a boolean expression
        #    (comparison, `not`, boolean op, literal True/False/None):
        #    wrap with coalesce so NULL is treated as False.
        # 2. The test is a bare value (parameter name, numeric/string
        #    constant): lower using Python's type-specific truthiness
        #    rules (0/""/None are falsy, everything else truthy) based on
        #    the operand's inferred category.
        if _is_definitely_boolean(test_node):
            safe_test = coalesce(test_col, lit(False))
        else:
            cat = self._safe_category(params, test_node)
            _maybe_test = _truthiness_col(cat, test_col)
            if _maybe_test is None:
                raise UnsupportedOperationException(
                    f"bare truthiness test ({ast.dump(test_node)}) in if/ternary: "
                    "the operand's category is unknown or unsupported, so the "
                    "transpiler falls back to interpreted Python"
                )
            safe_test = _maybe_test
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

    def _reject_series_string_repeat(self, count_node: ast.AST) -> None:
        """Refuse string-repeat by a non-literal count under series semantics.

        For a Python scalar, `'ab' * n` is a fine repeat and the lowering matches. For a
        pandas Series the count is a whole column, and `Series(str) * Series(numeric)` is
        unsound: pandas raises TypeError on any genuine float, so it only produces a value
        when the batch is entirely missing (yielding NULL), while the lowered
        `repeat(s, cast(n as int))` returns a repeated string for a real value and raises
        CAST_OVERFLOW for a NaN. An integer literal count avoids all of this -- it cannot be
        NaN or NULL -- so `s * 2` is kept and everything else falls back to interpreted
        Python. A no-op for the scalar eval types (``series_semantics`` is False), which keep
        the broader behavior, including the documented `s * 1.5` -> truncated-repeat wart.
        """
        if self._series_semantics and not _is_int_literal(count_node):
            raise UnsupportedOperationException(
                "string repeat by a non-literal count is not supported for a scalar pandas "
                "UDF: the count is a Series, and `str * numeric` there is NULL for an "
                "all-missing batch but a repeated string or CAST_OVERFLOW otherwise, so the "
                "transpiler falls back to interpreted Python. Use an integer literal (`s * 2`)"
            )

    def _lower_series_null_check(
        self, params: List[str], receiver: ast.Name, positive: bool
    ) -> Column:
        """Lower ``s.isnull()`` / ``.isna()`` / ``.notnull()`` / ``.notna()`` for a Series.

        ``pandas.Series.isnull`` is True for NaN as well as for a missing value on a
        numpy-backed Series, and by the time a scalar pandas UDF runs there is no difference
        left to observe: the Arrow-to-pandas conversion turns a NULL in a fractional column
        into NaN. Catalyst's ``isnull`` covers only the NULL, so the NaN arm is what makes the
        two agree. (On a pandas masked extension array ``isnull()`` is the mask alone and NaN
        is not missing -- but ``preferIntExtensionDtype`` is refused before we get here, see
        the module docstring.)

        That arm is emitted for every numeric category rather than only for float/double
        because the bound column's width is not known here -- options are pruned against the
        argument types later, on the JVM. ``isnan(cast(<integral> as double))`` is
        constant-false, so on an integral column this is one extra node and no change in
        result. A string column needs no arm: its missing values are exactly the NULLs.

        Neither ``isnull`` nor ``isnan`` ever returns NULL, so the ``notnull`` negation is
        total two-valued logic and matches pandas without a ``coalesce``.
        """
        subject = self._convert_chunk(params, receiver)
        if self._category(params, receiver) == "numeric":
            missing = subject.isNull() | isnan(subject.cast("double"))
        else:
            missing = subject.isNull()
        return missing if positive else ~missing

    def _category(self, params: List[str], node: ast.AST) -> str:
        """Infer ``"numeric"`` or ``"string"`` for ``node`` under the current
        ``self._param_categories`` assumption (set per input-type variant).

        Drives operator selection (``+`` -> add vs concat, ``*`` -> multiply vs
        repeat) and raises ``UnsupportedOperationException`` when an operator's
        operands are type-incompatible, so the caller drops that variant and the
        JVM picks another option / falls back to the Python UDF.
        """
        cache_key = id(node)
        cached = self._category_cache.get(cache_key)
        if cached is not None:
            return cached
        category = self._category_uncached(params, node)
        self._category_cache[cache_key] = category
        return category

    def _category_uncached(self, params: List[str], node: ast.AST) -> str:
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
                # ``params`` is the caller-facing list, so its indexes are already
                # the ``_udf_param_N`` / category indexes -- see ``_transpile_func``.
                return self._param_categories.get(params.index(name), "numeric")
            case ast.BinOp(left=left, op=op, right=right):
                lc = self._category(params, left)
                rc = self._category(params, right)
                if isinstance(op, ast.Add) and lc == rc:
                    return lc  # str + str -> str, num + num -> num
                if isinstance(op, ast.Mult):
                    if {lc, rc} == {"numeric", "numeric"}:
                        return "numeric"
                    if {lc, rc} == {"numeric", "string"}:
                        return "string"  # str * int / int * str -> repeat
                if isinstance(op, (ast.Sub, ast.Mod)) and lc == rc == "numeric":
                    return "numeric"
                raise UnsupportedOperationException(
                    f"operands of `{type(op).__name__}` are not type-compatible "
                    "for this input-type variant"
                )
            case ast.Call(func=ast.Name(id="len"), args=[arg], keywords=[]):
                # ``len(s)`` is an int. Only strings are lowered (SPARK-55214);
                # other argument categories raise so this variant is dropped.
                if self._category(params, arg) != "string":
                    raise UnsupportedOperationException(
                        "`len` is only lowered for string operands; other "
                        "types fall back to interpreted Python"
                    )
                return "numeric"
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
            case ast.Call() if (
                self._series_semantics and _match_series_null_check(node, params) is not None
            ):
                # `s.isnull()` is a boolean Series. Without this arm the catch-all below
                # would call it numeric, which would demand a numeric return type and put a
                # boolean through the NaN normalization's cast.
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
                            self._reject_series_string_repeat(right)
                            return repeat(left_col, right_col.cast("int"))
                        if lc == "numeric" and rc == "string":
                            self._reject_series_string_repeat(left)
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
                    # ``params`` excludes any bound receiver (see ``_transpile_func``),
                    # so its indexes ARE the placeholder indexes. A body referencing
                    # the receiver (``return self``) is not in this list and so takes
                    # the branch below, which refuses -- there is no column for it.
                    return col(f"_udf_param_{params.index(name)}")
                else:
                    # TODO (SPARK-55207): Handle assignments, class vars, and closures
                    # via scope evaluation.
                    raise UnsupportedOperationException(
                        f"name {name!r} is not in the UDF's parameter list "
                        "and free variables / closures are not supported"
                    )
            case ast.Call() if self._series_semantics:
                # The only calls a scalar pandas UDF may use. `_check_series_semantics` has
                # already refused the others before we get here; checked ahead of the `len()`
                # arm below (Python-scalar-only) so a direct caller skipping that pre-check
                # still gets refused instead of the string-length lowering.
                null_check = _match_series_null_check(body, params)
                if null_check is None:
                    raise UnsupportedOperationException(
                        "the only calls the transpiler lowers for a scalar pandas UDF are "
                        "`<param>.isnull()` / `.isna()` / `.notnull()` / `.notna()`; "
                        f"falling back to interpreted Python for {ast.dump(body)[:120]}"
                    )
                receiver, positive = null_check
                return self._lower_series_null_check(params, receiver, positive)
            case ast.Call(func=ast.Name(id="len"), args=[arg], keywords=[]):
                # SPARK-55214: Python ``len`` on a str is the number of Unicode
                # code points; Spark ``length`` on a string column is character
                # length -- they match for well-formed UTF-8. ``len(None)``
                # raises TypeError in Python, while Spark ``length(NULL)`` is
                # NULL, so guard like value comparisons: raise on NULL, else
                # ``length``. A caller that already proved non-null (``if x is
                # not None: return len(x)``) takes the otherwise branch.
                if self._category(params, arg) != "string":
                    raise UnsupportedOperationException(
                        "`len` is only lowered for string operands; other "
                        "types fall back to interpreted Python"
                    )
                arg_col = self._convert_chunk(params, arg)
                err = lit(
                    "Python UDF transpiler: cannot call len() on NULL; "
                    "Python would raise TypeError here. Add an "
                    "`is not None` guard or filter NULLs upstream."
                )
                return when(arg_col.isNull(), raise_error(err)).otherwise(length(arg_col))
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
        param_categories: Optional[dict],
        series_semantics: bool,
    ) -> Optional[Column]:
        # Short circuit on nothing to transpile.
        if src == "" or ast_info is None:
            return None
        if series_semantics:
            # Enforced here, next to the lowerings it constrains, so the two cannot drift:
            # every construct `_check_series_semantics` refuses has a lowering below that
            # would be wrong on a Series, and `_convert_chunk` only guards `ast.Call`.
            # `_transpile_func` also checks it once up front, so that a refused body reports
            # one reason rather than one per input-type variant; this call is what makes the
            # guarantee hold for any other caller.
            _check_series_semantics(function_ast, params)
        # Per-variant input-type assumption ({public_param_index -> category}),
        # read by ``_category`` to choose str vs numeric operators.
        self._param_categories = param_categories or {}
        # Category inference depends on the per-variant assumptions above. Cache
        # each AST node only for this lowering so recursive conversion stays linear.
        self._category_cache = {}
        # Whether the arguments are ``pandas.Series`` rather than Python scalars. Required
        # rather than defaulted, so that the unsafe value can never be the quiet one; see
        # ``AbstractTranspiler._transpile_from_ast``.
        self._series_semantics = series_semantics
        function_body = function_ast.body
        if len(function_body) != 1:
            raise UnsupportedOperationException(_MULTI_STATEMENT_ERROR)
        body_cat = self._safe_category(params, function_body[0])
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
        if series_semantics and body_cat == "numeric":
            # A scalar pandas UDF's numpy-backed result Series is serialized with
            # ``mask = series.isnull()``, and ``isnull`` is True for NaN, so every NaN the
            # function produces reaches Spark as NULL. Catalyst keeps NaN, so normalize to
            # match. Two ways to get there: a fractional input column carrying NaN (the
            # interpreted path cannot even tell that from a NULL, since the Arrow-to-pandas
            # conversion merges them), and NaN arising from arithmetic on infinities.
            #
            # This models the DEFAULT dtype regime only. A pandas masked extension array
            # takes ``mask=None`` instead, so there a NaN stays NaN and this normalization
            # would be exactly backwards -- which is why ``preferIntExtensionDtype`` refuses
            # transpilation outright, in udf.py and again in ConvertToCatalyst.
            #
            # Emitted for every numeric body rather than only the fractional ones, because
            # the bound column's width is not known until the JVM prunes the options against
            # the argument types. ``isnan(cast(<integral> as double))`` is constant-false, so
            # on an integral column this is an extra node and no change in result. The cast
            # exists only to satisfy ``isnan``'s float/double input type; the value returned
            # is always the un-cast one, so no precision is lost.
            #
            # This references ``converted`` twice, so the lowered body appears twice in the
            # option (subexpression elimination does not extract a value used in a CaseWhen
            # condition and only one branch). The clean single-reference form is
            # ``NaNvl(body, null)``, but that is only type-valid for float/double, and the
            # body's resolved type is not known until the JVM prunes options -- and by then
            # the cast to the return type is already baked in below, past the fractional
            # value ``NaNvl`` would need. Applying it type-aware in
            # ``ResolveTranspiledPythonUDFOptions`` (before that cast) would remove the
            # duplication; it is left for a separate change, since the duplication is a
            # plan-size/CPU cost only and the baseline it replaces is a per-row Python call.
            # Correctness is unaffected.
            converted = when(isnan(converted.cast("double")), lit(None)).otherwise(converted)
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
    all_args = _positional_args(function_ast)
    public_args = all_args[len(all_args) - n :]
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


def _call_dunder(func: Callable) -> Any:
    """The ``__call__`` entry from ``func``'s type.

    Not ``getattr(func, "__call__")``, which is wrong in two ways that both end with
    lowering a body that never runs: an instance attribute ``obj.__call__ = f``
    shadows the type's for ``getattr`` but is ignored when ``obj`` is called, and on
    a CLASS object it finds the ``__call__`` its instances use while calling the
    class runs ``__init__``.

    ``getattr_static`` looks the name up without firing the descriptor protocol, so
    deciding what to transpile never runs user code -- a custom descriptor used as
    ``__call__`` would otherwise have its ``__get__`` called here.

    Everything comes back undisturbed, so a ``staticmethod`` or ``classmethod``
    arrives as the descriptor rather than the function inside it -- see
    ``_call_impl``. There is always something to return: ``getattr_static`` on a type
    falls through to the metatype, so the floor is ``type.__call__``.
    """
    return inspect.getattr_static(type(func), "__call__")


def _call_impl(entry: Any) -> Any:
    """The function inside a ``staticmethod`` / ``classmethod``, else ``entry`` itself.

    Both get in the way, in opposite directions: they do not forward the wrapped
    function's ``__code__``, and they synthesize a ``__wrapped__`` pointing at it even
    when no decorator is involved. So asking the descriptor directly finds no code
    object and a wraps decorator that is not there -- unwrap before either question.

    Narrow on purpose: unwrapping any ``__func__`` would follow the attribute on
    unrelated callables that expose one, and read the wrong code object.
    """
    return entry.__func__ if isinstance(entry, (staticmethod, classmethod)) else entry


def _held_code(func: Callable) -> Any:
    """The code object that runs when ``func`` is called, or ``None``.

    A function or method runs its own ``__code__``; anything else runs its type's
    ``__call__``. Used only to ask whether we are holding a lambda.
    """
    target = func if (inspect.isfunction(func) or inspect.ismethod(func)) else _call_dunder(func)
    return getattr(_call_impl(target), "__code__", None)


_WARNINGS_LOCK = threading.Lock()


@contextlib.contextmanager
def _syntax_warnings_suppressed() -> Iterator[None]:
    """Parse without re-emitting, or tripping over, the source's own SyntaxWarnings.

    The import already reported them. Without this, ``udf()`` repeats the warning,
    and under warnings-as-errors the parse raises and lowering silently turns off.
    Before 3.12 an invalid escape sequence was a DeprecationWarning, so ignore that
    too rather than lose lowering on the oldest Python we support.

    The lock serializes our own use of ``warnings``, whose state is process-global.
    It cannot serialize anyone else's: a thread entering ``catch_warnings`` while
    this is open has its filters restored from our older snapshot on exit, and
    entering at all bumps the filter version, so a "once"-filtered warning
    elsewhere in the process can fire again. Both are inherent to the stdlib API,
    and are why the parse is the only thing inside here.
    """
    with _WARNINGS_LOCK:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=SyntaxWarning)
            if sys.version_info < (3, 12):
                warnings.filterwarnings(
                    "ignore", message="invalid escape sequence", category=DeprecationWarning
                )
            yield


def _get_src_ast_from_func(func: Callable) -> Tuple[Optional[str], Optional[ast.AST]]:
    """Try and get the AST from a given callable

    KNOWN LIMITATION: this is the source on disk NOW, not necessarily the source
    ``func`` was compiled from. ``inspect.getsource`` reads through ``linecache``,
    which re-reads an edited file while the code object stays as it was at import,
    so editing a module in a long-lived driver and then building a UDF from a
    function imported earlier lowers the NEW body while Python runs the old one --
    verified: rewriting ``lambda x: x + 1`` to ``x * 9`` gives Python 6, Spark 45.
    Closing it needs the parsed node checked against the held code object; it is
    not tracked separately, being part of the experimental transpiler
    (SPARK-54783). Until then, transpilation assumes source files are not edited
    underneath a running session.
    """
    # Note: consider maybe dill? (see the JYTHON PR)
    # inspect getsource does not work for functions defined in vanilla
    # repl, but does for those in files or in ipython.
    # It also fails when we give it an instance of a callable class.
    try:
        src = inspect.getsource(func)
        src = textwrap.dedent(src).strip()
        with _syntax_warnings_suppressed():
            ast_info = ast.parse(src)
    except Exception:
        try:
            src = inspect.getsource(_call_dunder(func))
            src = textwrap.dedent(src).strip()
            with _syntax_warnings_suppressed():
                ast_info = ast.parse(src)
        except Exception:
            # No usable source (REPL/stdin definition, builtin, ...) --
            # return cleanly so the caller reports "cannot transpile"
            # instead of surfacing an UnboundLocalError as the reason.
            return None, None
    return src, ast_info


def _positional_args(node: Union[ast.FunctionDef, ast.Lambda]) -> List[ast.arg]:
    """Return the positional argument nodes in order, positional-only first."""
    return node.args.posonlyargs + node.args.args


def _get_parameter_list(node: Union[ast.FunctionDef, ast.Lambda]) -> list[str]:
    """Return the positional argument names in order, positional-only first."""
    return [arg.arg for arg in _positional_args(node)]


def _get_function_from_ast(body: ast.AST, held_code: Any) -> Tuple[Optional[ast.FunctionDef], str]:
    """
    Extract a :class:`ast.FunctionDef` node from an AST produced by
    ``ast.parse(inspect.getsource(udf_func))``.

    Handles the following source patterns (in order):

    * ``f = lambda x: x + 1`` -- lambda bound to a name, annotated or not
    * ``lambda x: x + 1`` -- bare expression (getsource on a raw lambda)
    * ``def f(x): ... return x + 1``
    * a class with a ``__call__`` method

    ``held_code`` is the code object that runs when the callable is called; a
    ``co_name`` of ``<lambda>`` is what makes the ambiguity checks below apply, and
    its parameter names are what tell a located lambda apart from a rival.

    Returns the node and an empty reason, or ``None`` and why -- paired so no refusal
    reaches the caller unexplained.
    """
    if not hasattr(body, "body") or not body.body:
        return None, "no statement was found in the source read for this callable"

    stmt = body.body[0]

    # Grab the value side of a top level assign (e.g. x = lambda ...). An annotated
    # binding is the same shape, and the form a typed codebase writes.
    if isinstance(stmt, ast.Assign):
        stmt = stmt.value
    elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
        stmt = stmt.value

    # Bare ``lambda x: ...`` (when ``inspect.getsource`` returns a raw
    # lambda expression at module top level) parses as ``Expr(Lambda)``.
    if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Lambda):
        stmt = stmt.value

    # ``inspect.getsource`` works in whole lines, so refuse unless the lambda located
    # here IS the one we hold: anything else lowers a body that never runs
    # (SPARK-58650).
    if getattr(held_code, "co_name", None) == "<lambda>":
        if not isinstance(stmt, ast.Lambda):
            return None, (
                "the source read for this lambda does not define it as a statement of "
                "its own -- it is wrapped in a call or a tuple assignment, or a "
                "surrounding definition, or the file has changed since import and no "
                "longer holds it -- so which lambda to lower cannot be determined"
            )
        # The located lambda must take the parameters the held one does, or it is a
        # different lambda that merely sits where ours was read from. This is what
        # separates a lambda nested in the body of the one we hold (fine: it can never
        # be the UDF) from one that RETURNED the lambda we hold, as in the one-line
        # ``make_adder = lambda n: lambda x: x + n`` -- there the outer lambda is
        # located and the inner is held, and lowering the outer would be wrong.
        located_args = _get_parameter_list(stmt)
        if located_args != list(held_code.co_varnames[: held_code.co_argcount]):
            return None, (
                "the lambda defined in the source read for this one takes different "
                f"parameters ({', '.join(located_args) or 'none'}), so it is not the "
                "lambda being transpiled -- a lambda returning another lambda on one "
                "line, or a file changed since import; put each lambda on its own line"
            )
        # Only lambdas OUTSIDE ``stmt`` are rivals; one in its body cannot be the UDF,
        # and the user could not split it onto another line.
        own = set(map(id, ast.walk(stmt)))
        if any(id(node) not in own for node in ast.walk(body) if isinstance(node, ast.Lambda)):
            return None, (
                "more than one lambda is visible in the source line(s) this one was "
                "read from, and nothing there says which is the UDF, so it is not "
                "safe to lower; put each lambda on its own line to transpile it"
            )

    if isinstance(stmt, ast.Lambda):
        # Synthesize a one-statement FunctionDef wrapping the lambda body so
        # the rest of the transpiler can treat lambdas and ``def`` uniformly.
        fn_ctor: Any = ast.FunctionDef
        synthesized = fn_ctor(
            name="<lambda>",
            args=stmt.args,
            body=[ast.Return(value=stmt.body)],
            decorator_list=[],
        )
        # A node without ``lineno`` cannot be unparsed or compiled; seed from the
        # lambda so positions point at real source rather than line 1.
        return ast.fix_missing_locations(ast.copy_location(synthesized, stmt)), ""

    if isinstance(stmt, ast.FunctionDef):
        return stmt, ""
    return None, (
        f"the source read for this callable is a {type(stmt).__name__}, which the "
        "transpiler cannot reduce to a single function definition"
    )


def _transpile_func(
    session: "SparkSession",
    func: Callable[..., Any],
    returnType: "DataTypeOrString",
    evalType: int,
) -> Tuple[List[Column], List[str], List[str], List[List[str]], List[str]]:
    """
    An experimental internal function that attempts to transpile a callable function.

    ``evalType`` selects the semantics to reproduce, and is required rather than defaulted:
    the row-at-a-time eval types (``SQL_BATCHED_UDF``, ``SQL_ARROW_BATCHED_UDF``) hand the
    function Python scalars, while ``SQL_SCALAR_PANDAS_UDF`` hands it ``pandas.Series``, which
    admits a much smaller subset of bodies (see the module docstring). A default would make
    the wrong value the quiet one -- a caller that forgot to pass it would get the Python
    scalar lowerings applied to a Series -- so omitting it is an error instead. The set of
    eval types allowed to reach here at all lives in ``pyspark.sql.udf``.

    Returns
    -------
    list of transpiled options (one per backend x input-type variant)
    list of errors as strings
    list of positional parameter names (excluding a receiver already bound, as on a
    method or callable instance) -- needed so the caller can resolve named-argument
    invocations to positional order at call time, since the ``_udf_param_N``
    substitution in :class:`UserDefinedPythonFunction` is positional.
    list of per-option input-type categories (``"numeric"`` / ``"string"`` per
    public param) -- the JVM picks the option whose categories match the bound
    column types, or falls back to the Python UDF when none match.
    list of the public parameter names Python forbids calling by keyword
    (positional-only) -- the caller must NOT resolve a kwarg matching one of these
    to a position, since Python itself rejects that call.
    """
    series_semantics = evalType == PythonEvalType.SQL_SCALAR_PANDAS_UDF
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
                [],
            )
        # A functools.wraps-style decorator makes ``inspect.getsource`` return
        # the WRAPPED function's source (getsource follows ``__wrapped__``),
        # while the UDF actually executes the wrapper. Transpiling would
        # silently reproduce the wrong behavior, so refuse and fall back.
        # ``_call_impl`` first: a ``staticmethod`` / ``classmethod`` exposes a
        # ``__wrapped__`` of its own, so asking the descriptor refuses every one of
        # them for a wraps decorator that is not there.
        if (
            getattr(func, "__wrapped__", None) is not None
            or getattr(_call_impl(_call_dunder(func)), "__wrapped__", None) is not None
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
                [],
            )
        # Not ``ast``: that name would shadow the module for this whole function.
        src, ast_info = _get_src_ast_from_func(func)
        if ast_info is None:
            return ([], ["Error getting ast for function, cannot transpile"], [], [], [])
        # Get the lambda body and parameters
        function_ast, extraction_error = _get_function_from_ast(ast_info, _held_code(func))
        if function_ast is None:
            return ([], [extraction_error], [], [], [])
        # Default, variadic (``*args`` / ``**kwargs``) and keyword-only params
        # can't be represented by the positional ``_udf_param_N`` placeholder
        # scheme: a call site may omit a defaulted argument, leaving the
        # placeholder referencing a position the call never bound. A
        # positional-only param has no such gap -- it is always bound by
        # position -- so it is not refused here; a DEFAULTED one still hits
        # ``fn_args.defaults`` above.
        fn_args = function_ast.args
        if (
            fn_args.defaults
            or any(d is not None for d in fn_args.kw_defaults)
            or fn_args.kwonlyargs
            or fn_args.vararg is not None
            or fn_args.kwarg is not None
        ):
            return (
                [],
                [
                    "functions with default, variadic, or keyword-only "
                    "arguments are not supported by the transpiler"
                ],
                [],
                [],
                [],
            )
        params = _get_parameter_list(function_ast)
        # Drop a receiver that is already bound, so what is left is what the call
        # site supplies. Decided by HOW ``func`` dispatches, not by the parameter's
        # name: a bound ``__call__(this, x)`` or ``@classmethod f(cls, x)`` has a
        # receiver not named ``self``, while a plain ``def f(self, x)`` supplies its
        # ``self`` at the call site. Going by the name misnumbered every
        # ``_udf_param_N`` -- a two-column call on ``__call__(this, x)`` read column b
        # for ``x`` and returned a value where Python raises TypeError. Asking
        # ``inspect.signature`` is both weaker (a ``__signature__`` off by exactly one
        # is undetectable) and worse behaved (it runs user code).
        #
        # For a callable instance, two things can consume a leading parameter, and
        # they compose: what the descriptor prepends when Python looks ``__call__``
        # up -- the instance for a plain function, the class for a ``classmethod``,
        # nothing for a ``staticmethod`` or for an already-bound method, whose
        # ``__get__`` returns itself -- and whatever the callable already has bound.
        # Each count below is checked against what Python returns for that shape.
        if inspect.isfunction(func):
            spoken_for = 0
        elif inspect.ismethod(func):
            spoken_for = 1
        else:
            call_entry = _call_dunder(func)
            call_target = _call_impl(call_entry)
            if not (inspect.isfunction(call_target) or inspect.ismethod(call_target)):
                # A slot wrapper, property, partial, or other descriptor: what it
                # prepends is not knowable from here.
                return (
                    [],
                    [
                        f"a {type(call_entry).__name__} as __call__ does not say which "
                        "parameters the call site supplies, so the placeholder "
                        "positions cannot be assigned"
                    ],
                    [],
                    [],
                    [],
                )
            spoken_for = int(
                inspect.isfunction(call_entry) or isinstance(call_entry, classmethod)
            ) + int(inspect.ismethod(call_target))
        if spoken_for > 1 or spoken_for > len(params):
            # Two receivers at once -- a ``classmethod`` over an already-bound method
            # prepends the class ON TOP of the method's own ``__self__`` -- or one with
            # no parameter to hold it. Python raises for whatever the call site passes,
            # so there is nothing correct to lower.
            return ([], ["callable leaves no parameter for the call site to bind"], [], [], [])
        # Caller-facing params: callers match user-supplied kwargs against this,
        # and the receiver is not named at the call site. Everything downstream
        # indexes off THIS list, so the placeholder numbering needs no offset.
        public_params = params[spoken_for:]
        # Subset of ``public_params`` Python forbids calling by keyword. The
        # call-site kwargs-to-positional rewrite in ``udf.py`` must not "fix" a
        # keyword call to one of these, since Python itself would reject it.
        posonly_names = {arg.arg for arg in function_ast.args.posonlyargs}
        positional_only_public_params = [p for p in public_params if p in posonly_names]
        # The Series-safe subset does not depend on the input-type variant, so check it once
        # here rather than per combo: otherwise a rejected body would report the same reason
        # once per option in the warning.
        if series_semantics:
            try:
                _check_series_semantics(function_ast, public_params)
            except UnsupportedOperationException as e:
                return ([], [str(e)], [], [], [])
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
                        src,
                        ast_info,
                        function_ast,
                        public_params,
                        returnType,
                        combo,
                        series_semantics=series_semantics,
                    )
                    if transpiled_column is not None:
                        transpiled.append(transpiled_column)
                        input_categories.append(
                            [combo.get(i, "numeric") for i in range(len(public_params))]
                        )
                except Exception as e:
                    errors.append(str(e))
        return (
            transpiled,
            errors,
            public_params,
            input_categories,
            positional_only_public_params,
        )
    except Exception as e:
        # Don't re-raise: an inability to transpile must never break a
        # working UDF. The caller treats an empty ``transpiled`` list as a
        # silent fall-back to interpreted Python.
        return ([], [str(e)], [], [], [])
