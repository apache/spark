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

Constructs that short-circuit in Python -- ``and`` / ``or`` and chained
comparisons such as ``0 <= x <= 100`` -- lower to nested ``CASE WHEN``s
rather than to ``&`` / ``|``, so that an operand Python never evaluates
cannot raise; see ``_lazy_and``. Their operands must also be provably
non-NULL booleans; see ``_is_never_null_boolean``.
"""

import ast
import contextlib
import inspect
import itertools
import operator
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

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql._typing import DataTypeOrString


# Caps on how large a comparison we will lower, to bound plan growth: longer
# ones fall back to interpreted Python. Both are generous next to what shows up
# in real code (``0 <= x <= 100``, an allowlist of a few dozen values), and both
# matter because the lowered subtree is duplicated -- once per adjacent pair for
# a chain operand, once per input-category variant for every option -- and
# CheckAnalysis walks each copy.
_MAX_CHAIN_OPS = 4
_MAX_IN_ELEMENTS = 64

# The four ordering comparisons, which all lower through
# ``_lower_value_compare`` and differ only in the operator to apply and the
# symbol to name it in an error message.
_ORDERING_OPS: dict[type, Tuple[Callable[[Any, Any], Column], str]] = {
    ast.Lt: (operator.lt, "<"),
    ast.LtE: (operator.le, "<="),
    ast.Gt: (operator.gt, ">"),
    ast.GtE: (operator.ge, ">="),
}


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
        """Lower ``function_ast`` to a :class:`Column`, or return ``None`` to decline.

        The override point for ``spark.sql.experimental.optimizer.pyTranspilers``.

        ``params`` is the CALLER-FACING parameter list: a receiver already bound, as
        on a method or callable instance, has been removed, so ``params[i]`` is the
        name bound to placeholder ``_udf_param_i`` with no offsetting needed. It is
        also the list ``param_categories`` is keyed by.
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


def _is_boolean_expr(node: ast.AST, *, allow_null: bool) -> bool:
    """Shared traversal behind ``_is_definitely_boolean`` (``allow_null=True``)
    and ``_is_never_null_boolean`` (``allow_null=False``).

    The two predicates differ only in whether a NULL-valued operand is
    acceptable, so they share one match rather than two near-copies: with
    separate copies every new lowering had to be reflected in both, and
    forgetting one silently re-admits a NULL operand into ``_lazy_and``, where a
    NULL ``CASE WHEN`` condition reads as not-taken and turns Python's ``None``
    into ``False``.
    """
    match node:
        case ast.Constant(value=v):
            # A literal None is "boolean" for coalesce purposes but IS NULL, so
            # only the NULL-tolerant caller may accept it.
            return isinstance(v, bool) or (allow_null and v is None)
        case ast.Compare(left=left, ops=ops, comparators=comparators):
            # Every Python comparison produces a bool, chained ones and
            # ``in`` / ``not in`` included, and every comparison lowering is
            # total (see ``_lower_compare_pair``) -- so a NULL-intolerant caller
            # needs no extra restriction here, only the operand shapes below.
            # Don't inspect an ``in`` right operand: it is a CONTAINER, which
            # ``_is_definitely_basic_type`` rejects, yet the position still
            # produces a bool. Being over-permissive is safe --
            # ``_convert_chunk`` fails closed on anything it cannot lower.
            return _is_definitely_basic_type(left) and all(
                isinstance(op, (ast.In, ast.NotIn)) or _is_definitely_basic_type(c)
                for op, c in zip(ops, comparators)
            )
        case ast.UnaryOp(op=ast.Not()):
            # `not x` always produces bool, and lowers to
            # ``coalesce(~operand, lit(True))``, which is total.
            return True
        case ast.BoolOp(values=values):
            return all(_is_boolean_expr(v, allow_null=allow_null) for v in values)
        case ast.IfExp(body=body, orelse=orelse):
            # Ternary is boolean only if both branches are.
            return _is_boolean_expr(body, allow_null=allow_null) and _is_boolean_expr(
                orelse, allow_null=allow_null
            )
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
    return _is_boolean_expr(node, allow_null=True)


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


def _is_never_null_boolean(node: ast.AST) -> bool:
    """Return True when ``node`` lowers to a boolean column that either raises
    or yields True/False -- never NULL.

    Stricter than ``_is_definitely_boolean``, which also accepts operands that
    may evaluate to NULL (a literal ``None``, a ternary with a ``None`` branch).
    ``_lazy_and`` / ``_lazy_or`` need this: a CASE WHEN treats a NULL condition
    as not-taken, so a NULL operand would silently become ``False`` where Python
    returns ``None``. Bodies that fail this go back to interpreted Python.
    ``not`` and if/ternary *tests* keep using ``_is_definitely_boolean``, since
    they wrap the operand in ``coalesce`` and so handle NULL explicitly.
    """
    return _is_boolean_expr(node, allow_null=False)


def _lazy_and(cols: List[Column]) -> Column:
    """Fold ``cols`` into a boolean column with Python's ``and`` semantics,
    evaluating each element only when every earlier one was True.

    Nests CASE WHENs instead of folding with ``&``. The reason is NOT that
    ``And`` fails to short-circuit -- both ``And.eval`` and ``And.doGenCode``
    skip the right operand when the left is false. It is the optimizer:
    ``splitConjunctivePredicates`` decomposes ``And`` and nothing else, and
    ``PushPredicateThroughJoin`` gates pushdown on
    ``cond.deterministic && !cond.throwable``. ``Expression.throwable`` defaults
    to ``children.exists(_.throwable)`` and ``RaiseError`` does not override it
    (its children are literals), so Catalyst believes the NULL guard that
    ``_lower_value_compare`` emits cannot throw, and will push one conjunct to
    the far side of a join where it sees rows another conjunct excluded.

    A CaseWhen resists that: it is atomic to predicate splitting, evaluates
    branches in order (``CaseWhen.eval`` stops at the first TRUE condition),
    stays non-foldable (``ConditionalExpression.foldable`` needs every child
    foldable and ``RaiseError.foldable`` is false), and hides its lazy tail from
    subexpression elimination -- ``EquivalentExpressions`` reaches the tail only
    via ``branchGroups``, which intersects the branch values, and one of ours is
    a ``Literal`` that ``updateExprTree`` skips, leaving that intersection
    empty.

    SCOPE, precisely: this stops predicate splitting from tearing ONE lowered
    predicate into independently-pushable conjuncts. It does NOT stop the whole
    predicate being pushed as a single conjunct, since ``throwable`` is still
    false for it -- ``left.join(right, "k").filter(f(col("c")))`` with ``c`` on
    the right side still pushes below the join, so the guard fires on rows that
    never join and raises where interpreted Python never saw them. That
    divergence pre-dates chained comparisons (a single comparison already lowers
    to one guarded CASE WHEN); only overriding ``throwable`` on ``RaiseError``
    in Catalyst closes it, which is a separate change because it shifts pushdown
    behavior for every user of ``raise_error()`` / ``assert_true()``.

    Callers must have proven every element non-NULL (``_is_never_null_boolean``),
    which is what lets this skip ``coalesce``.

    COST, and it is not free. When every operand column is non-nullable the
    ``&`` fold used to simplify all the way down: ``NullPropagation`` folds
    ``IsNull(e)`` to false for a non-nullable ``e``, so the guard disappears and
    what remains is a plain ``And`` that ``splitConjunctivePredicates`` can tear
    into conjuncts for datasource pushdown, partition pruning and
    ``InferFiltersFromConstraints``. ``CaseWhen(Seq((c1, c2)), Some(false))`` has
    no matching rule in ``SimplifyConditionals`` (which only collapses the
    ``TrueLiteral`` / ``FalseLiteral`` branch shapes), so it survives as an
    opaque predicate and none of that applies. Measured on a non-nullable
    column: ``x > 20 and x < 80`` optimized to ``((id > 20) AND (id < 80))``
    under the old fold and stays ``CASE WHEN (id > 20) THEN (id < 80) ELSE
    false END`` under this one.

    The trade is deliberate but narrow. The pushdown loss only bites when the
    guard would have folded away -- i.e. non-nullable columns -- and that is
    exactly the case where the ``And`` form was already SAFE, because with no
    ``raise_error`` left there is nothing for the optimizer to relocate. When
    the guard does survive (nullable columns) it blocks datasource pushdown in
    either form, so nothing is lost. Overriding ``throwable`` on ``RaiseError``
    would let this go back to ``&`` / ``|`` and get both properties at once;
    that is the right end state and is tracked separately.
    """
    result = cols[-1]
    for c in reversed(cols[:-1]):
        result = when(c, result).otherwise(lit(False))
    return result


def _lazy_or(cols: List[Column]) -> Column:
    """Fold ``cols`` with Python's ``or`` semantics, evaluating each element only
    when every earlier one was False. The mirror of ``_lazy_and``; see there for
    why this nests CASE WHENs rather than folding with ``|``.
    """
    result = cols[-1]
    for c in reversed(cols[:-1]):
        result = when(c, lit(True)).otherwise(result)
    return result


def _const_container_elements(node: ast.AST) -> Optional[List[ast.expr]]:
    """Element nodes of a constant container literal, or ``None`` when ``node``
    is not one.

    Recognises ``ast.Tuple`` / ``ast.List`` / ``ast.Set`` displays whose every
    element is an ``ast.Constant`` or a unary ``+`` / ``-`` on a non-bool numeric
    ``ast.Constant`` (``x in (-1, 2)`` parses ``-1`` as a ``UnaryOp``). Anything
    else -- a ``Name``, a ``Call``, a starred or nested element, a ``dict`` /
    ``str`` / ``bytes`` right operand -- returns ``None`` so the caller falls
    back; see ``CatalystTranspiler._lower_in`` for why element values must be
    known at transpile time.
    """
    if not isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        return None
    for e in node.elts:
        if isinstance(e, ast.Constant):
            continue
        if (
            isinstance(e, ast.UnaryOp)
            and isinstance(e.op, (ast.USub, ast.UAdd))
            and isinstance(e.operand, ast.Constant)
            and isinstance(e.operand.value, (int, float))
            and not isinstance(e.operand.value, bool)
        ):
            continue
        return None
    return list(node.elts)


def _is_none_constant(node: ast.AST) -> bool:
    """Return True for the literal ``None``."""
    return isinstance(node, ast.Constant) and node.value is None


class CatalystTranspiler(AbstractTranspiler):
    """Transpiler that attempts to convert a Python UDF into native Spark SQL expressions."""

    variety = "catalyst"

    def __init__(self) -> None:
        self._param_categories: dict[int, str] = {}
        self._category_cache: dict[int, str] = {}

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
        if _is_none_constant(node):
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

    def _lower_compare_pair(
        self,
        params: List[str],
        op: ast.cmpop,
        left_node: ast.AST,
        right_node: ast.AST,
    ) -> Column:
        """Lower ONE adjacent comparison of a (possibly chained) ``ast.Compare``.

        TOTALITY INVARIANT, which callers depend on: when the returned column
        returns a value at all, that value is never NULL.

        * ``is`` / ``is not`` emit ``isNull`` / ``isNotNull``, non-nullable by
          construction.
        * ``_lower_eq`` answers every NULL combination with a boolean literal,
          reaching its ``otherwise`` only when both operands are non-NULL.
        * ``_lower_value_compare`` raises rather than returning NULL.

        Equal operand categories plus the JVM-side category contract in
        ``ResolveTranspiledPythonUDFOptions`` (non-decimal ``NumericType`` for
        "numeric", UTF8_BINARY ``StringType`` for "string") leave numeric
        widening as the only coercion Catalyst inserts, and under ANSI a failing
        cast raises instead of yielding NULL.

        That invariant is what lets ``_lazy_and`` and ``_lower_in``'s negation
        use plain ``when`` / ``~`` with no ``coalesce``.
        """
        match op:
            case ast.Is() | ast.IsNot():
                # Only lower `x is None` / `None is x` (and their
                # `is not` variants) to isNull/isNotNull. For any
                # other comparator (e.g. `x is 0`, `x is y`) Python
                # performs an object-identity check that has no SQL
                # equivalent, so we must fall back to interpreted
                # Python rather than silently emitting a null check.
                is_none_left = _is_none_constant(left_node)
                is_none_right = _is_none_constant(right_node)
                if not (is_none_left or is_none_right):
                    raise UnsupportedOperationException(
                        "`is`/`is not` is only supported when one "
                        "operand is the literal None; other identity "
                        "checks (e.g. `x is 0`, `x is y`) cannot be "
                        "lowered to SQL and the UDF falls back to "
                        "interpreted Python"
                    )
                subject_node = right_node if is_none_left else left_node
                subject_col = self._convert_chunk(params, subject_node)
                if isinstance(op, ast.Is):
                    return subject_col.isNull()
                else:
                    return subject_col.isNotNull()
            case ast.Eq():
                return self._lower_eq(params, left_node, right_node, equal=True)
            case ast.NotEq():
                return self._lower_eq(params, left_node, right_node, equal=False)
            case ast.Lt() | ast.LtE() | ast.Gt() | ast.GtE():
                # All four orderings share `_lower_value_compare` and differ only
                # in the operator, so look it up rather than repeating the call.
                op_fn, op_str = _ORDERING_OPS[type(op)]
                return self._lower_value_compare(params, left_node, right_node, op_fn, op_str)
            case _:
                raise UnsupportedOperationException(
                    f"comparison operator {type(op).__name__} is not supported by the transpiler"
                )

    def _lower_in(
        self,
        params: List[str],
        subject_node: ast.AST,
        container_node: ast.AST,
        negate: bool,
    ) -> Column:
        """Lower ``x in (c1, ..., cn)`` / ``x not in (...)`` over a literal
        tuple / list / set of constants.

        Python's ``in`` over a container is EQUALITY-based and never raises on a
        ``None`` probe: ``None in (1, 2)`` is False and ``None in (1, None)`` is
        True. Spark's ``IN`` returns NULL when the probe is NULL *or* when any
        list element is NULL, so we answer the NULL-probe case directly with a
        literal and DROP the NULL elements from the ``isin`` list. Leaving one in
        would make the whole expression NULL for a non-matching probe, breaking
        the totality that ``not in``'s ``~`` relies on.

        The empty container is spelled out as ``lit(False)`` rather than
        delegating to ``IN ()``: ``In.eval`` returns false for an empty list only
        while ``spark.sql.legacy.nullInEmptyListBehavior`` is off; with it on,
        ``OptimizeIn`` rewrites ``IN ()`` to ``If(IsNotNull(v), false, null)``
        and a NULL probe would yield NULL.

        Every element must share the SUBJECT's category. That guard is
        load-bearing: ``InTypeCoercion`` finds no wider common type for e.g.
        bigint-vs-string under ANSI, so ``In.checkInputDataTypes`` fails with
        DATA_DIFF_TYPES -- and because the option rides along as a child of
        ``TranspiledPythonUDF``, that fails the WHOLE query at CheckAnalysis
        instead of falling back. Python's cross-type ``in`` is simply False, so
        coercing would buy nothing anyway.
        """
        elements = _const_container_elements(container_node)
        if elements is None:
            raise UnsupportedOperationException(
                "`in` / `not in` is only supported over a literal tuple, list "
                "or set of constants (e.g. `x in (1, 2)`): `in` on a str or "
                "bytes is substring/subsequence containment rather than "
                "equality (`'b' in 'abc'` is True while `'b' == 'abc'` is "
                "False, and `b'ab'` iterates ints so `97 in b'ab'` is True), "
                "dict membership tests keys, and a container holding runtime "
                "values would need runtime NULL analysis; the transpiler falls "
                "back to interpreted Python"
            )
        if len(elements) > _MAX_IN_ELEMENTS:
            raise UnsupportedOperationException(
                f"`in` / `not in` container has {len(elements)} elements, more "
                f"than the {_MAX_IN_ELEMENTS} the transpiler will lower; the "
                "UDF falls back to interpreted Python to bound plan growth "
                "across the input-type option matrix"
            )
        # Raises for a literal-None or complex subject, dropping this variant.
        subject_cat = self._category(params, subject_node)
        has_none = False
        value_nodes: List[ast.expr] = []
        for element in elements:
            if _is_none_constant(element):
                has_none = True
                continue
            # Classify a unary +/- element by its operand rather than leaning on
            # `_category`'s catch-all, which happens to answer "numeric" for an
            # `ast.UnaryOp` today. `_const_container_elements` has already
            # restricted the operand to a non-bool numeric constant.
            classify = element.operand if isinstance(element, ast.UnaryOp) else element
            element_cat = self._category(params, classify)
            if element_cat != subject_cat:
                raise UnsupportedOperationException(
                    f"`in` / `not in` element category ({element_cat}) does not "
                    f"match the subject's ({subject_cat}); Python compares "
                    "across types as unequal while Spark would coerce or fail "
                    "analysis, so the transpiler falls back to interpreted "
                    "Python"
                )
            value_nodes.append(element)
        subject_col = self._convert_chunk(params, subject_node)
        element_cols = [self._convert_chunk(params, e) for e in value_nodes]
        membership = subject_col.isin(*element_cols) if element_cols else lit(False)
        result = when(subject_col.isNull(), lit(has_none)).otherwise(membership)
        return result.__invert__() if negate else result

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
                    if _is_none_constant(b):
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
            case ast.Compare():
                # EVERY Python comparison evaluates to a bool -- `in` / `not in`
                # and chained forms included -- whatever its operands look like.
                # `_is_definitely_boolean` can't answer this because its Compare
                # arm also requires basic-type operands, so it says False for
                # e.g. `(x if c else y) in (1, 2)` or `(a < b) == True`. Those
                # then fell through to the numeric catch-all below, which let the
                # return-type match in `_transpile_from_ast` accept a numeric
                # declared return type for a boolean body: the lowered option
                # returned `cast(<boolean> as bigint)` (1/0) where the
                # interpreted SQL_BATCHED_UDF path returns NULL, a silent
                # divergence. Classify on the node kind instead, which is a fact
                # about Python rather than an operand analysis.
                return "bool"
            case _ if _is_definitely_boolean(node):
                # `not` and boolean ops produce a boolean column. Labeling them
                # "numeric" (the old catch-all) let booleans into
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
                # produced by Compare / UnaryOp(Not) / nested BoolOps that maps
                # cleanly onto a nested CASE WHEN. For non-boolean operands
                # (including bare parameter names whose runtime type is unknown)
                # the right semantics would need Python's truthiness rules
                # (0 / "" / None / [] all falsy), which we can't reproduce
                # without the input column types -- Spark's `&` / `|` would do
                # bitwise instead. Require statically known booleans so the
                # caller falls back rather than emitting a divergent plan.
                #
                # The gate is `_is_never_null_boolean`, not
                # `_is_definitely_boolean`, because `_lazy_and` / `_lazy_or` need
                # non-NULL operands: a NULL CASE WHEN condition is not-taken, so
                # `and` would yield False where Python returns None. That
                # refuses a literal None operand (`None and (x > 0)` is None in
                # Python) and a ternary with a None branch -- both of which
                # diverge under Spark's three-valued `&` / `|` as well.
                if not all(_is_never_null_boolean(v) for v in values):
                    raise UnsupportedOperationException(
                        "`and` / `or` operand is not statically known to be a "
                        "non-NULL boolean; Python's short-circuit returns the "
                        "operand itself (so `None and x` is None) and Spark's "
                        "`&` / `|` are bitwise rather than Python truthiness, "
                        "so the transpiler refuses to lower this and the UDF "
                        "falls back to interpreted Python"
                    )
                cols = [self._convert_chunk(params, v) for v in values]
                if isinstance(op, ast.And):
                    return _lazy_and(cols)
                if isinstance(op, ast.Or):
                    return _lazy_or(cols)
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
                # `in` / `not in` are container membership, not a chainable
                # value comparison, so only the un-chained single-operator form
                # is lowered. `a < b in (1, 2)` is legal Python; refuse it
                # explicitly rather than relying on `_convert_chunk`
                # incidentally rejecting the ast.Tuple operand.
                if any(isinstance(o, (ast.In, ast.NotIn)) for o in ops):
                    if len(ops) != 1:
                        raise UnsupportedOperationException(
                            "`in` / `not in` inside a chained comparison (e.g. "
                            "`a < b in (1, 2)`) is not supported by the "
                            "transpiler; falling back to interpreted Python"
                        )
                    return self._lower_in(
                        params, left, comps[0], negate=isinstance(ops[0], ast.NotIn)
                    )
                if len(ops) > _MAX_CHAIN_OPS:
                    raise UnsupportedOperationException(
                        f"chained comparison with {len(ops)} operators exceeds "
                        f"the {_MAX_CHAIN_OPS} the transpiler will lower; the "
                        "UDF falls back to interpreted Python"
                    )
                # Python evaluates `a OP1 b OP2 c` as `(a OP1 b) and (b OP2 c)`
                # with `b` evaluated once and with SHORT-CIRCUIT: when
                # `a OP1 b` is falsy, `b OP2 c` is never evaluated. That is not
                # cosmetic here, because `_lower_value_compare` emits a
                # `raise_error` for NULL operands to mirror Python's TypeError:
                # `f(1, 0, None)` for `lambda a, b, c: a < b < c` must return
                # False, not raise. `_lazy_and` explains why reproducing that
                # needs nested CASE WHENs rather than `&`, and why every link
                # being total (see `_lower_compare_pair`) removes the need for
                # `coalesce`.
                #
                # Python's "evaluate `b` once" rule is about side effects. The
                # *Python-level* operand grammar we accept (Constant / Name /
                # BinOp / UnaryOp / IfExp / Compare / BoolOp) is deterministic
                # and side-effect free -- ANSI raises aside, and those are
                # idempotent -- so re-lowering an interior operand once per
                # adjacent pair is safe. Not so for the CALL-SITE arguments: a
                # parameter lowers to a `_udf_param_N` placeholder and the JVM's
                # `resolveUDFParams` substitutes the caller's expression at EVERY
                # occurrence, four of them for a two-link ordering chain. A
                # NON-DETERMINISTIC argument therefore becomes that many
                # independent expressions with independent state, and because the
                # fold above evaluates later links only on the rows where earlier
                # ones held, those copies desynchronize and stop describing the
                # same value: `50 < x < 60` over `rand(7) * 100` yields ~30% true
                # where Python yields ~10%.
                #
                # That is an OPEN BUG, not something this code can fix: the
                # transpiler runs at UDF construction, long before a call site
                # exists. The fix belongs in the analyzer, which is the first
                # place a resolved argument can be tested for determinism, and is
                # being handled as a companion change; until it lands the
                # divergence is pinned by a deliberately failing test in
                # test_udf_transpile_unit.py.
                #
                # Be precise about the blast radius, because it is easy to
                # overstate as merely pre-existing. `and` / `or` were already
                # wrong this way, since their operands are separate subtrees. A
                # SINGLE comparison is NOT: its duplicate operand appears only
                # inside an `IS NULL` test, whose outcome does not depend on the
                # value drawn, so `50 < x` over `rand()` samples correctly. So
                # lowering chained comparisons -- which previously fell back to
                # interpreted Python, and were therefore correct -- newly exposes
                # them to this bug. That argues for landing the analyzer guard
                # before or with this change.
                nodes = [left, *comps]
                return _lazy_and(
                    [
                        self._lower_compare_pair(params, op, left_node, right_node)
                        for op, left_node, right_node in zip(ops, nodes, nodes[1:])
                    ]
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
        param_categories: Optional[dict] = None,
    ) -> Optional[Column]:
        # Short circuit on nothing to transpile.
        if src == "" or ast_info is None:
            return None
        # Per-variant input-type assumption ({public_param_index -> category}),
        # read by ``_category`` to choose str vs numeric operators.
        self._param_categories = param_categories or {}
        # Category inference depends on the per-variant assumptions above. Cache
        # each AST node only for this lowering so recursive conversion stays linear.
        self._category_cache = {}
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
) -> Tuple[List[Column], List[str], List[str], List[List[str]], List[str]]:
    """
    An experimental internal function that attempts to transpile a callable function.

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
                        src, ast_info, function_ast, public_params, returnType, combo
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
