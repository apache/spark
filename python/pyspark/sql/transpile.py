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
keeps the option matrix small; prefer doing so. To bound plan growth, a
function with more than three untyped parameters only emits one uniform
variant per category -- more than *two* when the body needs the
integral/fractional split described below, since that tries three
categories per parameter rather than two.

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

Exactness and the integral/fractional split
-------------------------------------------
We only lower an operator when it matches CPython exactly, or when it raises
where CPython returns a value (the ANSI overflow case below). Anything that
would be silently wrong isn't lowered at all, so the UDF falls back.

Some operators can't meet that bar until we know the *kind* of number, since
``numeric`` covers both integers and doubles: ``**`` lowers to repeated
multiplication (exact on ints, rounds per step on doubles, where CPython
calls libm ``pow``), ``//`` needs ``div``, which rejects fractional input,
and the bitwise operators and shifts are int-only in Python. ``/`` is the
mirror image: it needs a fractional operand, or two int32 ones. Bodies using
any of those emit one option per kind, tagged ``"integral"`` or
``"fractional"``; everything else keeps the single ``"numeric"`` option it
always had. Annotating ``int`` / ``float`` pins the kind to one option.

A word on "float", which is unavoidably overloaded here: Python's ``float``
*is* an IEEE binary64, so it corresponds to Spark's ``DoubleType``, and this
docstring says "double" for that throughout. Spark's ``FloatType`` is
single-precision and has no Python counterpart at all -- see the last
fall-back below.

No exact lowering exists for these, so they fall back:

* ``a / b`` on two bigints. Python divides the exact integers and rounds
  once, Spark casts each to double first, so past 2^53 they differ by an
  ULP. int32 columns are exact (both operands are representable, so the
  double division is correctly rounded), and so is any ``/`` with a double
  operand.
* ``round()`` on a double. ``bround`` rounds the shortest decimal repr, so
  ``bround(2.675, 2)`` is 2.68 where Python's ``round(2.675, 2)`` is 2.67.
* ``//``, ``**``, ``min()`` and ``max()`` on doubles -- the last two because
  Python's NaN comparisons are all False, so ``min`` / ``max`` return their
  first argument, while ``least`` / ``greatest`` order NaN highest either way.
* Anything on a ``FloatType`` column. Python has no single-precision float:
  the value arrives widened to a Python float (a double) and every step runs
  in double, while an expression that stays in ``FloatType`` rounds to 24 bits
  per step. Under ANSI coercion a literal promotes the expression to double
  (``x + 4`` and ``x * 3.0`` are double, and so is ``x / y``), but two float
  operands keep it there -- ``x + y``, ``x * y`` and ``x % y`` all diverge, on
  398 of 398 sampled random pairs for ``(x + y) * y``. Nor is it only trailing
  digits: float32 overflows to infinity where Python has room to spare
  (``-9.726323523430302e29 * -260872823898112.0`` is ``inf`` in FloatType and
  ``2.5373334837038975e44`` in double). A FloatType *return* type would mask
  the rounding for one operation but not for a chain, and not the overflow at
  all. ``ResolveTranspiledPythonUDFOptions`` therefore excludes ``FloatType``
  from every numeric category, next to ``DecimalType``.

  That exclusion is deliberately coarser than exactness alone requires:
  comparisons, ``abs`` and unary minus *are* exact on a float column (ANSI
  coercion sends a float-vs-integral comparison to double, and comparing two
  floats gives the same answer at either width). Refusing them too costs those
  bodies a lowering, which is throughput rather than correctness, and buys a
  boundary drawn per column type instead of per operator -- the same trade
  ``DecimalType`` already takes. Making it per-operator is a reasonable
  follow-up, not a prerequisite.

Three tiers of divergence
-------------------------
Not every difference between the lowered expression and CPython is a defect.
They sort into three tiers. Most rows below are pinned by a test in
``test_udf_transpile_hypothesis.py`` (see its "Bounds 4" section for the
tier-2 pins) or ``test_udf_transpile_unit.py``; the "evaluation count" row is
a plan-shape property with no result to assert, so it is documentation only.

**1. Semantically equal.** The two sides are indistinguishable to any
consumer of the UDF's declared return type, so these are documented rather
than guarded:

* *Which exception object.* Where both sides raise, the type and message
  differ -- Python's ``ZeroDivisionError`` against ANSI's ``DIVIDE_BY_ZERO``,
  ``ValueError`` on a negative shift count against our ``raise_error``. The
  UDF contract is "this input raises", and both honour it.
* *The sign of a zero.* ``_python_mod`` follows the dividend for a zero
  result where Python follows the divisor, so ``4.0 % -2.0`` is ``0.0`` here
  and ``-0.0`` in Python. They compare equal, hash equal, and print
  differently only through ``math.copysign`` / ``repr``.
* *Intermediate width.* ``**`` and the shifts cast their operand to
  ``LongType`` before operating, so a tinyint input runs its arithmetic wider
  than the column type. Python has no width at all, so widening moves the
  lowering *towards* Python; the declared return type is what either side
  finally produces. Visible in ``explain()``, not in results.
* *Evaluation count.* The NULL guards read each operand a second time
  (``x ** 0`` excepted -- its result is a literal), as do ``//``'s floor
  correction and ``<<``'s overflow round-trip, where Python evaluates it
  once. Operands here are pure column reads, so this costs plan size and
  nothing else.

**2. Value-visible, and deliberately kept.** These need runtime values rather
than types to detect, so no static check can route around them. They are
accepted, documented, and pinned:

* *A result too wide for the declared return type.* Python has no width at all,
  so it just answers; a UDF declared ``-> LongType`` cannot return
  13835058055282163709 however wide we compute, and raises CAST_OVERFLOW on the
  final cast. Interpreted Python does not raise here -- ``makeFromJava`` accepts
  only Byte/Short/Int/Long for LongType, so a wider Python int matches no case
  and the row becomes NULL. We deliberately don't copy that: raising tells the
  caller their return type is too narrow, where a NULL silently loses the row.
  For a *narrower* declared type the interpreted path is worse still -- it
  applies ``.toByte`` / ``.toShort`` / ``.toInt``, so a UDF declared
  ``-> ByteType`` turns 500 into -12 without a word. Raising beats reproducing
  that.

  This used to be a much blunter edge, and ``+``, ``-``, ``*``, ``**`` and
  ``abs()`` no longer reach it merely by overflowing an *intermediate*. Operand
  promotion (see ``_promoting`` and ``PythonNumericPromotion``) widens the
  arithmetic to a type the input widths prove cannot overflow, so ``x + 4`` on
  an int column at Integer.MaxValue and ``abs(x)`` on a smallint holding -32768
  both compute now instead of raising. ``<<`` and a negative ``round()`` scale
  are the two that still overflow on their own terms.

NULL against TypeError used to sit in this tier: Spark's arithmetic propagates
NULL, so ``x + 1`` on NULL came back NULL where Python raises ``TypeError``.
The answer was at least never a wrong *number*, but a row that should have
failed came back silently missing, so every numeric lowering now guards the way
the comparisons, ``min``/``max`` and string concat always have: a NULL operand
raises (via ``raise_error``, see ``_raise_on_null_operand``) rather than
propagating.

NaN used to sit in this tier. It no longer does: Spark treats ``NaN = NaN`` as
true and orders NaN above every value where Python makes every NaN comparison
False, but NaN-ness is a runtime test rather than a static one, so
``_nan_guard`` reproduces Python exactly. It is emitted only where NaN can
actually occur -- both operands numeric, and not both provably integral -- and
never for strings, where ANSI ``isnan`` would raise CAST_INVALID_INPUT.

**3. Refused.** Everything else -- the fall-backs listed above. Where a
Python error has no Catalyst equivalent we raise rather than return a
different value: both shifts on a negative count, and ``min()`` / ``max()``
on NULL, which ``least`` / ``greatest`` would silently skip.
"""

import ast
import builtins
import contextlib
import inspect
import itertools
import sys
import textwrap
import threading
import warnings
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional, Set, Tuple, Union

from pyspark.errors import UnsupportedOperationException
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    abs as _abs,
)
from pyspark.sql.functions import (
    bitwise_not,
    bround,
    call_function,
    coalesce,
    col,
    concat,
    greatest,
    isnan,
    least,
    length,
    lit,
    raise_error,
    repeat,
    when,
)
from pyspark.sql.internal import InternalFunction
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DecimalType,
    DoubleType,
    FloatType,
    NumericType,
    StringType,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql._typing import DataTypeOrString


# Input-type categories. "numeric" spans every numeric column type that has a
# faithful Python counterpart -- Byte/Short/Integer/Long/DoubleType, so neither
# DecimalType nor FloatType (see the module docstring); "integral" and
# "fractional" narrow it for the lowerings that are only exact on one kind of
# number, and "integral32" narrows "integral" further to the widths whose values
# are all exactly representable as a double. The JVM side of this vocabulary
# lives in ResolveTranspiledPythonUDFOptions.
_INTEGRAL_CATEGORIES = frozenset({"integral", "integral32"})
_NUMERIC_CATEGORIES = frozenset({"numeric", "fractional"}) | _INTEGRAL_CATEGORIES
# Categories a lowering may tighten (see ``CatalystTranspiler._narrow``), in
# increasing narrowness, so a later entry matches strictly fewer column types than
# an earlier one. Requirements from different lowerings in one option are
# conjunctive -- every one of them has to hold for the option to be exact -- so
# when two apply to the same parameter the narrowest wins.
#
# Only the numeric categories spanning more than one kind of number are in here:
# narrowing "fractional" to an integral category would change the assumption rather
# than tighten it, and the non-numeric categories have nothing to narrow to.
_NARROWING_ORDER = ("numeric", "integral", "integral32")
_NARROWABLE_CATEGORIES = frozenset(_NARROWING_ORDER)
# Python builtins the transpiler can lower. Only reached after the name is proven
# to still refer to the builtin (see ``_resolvable_builtins``).
_SUPPORTED_BUILTINS = frozenset({"abs", "min", "max", "round"})
# `x ** k` expands to k-1 multiplications, so cap k to keep plans small. Raising
# the cap buys little: past it, all but the smallest bases overflow LongType.
_MAX_POW_EXPANSION = 8
# An int literal past this is conservatively treated as too wide for IntegerType,
# so Spark would promote the expression to LongType and the int32 exactness
# argument for `/` stops holding. (-2**31 does fit; we don't bother.)
_INT32_MAX = 2**31 - 1
# Shifts widen their operand to a long, so this is the count at which Python has
# shifted every bit out -- and the point at which Spark's masked shift stops
# agreeing with it.
_LONG_BITS = 64


def _promoting_if_numeric(
    op: str, categories: List[Optional[str]], plain: Callable[[], Column], *cols: Column
) -> Column:
    """``_promoting(op, ...)`` when every operand is a number, else ``plain()``.

    The gate is on *numeric* rather than *integral* deliberately, and getting that wrong is worth
    a note. An unannotated parameter's category is plain ``"numeric"`` -- it could be an int
    column or a double one, and only the JVM finds out which. Gating on ``_INTEGRAL_CATEGORIES``
    therefore refuses promotion for exactly the common case, `udf(lambda x, y: x + y)` over two
    int columns, which is the shape this whole exercise is about. It has to be the coarse
    category here and the concrete type over there.

    Nothing is lost by handing a double to the promoting operator: ``PythonNumericPromotion``
    declines to widen a fractional type (IEEE arithmetic saturates to infinity, which is what
    Python does too), and aligns the operands so the replacement still resolves.
    """
    if all(_is_numeric_cat(c) for c in categories):
        return _promoting(op, *cols)
    return plain()


def _promoting(op: str, *cols: Column) -> Column:
    """Emit the operand-widening version of ``op``.

    Python integers have no width, so `x * 100` on a tinyint column is fine there and
    ARITHMETIC_OVERFLOW here -- `Multiply` keeps its operands' type. The JVM side widens the
    operands far enough that the worst case the *column types* allow cannot overflow; see
    ``PythonNumericPromotion``. It has to happen over there rather than in this file, because
    only the analyzer knows the concrete column widths -- we only ever see a category, and
    "integral" spans tinyint through bigint.

    Reached through ``InternalFunction`` rather than ``call_function``: these are registered as
    internal expressions, so they are invisible to ``SHOW FUNCTIONS`` and to SQL. That is the
    right side of the line -- for a SQL user `a + b` overflowing is the documented ANSI contract,
    and a public registration would additionally owe an ``@ExpressionDescription`` and a row in
    ``sql-expression-schema.md``, for a function nobody should call by name.
    """
    return InternalFunction._invoke_internal_function_over_columns(f"python_promoting_{op}", *cols)


def _raise_on_null_operand(op_repr: str, result: Column, *cols: Column) -> Column:
    """``result``, but raising where CPython gets a ``None`` operand.

    Spark's arithmetic propagates NULL (``x + 1`` on NULL is NULL) where Python
    raises ``TypeError`` (``None + 1``), and a row that should have failed
    coming back silently missing is worse than any wrong answer, so every
    lowered operator guards the way the comparisons and ``min`` / ``max`` do:
    NULL in, ``raise_error`` out. A caller that already proved non-null (``if x
    is not None: return x + 1``) takes the otherwise branch and never trips it.
    """
    err = lit(
        f"Python UDF transpiler: cannot apply `{op_repr}` to NULL; "
        "Python would raise TypeError here. Add an `is not None` guard or "
        "filter NULLs upstream."
    )
    any_null = cols[0].isNull()
    for c in cols[1:]:
        any_null = any_null | c.isNull()
    return when(any_null, raise_error(err)).otherwise(result)


def _is_numeric_cat(category: Optional[str]) -> bool:
    """True for every category that denotes a number column."""
    return category in _NUMERIC_CATEGORIES


def _coarse_category(category: Optional[str]) -> Optional[str]:
    """Collapse the numeric refinements back to plain ``"numeric"``.

    Comparability doesn't care which kind of number it is -- Python is happy
    with ``1 < 1.5`` and ``2 == 2.0`` -- so the comparison lowerings gate on
    this instead of the refined category, which would refuse mixed int/float
    comparisons that used to transpile.
    """
    return "numeric" if _is_numeric_cat(category) else category


def _unify_numeric(left: str, right: str) -> Optional[str]:
    """Merge two numeric categories the way Python promotes, or ``None`` when
    either side isn't a number.

    ``int op float`` is a float so "fractional" wins, and an unrefined
    "numeric" operand keeps the result unrefined.
    """
    if not (_is_numeric_cat(left) and _is_numeric_cat(right)):
        return None
    if "fractional" in (left, right):
        return "fractional"
    if "numeric" in (left, right):
        return "numeric"
    return "integral"


def _int_constant(node: ast.AST) -> Optional[int]:
    """The value of an integer literal, or ``None`` for anything else.

    Handles the unary forms too, since ``x ** -1`` parses as ``Pow`` over
    ``UnaryOp(USub, Constant(1))`` rather than a negative constant. ``bool`` is
    excluded even though it subclasses ``int``: ``x ** True`` is legal Python
    with no faithful lowering.
    """
    match node:
        case ast.Constant(value=v) if isinstance(v, int) and not isinstance(v, bool):
            return v
        case ast.UnaryOp(op=ast.USub(), operand=operand):
            inner = _int_constant(operand)
            return None if inner is None else -inner
        case ast.UnaryOp(op=ast.UAdd(), operand=operand):
            return _int_constant(operand)
        case _:
            return None


def _is_negative_int_constant(node: ast.AST) -> bool:
    value = _int_constant(node)
    return value is not None and value < 0


def _resolvable_builtins(func: Optional[Callable], params: List[str]) -> Set[str]:
    """The ``_SUPPORTED_BUILTINS`` that ``func`` still resolves to the builtin.

    ``abs`` / ``min`` / ``max`` / ``round`` are ordinary names a UDF can rebind
    (``from mymath import round``, a closure variable, a parameter, a module
    global). Lowering builtin semantics for a rebound name would run different
    code than the UDF does, so we drop those and refuse the call. No ``func``
    means no lowered calls, which is just a missed optimization.
    """
    if func is None:
        return set()
    shadowed = set(params)
    # Resolve the callable Python actually dispatches, matching `_transpile_func`:
    # `getattr(func, "__call__")` finds an instance attribute when one exists,
    # but Python ignores it and calls the type's `__call__` instead. Reading the
    # wrong code object here would miss a free variable and lower a shadowed
    # builtin (e.g. `round`) as the builtin -- a silent wrong result.
    if inspect.isfunction(func) or inspect.ismethod(func):
        target: Any = func
    else:
        target = _call_impl(_call_dunder(func))
    code = getattr(target, "__code__", None)
    if code is not None:
        # Closures and locals: a name bound anywhere in the function shadows the
        # builtin for the whole body, regardless of statement order.
        shadowed.update(code.co_freevars)
        shadowed.update(code.co_varnames)
    globals_dict = getattr(target, "__globals__", None) or {}
    return {
        name
        for name in _SUPPORTED_BUILTINS
        if name not in shadowed
        and name not in globals_dict
        and getattr(builtins, name, None) is not None
    }


def _accepts_func_kwarg(transpiler: "AbstractTranspiler") -> bool:
    """Whether ``transpiler._transpile_from_ast`` accepts the ``func`` keyword.

    ``func`` was added to the override point after the fact; a custom transpiler
    with the old signature raises ``TypeError`` on the keyword, which the broad
    ``except`` at the call site would silently turn into "every option dropped".
    """
    try:
        params = inspect.signature(transpiler._transpile_from_ast).parameters
    except (ValueError, TypeError):
        # Some callables (built-ins, C extensions) have no inspectable signature;
        # assume they don't take it and let the call fail normally if they do.
        return False
    return "func" in params or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())


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
        func: Optional[Callable] = None,
    ) -> Optional[Column]:
        """Lower ``function_ast`` to a :class:`Column`, or return ``None`` to decline.

        The override point for ``spark.sql.experimental.optimizer.pyTranspilers``.
        Raising also declines: the caller drops that variant and records the
        message, so a lowering can bail out mid-walk instead of unwinding by hand.

        ``params`` is the CALLER-FACING parameter list: a receiver already bound, as
        on a method or callable instance, has been removed, so ``params[i]`` is the
        name bound to placeholder ``_udf_param_i`` with no offsetting needed. It is
        also the list ``param_categories`` is keyed by.

        ``param_categories`` maps public parameter index to the input-type category
        assumed for this variant. ``func`` is the callable itself, which the built-in
        transpiler uses to check that a name like ``round`` has not been rebound; it
        is optional so a transpiler that doesn't need it can ignore it.
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
        case ast.Call(func=ast.Name(id=name), args=args) if name in _SUPPORTED_BUILTINS:
            # abs/min/max/round of basic types are basic types. A name that no
            # longer refers to the builtin is caught when the call is lowered
            # (``_lower_builtin_call``), which raises there rather than here.
            return all(_is_definitely_basic_type(a) for a in args)
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

    Gated on the coarse category, like the comparison lowerings: which KIND of number
    it is cannot change whether the number is zero, so a parameter narrowed to
    "integral" / "integral32" / "fractional" by some other operator in the same body
    still gets its truthiness test.
    """
    cat = _coarse_category(cat)
    if cat == "bool":
        return coalesce(c, lit(False))
    if cat == "string":
        return coalesce(length(c) > lit(0), lit(False))
    if cat == "numeric":
        return coalesce(c != lit(0), lit(False))
    return None


class CatalystTranspiler(AbstractTranspiler):
    """Transpiler that attempts to convert a Python UDF into native Spark SQL expressions."""

    variety = "catalyst"

    def __init__(self) -> None:
        # All four are reset per input-type variant by ``_transpile_from_ast``:
        # the categories assumed for this variant, the builtin names the UDF has
        # not rebound, the narrower category a lowering needs for some parameter,
        # and the per-lowering category cache.
        self._param_categories: dict[int, str] = {}
        self._allowed_builtins: set[str] = set()
        self._narrowed: dict[int, str] = {}
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

    def _builtin_call_category(self, params: List[str], name: str, args: List[ast.expr]) -> str:
        """Category of a supported builtin call, following Python's own typing."""
        if name == "abs" and len(args) == 1:
            return self._category(params, args[0])
        if name in ("min", "max") and len(args) == 2:
            left = self._category(params, args[0])
            right = self._category(params, args[1])
            numeric = _unify_numeric(left, right)
            if numeric is not None:
                return numeric
            if left == right:
                return left  # min/max over two strings compares codepoints
            raise UnsupportedOperationException(
                f"`{name}()` operands have incompatible categories ({left} vs "
                f"{right}); Python would raise TypeError, so the transpiler falls "
                "back to interpreted Python"
            )
        if name == "round" and len(args) in (1, 2):
            # `round(x)` returns an int in Python whatever x is; `round(x, n)`
            # preserves x's kind.
            return "integral" if len(args) == 1 else self._category(params, args[0])
        raise UnsupportedOperationException(
            f"`{name}()` with {len(args)} argument(s) is not supported by the transpiler"
        )

    def _result_is_python_float(self, params: List[str], node: Optional[ast.AST]) -> bool:
        """True when the body provably returns a Python ``float``.

        Such a body can't take an integral return type: ``makeFromJava`` accepts
        only Byte/Short/Int/Long for LongType, so the interpreted UDF nulls the
        float while the lowered expression would cast the double and return a
        truncated number. We refuse and stay interpreted.

        Only provable cases count, so the same divergence is still reachable
        through an unrefined body (``udf(lambda x: x * 2, LongType())`` on a
        double column): a bare parameter is a float exactly when its column is,
        which we only know for a "fractional" variant.
        """
        match node:
            case None:
                return False
            case ast.Return(value=value):
                return self._result_is_python_float(params, value)
            case ast.Constant(value=v):
                return isinstance(v, float)
            case ast.Name(id=name) if name in params:
                return self._category(params, node) == "fractional"
            case ast.BinOp(left=left, op=op, right=right):
                if isinstance(op, ast.Div):
                    return True  # true division is always float in Python
                if isinstance(op, ast.Pow) and _is_negative_int_constant(right):
                    return True  # `2 ** -1` is 0.5
                return self._result_is_python_float(params, left) or self._result_is_python_float(
                    params, right
                )
            case ast.UnaryOp(op=(ast.USub() | ast.UAdd()), operand=operand):
                return self._result_is_python_float(params, operand)
            case ast.IfExp(body=body, orelse=orelse):
                return self._result_is_python_float(params, body) or self._result_is_python_float(
                    params, orelse
                )
            case ast.If(body=body, orelse=orelse):
                return any(self._result_is_python_float(params, s) for s in body) or any(
                    self._result_is_python_float(params, s) for s in orelse
                )
            case ast.Call(func=ast.Name(id=name), args=args) if name in _SUPPORTED_BUILTINS:
                if name == "round":
                    # `round(x)` narrows to int; `round(x, n)` keeps x's kind.
                    return len(args) == 2 and self._result_is_python_float(params, args[0])
                return any(self._result_is_python_float(params, a) for a in args)
            case _:
                return False

    def _int32_exact(self, params: List[str], node: ast.AST, seen: Set[int]) -> bool:
        """True when ``node`` provably evaluates within IntegerType.

        This is what makes ``a / b`` safe on integer columns. Spark divides by
        casting both sides to double, so it only matches Python's exact int/int
        division when both operands are exactly representable -- then IEEE
        division is correctly rounded, as CPython's is. If every leaf is an int32
        column or an int32 literal, ANSI keeps every intermediate in IntegerType
        (overflow raises rather than widening), so nothing exceeds 2^31.

        Records each parameter reached in ``seen`` so the caller narrows exactly
        those, and no more: we only walk value positions (an ``IfExp``'s branches,
        not its test), so a parameter used just in a condition isn't dragged in.

        Only operators whose Spark result stays IntegerType: ``//`` is out because
        ``div`` returns LongType, and calls are out.
        """
        match node:
            case ast.Constant(value=v) if isinstance(v, int) and not isinstance(v, bool):
                return abs(v) <= _INT32_MAX
            case ast.Name(id=name) if name in params:
                # An integral parameter qualifies only once narrowed to int32,
                # which the caller does for everything recorded here.
                if self._category(params, node) not in _INTEGRAL_CATEGORIES:
                    return False
                seen.add(self._param_index(params, name))
                return True
            # `*` is deliberately absent, and it used to be here. The argument for
            # allowing it was that ANSI keeps every intermediate in IntegerType, so a
            # product of int32 operands either stays under 2**31 or raises -- true
            # until operand promotion, which now evaluates that product in bigint and
            # lets it reach 2**62. Past 2**53 the cast to double rounds before
            # dividing, so `(a * b) / c` on three int columns returned
            # 422461242.77102745 where CPython gives 422461242.7710275. Addition and
            # subtraction stay: their worst case is 2**32, comfortably exact as a
            # double even after promotion widens them.
            case ast.BinOp(left=left, op=ast.Add() | ast.Sub() | ast.Mod(), right=right):
                return self._int32_exact(params, left, seen) and self._int32_exact(
                    params, right, seen
                )
            case ast.UnaryOp(op=ast.USub() | ast.UAdd(), operand=operand):
                return self._int32_exact(params, operand, seen)
            case ast.IfExp(body=body, orelse=orelse):
                return self._int32_exact(params, body, seen) and self._int32_exact(
                    params, orelse, seen
                )
            case _:
                return False

    def _param_index(self, params: List[str], name: str) -> int:
        """Placeholder index of ``name``.

        ``params`` is caller-facing -- a bound receiver is already gone by the time
        it reaches us -- so this is a plain lookup. It used to subtract one for a
        leading ``self``, which double-counted the receiver once the caller started
        stripping it, and mis-indexed a plain function whose first parameter just
        happens to be named ``self``.
        """
        return params.index(name)

    def _narrow(self, params: List[str], category: str, *nodes: ast.AST) -> None:
        """Require every parameter whose *value* reaches ``nodes`` to be ``category``.

        For lowerings that are exact only on a narrower column type than the
        variant assumed: ``/`` needs int32 operands and a string repeat needs an
        integral count (``"ab" * 2.5`` is a TypeError in Python). Narrowing is
        safe -- it only ever matches fewer column types, so the JVM drops the
        option and falls back rather than picking a wrong one.

        Value positions only, which is the same line ``_int32_exact`` draws and for
        the same reason: what a parameter contributes to a *condition* cannot change
        the kind of the value, so dragging it in refuses columns the lowering
        handles perfectly well. ``s * (2 if n > 1.0 else 3)`` is the shape that
        showed it -- the count is the literal 2 or 3 whatever ``n`` is, yet walking
        the whole subtree tagged ``n`` integral and dropped the option for a double.
        """
        for node in nodes:
            self._narrow_indexes(self._value_params(params, node), category)

    def _value_params(self, params: List[str], node: Optional[ast.AST]) -> Set[int]:
        """Indexes of the parameters whose value reaches ``node``'s result.

        Deliberately not ``ast.walk``: an ``IfExp``'s test and an ``If``'s test are
        skipped, since they steer which value comes back without contributing to it.
        Everything a value can actually flow through is listed, and an unrecognised
        node contributes nothing -- the lowering for it would have raised first.
        """
        match node:
            case ast.Name(id=name) if name in params:
                return {self._param_index(params, name)}
            case ast.BinOp(left=left, right=right):
                return self._value_params(params, left) | self._value_params(params, right)
            case ast.UnaryOp(operand=operand):
                return self._value_params(params, operand)
            # `a and b` evaluates to one of its operands in Python, so both are
            # value positions -- unlike a comparison, whose operands only feed the
            # bool it produces (harmless to include, and cheaper than proving it
            # cannot be reached).
            case ast.BoolOp(values=values):
                return set().union(set(), *(self._value_params(params, v) for v in values))
            case ast.Compare(left=left, comparators=comparators):
                return set().union(
                    self._value_params(params, left),
                    *(self._value_params(params, c) for c in comparators),
                )
            case ast.IfExp(body=body, orelse=orelse):
                return self._value_params(params, body) | self._value_params(params, orelse)
            case ast.Return(value=value):
                return self._value_params(params, value)
            case ast.If(body=body, orelse=orelse):
                return set().union(
                    set(),
                    *(self._value_params(params, s) for s in body),
                    *(self._value_params(params, s) for s in orelse),
                )
            case ast.Call(args=args):
                return set().union(set(), *(self._value_params(params, a) for a in args))
            case _:
                return set()

    def _narrow_indexes(self, indexes: Set[int], category: str) -> None:
        """``_narrow`` for parameter indexes already identified."""
        for index in indexes:
            self._record_narrowing(index, category)

    def _record_narrowing(self, index: int, category: str) -> None:
        """Keep the narrowest requirement for a parameter.

        Several lowerings in one option can each need something of the same
        parameter, and those needs are conjunctive. Plain assignment would let a
        later, wider request undo an earlier one: in ``s * (n if n / 3 > 1 else
        1)`` the repeat's "integral" would replace the division's "integral32",
        and the option would then be kept for a bigint column -- exactly what the
        division can't reproduce.
        """
        current = self._narrowed.get(index)
        if current is None or _NARROWING_ORDER.index(category) > _NARROWING_ORDER.index(current):
            self._narrowed[index] = category

    def _python_mod(self, left_col: Column, right_col: Column) -> Column:
        """Python's ``%``, which takes the sign of the divisor where Spark's takes
        the dividend's.

        Same shape as CPython's ``long_mod`` / ``float_rem``: take the remainder,
        then add the divisor back when the signs disagree. That is exact for both
        kinds of number and can't overflow -- ``|r| < |b|`` with opposite signs
        means ``|r + b| < |b|``. The sign of a *zero* result still follows the
        dividend rather than the divisor (Python's ``4.0 % -2.0`` is ``-0.0``,
        ours is ``0.0``); the two compare equal, so nothing downstream sees it.

        ``sign(b) * pmod(sign(b) * a, abs(b))`` was the older form. It lost small
        dividends on doubles (``7.5 % -1e17`` gave ``-0.0`` instead of ``-1e17``,
        because the intermediate ``-7.5 + 1e17`` rounds to ``1e17``) and raised at
        the LongType boundaries where the negate or the ``abs`` overflowed.
        """
        remainder = left_col.__mod__(right_col)
        signs_differ = (remainder < lit(0)) != (right_col < lit(0))
        mod = when((remainder != lit(0)) & signs_differ, remainder + right_col).otherwise(remainder)
        return _raise_on_null_operand("%", mod, left_col, right_col)

    def _lower_int_pow(self, base_col: Column, exponent: ast.AST) -> Column:
        """Lower ``base ** k`` for a constant non-negative integer ``k``.

        Repeated multiplication is exact on integers, unlike Spark's ``pow``,
        which is DOUBLE and loses precision past 2^53 -- the reason ``**`` was
        refused outright before. Overflow raises under ANSI where Python promotes
        to a big int, the same caveat ``*`` carries (``**`` just gets there fast).
        """
        k = _int_constant(exponent)
        if k is None:
            raise UnsupportedOperationException(
                "`**` is only lowered for a constant integer exponent: a column "
                "exponent has no bounded expansion into multiplications, so the "
                "UDF falls back to interpreted Python"
            )
        if k < 0:
            raise UnsupportedOperationException(
                "`**` with a negative exponent returns a float in Python and has "
                "no exact integral lowering, so the UDF falls back to interpreted "
                "Python"
            )
        if k > _MAX_POW_EXPANSION:
            raise UnsupportedOperationException(
                f"`**` exponents above {_MAX_POW_EXPANSION} are not lowered (the "
                "expansion into multiplications would bloat the plan, and past it "
                "all but the smallest bases overflow LongType), so the UDF falls "
                "back to interpreted Python"
            )
        if k == 0:
            # `x ** 0` is 1 for any x that exists -- `None ** 0` raises TypeError.
            # Folding to a bare `lit(1)` would drop the base, inventing 1 for a
            # NULL input and swallowing errors from computing the base, so keep it
            # in the condition.
            return _raise_on_null_operand("**", lit(1), base_col)
        # Each step is a promoting multiply, so the expansion widens as it grows rather than
        # all at once: `x ** 2` on a tinyint lands on smallint, on an int it lands on bigint,
        # and on a bigint there is nowhere left to go so it raises exactly as it did before
        # promotion existed. That is why this no longer casts to long up front -- doing so
        # would throw away the operand's real width and make every power look like a bigint
        # one, promoting further than the column needs and losing the narrow-column cases.
        result = base_col
        for _ in range(k - 1):
            result = _promoting("multiply", result, base_col)
        return _raise_on_null_operand("**", result, base_col)

    def _lower_shift(self, left_col: Column, right_col: Column, left_shift: bool) -> Column:
        """Lower ``a << n`` / ``a >> n``, handling Python's error cases exactly.

        Java -- and so Spark -- masks the shift distance to the operand's width
        (``& 63`` for a long, ``& 31`` for an int) where Python shifts by the full
        count. That means two things, and both are silently wrong if missed:

        * We widen the operand to a long. Otherwise ``x >> 32`` on an int column
          masks to a no-op and returns ``x`` instead of 0. It also drops spurious
          ``<<`` overflows on narrow columns, since Python has no width.
        * A count of 64+ needs its own branch. Shifting back and comparing can't
          catch it -- masking makes the shift a no-op, so the round trip agrees
          with itself. ``>>`` has an exact answer (every bit shifts out, leaving 0
          or -1 for a negative operand, as both languages shift arithmetically);
          ``<<`` only agrees for a zero operand and otherwise overflows.

        Then: a negative count raises like Python's ValueError, and a ``<<`` that
        drops bits raises the way ``*`` reports overflow under ANSI. Under 64 the
        round trip is a real check, since nothing is masked. The count is clamped
        before the cast below, so both of those report the transpiler's own error
        rather than a cast overflow however far out of range the count is. A NULL
        operand raises the guard's error like everywhere else; the guard wraps
        the whole chain, so the branches below only ever see non-NULL operands.
        """
        # The count must be IntegerType (BitShiftOperation's inputTypes). Implicit
        # coercion would insert that cast itself, but then an out-of-range count
        # would hit it before the clamp -- and Python is fine with such counts
        # (`48 >> 2**40` is 0, not an error). Clamp first: every count at or past
        # the width behaves the same and every negative one raises, so clamping
        # leaves the branches below exact.
        count = (
            when(right_col > lit(_LONG_BITS), lit(_LONG_BITS))
            .when(right_col < lit(0), lit(-1))
            .otherwise(right_col)
            .cast("int")
        )
        base = left_col.cast("long")
        negative_count = lit(
            "Python UDF transpiler: negative shift count; Python would raise ValueError here."
        )
        guarded: Column = when(count < lit(0), raise_error(negative_count))
        if not left_shift:
            chain = guarded.when(
                count >= lit(_LONG_BITS), when(base < lit(0), lit(-1)).otherwise(lit(0))
            ).otherwise(call_function("shiftright", base, count))
            return _raise_on_null_operand(">>", chain, left_col, right_col)
        shifted = call_function("shiftleft", base, count)
        overflow = lit(
            "Python UDF transpiler: `<<` overflowed the column type; Python "
            "would promote to an arbitrary-precision int here."
        )
        chain = (
            # A zero operand survives any count, so it falls through to the round
            # trip and agrees; anything else has lost every bit.
            guarded.when((count >= lit(_LONG_BITS)) & (base != lit(0)), raise_error(overflow))
            .when(call_function("shiftright", shifted, count) != base, raise_error(overflow))
            .otherwise(shifted)
        )
        return _raise_on_null_operand("<<", chain, left_col, right_col)

    def _lower_builtin_call(self, params: List[str], node: ast.Call) -> Column:
        """Lower a call to one of ``abs`` / ``min`` / ``max`` / ``round``."""
        if node.keywords or not isinstance(node.func, ast.Name):
            raise UnsupportedOperationException(
                "only positional calls to a small set of builtins are supported by the transpiler"
            )
        name = node.func.id
        if name not in self._allowed_builtins:
            # Either an unsupported callable, or a supported name the UDF rebound
            # (`from mymath import round`, a local, a closure variable). Lowering
            # builtin semantics for a rebound name would run different code than
            # the UDF does, so refuse.
            raise UnsupportedOperationException(
                f"call to {name!r} is not supported by the transpiler: it is "
                "either not a lowerable builtin or no longer refers to the "
                "builtin in this UDF's scope"
            )
        args = node.args
        if any(isinstance(a, ast.Starred) for a in args):
            raise UnsupportedOperationException(
                f"`{name}(*args)` is not supported by the transpiler"
            )
        # Validates the argument count and that the operand categories are
        # compatible with each other; the result is the call's own category.
        result_cat = self._builtin_call_category(params, name, args)
        if name == "abs":
            # Exact for either kind of number; `abs(Long.MinValue)` raises, the
            # usual overflow caveat. A string operand would be promoted by ANSI
            # (`abs('-5')` is 5.0) and a bool/binary one fails Abs's input check,
            # breaking the query instead of falling back.
            if not _is_numeric_cat(result_cat):
                raise UnsupportedOperationException(
                    "`abs()` is only supported for numeric operands (Python raises "
                    "TypeError otherwise, and Spark would coerce or fail analysis); "
                    "the transpiler falls back to interpreted Python"
                )
            # Promoting, because `Abs` keeps its operand's type and two's complement has no
            # positive counterpart for a width's minimum: `abs(x)` on a smallint holding
            # -32768 raised where Python answers 32768.
            abs_col = self._convert_chunk(params, args[0])
            return _raise_on_null_operand(
                "abs()",
                _promoting_if_numeric("abs", [result_cat], lambda: _abs(abs_col), abs_col),
                abs_col,
            )
        if name in ("min", "max"):
            # `least`/`greatest` skip nulls, so `min(None, 3)` would return 3 where
            # Python raises -- guard rather than diverge. Floats are out because
            # Python returns the first argument for a NaN operand while Spark orders
            # NaN highest; checking the unified category covers both orders. Only a
            # pair is handled: `least`/`greatest` are variadic, but category
            # unification (`_builtin_call_category`) is written for two operands.
            if _is_numeric_cat(result_cat) and result_cat not in _INTEGRAL_CATEGORIES:
                raise UnsupportedOperationException(
                    f"`{name}()` is only lowered for integral or string "
                    "operands: on floats Python returns whichever argument came "
                    "first while Spark orders NaN highest, so the UDF falls back "
                    "to interpreted Python"
                )
            cols = [self._convert_chunk(params, a) for a in args]
            picked = least(*cols) if name == "min" else greatest(*cols)
            return _raise_on_null_operand(f"{name}()", picked, *cols)
        # round: HALF_EVEN, like Python. Integral operands only. A negative scale
        # can overflow the column type (`round(Long.MaxValue, -1)` raises where
        # Python promotes) -- the usual overflow caveat.
        if self._category(params, args[0]) not in _INTEGRAL_CATEGORIES:
            raise UnsupportedOperationException(
                "`round()` is only lowered for integral operands: Spark's `bround` "
                "rounds the shortest decimal representation of a double, which "
                "differs from Python's rounding of the exact binary value, so the "
                "UDF falls back to interpreted Python"
            )
        # `round(x)` is `round(x, 0)`; a non-literal scale can't be lowered
        # because Spark's `bround` requires a foldable one.
        scale = 0 if len(args) == 1 else _int_constant(args[1])
        if scale is None:
            raise UnsupportedOperationException(
                "`round()`'s second argument must be an integer literal (Spark's "
                "`bround` requires a foldable scale), so the UDF falls back to "
                "interpreted Python"
            )
        if abs(scale) > _INT32_MAX:
            # `bround`'s scale is IntegerType, and ANSI narrows a bigint literal to
            # it with a cast that fails CAST_OVERFLOW -- breaking the query instead
            # of falling back. Python takes any scale (`round(5, 2**40)` is 5).
            raise UnsupportedOperationException(
                "`round()`'s scale does not fit in an int, which Spark's `bround` "
                "requires; the transpiler falls back to interpreted Python"
            )
        # TODO: a negative scale still overflows a narrow column, and this is left for a
        # follow-up. `bround` keeps its child's type and multiplies the magnitude by
        # 10**|scale|, so `round(x, -1)` on a tinyint holding 127 raises where Python
        # answers 130 -- the same shape as `abs` before promotion, and equally fixable,
        # since the worst case is `magnitude(input) * 10**|scale|`. It needs a widening
        # cast the transpiler cannot size (only the JVM knows the column width), so it
        # wants a promoting expression of its own rather than a Python-side hack.
        round_col = self._convert_chunk(params, args[0])
        return _raise_on_null_operand("round()", bround(round_col, scale), round_col)

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
                # Two numeric branches of different kinds still have a common
                # type (the wider of the two), so unify rather than give up.
                return _unify_numeric(body_c, else_c)
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
        if (
            body_cat is not None
            and else_cat is not None
            and _coarse_category(body_cat) != _coarse_category(else_cat)
        ):
            raise UnsupportedOperationException(
                f"if/else branches have incompatible categories ({body_cat} vs "
                f"{else_cat}); the lowered CASE WHEN has no common type under ANSI, "
                "so the transpiler falls back to interpreted Python"
            )
        return when(safe_test, body_col).otherwise(else_col)

    def _nan_guard(
        self,
        left_cat: Optional[str],
        right_cat: Optional[str],
        left_col: Column,
        right_col: Column,
    ) -> Optional[Column]:
        """``isnan(l) | isnan(r)``, or ``None`` when NaN cannot arise here.

        Python makes every comparison involving NaN False (so ``!=`` is True),
        while Spark treats ``NaN = NaN`` as true and orders NaN above every
        value. NaN-ness is a runtime test rather than a static one, so this is
        guarded exactly rather than refused -- but only where it can happen:

        * both operands have to be numbers. Strings are excluded for a concrete
          reason and not just tidiness: under ANSI ``isnan`` casts its argument,
          so ``isnan('x')`` raises CAST_INVALID_INPUT at runtime rather than
          returning false, which would break a working string comparison.
        * not both provably integral, since an integral column has no NaN to
          find and the extra branch would only grow the plan. An unrefined
          ``"numeric"`` operand might still be a double, so it does get guarded.

        Returning ``None`` rather than ``lit(False)`` lets the callers leave the
        branch out of the plan altogether instead of emitting a dead one.
        """
        if not (_is_numeric_cat(left_cat) and _is_numeric_cat(right_cat)):
            return None
        if left_cat in _INTEGRAL_CATEGORIES and right_cat in _INTEGRAL_CATEGORIES:
            return None
        return isnan(left_col) | isnan(right_col)

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

        Spark treats ``NaN = NaN`` as true, while Python's ``nan == nan`` is
        False, so a numeric variant that could hold a double tests for NaN
        before evaluating the Spark comparison -- see ``_nan_guard``. NULL is
        checked first, which is also Python's order: ``None == nan`` is False
        because the operands are of different types, not because of NaN.
        """
        lc = self._safe_category(params, left_node)
        rc = self._safe_category(params, right_node)
        # Compare coarsely: `2 == 2.0` is True in Python and in Spark, so an
        # int-vs-float pairing is compatible even though the refined categories
        # differ. Only a genuine cross-kind pairing (numeric vs string/bool)
        # forces the fallback.
        if lc is not None and rc is not None and _coarse_category(lc) != _coarse_category(rc):
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
            nan_val: Column = lit(False)
            value_cmp = left_col == right_col
        else:
            both_null_val = lit(False)
            one_null_val = lit(True)
            nan_val = lit(True)
            value_cmp = left_col != right_col
        nan_guard = self._nan_guard(lc, rc, left_col, right_col)
        if nan_guard is None:
            # No NaN can arise here (strings, provably-integral numerics, a None literal), and
            # Spark's `<=>` *is* Python's None-equality: both-null true, one-null false, values
            # otherwise. Measured against CPython over the full grid of {null, 0.0, -0.0, 1.0,
            # NaN} on doubles and {null, 0, 1, -1} on bigints, both operand orders, and against
            # an int literal on a bigint column (coercion intact) -- identical on every row.
            # So the four-branch chain below is hand-rolling an operator we already have.
            #
            # Only on this path, though. `<=>` calls NaN equal to itself where Python does not,
            # so where NaN is possible the guard has to stay -- and it deliberately stays *after*
            # the null branches rather than being hoisted above an `eqNullSafe`, because
            # `isnan(l) | isnan(r)` short-circuits: hoisting it would skip evaluating the right
            # operand when the left is NaN, and an operand that raises (`x == 1.0 / y`) would
            # then silently answer False instead of raising as Python does.
            equal_null_safe = left_col.eqNullSafe(right_col)
            return equal_null_safe if equal else ~equal_null_safe
        return (
            when(left_null & right_null, both_null_val)
            .when(left_null | right_null, one_null_val)
            .when(nan_guard, nan_val)
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

        Spark orders ``NaN`` as greater than every value, whereas Python's
        ``NaN`` comparisons are all ``False``, so a numeric variant that could
        hold a double returns false when either operand is NaN -- see
        ``_nan_guard``. The NULL raise comes first, matching Python, which throws
        TypeError on ``None > 0`` regardless of the other operand.
        """
        lc = self._category(params, left_node)
        rc = self._category(params, right_node)
        # Coarse comparison: Python orders ints against floats happily, so only a
        # cross-kind pairing (numeric vs string, ...) is the TypeError case.
        if _coarse_category(lc) != _coarse_category(rc):
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
        nan_guard = self._nan_guard(lc, rc, left_col, right_col)
        guarded = when(null_guard, raise_error(err))
        if nan_guard is not None:
            guarded = guarded.when(nan_guard, lit(False))
        return guarded.otherwise(op(left_col, right_col))

    def _category(self, params: List[str], node: ast.AST) -> str:
        """Infer a category for ``node`` under the current
        ``self._param_categories`` assumption (set per input-type variant).

        Drives operator selection (``+`` -> add vs concat, ``*`` -> multiply vs
        repeat) and raises ``UnsupportedOperationException`` when an operator's
        operands are type-incompatible, so the caller drops that variant and the
        JVM picks another option / falls back to the Python UDF.

        Numeric results carry their kind (``"integral"`` / ``"fractional"``)
        when it is known, since several lowerings are only exact for one of
        them; ``"numeric"`` means "a number, kind unknown" and comes from an
        unrefined parameter. ``_unify_numeric`` promotes the way Python does.
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
                # bool subclasses int, so classify it first: int -> integral,
                # float -> fractional, str -> string, bool -> bool, bytes ->
                # binary. None/complex/Ellipsis have no usable Spark column type,
                # so raise to drop this variant and fall back rather than emit an
                # option that fails CheckAnalysis or silently diverges (e.g.
                # `x + None` -> NULL where Python raises TypeError).
                if isinstance(v, bool):
                    return "bool"
                if isinstance(v, bytes):
                    return "binary"
                if isinstance(v, int):
                    return "integral"
                if isinstance(v, float):
                    return "fractional"
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
                numeric = _unify_numeric(lc, rc)
                if isinstance(op, ast.Add):
                    if numeric is not None:
                        return numeric  # num + num -> num
                    if lc == rc == "string":
                        return "string"  # str + str -> str
                if isinstance(op, ast.Mult):
                    if numeric is not None:
                        return numeric
                    if (lc == "string" and _is_numeric_cat(rc)) or (
                        _is_numeric_cat(lc) and rc == "string"
                    ):
                        return "string"  # str * int / int * str -> repeat
                if isinstance(op, (ast.Sub, ast.Mod, ast.FloorDiv)) and numeric is not None:
                    # `//` follows Python: int // int stays an int, and a float
                    # operand makes the result a float.
                    return numeric
                if isinstance(op, ast.Div) and numeric is not None:
                    # True division always produces a float in Python, even for
                    # `4 / 2`. The return-type gate keys off this.
                    return "fractional"
                if isinstance(op, ast.Pow) and numeric is not None:
                    # Only non-negative integer exponents are lowered, and those
                    # keep an integral base integral. A negative exponent yields a
                    # float in Python; report that faithfully so the return-type
                    # gate sees it (the lowering refuses it separately).
                    if _is_negative_int_constant(right):
                        return "fractional"
                    return numeric
                if (
                    isinstance(op, (ast.BitAnd, ast.BitOr, ast.BitXor, ast.LShift, ast.RShift))
                    and numeric is not None
                ):
                    # Integer-only in Python; the lowering enforces that the
                    # operands really are integral.
                    return numeric
                raise UnsupportedOperationException(
                    f"operands of `{type(op).__name__}` are not type-compatible "
                    "for this input-type variant"
                )
            case ast.Call(func=ast.Name(id="len"), args=[arg], keywords=[]):
                # ``len(s)`` is always a Python int, so report "integral" not the
                # unrefined "numeric": the integral-only lowerings (``//``, ``&``,
                # shifts, ``round``) would otherwise miss ``len(s) // 2`` etc.
                # and the UDF would fall back unnecessarily. Only strings are
                # lowered (SPARK-55214); other argument categories raise.
                if self._category(params, arg) != "string":
                    raise UnsupportedOperationException(
                        "`len` is only lowered for string operands; other "
                        "types fall back to interpreted Python"
                    )
                return "integral"
            case ast.Call(func=ast.Name(id=name), args=args) if name in _SUPPORTED_BUILTINS:
                return self._builtin_call_category(params, name, args)
            case ast.UnaryOp(op=(ast.USub() | ast.UAdd() | ast.Invert()), operand=operand):
                # Negation, unary plus, and `~` all preserve the operand's kind,
                # so `-3` stays integral and `x // -3` keeps its exact lowering.
                # `not` is deliberately not here: it falls through to the boolean
                # arm below. (These are separate from the catch-all because that
                # would report the unrefined "numeric" and cost the integral
                # lowerings.)
                return self._category(params, operand)
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
                    # Two numeric branches of different kinds are fine -- Python's
                    # `1 if c else 2.5` is an int or a float depending on the test,
                    # and the lowered CASE WHEN promotes to the wider type -- so
                    # unify those instead of refusing.
                    unified = _unify_numeric(body_cat, else_cat)
                    if unified is None:
                        raise UnsupportedOperationException(
                            f"ternary branches have mismatched categories ({body_cat} "
                            f"vs {else_cat}) and cannot drive operator selection"
                        )
                    return unified
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
                if not _is_numeric_cat(self._category(params, operand)):
                    raise UnsupportedOperationException(
                        "unary `+`/`-` is only supported for numeric operands "
                        "(Python raises TypeError on strings, and Spark would "
                        "coerce or fail analysis); the transpiler falls back "
                        "to interpreted Python"
                    )
                if isinstance(op, ast.USub):
                    # Handles both literal negative ints (USub on a Constant)
                    # and runtime negation of a column. Promoting for the same
                    # reason `abs` is: `UnaryMinus` keeps its operand's type and
                    # negates exactly, so `-x` on an int column holding
                    # Integer.MinValue raised where Python answers 2147483648 --
                    # two's complement having no positive counterpart for a
                    # minimum. `forNegation` is the shared rule.
                    neg_cat = self._category(params, operand)
                    neg_col = self._convert_chunk(params, operand)
                    return _raise_on_null_operand(
                        "unary -",
                        _promoting_if_numeric(
                            "negate", [neg_cat], lambda: neg_col.__neg__(), neg_col
                        ),
                        neg_col,
                    )
                # `+x` -- identity on a value, but `+None` raises TypeError like
                # every other operator, so it gets the guard too.
                plus_col = self._convert_chunk(params, operand)
                return _raise_on_null_operand("unary +", plus_col, plus_col)
            case ast.UnaryOp(op=ast.Invert(), operand=operand):
                # `~x` is `-x - 1` in both languages, but int-only: Python raises
                # on a float and BitwiseNot fails analysis for a double child,
                # which breaks the query instead of falling back (options are
                # type-checked as children of TranspiledPythonUDF).
                if self._category(params, operand) not in _INTEGRAL_CATEGORIES:
                    raise UnsupportedOperationException(
                        "`~` is only supported for integral operands (Python raises "
                        "TypeError on floats and strings, and Spark would fail "
                        "analysis); the transpiler falls back to interpreted Python"
                    )
                # Normalise to a long, as the bitwise binary operators do. Also added for
                # a promoted decimal child that the LongType ceiling now makes impossible,
                # so likewise defensive rather than load bearing.
                inv_col = self._convert_chunk(params, operand)
                return _raise_on_null_operand("~", bitwise_not(inv_col.cast("long")), inv_col)
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
                # `//`, `**` and the bitwise operators also need *integral*
                # operands; `/` needs a fractional or int32-exact one. The module
                # docstring has the reasoning and the resulting fall-backs;
                # SPARK-55220 tracks the strictness knob for the value-level ones.
                lc = self._category(params, left)
                rc = self._category(params, right)
                both_numeric = _unify_numeric(lc, rc) is not None
                both_integral = lc in _INTEGRAL_CATEGORIES and rc in _INTEGRAL_CATEGORIES
                left_col = self._convert_chunk(params, left)
                right_col = self._convert_chunk(params, right)
                match op:
                    case ast.Add():
                        if lc == rc == "string":
                            # `concat` propagates NULL where Python raises
                            # TypeError on `'a' + None`, so guard rather than
                            # return a value Python never would -- the same
                            # guard every numeric lowering carries.
                            concat_null = lit(
                                "Python UDF transpiler: cannot concatenate NULL; "
                                "Python would raise TypeError here. Add an `is not "
                                "None` guard or filter NULLs upstream."
                            )
                            return when(
                                left_col.isNull() | right_col.isNull(),
                                raise_error(concat_null),
                            ).otherwise(concat(left_col, right_col))
                        if both_numeric:
                            return _raise_on_null_operand(
                                "+",
                                _promoting_if_numeric(
                                    "add",
                                    [lc, rc],
                                    lambda: left_col.__add__(right_col),
                                    left_col,
                                    right_col,
                                ),
                                left_col,
                                right_col,
                            )
                    case ast.Sub():
                        if both_numeric:
                            return _raise_on_null_operand(
                                "-",
                                _promoting_if_numeric(
                                    "subtract",
                                    [lc, rc],
                                    lambda: left_col.__sub__(right_col),
                                    left_col,
                                    right_col,
                                ),
                                left_col,
                                right_col,
                            )
                    case ast.Mult():
                        if both_numeric:
                            return _raise_on_null_operand(
                                "*",
                                _promoting_if_numeric(
                                    "multiply",
                                    [lc, rc],
                                    lambda: left_col.__mul__(right_col),
                                    left_col,
                                    right_col,
                                ),
                                left_col,
                                right_col,
                            )
                        # The repeat count has to be an int -- `"ab" * 2.5` is a
                        # TypeError, not a truncation. A float literal shows up as
                        # "fractional" here; for a parameter only the JVM knows, so
                        # narrow the count and let a double column drop the option.
                        # "integral32" not "integral": `repeat` casts the count to
                        # int, which raises under ANSI for a bigint above Int.MaxValue
                        # where Python returns "" (negative) or a huge string. A
                        # bigint column therefore drops the option and falls back.
                        if lc == "string" and _is_numeric_cat(rc) and rc != "fractional":
                            self._narrow(params, "integral32", right)
                            return _raise_on_null_operand(
                                "*", repeat(left_col, right_col.cast("int")), left_col, right_col
                            )
                        if _is_numeric_cat(lc) and lc != "fractional" and rc == "string":
                            self._narrow(params, "integral32", left)
                            return _raise_on_null_operand(
                                "*", repeat(right_col, left_col.cast("int")), left_col, right_col
                            )
                    case ast.Mod():
                        if both_numeric:
                            return self._python_mod(left_col, right_col)
                    case ast.Div():
                        # `/` is always float division in Python, and ANSI's
                        # DIVIDE_BY_ZERO lines up with ZeroDivisionError. A
                        # fractional operand makes it exact, since Python converts
                        # the other side to double just as we do -- but both sides
                        # still have to be numbers, or ANSI string promotion would
                        # compute `10.0 / '2'` where Python raises. Two integers
                        # only match while every value stays in IntegerType: past
                        # 2^53 Spark rounds on the cast to double before dividing,
                        # where Python divides the exact integers. Narrow to int32
                        # so a bigint column drops the option rather than using it.
                        if both_numeric and (lc == "fractional" or rc == "fractional"):
                            return _raise_on_null_operand(
                                "/", left_col.__div__(right_col), left_col, right_col
                            )
                        if both_numeric:
                            int32_params: Set[int] = set()
                            if not (
                                self._int32_exact(params, left, int32_params)
                                and self._int32_exact(params, right, int32_params)
                            ):
                                raise UnsupportedOperationException(
                                    "`/` on integers is only lowered when both "
                                    "operands provably stay within IntegerType; a "
                                    "bigint operand above 2^53 would round before "
                                    "dividing and diverge from Python, so the UDF "
                                    "falls back to interpreted Python"
                                )
                            self._narrow_indexes(int32_params, "integral32")
                            return _raise_on_null_operand(
                                "/", left_col.__div__(right_col), left_col, right_col
                            )
                    case ast.FloorDiv():
                        # Python floors toward -inf where `div` truncates toward
                        # zero, so subtract one when the remainder is non-zero and
                        # its sign differs from the divisor's -- the cases where
                        # truncation went the wrong way. `%` is Spark's Remainder
                        # (dividend's sign), which is what makes that test work.
                        #
                        # `div` returns LongType and rejects fractional input, so
                        # integral only: a float `//` would need `floor(a / b)`,
                        # which can land on the far side of an integer when `a / b`
                        # rounds up. Divide-by-zero raises, as it does in Python.
                        if both_integral:
                            quotient = call_function("div", left_col, right_col)
                            remainder = left_col.__mod__(right_col)
                            # `div` truncates toward zero where Python `//` floors toward
                            # -inf, so subtract one when the division is not exact and the
                            # operands' signs differ -- the cases truncation went the
                            # wrong way. Read the signs off the operands directly rather
                            # off `remainder`'s sign: Spark's `%` (Remainder) takes the
                            # dividend's sign, which happens to match, but spelling the
                            # condition in terms of the operands keeps the lowering
                            # correct if that invariant ever moves.
                            needs_floor = (remainder != lit(0)) & (
                                (left_col < lit(0)) != (right_col < lit(0))
                            )
                            floored = when(needs_floor, quotient - lit(1)).otherwise(quotient)
                            return _raise_on_null_operand("//", floored, left_col, right_col)
                        if both_numeric:
                            raise UnsupportedOperationException(
                                "`//` is only lowered for integral operands: on "
                                "floats `floor(a / b)` can differ from Python's "
                                "floor division by one, so the UDF falls back to "
                                "interpreted Python"
                            )
                    case ast.Pow():
                        if both_integral:
                            return self._lower_int_pow(left_col, right)
                        if both_numeric:
                            raise UnsupportedOperationException(
                                "`**` is only lowered for integral operands: "
                                "repeated multiplication rounds once per step on "
                                "doubles where Python calls libm `pow`, so the UDF "
                                "falls back to interpreted Python"
                            )
                    case ast.BitAnd() | ast.BitOr() | ast.BitXor():
                        # Int-only in Python, and these map straight across. They
                        # have to go through `Column.bitwiseAND` and friends --
                        # `&` / `|` on a Column build logical And/Or, not bitwise.
                        if both_integral:
                            # Normalise both sides to a long first, the way the shifts do.
                            # Python's bitwise operators are int-only, so a single width for
                            # both operands is what the semantics actually are; it also keeps
                            # `bitwiseAND` off the mixed-width path, where an operand pair it
                            # cannot coerce fails *analysis* with BINARY_OP_DIFF_TYPES and
                            # breaks the query rather than falling back.
                            left_bits = left_col.cast("long")
                            right_bits = right_col.cast("long")
                            if isinstance(op, ast.BitAnd):
                                combined = left_bits.bitwiseAND(right_bits)
                                symbol = "&"
                            elif isinstance(op, ast.BitOr):
                                combined = left_bits.bitwiseOR(right_bits)
                                symbol = "|"
                            else:
                                combined = left_bits.bitwiseXOR(right_bits)
                                symbol = "^"
                            return _raise_on_null_operand(symbol, combined, left_col, right_col)
                        if both_numeric:
                            raise UnsupportedOperationException(
                                f"`{type(op).__name__}` is only lowered for integral "
                                "operands (Python raises TypeError on floats), so "
                                "the UDF falls back to interpreted Python"
                            )
                    case ast.LShift() | ast.RShift():
                        if both_integral:
                            return self._lower_shift(
                                left_col, right_col, left_shift=isinstance(op, ast.LShift)
                            )
                        if both_numeric:
                            raise UnsupportedOperationException(
                                f"`{type(op).__name__}` is only lowered for integral "
                                "operands (Python raises TypeError on floats), so "
                                "the UDF falls back to interpreted Python"
                            )
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
            # Only a call through a plain name can be one of the builtins we lower.
            # Anything else -- `(lambda y: y + 1)(x)`, a method call, a call through a
            # subscript -- falls through to the generic "AST node Call is not
            # supported" message below, which names the node and is the more useful
            # thing to read. Matching every Call here instead reported the
            # builtin-positional-args complaint for bodies that were never calling a
            # builtin at all.
            case ast.Call(func=ast.Name()):
                return self._lower_builtin_call(params, body)
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
        func: Optional[Callable] = None,
    ) -> Optional[Column]:
        # Short circuit on nothing to transpile.
        if src == "" or ast_info is None:
            return None
        # Per-variant input-type assumption ({public_param_index -> category}),
        # read by ``_category`` to choose str vs numeric operators.
        self._param_categories = param_categories or {}
        # Which builtin names this UDF has NOT rebound, so `abs(x)` and friends
        # can only lower when they still mean the builtin.
        self._allowed_builtins = _resolvable_builtins(func, params)
        # Parameters a lowering needs narrowed beyond this variant's own
        # assumption (`/` needs int32, a string repeat needs an integral count);
        # reset per variant and read back by the caller after a successful run.
        self._narrowed = {}
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
                    _is_numeric_cat(body_cat)
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
            # Within-numeric conversions are allowed above, but not when the body
            # provably returns a Python float and the return type is integral: the
            # interpreted path yields NULL there (makeFromJava takes only
            # Byte/Short/Int/Long for LongType) while the lowered cast would
            # truncate the double to a *number*. That is a silent divergence, and
            # it is the common shape for `/` -- `udf(lambda x: x / 2, LongType())`.
            if self._result_is_python_float(params, function_body[0]) and not isinstance(
                returnType, (FloatType, DoubleType)
            ):
                raise UnsupportedOperationException(
                    f"the body returns a Python float but the declared return type "
                    f"is {returnType.simpleString()}; interpreted, the UDF yields "
                    "NULL there, so lowering it to a truncating cast would silently "
                    "diverge -- declare a float/double return type to transpile this"
                )
            # That check only catches the *provable* floats. A bare parameter is a
            # float exactly when its column is, and an unrefined body's category is
            # plain "numeric", which admits DoubleType -- so
            # `udf(lambda x: x * 2, LongType())` on a double column holding 3.7
            # lowered to `cast((x * 2.0) as bigint)` and answered 7, where the
            # interpreted UDF returns NULL. Narrowing closes it without emitting a
            # single extra variant: the option stops matching a double column, and
            # that column falls back to interpreted Python, which is where the NULL
            # comes from. An integral return type is the whole condition -- a
            # float/double one converts the same way on both paths.
            if isinstance(returnType, NumericType) and not isinstance(
                returnType, (FloatType, DoubleType, DecimalType)
            ):
                self._narrow(params, "integral", function_body[0])
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


def _annotation_category(annotation: Optional[ast.AST], refined: bool = False) -> Optional[str]:
    """Map a parameter's type annotation to a category
    (``"numeric"``/``"string"``/``"bool"``/``"binary"``), or ``None`` when it's
    absent or unrecognised (the caller then tries both numeric and string).

    With ``refined``, ``int`` and ``float`` map to ``"integral"`` and
    ``"fractional"`` instead of collapsing to ``"numeric"`` -- used for bodies
    whose lowering depends on the kind of number (see
    ``_body_needs_numeric_refinement``). Annotating those parameters therefore
    pins a single variant instead of emitting one per kind.
    """
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
    if name == "int":
        return "integral" if refined else "numeric"
    if name == "float":
        return "fractional" if refined else "numeric"
    if name == "bool":
        return "bool"
    if name == "bytes":
        return "binary"
    return None


def _body_needs_numeric_refinement(function_ast: ast.FunctionDef) -> bool:
    """True when the body uses an operator whose exact lowering depends on the
    *kind* of number, so the numeric variant has to split into integral and
    fractional ones.

    Every other body keeps the single ``"numeric"`` variant it always emitted,
    which is what makes the split free for existing UDFs -- no extra plan options
    and no change in which columns match.
    """
    refined_ops = (
        ast.Div,
        ast.FloorDiv,
        ast.Pow,
        ast.BitAnd,
        ast.BitOr,
        ast.BitXor,
        ast.LShift,
        ast.RShift,
        ast.Invert,
    )
    for node in ast.walk(function_ast):
        if isinstance(node, (ast.BinOp, ast.AugAssign)) and isinstance(node.op, refined_ops):
            return True
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, refined_ops):
            return True
        # `min`/`max`/`round` are integral-only; `abs` works on either kind and so
        # needs no split.
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in ("min", "max", "round")
        ):
            return True
    return False


def _param_category_combos(function_ast: ast.FunctionDef, public_params: List[str]) -> List[dict]:
    """Per-variant maps ``{public_param_index -> category}``.

    A typed param (``def f(a: str, b: int)``) is pinned to its category; an
    untyped param is tried as every category its operators could need. To cap plan
    growth we collapse the untyped ones to a single uniform variant per category
    once too many are untyped (type your inputs to keep the matrix small), while
    keeping every typed param pinned.

    Bodies needing the integral/fractional split try three categories per untyped
    param instead of two, so the collapse kicks in one param sooner to keep 3**n
    in check.
    """
    refined = _body_needs_numeric_refinement(function_ast)
    kinds = ["integral", "fractional", "string"] if refined else ["numeric", "string"]
    max_untyped = 2 if refined else 3
    n = len(public_params)
    all_args = _positional_args(function_ast)
    public_args = all_args[len(all_args) - n :]
    candidates: List[List[str]] = []
    untyped = 0
    for arg in public_args:
        cat = _annotation_category(arg.annotation, refined=refined)
        if cat is None:
            candidates.append(list(kinds))
            untyped += 1
        else:
            candidates.append([cat])
    if untyped > max_untyped:
        # Cap the len(kinds)**untyped blow-up, but keep each typed param pinned to
        # its category (a single-element ``candidates`` entry); only the untyped
        # params collapse to one uniform variant per category.
        return [
            {i: c[0] if len(c) == 1 else fill for i, c in enumerate(candidates)} for fill in kinds
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
    list of per-option input-type categories (one per public param, e.g.
    ``"numeric"`` / ``"integral"`` / ``"string"`` -- see
    ``ResolveTranspiledPythonUDFOptions`` for the full vocabulary) -- the JVM
    picks the option whose categories match the bound column types, or falls back
    to the Python UDF when none match.
    list of the public parameter names Python forbids calling by keyword
    (positional-only) -- the caller must NOT resolve a kwarg matching one of these
    to a position, since Python itself rejects that call.
    """
    try:
        # The transpiler lowers to atomic (numeric/string/boolean/binary)
        # expressions and casts the result to the declared return type. For a
        # return type no lowering can even category-match (maps, structs,
        # datetimes, ...), that Cast either never resolves -- and because the
        # options ride along as children of TranspiledPythonUDF, an
        # unresolvable Cast fails the WHOLE query at CheckAnalysis instead of
        # falling back -- or diverges from the interpreted converter, which
        # nulls type-mismatched results. Restrict transpilation to return
        # types some lowering can match (the strict per-variant body-category
        # check lives in ``_transpile_from_ast``); everything else falls back
        # to interpreted Python.
        #
        # An array of an atomic element is allowed through: the built-in
        # CatalystTranspiler still refuses every array-typed body in its
        # per-variant check (only a NULL lowering category-matches, and NULL
        # casts to any array type cleanly), while other transpiler varieties
        # can produce full array-typed expressions whose cast to the declared
        # array type resolves (and whose results the interpreted converter
        # accepts as lists).
        if isinstance(returnType, str):
            from pyspark.sql.types import _parse_datatype_string

            returnType = _parse_datatype_string(returnType)
        atomic = (NumericType, StringType, BooleanType, BinaryType)
        if isinstance(returnType, ArrayType):
            return_supported = isinstance(returnType.elementType, atomic)
        else:
            return_supported = isinstance(returnType, atomic)
        if not return_supported:
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
            # `func` is a newer keyword on `_transpile_from_ast`; a custom
            # transpiler overriding it with the old signature would raise
            # TypeError on the keyword, which the broad except below silently
            # turns into "every option dropped". Feature-detect once so we
            # only pass it to implementations that accept it.
            accepts_func = _accepts_func_kwarg(transpiler)
            for combo in combos:
                try:
                    transpiled_column = transpiler._transpile_from_ast(
                        src,
                        ast_info,
                        function_ast,
                        public_params,
                        returnType,
                        combo,
                        **({"func": func} if accepts_func else {}),
                    )
                    if transpiled_column is not None:
                        transpiled.append(transpiled_column)
                        # A lowering may need a narrower column type than its own
                        # variant assumed (`/` only matches on int32 widths, a
                        # string repeat needs an integral count), so let it tighten
                        # what it reports. Tightening only drops the option and
                        # falls back; it can't pick a wrong one.
                        narrowed: Dict[int, str] = getattr(transpiler, "_narrowed", {})
                        input_categories.append(
                            [
                                narrowed[i]
                                if i in narrowed
                                and combo.get(i, "numeric") in _NARROWABLE_CATEGORIES
                                else combo.get(i, "numeric")
                                for i in range(len(public_params))
                            ]
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
