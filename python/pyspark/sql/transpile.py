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
``spark.sql.experimental.optimizer.transpilePyUDFS=true`` and
``spark.sql.ansi.enabled=true``. The generated Catalyst expressions
target ANSI-mode SQL semantics (overflow raises, divide-by-zero raises,
etc.); running them under non-ANSI mode would silently diverge from the
Python interpretation in ways we don't currently track. If you flip
transpilation on with ANSI off the UDF will fall back to interpreted
Python execution and a warning is logged at UDF construction time.
"""

import ast
from typing import Any, Callable, List, Optional, Tuple, TYPE_CHECKING
import inspect
import textwrap
from pyspark.errors import UnsupportedOperationException
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    abs as _abs,
    coalesce,
    col,
    lit,
    pmod,
    raise_error,
    sign,
    when,
)


if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql._typing import DataTypeOrString


class AbstractTranspiler(object):
    """Base class for transpilers. All experimental."""

    varieties: dict[str, type["AbstractTranspiler"]] = {}
    # Specify the "friendly" name a user can add to spark.sql.experimental.optimizer.transpilers
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
    ) -> Optional[Column]:
        pass


def _is_definitely_boolean(node: ast.AST) -> bool:
    """Return True when ``node`` is statically guaranteed to produce a Python
    ``bool`` (or ``None``, which round-trips through ``coalesce``).

    Used to gate ``if``/ternary lowering: we only allow the test expression
    into Catalyst's ``when(coalesce(test, false), ...)`` form when it provably
    produces a boolean. Everything else (bare Name, arithmetic, function calls,
    subscript, …) must force a fallback to interpreted Python instead of
    silently diverging.
    """
    match node:
        case ast.Constant(value=v):
            return v is None or isinstance(v, bool)
        case ast.Compare():
            # All comparison operators produce bool.
            return True
        case ast.BoolOp():
            # and / or of booleans produces bool.
            return True
        case ast.UnaryOp(op=ast.Not()):
            # `not x` always produces bool.
            return True
        case ast.IfExp(body=body, orelse=orelse):
            # Ternary is boolean only if both branches are.
            return _is_definitely_boolean(body) and _is_definitely_boolean(orelse)
        case _:
            return False


def _is_definitely_non_boolean(node: ast.AST) -> bool:
    """Return True when ``node`` is statically guaranteed to evaluate to a
    value that is *not* a Python ``bool``.

    Used to gate the bitwise lowering of ``and`` / ``or`` / ``not``: Python's
    short-circuit operators return one of their operands rather than a strict
    bool, so ``x or 0`` against an int column would silently get
    bitwise-style behaviour from Spark's ``|`` instead of Python's truthiness
    fallback. We can't always tell statically (a bare ``ast.Name`` could be
    bound to any type), so we conservatively only refuse to lower when an
    operand is *provably* non-boolean -- numeric / string literals, an
    arithmetic ``BinOp``, a numeric ``UnaryOp(USub/UAdd)``. Everything else
    (Names, Compare, Not, nested BoolOps, IfExp, conservative cases) is
    treated as "possibly boolean" and we let the bitwise lowering proceed,
    relying on the input being a boolean column at runtime.
    """
    match node:
        case ast.Constant(value=v):
            # ``True`` and ``False`` are themselves bool; ``None`` we
            # accept (it round-trips through coalesce). Everything else
            # is definitely not bool.
            return not (v is None or isinstance(v, bool))
        case ast.BinOp(
            op=ast.Add()
            | ast.Sub()
            | ast.Mult()
            | ast.Div()
            | ast.FloorDiv()
            | ast.Mod()
            | ast.Pow()
            | ast.LShift()
            | ast.RShift()
            | ast.MatMult()
        ):
            # Arithmetic / shift BinOps produce numeric (or matrix) results,
            # never booleans, so they're provably non-boolean. Bitwise
            # ``&`` / ``|`` / ``^`` are deliberately NOT matched: they
            # produce a boolean when both operands are boolean (e.g.
            # ``(x > 0) & (y > 0)``), so leaving them in the "possibly
            # boolean" bucket lets the BoolOp / Not lowering proceed.
            return True
        case ast.UnaryOp(op=ast.USub()) | ast.UnaryOp(op=ast.UAdd()):
            return True
        case ast.IfExp(body=body, orelse=orelse):
            # Conditional only known non-boolean if both branches are.
            return _is_definitely_non_boolean(body) and _is_definitely_non_boolean(orelse)
        case ast.Call():
            # Function calls: we don't know the return type statically, so we
            # can't claim they're non-boolean. Leave as "possibly boolean" and
            # let the caller attempt lowering; if the call itself is not
            # supported the catch-all arm will raise UnsupportedOperationException.
            return False
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

    def _convert_if_like(
        self,
        test_col: Column,
        body_col: Column,
        else_col: Column,
        test_node: ast.AST,
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
                "bare truthiness tests in if-expressions are not currently "
                "supported by the transpiler"
            )
        safe_test = coalesce(test_col, lit(False))
        return when(safe_test, body_col).otherwise(else_col)

    def _lower_eq(
        self,
        params: List[str],
        left_col: Column,
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
        """
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
        left_col: Column,
        right_node: ast.AST,
        op: Callable[[Column, Column], Column],
        op_repr: str,
    ) -> Column:
        """Lower a value comparison (``<``, ``<=``, ``>``, ``>=``, ``==``, ``!=``).

        Python raises ``TypeError`` when an operand of these operators is
        ``None`` (e.g. ``None > 0``), whereas Spark's three-valued logic
        returns ``NULL``. To stay faithful to the source UDF we guard the
        comparison: if either operand is ``NULL`` we raise via
        ``raise_error``, otherwise we evaluate ``left op right`` as usual.
        Callers that have already proven the operand non-null (``if x is
        not None: x > 0``) take the otherwise branch, so they never trip
        the raise.
        """
        right_col = self._convert_chunk(params, right_node)
        null_guard = left_col.isNull() | right_col.isNull()
        err = lit(
            "Python UDF transpiler: cannot compare NULL with operator "
            f"`{op_repr}`; Python would raise TypeError here. Add an "
            "`is not None` guard or filter NULLs upstream."
        )
        return when(null_guard, raise_error(err)).otherwise(op(left_col, right_col))

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
            case ast.UnaryOp(op=ast.USub(), operand=operand):
                # `-x` -- handle both literal negative ints (USub on a
                # Constant) and runtime negation of a column.
                return self._convert_chunk(params, operand).__neg__()
            case ast.UnaryOp(op=ast.UAdd(), operand=operand):
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
                    self._convert_chunk(params, test),
                    self._convert_chunk(params, body_expr),
                    self._convert_chunk(params, orelse_expr),
                    test,
                )
            case ast.If(test, success, orelse):
                return self._convert_if_like(
                    self._convert_chunk(params, test),
                    self._convert_branch(params, success, "body"),
                    self._convert_branch(params, orelse, "else body"),
                    test,
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
                        left_col = self._convert_chunk(params, left)
                        return self._lower_eq(params, left_col, comp, equal=True)
                    case ast.NotEq():
                        left_col = self._convert_chunk(params, left)
                        return self._lower_eq(params, left_col, comp, equal=False)
                    case ast.Lt():
                        left_col = self._convert_chunk(params, left)
                        return self._lower_value_compare(
                            params, left_col, comp, lambda l, r: l < r, "<"
                        )
                    case ast.LtE():
                        left_col = self._convert_chunk(params, left)
                        return self._lower_value_compare(
                            params, left_col, comp, lambda l, r: l <= r, "<="
                        )
                    case ast.Gt():
                        left_col = self._convert_chunk(params, left)
                        return self._lower_value_compare(
                            params, left_col, comp, lambda l, r: l > r, ">"
                        )
                    case ast.GtE():
                        left_col = self._convert_chunk(params, left)
                        return self._lower_value_compare(
                            params, left_col, comp, lambda l, r: l >= r, ">="
                        )
                    case _:
                        raise UnsupportedOperationException(
                            f"comparison operator {type(ops[0]).__name__} "
                            "is not supported by the transpiler"
                        )
            case ast.BinOp(left=left, op=op, right=right):
                left_col = self._convert_chunk(params, left)
                if left_col is None:
                    raise UnsupportedOperationException(
                        "BinOp left operand could not be lowered to a Column"
                    )
                right_col = self._convert_chunk(params, right)
                if right_col is None:
                    raise UnsupportedOperationException(
                        "BinOp right operand could not be lowered to a Column"
                    )
                match op:
                    # TODO (SPARK-55210): Use try-variant functions to map Python exceptional
                    # cases (e.g. overflow, divide-by-zero) to Catalyst errors more precisely.
                    case ast.Add():
                        return left_col.__add__(right_col)
                    case ast.Sub():
                        return left_col.__sub__(right_col)
                    case ast.Mult():
                        return left_col.__mul__(right_col)
                    case ast.Mod():
                        # Python's `%` returns a result with the sign of the
                        # divisor; Spark's `%` returns the sign of the
                        # dividend, and Spark's `pmod` is documented for
                        # non-negative divisors only. The composition
                        # `sign(b) * pmod(sign(b) * a, abs(b))` reproduces
                        # Python's semantics for any non-zero divisor without
                        # us having to reach into Catalyst internals --
                        # `pmod` does the unsigned remainder, `sign` and
                        # `abs` line the inputs and output up with the
                        # divisor's sign.
                        sb = sign(right_col)
                        return sb * pmod(sb * left_col, _abs(right_col))
                    case ast.Pow():
                        return left_col.__pow__(right_col)
                    case _:
                        raise UnsupportedOperationException(
                            f"binary operator {type(op).__name__} is not "
                            "supported by the transpiler"
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
    ) -> Optional[Column]:
        # Short circuit on nothing to transpile.
        if src == "" or ast_info is None:
            return None
        function_body = function_ast.body
        if len(function_body) != 1:
            raise UnsupportedOperationException(
                "functions with more than one top-level statement are not "
                "supported by the transpiler"
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
        if hasattr(func, "__call__"):
            src = inspect.getsource(func.__call__)
            src = textwrap.dedent(src).strip()
            ast_info = ast.parse(src)
    return src, ast_info


def _get_parameter_list(node: ast.FunctionDef) -> list[str]:
    """Return the positional argument names of in order."""
    return [arg.arg for arg in node.args.args]


def _get_function_from_ast(body: ast.AST) -> ast.FunctionDef | None:
    """
    Extract a :class:`ast.FunctionDef` node from an AST produced by
    ``ast.parse(inspect.getsource(udf_func))``.

    Handles the following source patterns (in order):

    * ``f = lambda x: x + 1``  — direct assignment
    * ``f = some_wrapper(lambda x: x + 1, ...)``  — lambda as first positional
      arg of a call (e.g. ``functools.partial``)
    * ``lambda x: x + 1``  — bare expression (getsource on a raw lambda)
    * ``def f(x): ... return x + 1``
    * class with callable

    Returns ``None`` when no single unambiguous function can be identified.
    Not yet handled: local class variables.
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
) -> Tuple[List[Column], List[str], List[str]]:
    """
    An experimental internal function that attempts to transpile a callable function.

    Returns
    -------
    list of transpiled functions
    list of errors as strings
    list of positional parameter names (excluding ``self`` for callable
    instances) -- needed so the caller can resolve named-argument
    invocations to positional order at call time, since the ``_udf_param_N``
    substitution in :class:`UserDefinedPythonFunction` is positional.
    """
    try:
        src, ast = _get_src_ast_from_func(func)
        if ast is None:
            return ([], ["Error getting ast for function, can not transpile"], [])
        # Get the lambda body and parameters
        function_ast = _get_function_from_ast(ast)
        if function_ast is None:
            return ([], ["Error extracting function body from ast, can not transpile"], [])
        params = _get_parameter_list(function_ast)
        # Strip ``self`` for the caller-facing param list -- callers will
        # match user-supplied kwargs against this, and the user doesn't
        # name ``self`` at the call site.
        public_params = params[1:] if params and params[0] == "self" else list(params)
        transpiled: list[Column] = []
        errors = []
        # Maybe multiple transpilers (think CUDA, etc.).
        transpilers = _get_transpilers(session)
        for transpiler in transpilers:
            try:
                transpiled_column = transpiler._transpile_from_ast(
                    src, ast, function_ast, params, returnType
                )
                if transpiled_column is not None:
                    transpiled.append(transpiled_column)
                else:
                    errors.append(f"Transpiler {transpiler} returned no column")
            except Exception as e:
                errors.append(str(e))
        return (transpiled, errors, public_params)
    except Exception as e:
        # Don't re-raise: an inability to transpile must never break a
        # working UDF. The caller treats an empty ``transpiled`` list as a
        # silent fall-back to interpreted Python.
        return ([], [str(e)], [])
