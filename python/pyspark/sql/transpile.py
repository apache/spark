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
from pyspark.sql.functions import abs as _abs, coalesce, col, lit, pmod, sign, when


if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql._typing import DataTypeOrString


class AbstractTranspiler(object):
    """Base class for transpilers. All experimental."""

    varieties: dict[str, type[AbstractTranspiler]] = {}
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


class CatalystTranspiler(AbstractTranspiler):
    """Transpiler that attempts to convert a Python UDF into native Spark SQL expressions."""

    variety = "catalyst"

    # TODO:
    # handle
    # def f(x):
    #     x + x
    # should return None because there is no return statement
    # (although maybe we should log since it's likely a mistake).
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
    ) -> Column:
        # Python evaluates `if test:` by treating None as falsy first
        # and then checking truthiness. Spark's `when` already routes
        # NULL into the otherwise branch, so for boolean-typed tests
        # (the common case after Compare / UnaryOp / IsNotNull) the
        # cleanest mapping is `when(test, body).otherwise(else)`. We
        # coalesce the test against `lit(False)` so that a NULL test
        # deterministically takes the else branch: the previous
        # double-wrapped form `otherwise(when(~test, else))` silently
        # produced NULL when `test` itself was NULL (since `~NULL` is
        # NULL), which doesn't match Python's `if None: ... else: ...`
        # picking the else branch. Non-boolean tests (e.g. a bare
        # `if some_int:`) still won't match Python truthiness exactly;
        # doing that needs the actual input column type, which we
        # don't currently thread through to the transpiler.
        safe_test = coalesce(test_col, lit(False))
        return when(safe_test, body_col).otherwise(else_col)

    def _convert_chunk(self, params: List[str], body: ast.AST | None) -> Column:
        match body:
            case None:
                # Special case literal None, the implicit return None
                return lit(None)
            case ast.UnaryOp(op=ast.Not(), operand=operand):
                # Python's `not None` is `True` (None is falsy), but Spark's
                # `~NULL` is `NULL`. Coalesce against `lit(True)` so a NULL
                # operand mirrors Python's "None is falsy" rule. For
                # already-boolean operands the coalesce is a no-op on
                # non-NULL values (`~True` and `~False` both stay
                # non-NULL). Non-boolean operands aren't currently
                # special-cased -- `not 0` against an int column will
                # still rely on Spark's invert behavior.
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
                # produced by Compare / UnaryOp(Not) / IsNotNull this maps
                # cleanly onto Spark Column `&` / `|`. Non-boolean
                # operands are not currently special-cased here -- if a
                # user writes `x or 0` against an int column they'll
                # get bitwise-style behaviour from Spark, not Python's
                # truthiness fallback.
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
                )
            case ast.If(test, success, orelse):
                return self._convert_if_like(
                    self._convert_chunk(params, test),
                    self._convert_branch(params, success, "body"),
                    self._convert_branch(params, orelse, "else body"),
                )
            case ast.Compare(left, ops, comps):
                if len(ops) != 1 or len(comps) != 1:
                    raise UnsupportedOperationException(
                        "chained comparisons (e.g. `a < b < c`) are not supported by the transpiler"
                    )
                left_col = self._convert_chunk(params, left)
                match ops[0]:
                    case ast.IsNot():
                        return left_col.isNotNull()
                    case ast.Is():
                        return left_col.isNull()
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
                    # TODO: Maybe use one of the try functions so we can control errors and map topython exceptional cases better.
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
                    # TODO: Handle assignments, class vars, etc.
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
        return ast.FunctionDef(
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
                if transpiled_column:
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
