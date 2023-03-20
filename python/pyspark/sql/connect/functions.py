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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

import inspect
import warnings
import functools
from typing import (
    Any,
    Dict,
    TYPE_CHECKING,
    Union,
    List,
    overload,
    Optional,
    Tuple,
    Callable,
    ValuesView,
    cast,
)

import numpy as np

from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.expressions import (
    CaseWhen,
    Expression,
    LiteralExpression,
    ColumnReference,
    UnresolvedFunction,
    UnresolvedStar,
    SQLExpression,
    LambdaFunction,
    UnresolvedNamedLambdaVariable,
)
from pyspark.sql.connect.udf import _create_udf
from pyspark.sql import functions as pysparkfuncs
from pyspark.sql.types import _from_numpy_type, DataType, StructType, ArrayType, StringType

# The implementation of pandas_udf is embedded in pyspark.sql.function.pandas_udf
# for code reuse.
from pyspark.sql.functions import pandas_udf  # noqa: F401


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import (
        ColumnOrName,
        DataTypeOrString,
        UserDefinedFunctionLike,
    )
    from pyspark.sql.connect.dataframe import DataFrame


def _to_col_with_plan_id(col: str, plan_id: Optional[int]) -> Column:
    if col == "*":
        return Column(UnresolvedStar(unparsed_target=None))
    elif col.endswith(".*"):
        return Column(UnresolvedStar(unparsed_target=col))
    else:
        return Column(ColumnReference(unparsed_identifier=col, plan_id=plan_id))


def _to_col(col: "ColumnOrName") -> Column:
    assert isinstance(col, (Column, str))
    return col if isinstance(col, Column) else column(col)


def _invoke_function(name: str, *args: Union[Column, Expression]) -> Column:
    """
    Simple wrapper function that converts the arguments into the appropriate types.
    Parameters
    ----------
    name Name of the function to be called.
    args The list of arguments.

    Returns
    -------
    :class:`Column`
    """
    expressions: List[Expression] = []
    for arg in args:
        assert isinstance(arg, (Column, Expression))
        if isinstance(arg, Column):
            expressions.append(arg._expr)
        else:
            expressions.append(arg)
    return Column(UnresolvedFunction(name, expressions))


def _invoke_function_over_columns(name: str, *cols: "ColumnOrName") -> Column:
    """
    Invokes n-ary function identified by name
    and wraps the result with :class:`~pyspark.sql.Column`.
    """
    _cols = [_to_col(c) for c in cols]
    return _invoke_function(name, *_cols)


def _invoke_binary_math_function(name: str, col1: Any, col2: Any) -> Column:
    """
    Invokes binary math function identified by name
    and wraps the result with :class:`~pyspark.sql.Column`.
    """

    # For legacy reasons, the arguments here can be implicitly converted into column
    _cols = [_to_col(c) if isinstance(c, (str, Column)) else lit(c) for c in (col1, col2)]
    return _invoke_function(name, *_cols)


def _get_lambda_parameters(f: Callable) -> ValuesView[inspect.Parameter]:
    signature = inspect.signature(f)
    parameters = signature.parameters.values()

    # We should exclude functions that use, variable args and keyword argument
    # names, as well as keyword only args.
    supported_parameter_types = {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
    }

    # Validate that the function arity is between 1 and 3.
    if not (1 <= len(parameters) <= 3):
        raise PySparkValueError(
            error_class="WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION",
            message_parameters={"func_name": f.__name__, "num_args": str(len(parameters))},
        )

    # Verify that all arguments can be used as positional arguments.
    if not all(p.kind in supported_parameter_types for p in parameters):
        raise PySparkValueError(
            error_class="UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION",
            message_parameters={"func_name": f.__name__},
        )

    return parameters


def _create_lambda(f: Callable) -> LambdaFunction:
    """
    Create `o.a.s.sql.expressions.LambdaFunction` corresponding
    to transformation described by f

    :param f: A Python of one of the following forms:
            - (Column) -> Column: ...
            - (Column, Column) -> Column: ...
            - (Column, Column, Column) -> Column: ...
    """
    parameters = _get_lambda_parameters(f)

    arg_names = ["x", "y", "z"][: len(parameters)]
    arg_exprs = [
        UnresolvedNamedLambdaVariable([UnresolvedNamedLambdaVariable.fresh_var_name(arg_name)])
        for arg_name in arg_names
    ]
    arg_cols = [Column(arg_expr) for arg_expr in arg_exprs]

    result = f(*arg_cols)

    if not isinstance(result, Column):
        raise PySparkValueError(
            error_class="HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN",
            message_parameters={"func_name": f.__name__, "return_type": type(result).__name__},
        )

    return LambdaFunction(result._expr, arg_exprs)


def _invoke_higher_order_function(
    name: str,
    cols: List["ColumnOrName"],
    funs: List[Callable],
) -> Column:
    """
    Invokes expression identified by name,
    (relative to ```org.apache.spark.sql.catalyst.expressions``)
    and wraps the result with Column (first Scala one, then Python).

    :param name: Name of the expression
    :param cols: a list of columns
    :param funs: a list of((*Column) -> Column functions.

    :return: a Column
    """
    _cols = [_to_col(c) for c in cols]
    _funs = [_create_lambda(f) for f in funs]

    return _invoke_function(name, *_cols, *_funs)


def _options_to_col(options: Dict[str, Any]) -> Column:
    _options: List[Column] = []
    for k, v in options.items():
        _options.append(lit(str(k)))
        _options.append(lit(str(v)))
    return create_map(*_options)


# Normal Functions


def col(col: str) -> Column:
    return _to_col_with_plan_id(col=col, plan_id=None)


col.__doc__ = pysparkfuncs.col.__doc__


column = col


def lit(col: Any) -> Column:
    if isinstance(col, Column):
        return col
    elif isinstance(col, list):
        if any(isinstance(c, Column) for c in col):
            raise PySparkValueError(
                error_class="COLUMN_IN_LIST", message_parameters={"func_name": "lit"}
            )
        return array(*[lit(c) for c in col])
    elif isinstance(col, np.ndarray) and col.ndim == 1:
        if _from_numpy_type(col.dtype) is None:
            raise PySparkTypeError(
                error_class="UNSUPPORTED_NUMPY_ARRAY_SCALAR",
                message_parameters={"dtype": col.dtype.name},
            )

        # NumpyArrayConverter for Py4J can not support ndarray with int8 values.
        # Actually this is not a problem for Connect, but here still convert it
        # to int16 for compatibility.
        if col.dtype == np.int8:
            col = col.astype(np.int16)

        return array(*[lit(c) for c in col])
    else:
        return Column(LiteralExpression._from_value(col))


lit.__doc__ = pysparkfuncs.lit.__doc__


def bitwiseNOT(col: "ColumnOrName") -> Column:
    warnings.warn("Deprecated in 3.4, use bitwise_not instead.", FutureWarning)
    return bitwise_not(col)


bitwiseNOT.__doc__ = pysparkfuncs.bitwiseNOT.__doc__


def bitwise_not(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("~", col)


bitwise_not.__doc__ = pysparkfuncs.bitwise_not.__doc__


def broadcast(df: "DataFrame") -> "DataFrame":
    from pyspark.sql.connect.dataframe import DataFrame

    if not isinstance(df, DataFrame):
        raise PySparkTypeError(
            error_class="NOT_DATAFRAME",
            message_parameters={"arg_name": "df", "arg_type": type(df).__name__},
        )
    return df.hint("broadcast")


broadcast.__doc__ = pysparkfuncs.broadcast.__doc__


def coalesce(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("coalesce", *cols)


coalesce.__doc__ = pysparkfuncs.coalesce.__doc__


def expr(str: str) -> Column:
    return Column(SQLExpression(str))


expr.__doc__ = pysparkfuncs.expr.__doc__


def greatest(*cols: "ColumnOrName") -> Column:
    if len(cols) < 2:
        raise PySparkValueError(
            error_class="WRONG_NUM_COLUMNS",
            message_parameters={"func_name": "greatest", "num_cols": "2"},
        )
    return _invoke_function_over_columns("greatest", *cols)


greatest.__doc__ = pysparkfuncs.greatest.__doc__


def input_file_name() -> Column:
    return _invoke_function("input_file_name")


input_file_name.__doc__ = pysparkfuncs.input_file_name.__doc__


def least(*cols: "ColumnOrName") -> Column:
    if len(cols) < 2:
        raise PySparkValueError(
            error_class="WRONG_NUM_COLUMNS",
            message_parameters={"func_name": "least", "num_cols": "2"},
        )
    return _invoke_function_over_columns("least", *cols)


least.__doc__ = pysparkfuncs.least.__doc__


def isnan(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("isnan", col)


isnan.__doc__ = pysparkfuncs.isnan.__doc__


def isnull(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("isnull", col)


isnull.__doc__ = pysparkfuncs.isnull.__doc__


def monotonically_increasing_id() -> Column:
    return _invoke_function("monotonically_increasing_id")


monotonically_increasing_id.__doc__ = pysparkfuncs.monotonically_increasing_id.__doc__


def nanvl(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("nanvl", col1, col2)


nanvl.__doc__ = pysparkfuncs.nanvl.__doc__


def rand(seed: Optional[int] = None) -> Column:
    if seed is not None:
        return _invoke_function("rand", lit(seed))
    else:
        return _invoke_function("rand")


rand.__doc__ = pysparkfuncs.rand.__doc__


def randn(seed: Optional[int] = None) -> Column:
    if seed is not None:
        return _invoke_function("randn", lit(seed))
    else:
        return _invoke_function("randn")


randn.__doc__ = pysparkfuncs.randn.__doc__


def spark_partition_id() -> Column:
    return _invoke_function("spark_partition_id")


spark_partition_id.__doc__ = pysparkfuncs.spark_partition_id.__doc__


def when(condition: Column, value: Any) -> Column:
    # Explicitly not using ColumnOrName type here to make reading condition less opaque
    if not isinstance(condition, Column):
        raise PySparkTypeError(
            error_class="NOT_COLUMN",
            message_parameters={"arg_name": "condition", "arg_type": type(condition).__name__},
        )

    value_col = value if isinstance(value, Column) else lit(value)

    return Column(CaseWhen(branches=[(condition._expr, value_col._expr)], else_value=None))


when.__doc__ = pysparkfuncs.when.__doc__


# Sort Functions


def asc(col: "ColumnOrName") -> Column:
    return _to_col(col).asc()


asc.__doc__ = pysparkfuncs.asc.__doc__


def asc_nulls_first(col: "ColumnOrName") -> Column:
    return _to_col(col).asc_nulls_first()


asc_nulls_first.__doc__ = pysparkfuncs.asc_nulls_first.__doc__


def asc_nulls_last(col: "ColumnOrName") -> Column:
    return _to_col(col).asc_nulls_last()


asc_nulls_last.__doc__ = pysparkfuncs.asc_nulls_last.__doc__


def desc(col: "ColumnOrName") -> Column:
    return _to_col(col).desc()


desc.__doc__ = pysparkfuncs.desc.__doc__


def desc_nulls_first(col: "ColumnOrName") -> Column:
    return _to_col(col).desc_nulls_first()


desc_nulls_first.__doc__ = pysparkfuncs.desc_nulls_first.__doc__


def desc_nulls_last(col: "ColumnOrName") -> Column:
    return _to_col(col).desc_nulls_last()


desc_nulls_last.__doc__ = pysparkfuncs.desc_nulls_last.__doc__


# Math Functions


def abs(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("abs", col)


abs.__doc__ = pysparkfuncs.abs.__doc__


def acos(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("acos", col)


acos.__doc__ = pysparkfuncs.acos.__doc__


def acosh(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("acosh", col)


acosh.__doc__ = pysparkfuncs.acosh.__doc__


def asin(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("asin", col)


asin.__doc__ = pysparkfuncs.asin.__doc__


def asinh(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("asinh", col)


asinh.__doc__ = pysparkfuncs.asinh.__doc__


def atan(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("atan", col)


atan.__doc__ = pysparkfuncs.atan.__doc__


def atan2(col1: Union["ColumnOrName", float], col2: Union["ColumnOrName", float]) -> Column:
    return _invoke_binary_math_function("atan2", col1, col2)


atan2.__doc__ = pysparkfuncs.atan2.__doc__


def atanh(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("atanh", col)


atanh.__doc__ = pysparkfuncs.atanh.__doc__


def bin(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bin", col)


bin.__doc__ = pysparkfuncs.bin.__doc__


def bround(col: "ColumnOrName", scale: int = 0) -> Column:
    return _invoke_function("bround", _to_col(col), lit(scale))


bround.__doc__ = pysparkfuncs.bround.__doc__


def cbrt(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("cbrt", col)


cbrt.__doc__ = pysparkfuncs.cbrt.__doc__


def ceil(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("ceil", col)


ceil.__doc__ = pysparkfuncs.ceil.__doc__


def conv(col: "ColumnOrName", fromBase: int, toBase: int) -> Column:
    return _invoke_function("conv", _to_col(col), lit(fromBase), lit(toBase))


conv.__doc__ = pysparkfuncs.conv.__doc__


def cos(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("cos", col)


cos.__doc__ = pysparkfuncs.cos.__doc__


def cosh(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("cosh", col)


cosh.__doc__ = pysparkfuncs.cosh.__doc__


def cot(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("cot", col)


cot.__doc__ = pysparkfuncs.cot.__doc__


def csc(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("csc", col)


csc.__doc__ = pysparkfuncs.csc.__doc__


def degrees(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("degrees", col)


degrees.__doc__ = pysparkfuncs.degrees.__doc__


def exp(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("exp", col)


exp.__doc__ = pysparkfuncs.exp.__doc__


def expm1(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("expm1", col)


expm1.__doc__ = pysparkfuncs.expm1.__doc__


def factorial(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("factorial", col)


factorial.__doc__ = pysparkfuncs.factorial.__doc__


def floor(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("floor", col)


floor.__doc__ = pysparkfuncs.floor.__doc__


def hex(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("hex", col)


hex.__doc__ = pysparkfuncs.hex.__doc__


def hypot(col1: Union["ColumnOrName", float], col2: Union["ColumnOrName", float]) -> Column:
    return _invoke_binary_math_function("hypot", col1, col2)


hypot.__doc__ = pysparkfuncs.hypot.__doc__


def log(arg1: Union["ColumnOrName", float], arg2: Optional["ColumnOrName"] = None) -> Column:
    if arg2 is None:
        # in this case, arg1 should be "ColumnOrName"
        return _invoke_function("ln", _to_col(cast("ColumnOrName", arg1)))
    else:
        # in this case, arg1 should be a float
        return _invoke_function("log", lit(cast(float, arg1)), _to_col(arg2))


log.__doc__ = pysparkfuncs.log.__doc__


def log10(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("log10", col)


log10.__doc__ = pysparkfuncs.log10.__doc__


def log1p(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("log1p", col)


log1p.__doc__ = pysparkfuncs.log1p.__doc__


def log2(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("log2", col)


log2.__doc__ = pysparkfuncs.log2.__doc__


def pmod(dividend: Union["ColumnOrName", float], divisor: Union["ColumnOrName", float]) -> Column:
    return _invoke_binary_math_function("pmod", dividend, divisor)


pmod.__doc__ = pysparkfuncs.pmod.__doc__


def pow(col1: Union["ColumnOrName", float], col2: Union["ColumnOrName", float]) -> Column:
    return _invoke_binary_math_function("power", col1, col2)


pow.__doc__ = pysparkfuncs.pow.__doc__


def radians(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("radians", col)


radians.__doc__ = pysparkfuncs.radians.__doc__


def rint(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("rint", col)


rint.__doc__ = pysparkfuncs.rint.__doc__


def round(col: "ColumnOrName", scale: int = 0) -> Column:
    return _invoke_function("round", _to_col(col), lit(scale))


round.__doc__ = pysparkfuncs.round.__doc__


def sec(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("sec", col)


sec.__doc__ = pysparkfuncs.sec.__doc__


def shiftLeft(col: "ColumnOrName", numBits: int) -> Column:
    warnings.warn("Deprecated in 3.4, use shiftleft instead.", FutureWarning)
    return shiftleft(col, numBits)


shiftLeft.__doc__ = pysparkfuncs.shiftLeft.__doc__


def shiftleft(col: "ColumnOrName", numBits: int) -> Column:
    return _invoke_function("shiftleft", _to_col(col), lit(numBits))


shiftleft.__doc__ = pysparkfuncs.shiftleft.__doc__


def shiftRight(col: "ColumnOrName", numBits: int) -> Column:
    warnings.warn("Deprecated in 3.4, use shiftright instead.", FutureWarning)
    return shiftright(col, numBits)


shiftRight.__doc__ = pysparkfuncs.shiftRight.__doc__


def shiftright(col: "ColumnOrName", numBits: int) -> Column:
    return _invoke_function("shiftright", _to_col(col), lit(numBits))


shiftright.__doc__ = pysparkfuncs.shiftright.__doc__


def shiftRightUnsigned(col: "ColumnOrName", numBits: int) -> Column:
    warnings.warn("Deprecated in 3.4, use shiftrightunsigned instead.", FutureWarning)
    return shiftrightunsigned(col, numBits)


shiftRightUnsigned.__doc__ = pysparkfuncs.shiftRightUnsigned.__doc__


def shiftrightunsigned(col: "ColumnOrName", numBits: int) -> Column:
    return _invoke_function("shiftrightunsigned", _to_col(col), lit(numBits))


shiftrightunsigned.__doc__ = pysparkfuncs.shiftrightunsigned.__doc__


def signum(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("signum", col)


signum.__doc__ = pysparkfuncs.signum.__doc__


def sin(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("sin", col)


sin.__doc__ = pysparkfuncs.sin.__doc__


def sinh(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("sinh", col)


sinh.__doc__ = pysparkfuncs.sinh.__doc__


def sqrt(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("sqrt", col)


sqrt.__doc__ = pysparkfuncs.sqrt.__doc__


def tan(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("tan", col)


tan.__doc__ = pysparkfuncs.tan.__doc__


def tanh(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("tanh", col)


tanh.__doc__ = pysparkfuncs.tanh.__doc__


def toDegrees(col: "ColumnOrName") -> Column:
    warnings.warn("Deprecated in 3.4, use degrees instead.", FutureWarning)
    return degrees(col)


toDegrees.__doc__ = pysparkfuncs.toDegrees.__doc__


def toRadians(col: "ColumnOrName") -> Column:
    warnings.warn("Deprecated in 3.4, use radians instead.", FutureWarning)
    return radians(col)


toRadians.__doc__ = pysparkfuncs.toRadians.__doc__


def unhex(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("unhex", col)


unhex.__doc__ = pysparkfuncs.unhex.__doc__


def approxCountDistinct(col: "ColumnOrName", rsd: Optional[float] = None) -> Column:
    warnings.warn("Deprecated in 3.4, use approx_count_distinct instead.", FutureWarning)
    return approx_count_distinct(col, rsd)


approxCountDistinct.__doc__ = pysparkfuncs.approxCountDistinct.__doc__


def approx_count_distinct(col: "ColumnOrName", rsd: Optional[float] = None) -> Column:
    if rsd is None:
        return _invoke_function("approx_count_distinct", _to_col(col))
    else:
        return _invoke_function("approx_count_distinct", _to_col(col), lit(rsd))


approx_count_distinct.__doc__ = pysparkfuncs.approx_count_distinct.__doc__


def avg(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("avg", col)


avg.__doc__ = pysparkfuncs.avg.__doc__


def collect_list(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("collect_list", col)


collect_list.__doc__ = pysparkfuncs.collect_list.__doc__


def collect_set(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("collect_set", col)


collect_set.__doc__ = pysparkfuncs.collect_set.__doc__


def corr(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("corr", col1, col2)


corr.__doc__ = pysparkfuncs.corr.__doc__


def count(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("count", col)


count.__doc__ = pysparkfuncs.count.__doc__


def countDistinct(col: "ColumnOrName", *cols: "ColumnOrName") -> Column:
    return count_distinct(col, *cols)


countDistinct.__doc__ = pysparkfuncs.countDistinct.__doc__


def count_distinct(col: "ColumnOrName", *cols: "ColumnOrName") -> Column:
    _exprs = [_to_col(c)._expr for c in [col] + list(cols)]
    return Column(UnresolvedFunction("count", _exprs, is_distinct=True))


count_distinct.__doc__ = pysparkfuncs.count_distinct.__doc__


def covar_pop(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("covar_pop", col1, col2)


covar_pop.__doc__ = pysparkfuncs.covar_pop.__doc__


def covar_samp(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("covar_samp", col1, col2)


covar_samp.__doc__ = pysparkfuncs.covar_samp.__doc__


def first(col: "ColumnOrName", ignorenulls: bool = False) -> Column:
    return _invoke_function("first", _to_col(col), lit(ignorenulls))


first.__doc__ = pysparkfuncs.first.__doc__


def grouping(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("grouping", col)


grouping.__doc__ = pysparkfuncs.grouping.__doc__


def grouping_id(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("grouping_id", *cols)


grouping_id.__doc__ = pysparkfuncs.grouping_id.__doc__


def kurtosis(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("kurtosis", col)


kurtosis.__doc__ = pysparkfuncs.kurtosis.__doc__


def last(col: "ColumnOrName", ignorenulls: bool = False) -> Column:
    return _invoke_function("last", _to_col(col), lit(ignorenulls))


last.__doc__ = pysparkfuncs.last.__doc__


def max(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("max", col)


max.__doc__ = pysparkfuncs.max.__doc__


def max_by(col: "ColumnOrName", ord: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("max_by", col, ord)


max_by.__doc__ = pysparkfuncs.max_by.__doc__


def mean(col: "ColumnOrName") -> Column:
    return avg(col)


mean.__doc__ = pysparkfuncs.mean.__doc__


def median(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("median", col)


median.__doc__ = pysparkfuncs.median.__doc__


def min(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("min", col)


min.__doc__ = pysparkfuncs.min.__doc__


def min_by(col: "ColumnOrName", ord: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("min_by", col, ord)


min_by.__doc__ = pysparkfuncs.min_by.__doc__


def mode(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("mode", col)


mode.__doc__ = pysparkfuncs.mode.__doc__


def percentile_approx(
    col: "ColumnOrName",
    percentage: Union[Column, float, List[float], Tuple[float]],
    accuracy: Union[Column, float] = 10000,
) -> Column:
    if isinstance(percentage, Column):
        percentage_col = percentage
    elif isinstance(percentage, (list, tuple)):
        # Convert tuple to list
        percentage_col = lit(list(percentage))
    else:
        # Probably scalar
        percentage_col = lit(percentage)

    return _invoke_function("percentile_approx", _to_col(col), percentage_col, lit(accuracy))


percentile_approx.__doc__ = pysparkfuncs.percentile_approx.__doc__


def product(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("product", col)


product.__doc__ = pysparkfuncs.product.__doc__


def skewness(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("skewness", col)


skewness.__doc__ = pysparkfuncs.skewness.__doc__


def stddev(col: "ColumnOrName") -> Column:
    return stddev_samp(col)


stddev.__doc__ = pysparkfuncs.stddev.__doc__


def stddev_samp(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("stddev_samp", col)


stddev_samp.__doc__ = pysparkfuncs.stddev_samp.__doc__


def stddev_pop(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("stddev_pop", col)


stddev_pop.__doc__ = pysparkfuncs.stddev_pop.__doc__


def sum(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("sum", col)


sum.__doc__ = pysparkfuncs.sum.__doc__


def sumDistinct(col: "ColumnOrName") -> Column:
    warnings.warn("Deprecated in 3.4, use sum_distinct instead.", FutureWarning)
    return sum_distinct(col)


sumDistinct.__doc__ = pysparkfuncs.sumDistinct.__doc__


def sum_distinct(col: "ColumnOrName") -> Column:
    return Column(UnresolvedFunction("sum", [_to_col(col)._expr], is_distinct=True))


sum_distinct.__doc__ = pysparkfuncs.sum_distinct.__doc__


def var_pop(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("var_pop", col)


var_pop.__doc__ = pysparkfuncs.var_pop.__doc__


def var_samp(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("var_samp", col)


var_samp.__doc__ = pysparkfuncs.var_samp.__doc__


def variance(col: "ColumnOrName") -> Column:
    return var_samp(col)


variance.__doc__ = pysparkfuncs.variance.__doc__


# Window Functions


def cume_dist() -> Column:
    return _invoke_function("cume_dist")


cume_dist.__doc__ = pysparkfuncs.cume_dist.__doc__


def dense_rank() -> Column:
    return _invoke_function("dense_rank")


dense_rank.__doc__ = pysparkfuncs.dense_rank.__doc__


def lag(col: "ColumnOrName", offset: int = 1, default: Optional[Any] = None) -> Column:
    if default is None:
        return _invoke_function("lag", _to_col(col), lit(offset))
    else:
        return _invoke_function("lag", _to_col(col), lit(offset), lit(default))


lag.__doc__ = pysparkfuncs.lag.__doc__


def lead(col: "ColumnOrName", offset: int = 1, default: Optional[Any] = None) -> Column:
    if default is None:
        return _invoke_function("lead", _to_col(col), lit(offset))
    else:
        return _invoke_function("lead", _to_col(col), lit(offset), lit(default))


lead.__doc__ = pysparkfuncs.lead.__doc__


def nth_value(col: "ColumnOrName", offset: int, ignoreNulls: Optional[bool] = None) -> Column:
    if ignoreNulls is None:
        return _invoke_function("nth_value", _to_col(col), lit(offset))
    else:
        return _invoke_function("nth_value", _to_col(col), lit(offset), lit(ignoreNulls))


nth_value.__doc__ = pysparkfuncs.nth_value.__doc__


def ntile(n: int) -> Column:
    return _invoke_function("ntile", lit(n))


ntile.__doc__ = pysparkfuncs.ntile.__doc__


def percent_rank() -> Column:
    return _invoke_function("percent_rank")


percent_rank.__doc__ = pysparkfuncs.percent_rank.__doc__


def rank() -> Column:
    return _invoke_function("rank")


rank.__doc__ = pysparkfuncs.rank.__doc__


def row_number() -> Column:
    return _invoke_function("row_number")


row_number.__doc__ = pysparkfuncs.row_number.__doc__


def aggregate(
    col: "ColumnOrName",
    initialValue: "ColumnOrName",
    merge: Callable[[Column, Column], Column],
    finish: Optional[Callable[[Column], Column]] = None,
) -> Column:
    if finish is not None:
        return _invoke_higher_order_function("aggregate", [col, initialValue], [merge, finish])

    else:
        return _invoke_higher_order_function("aggregate", [col, initialValue], [merge])


aggregate.__doc__ = pysparkfuncs.aggregate.__doc__


def array(*cols: Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]) -> Column:
    if len(cols) == 1 and isinstance(cols[0], (list, set, tuple)):
        cols = cols[0]  # type: ignore[assignment]
    return _invoke_function_over_columns("array", *cols)  # type: ignore[arg-type]


array.__doc__ = pysparkfuncs.array.__doc__


def array_append(col: "ColumnOrName", value: Any) -> Column:
    return _invoke_function("array_append", _to_col(col), lit(value))


array_append.__doc__ = pysparkfuncs.array_append.__doc__


def array_contains(col: "ColumnOrName", value: Any) -> Column:
    return _invoke_function("array_contains", _to_col(col), lit(value))


array_contains.__doc__ = pysparkfuncs.array_contains.__doc__


def array_distinct(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("array_distinct", col)


array_distinct.__doc__ = pysparkfuncs.array_distinct.__doc__


def array_except(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("array_except", col1, col2)


array_except.__doc__ = pysparkfuncs.array_except.__doc__


def array_insert(arr: "ColumnOrName", pos: Union["ColumnOrName", int], value: Any) -> Column:
    _pos = lit(pos) if isinstance(pos, int) else _to_col(pos)
    return _invoke_function("array_insert", _to_col(arr), _pos, lit(value))


array_insert.__doc__ = pysparkfuncs.array_insert.__doc__


def array_intersect(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("array_intersect", col1, col2)


array_intersect.__doc__ = pysparkfuncs.array_intersect.__doc__


def array_compact(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("array_compact", col)


array_compact.__doc__ = pysparkfuncs.array_compact.__doc__


def array_join(
    col: "ColumnOrName", delimiter: str, null_replacement: Optional[str] = None
) -> Column:
    if null_replacement is None:
        return _invoke_function("array_join", _to_col(col), lit(delimiter))
    else:
        return _invoke_function("array_join", _to_col(col), lit(delimiter), lit(null_replacement))


array_join.__doc__ = pysparkfuncs.array_join.__doc__


def array_max(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("array_max", col)


array_max.__doc__ = pysparkfuncs.array_max.__doc__


def array_min(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("array_min", col)


array_min.__doc__ = pysparkfuncs.array_min.__doc__


def array_position(col: "ColumnOrName", value: Any) -> Column:
    return _invoke_function("array_position", _to_col(col), lit(value))


array_position.__doc__ = pysparkfuncs.array_position.__doc__


def array_remove(col: "ColumnOrName", element: Any) -> Column:
    return _invoke_function("array_remove", _to_col(col), lit(element))


array_remove.__doc__ = pysparkfuncs.array_remove.__doc__


def array_repeat(col: "ColumnOrName", count: Union["ColumnOrName", int]) -> Column:
    _count = lit(count) if isinstance(count, int) else _to_col(count)
    return _invoke_function("array_repeat", _to_col(col), _count)


array_repeat.__doc__ = pysparkfuncs.array_repeat.__doc__


def array_sort(
    col: "ColumnOrName", comparator: Optional[Callable[[Column, Column], Column]] = None
) -> Column:
    if comparator is None:
        return _invoke_function_over_columns("array_sort", col)
    else:
        return _invoke_higher_order_function("array_sort", [col], [comparator])


array_sort.__doc__ = pysparkfuncs.array_sort.__doc__


def array_union(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("array_union", col1, col2)


array_union.__doc__ = pysparkfuncs.array_union.__doc__


def arrays_overlap(a1: "ColumnOrName", a2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("arrays_overlap", a1, a2)


arrays_overlap.__doc__ = pysparkfuncs.arrays_overlap.__doc__


def arrays_zip(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("arrays_zip", *cols)


arrays_zip.__doc__ = pysparkfuncs.arrays_zip.__doc__


def concat(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("concat", *cols)


concat.__doc__ = pysparkfuncs.concat.__doc__


def create_map(
    *cols: Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]
) -> Column:
    if len(cols) == 1 and isinstance(cols[0], (list, set, tuple)):
        cols = cols[0]  # type: ignore[assignment]
    return _invoke_function_over_columns("map", *cols)  # type: ignore[arg-type]


create_map.__doc__ = pysparkfuncs.create_map.__doc__


def element_at(col: "ColumnOrName", extraction: Any) -> Column:
    return _invoke_function("element_at", _to_col(col), lit(extraction))


element_at.__doc__ = pysparkfuncs.element_at.__doc__


def exists(col: "ColumnOrName", f: Callable[[Column], Column]) -> Column:
    return _invoke_higher_order_function("exists", [col], [f])


exists.__doc__ = pysparkfuncs.exists.__doc__


def explode(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("explode", col)


explode.__doc__ = pysparkfuncs.explode.__doc__


def explode_outer(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("explode_outer", col)


explode_outer.__doc__ = pysparkfuncs.explode_outer.__doc__


def filter(
    col: "ColumnOrName",
    f: Union[Callable[[Column], Column], Callable[[Column, Column], Column]],
) -> Column:
    return _invoke_higher_order_function("filter", [col], [f])


filter.__doc__ = pysparkfuncs.filter.__doc__


def flatten(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("flatten", col)


flatten.__doc__ = pysparkfuncs.flatten.__doc__


def forall(col: "ColumnOrName", f: Callable[[Column], Column]) -> Column:
    return _invoke_higher_order_function("forall", [col], [f])


forall.__doc__ = pysparkfuncs.forall.__doc__


# TODO: support options
def from_csv(
    col: "ColumnOrName",
    schema: Union[Column, str],
    options: Optional[Dict[str, str]] = None,
) -> Column:
    if isinstance(schema, Column):
        _schema = schema
    elif isinstance(schema, str):
        _schema = lit(schema)
    else:
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_STR",
            message_parameters={"arg_name": "schema", "arg_type": type(schema).__name__},
        )

    if options is None:
        return _invoke_function("from_csv", _to_col(col), _schema)
    else:
        return _invoke_function("from_csv", _to_col(col), _schema, _options_to_col(options))


from_csv.__doc__ = pysparkfuncs.from_csv.__doc__


def from_json(
    col: "ColumnOrName",
    schema: Union[ArrayType, StructType, Column, str],
    options: Optional[Dict[str, str]] = None,
) -> Column:
    if isinstance(schema, Column):
        _schema = schema
    elif isinstance(schema, DataType):
        _schema = lit(schema.json())
    elif isinstance(schema, str):
        _schema = lit(schema)
    else:
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_DATATYPE_OR_STR",
            message_parameters={"arg_name": "schema", "arg_type": type(schema).__name__},
        )

    if options is None:
        return _invoke_function("from_json", _to_col(col), _schema)
    else:
        return _invoke_function("from_json", _to_col(col), _schema, _options_to_col(options))


from_json.__doc__ = pysparkfuncs.from_json.__doc__


def get(col: "ColumnOrName", index: Union["ColumnOrName", int]) -> Column:
    index = lit(index) if isinstance(index, int) else index

    return _invoke_function_over_columns("get", col, index)


get.__doc__ = pysparkfuncs.get.__doc__


def get_json_object(col: "ColumnOrName", path: str) -> Column:
    return _invoke_function("get_json_object", _to_col(col), lit(path))


get_json_object.__doc__ = pysparkfuncs.get_json_object.__doc__


def inline(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("inline", col)


inline.__doc__ = pysparkfuncs.inline.__doc__


def inline_outer(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("inline_outer", col)


inline_outer.__doc__ = pysparkfuncs.inline_outer.__doc__


def json_tuple(col: "ColumnOrName", *fields: str) -> Column:
    return _invoke_function("json_tuple", _to_col(col), *[lit(field) for field in fields])


json_tuple.__doc__ = pysparkfuncs.json_tuple.__doc__


def map_concat(
    *cols: Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]
) -> Column:
    if len(cols) == 1 and isinstance(cols[0], (list, set, tuple)):
        cols = cols[0]  # type: ignore[assignment]
    return _invoke_function_over_columns("map_concat", *cols)  # type: ignore[arg-type]


map_concat.__doc__ = pysparkfuncs.map_concat.__doc__


def map_contains_key(col: "ColumnOrName", value: Any) -> Column:
    return array_contains(map_keys(col), lit(value))


map_contains_key.__doc__ = pysparkfuncs.map_contains_key.__doc__


def map_entries(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("map_entries", col)


map_entries.__doc__ = pysparkfuncs.map_entries.__doc__


def map_filter(col: "ColumnOrName", f: Callable[[Column, Column], Column]) -> Column:
    return _invoke_higher_order_function("map_filter", [col], [f])


map_filter.__doc__ = pysparkfuncs.map_filter.__doc__


def map_from_arrays(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("map_from_arrays", col1, col2)


map_from_arrays.__doc__ = pysparkfuncs.map_from_arrays.__doc__


def map_from_entries(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("map_from_entries", col)


map_from_entries.__doc__ = pysparkfuncs.map_from_entries.__doc__


def map_keys(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("map_keys", col)


map_keys.__doc__ = pysparkfuncs.map_keys.__doc__


def map_values(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("map_values", col)


map_values.__doc__ = pysparkfuncs.map_values.__doc__


def map_zip_with(
    col1: "ColumnOrName",
    col2: "ColumnOrName",
    f: Callable[[Column, Column, Column], Column],
) -> Column:
    return _invoke_higher_order_function("map_zip_with", [col1, col2], [f])


map_zip_with.__doc__ = pysparkfuncs.map_zip_with.__doc__


def posexplode(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("posexplode", col)


posexplode.__doc__ = pysparkfuncs.posexplode.__doc__


def posexplode_outer(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("posexplode_outer", col)


posexplode_outer.__doc__ = pysparkfuncs.posexplode_outer.__doc__


def reverse(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("reverse", col)


reverse.__doc__ = pysparkfuncs.reverse.__doc__


def sequence(
    start: "ColumnOrName", stop: "ColumnOrName", step: Optional["ColumnOrName"] = None
) -> Column:
    if step is None:
        return _invoke_function_over_columns("sequence", start, stop)
    else:
        return _invoke_function_over_columns("sequence", start, stop, step)


sequence.__doc__ = pysparkfuncs.sequence.__doc__


def schema_of_csv(csv: "ColumnOrName", options: Optional[Dict[str, str]] = None) -> Column:
    if isinstance(csv, Column):
        _csv = csv
    elif isinstance(csv, str):
        _csv = lit(csv)
    else:
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_STR",
            message_parameters={"arg_name": "csv", "arg_type": type(csv).__name__},
        )

    if options is None:
        return _invoke_function("schema_of_csv", _csv)
    else:
        return _invoke_function("schema_of_csv", _csv, _options_to_col(options))


schema_of_csv.__doc__ = pysparkfuncs.schema_of_csv.__doc__


def schema_of_json(json: "ColumnOrName", options: Optional[Dict[str, str]] = None) -> Column:
    if isinstance(json, Column):
        _json = json
    elif isinstance(json, str):
        _json = lit(json)
    else:
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_STR",
            message_parameters={"arg_name": "json", "arg_type": type(json).__name__},
        )

    if options is None:
        return _invoke_function("schema_of_json", _json)
    else:
        return _invoke_function("schema_of_json", _json, _options_to_col(options))


schema_of_json.__doc__ = pysparkfuncs.schema_of_json.__doc__


def shuffle(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("shuffle", col)


shuffle.__doc__ = pysparkfuncs.shuffle.__doc__


def size(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("size", col)


size.__doc__ = pysparkfuncs.size.__doc__


def slice(
    col: "ColumnOrName", start: Union["ColumnOrName", int], length: Union["ColumnOrName", int]
) -> Column:
    if isinstance(start, (Column, str)):
        _start = start
    elif isinstance(start, int):
        _start = lit(start)
    else:
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_INT_OR_STR",
            message_parameters={"arg_name": "start", "arg_type": type(start).__name__},
        )

    if isinstance(length, (Column, str)):
        _length = length
    elif isinstance(length, int):
        _length = lit(length)
    else:
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_INT_OR_STR",
            message_parameters={"arg_name": "length", "arg_type": type(length).__name__},
        )

    return _invoke_function_over_columns("slice", col, _start, _length)


slice.__doc__ = pysparkfuncs.slice.__doc__


def sort_array(col: "ColumnOrName", asc: bool = True) -> Column:
    return _invoke_function("sort_array", _to_col(col), lit(asc))


sort_array.__doc__ = pysparkfuncs.sort_array.__doc__


def struct(
    *cols: Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]
) -> Column:
    if len(cols) == 1 and isinstance(cols[0], (list, set, tuple)):
        cols = cols[0]  # type: ignore[assignment]
    return _invoke_function_over_columns("struct", *cols)  # type: ignore[arg-type]


struct.__doc__ = pysparkfuncs.struct.__doc__


def to_csv(col: "ColumnOrName", options: Optional[Dict[str, str]] = None) -> Column:
    if options is None:
        return _invoke_function("to_csv", _to_col(col))
    else:
        return _invoke_function("to_csv", _to_col(col), _options_to_col(options))


to_csv.__doc__ = pysparkfuncs.to_csv.__doc__


def to_json(col: "ColumnOrName", options: Optional[Dict[str, str]] = None) -> Column:
    if options is None:
        return _invoke_function("to_json", _to_col(col))
    else:
        return _invoke_function("to_json", _to_col(col), _options_to_col(options))


to_json.__doc__ = pysparkfuncs.to_json.__doc__


def transform(
    col: "ColumnOrName",
    f: Union[Callable[[Column], Column], Callable[[Column, Column], Column]],
) -> Column:
    return _invoke_higher_order_function("transform", [col], [f])


transform.__doc__ = pysparkfuncs.transform.__doc__


def transform_keys(col: "ColumnOrName", f: Callable[[Column, Column], Column]) -> Column:
    return _invoke_higher_order_function("transform_keys", [col], [f])


transform_keys.__doc__ = pysparkfuncs.transform_keys.__doc__


def transform_values(col: "ColumnOrName", f: Callable[[Column, Column], Column]) -> Column:
    return _invoke_higher_order_function("transform_values", [col], [f])


transform_values.__doc__ = pysparkfuncs.transform_values.__doc__


def zip_with(
    left: "ColumnOrName",
    right: "ColumnOrName",
    f: Callable[[Column, Column], Column],
) -> Column:
    return _invoke_higher_order_function("zip_with", [left, right], [f])


zip_with.__doc__ = pysparkfuncs.zip_with.__doc__


# String/Binary functions


def upper(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("upper", col)


upper.__doc__ = pysparkfuncs.upper.__doc__


def lower(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("lower", col)


lower.__doc__ = pysparkfuncs.lower.__doc__


def ascii(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("ascii", col)


ascii.__doc__ = pysparkfuncs.ascii.__doc__


def base64(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("base64", col)


base64.__doc__ = pysparkfuncs.base64.__doc__


def unbase64(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("unbase64", col)


unbase64.__doc__ = pysparkfuncs.unbase64.__doc__


def ltrim(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("ltrim", col)


ltrim.__doc__ = pysparkfuncs.ltrim.__doc__


def rtrim(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("rtrim", col)


rtrim.__doc__ = pysparkfuncs.rtrim.__doc__


def trim(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("trim", col)


trim.__doc__ = pysparkfuncs.trim.__doc__


def concat_ws(sep: str, *cols: "ColumnOrName") -> Column:
    return _invoke_function("concat_ws", lit(sep), *[_to_col(c) for c in cols])


concat_ws.__doc__ = pysparkfuncs.concat_ws.__doc__


def decode(col: "ColumnOrName", charset: str) -> Column:
    return _invoke_function("decode", _to_col(col), lit(charset))


decode.__doc__ = pysparkfuncs.decode.__doc__


def encode(col: "ColumnOrName", charset: str) -> Column:
    return _invoke_function("encode", _to_col(col), lit(charset))


encode.__doc__ = pysparkfuncs.encode.__doc__


def format_number(col: "ColumnOrName", d: int) -> Column:
    return _invoke_function("format_number", _to_col(col), lit(d))


format_number.__doc__ = pysparkfuncs.format_number.__doc__


def format_string(format: str, *cols: "ColumnOrName") -> Column:
    return _invoke_function("format_string", lit(format), *[_to_col(c) for c in cols])


format_string.__doc__ = pysparkfuncs.format_string.__doc__


def instr(str: "ColumnOrName", substr: str) -> Column:
    return _invoke_function("instr", _to_col(str), lit(substr))


instr.__doc__ = pysparkfuncs.instr.__doc__


def overlay(
    src: "ColumnOrName",
    replace: "ColumnOrName",
    pos: Union["ColumnOrName", int],
    len: Union["ColumnOrName", int] = -1,
) -> Column:
    if not isinstance(pos, (int, str, Column)):
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_INT_OR_STR",
            message_parameters={"arg_name": "pos", "arg_type": type(pos).__name__},
        )
    if len is not None and not isinstance(len, (int, str, Column)):
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_INT_OR_STR",
            message_parameters={"arg_name": "len", "arg_type": type(len).__name__},
        )

    if isinstance(pos, int):
        pos = lit(pos)
    if isinstance(len, int):
        len = lit(len)

    return _invoke_function_over_columns("overlay", src, replace, pos, len)


overlay.__doc__ = pysparkfuncs.overlay.__doc__


def sentences(
    string: "ColumnOrName",
    language: Optional["ColumnOrName"] = None,
    country: Optional["ColumnOrName"] = None,
) -> Column:
    _language = lit("") if language is None else _to_col(language)
    _country = lit("") if country is None else _to_col(country)

    return _invoke_function("sentences", _to_col(string), _language, _country)


sentences.__doc__ = pysparkfuncs.sentences.__doc__


def substring(str: "ColumnOrName", pos: int, len: int) -> Column:
    return _invoke_function("substring", _to_col(str), lit(pos), lit(len))


substring.__doc__ = pysparkfuncs.substring.__doc__


def substring_index(str: "ColumnOrName", delim: str, count: int) -> Column:
    return _invoke_function("substring_index", _to_col(str), lit(delim), lit(count))


substring_index.__doc__ = pysparkfuncs.substring_index.__doc__


def levenshtein(left: "ColumnOrName", right: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("levenshtein", left, right)


levenshtein.__doc__ = pysparkfuncs.levenshtein.__doc__


def locate(substr: str, str: "ColumnOrName", pos: int = 1) -> Column:
    return _invoke_function("locate", lit(substr), _to_col(str), lit(pos))


locate.__doc__ = pysparkfuncs.locate.__doc__


def lpad(col: "ColumnOrName", len: int, pad: str) -> Column:
    return _invoke_function("lpad", _to_col(col), lit(len), lit(pad))


lpad.__doc__ = pysparkfuncs.lpad.__doc__


def rpad(col: "ColumnOrName", len: int, pad: str) -> Column:
    return _invoke_function("rpad", _to_col(col), lit(len), lit(pad))


rpad.__doc__ = pysparkfuncs.rpad.__doc__


def repeat(col: "ColumnOrName", n: int) -> Column:
    return _invoke_function("repeat", _to_col(col), lit(n))


repeat.__doc__ = pysparkfuncs.repeat.__doc__


def split(str: "ColumnOrName", pattern: str, limit: int = -1) -> Column:
    return _invoke_function("split", _to_col(str), lit(pattern), lit(limit))


split.__doc__ = pysparkfuncs.split.__doc__


def regexp_extract(str: "ColumnOrName", pattern: str, idx: int) -> Column:
    return _invoke_function("regexp_extract", _to_col(str), lit(pattern), lit(idx))


regexp_extract.__doc__ = pysparkfuncs.regexp_extract.__doc__


def regexp_replace(
    string: "ColumnOrName", pattern: Union[str, Column], replacement: Union[str, Column]
) -> Column:
    if isinstance(pattern, str):
        pattern = lit(pattern)

    if isinstance(replacement, str):
        replacement = lit(replacement)

    return _invoke_function("regexp_replace", _to_col(string), pattern, replacement)


regexp_replace.__doc__ = pysparkfuncs.regexp_replace.__doc__


def initcap(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("initcap", col)


initcap.__doc__ = pysparkfuncs.initcap.__doc__


def soundex(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("soundex", col)


soundex.__doc__ = pysparkfuncs.soundex.__doc__


def length(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("length", col)


length.__doc__ = pysparkfuncs.length.__doc__


def octet_length(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("octet_length", col)


octet_length.__doc__ = pysparkfuncs.octet_length.__doc__


def bit_length(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bit_length", col)


bit_length.__doc__ = pysparkfuncs.bit_length.__doc__


def translate(srcCol: "ColumnOrName", matching: str, replace: str) -> Column:
    return _invoke_function("translate", _to_col(srcCol), lit(matching), lit(replace))


translate.__doc__ = pysparkfuncs.translate.__doc__


# Date/Timestamp functions
# TODO(SPARK-41455): Resolve dtypes inconsistencies for:
#     to_timestamp, from_utc_timestamp, to_utc_timestamp,
#     timestamp_seconds, current_timestamp, date_trunc


def current_date() -> Column:
    return _invoke_function("current_date")


current_date.__doc__ = pysparkfuncs.current_date.__doc__


def current_timestamp() -> Column:
    return _invoke_function("current_timestamp")


current_timestamp.__doc__ = pysparkfuncs.current_timestamp.__doc__


def localtimestamp() -> Column:
    return _invoke_function("localtimestamp")


localtimestamp.__doc__ = pysparkfuncs.localtimestamp.__doc__


def date_format(date: "ColumnOrName", format: str) -> Column:
    return _invoke_function("date_format", _to_col(date), lit(format))


date_format.__doc__ = pysparkfuncs.date_format.__doc__


def year(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("year", col)


year.__doc__ = pysparkfuncs.year.__doc__


def quarter(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("quarter", col)


quarter.__doc__ = pysparkfuncs.quarter.__doc__


def month(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("month", col)


month.__doc__ = pysparkfuncs.month.__doc__


def dayofweek(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("dayofweek", col)


dayofweek.__doc__ = pysparkfuncs.dayofweek.__doc__


def dayofmonth(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("dayofmonth", col)


dayofmonth.__doc__ = pysparkfuncs.dayofmonth.__doc__


def dayofyear(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("dayofyear", col)


dayofyear.__doc__ = pysparkfuncs.dayofyear.__doc__


def hour(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("hour", col)


hour.__doc__ = pysparkfuncs.hour.__doc__


def minute(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("minute", col)


minute.__doc__ = pysparkfuncs.minute.__doc__


def second(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("second", col)


second.__doc__ = pysparkfuncs.second.__doc__


def weekofyear(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("weekofyear", col)


weekofyear.__doc__ = pysparkfuncs.weekofyear.__doc__


def make_date(year: "ColumnOrName", month: "ColumnOrName", day: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("make_date", year, month, day)


make_date.__doc__ = pysparkfuncs.make_date.__doc__


def date_add(start: "ColumnOrName", days: Union["ColumnOrName", int]) -> Column:
    days = lit(days) if isinstance(days, int) else days
    return _invoke_function_over_columns("date_add", start, days)


date_add.__doc__ = pysparkfuncs.date_add.__doc__


def date_sub(start: "ColumnOrName", days: Union["ColumnOrName", int]) -> Column:
    days = lit(days) if isinstance(days, int) else days
    return _invoke_function_over_columns("date_sub", start, days)


date_sub.__doc__ = pysparkfuncs.date_sub.__doc__


def datediff(end: "ColumnOrName", start: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("datediff", end, start)


datediff.__doc__ = pysparkfuncs.datediff.__doc__


def add_months(start: "ColumnOrName", months: Union["ColumnOrName", int]) -> Column:
    months = lit(months) if isinstance(months, int) else months
    return _invoke_function_over_columns("add_months", start, months)


add_months.__doc__ = pysparkfuncs.add_months.__doc__


def months_between(date1: "ColumnOrName", date2: "ColumnOrName", roundOff: bool = True) -> Column:
    return _invoke_function("months_between", _to_col(date1), _to_col(date2), lit(roundOff))


months_between.__doc__ = pysparkfuncs.months_between.__doc__


def to_date(col: "ColumnOrName", format: Optional[str] = None) -> Column:
    if format is None:
        return _invoke_function_over_columns("to_date", col)
    else:
        return _invoke_function("to_date", _to_col(col), lit(format))


to_date.__doc__ = pysparkfuncs.to_date.__doc__


@overload
def to_timestamp(col: "ColumnOrName") -> Column:
    ...


@overload
def to_timestamp(col: "ColumnOrName", format: str) -> Column:
    ...


def to_timestamp(col: "ColumnOrName", format: Optional[str] = None) -> Column:
    if format is None:
        return _invoke_function_over_columns("to_timestamp", col)
    else:
        return _invoke_function("to_timestamp", _to_col(col), lit(format))


to_timestamp.__doc__ = pysparkfuncs.to_timestamp.__doc__


def trunc(date: "ColumnOrName", format: str) -> Column:
    return _invoke_function("trunc", _to_col(date), lit(format))


trunc.__doc__ = pysparkfuncs.trunc.__doc__


def date_trunc(format: str, timestamp: "ColumnOrName") -> Column:
    return _invoke_function("date_trunc", lit(format), _to_col(timestamp))


date_trunc.__doc__ = pysparkfuncs.date_trunc.__doc__


def next_day(date: "ColumnOrName", dayOfWeek: str) -> Column:
    return _invoke_function("next_day", _to_col(date), lit(dayOfWeek))


next_day.__doc__ = pysparkfuncs.next_day.__doc__


def last_day(date: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("last_day", date)


last_day.__doc__ = pysparkfuncs.last_day.__doc__


def from_unixtime(timestamp: "ColumnOrName", format: str = "yyyy-MM-dd HH:mm:ss") -> Column:
    return _invoke_function("from_unixtime", _to_col(timestamp), lit(format))


from_unixtime.__doc__ = pysparkfuncs.from_unixtime.__doc__


@overload
def unix_timestamp(timestamp: "ColumnOrName", format: str = ...) -> Column:
    ...


@overload
def unix_timestamp() -> Column:
    ...


def unix_timestamp(
    timestamp: Optional["ColumnOrName"] = None, format: str = "yyyy-MM-dd HH:mm:ss"
) -> Column:
    if timestamp is None:
        return _invoke_function("unix_timestamp")
    return _invoke_function("unix_timestamp", _to_col(timestamp), lit(format))


unix_timestamp.__doc__ = pysparkfuncs.unix_timestamp.__doc__


def from_utc_timestamp(timestamp: "ColumnOrName", tz: "ColumnOrName") -> Column:
    if isinstance(tz, str):
        tz = lit(tz)
    return _invoke_function_over_columns("from_utc_timestamp", timestamp, tz)


from_utc_timestamp.__doc__ = pysparkfuncs.from_utc_timestamp.__doc__


def to_utc_timestamp(timestamp: "ColumnOrName", tz: "ColumnOrName") -> Column:
    if isinstance(tz, str):
        tz = lit(tz)
    return _invoke_function_over_columns("to_utc_timestamp", timestamp, tz)


to_utc_timestamp.__doc__ = pysparkfuncs.to_utc_timestamp.__doc__


def timestamp_seconds(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("timestamp_seconds", col)


timestamp_seconds.__doc__ = pysparkfuncs.timestamp_seconds.__doc__


def window(
    timeColumn: "ColumnOrName",
    windowDuration: str,
    slideDuration: Optional[str] = None,
    startTime: Optional[str] = None,
) -> Column:
    if windowDuration is None or not isinstance(windowDuration, str):
        raise PySparkTypeError(
            error_class="NOT_STR",
            message_parameters={
                "arg_name": "windowDuration",
                "arg_type": type(windowDuration).__name__,
            },
        )
    if slideDuration is not None and not isinstance(slideDuration, str):
        raise PySparkTypeError(
            error_class="NOT_STR",
            message_parameters={
                "arg_name": "slideDuration",
                "arg_type": type(slideDuration).__name__,
            },
        )
    if startTime is not None and not isinstance(startTime, str):
        raise PySparkTypeError(
            error_class="NOT_STR",
            message_parameters={"arg_name": "startTime", "arg_type": type(startTime).__name__},
        )

    time_col = _to_col(timeColumn)

    if slideDuration is not None and startTime is not None:
        return _invoke_function(
            "window", time_col, lit(windowDuration), lit(slideDuration), lit(startTime)
        )
    elif slideDuration is not None:
        return _invoke_function("window", time_col, lit(windowDuration), lit(slideDuration))
    elif startTime is not None:
        return _invoke_function(
            "window", time_col, lit(windowDuration), lit(windowDuration), lit(startTime)
        )
    else:
        return _invoke_function("window", time_col, lit(windowDuration))


window.__doc__ = pysparkfuncs.window.__doc__


def window_time(
    windowColumn: "ColumnOrName",
) -> Column:
    return _invoke_function("window_time", _to_col(windowColumn))


window_time.__doc__ = pysparkfuncs.window_time.__doc__


def session_window(timeColumn: "ColumnOrName", gapDuration: Union[Column, str]) -> Column:
    if gapDuration is None or not isinstance(gapDuration, (Column, str)):
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_STR",
            message_parameters={"arg_name": "gapDuration", "arg_type": type(gapDuration).__name__},
        )

    time_col = _to_col(timeColumn)

    if isinstance(gapDuration, Column):
        return _invoke_function("session_window", time_col, gapDuration)
    else:
        return _invoke_function("session_window", time_col, lit(gapDuration))


session_window.__doc__ = pysparkfuncs.session_window.__doc__


# Partition Transformation Functions


def bucket(numBuckets: Union[Column, int], col: "ColumnOrName") -> Column:
    if isinstance(numBuckets, int):
        _numBuckets = lit(numBuckets)
    elif isinstance(numBuckets, Column):
        _numBuckets = numBuckets
    else:
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_INT",
            message_parameters={
                "arg_name": "numBuckets",
                "arg_type": type(numBuckets).__name__,
            },
        )

    return _invoke_function("bucket", _numBuckets, _to_col(col))


bucket.__doc__ = pysparkfuncs.bucket.__doc__


def years(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("years", col)


years.__doc__ = pysparkfuncs.years.__doc__


def months(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("months", col)


months.__doc__ = pysparkfuncs.months.__doc__


def days(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("days", col)


days.__doc__ = pysparkfuncs.days.__doc__


def hours(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("hours", col)


hours.__doc__ = pysparkfuncs.hours.__doc__

# Misc Functions


def assert_true(col: "ColumnOrName", errMsg: Optional[Union[Column, str]] = None) -> Column:
    if errMsg is None:
        return _invoke_function_over_columns("assert_true", col)
    if not isinstance(errMsg, (str, Column)):
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_STR",
            message_parameters={"arg_name": "errMsg", "arg_type": type(errMsg).__name__},
        )
    _err_msg = lit(errMsg) if isinstance(errMsg, str) else _to_col(errMsg)
    return _invoke_function("assert_true", _to_col(col), _err_msg)


assert_true.__doc__ = pysparkfuncs.assert_true.__doc__


def raise_error(errMsg: Union[Column, str]) -> Column:
    if not isinstance(errMsg, (str, Column)):
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_STR",
            message_parameters={"arg_name": "errMsg", "arg_type": type(errMsg).__name__},
        )
    _err_msg = lit(errMsg) if isinstance(errMsg, str) else _to_col(errMsg)
    return _invoke_function("raise_error", _err_msg)


raise_error.__doc__ = pysparkfuncs.raise_error.__doc__


def crc32(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("crc32", col)


crc32.__doc__ = pysparkfuncs.crc32.__doc__


def hash(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("hash", *cols)


hash.__doc__ = pysparkfuncs.hash.__doc__


def xxhash64(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("xxhash64", *cols)


xxhash64.__doc__ = pysparkfuncs.xxhash64.__doc__


def md5(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("md5", col)


md5.__doc__ = pysparkfuncs.md5.__doc__


def sha1(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("sha1", col)


sha1.__doc__ = pysparkfuncs.sha1.__doc__


def sha2(col: "ColumnOrName", numBits: int) -> Column:
    return _invoke_function("sha2", _to_col(col), lit(numBits))


sha2.__doc__ = pysparkfuncs.sha2.__doc__


# User Defined Function


def call_udf(udfName: str, *cols: "ColumnOrName") -> Column:
    return _invoke_function(udfName, *[_to_col(c) for c in cols])


call_udf.__doc__ = pysparkfuncs.call_udf.__doc__


def unwrap_udt(col: "ColumnOrName") -> Column:
    return _invoke_function("unwrap_udt", _to_col(col))


unwrap_udt.__doc__ = pysparkfuncs.unwrap_udt.__doc__


def udf(
    f: Optional[Union[Callable[..., Any], "DataTypeOrString"]] = None,
    returnType: "DataTypeOrString" = StringType(),
) -> Union["UserDefinedFunctionLike", Callable[[Callable[..., Any]], "UserDefinedFunctionLike"]]:
    from pyspark.rdd import PythonEvalType

    if f is None or isinstance(f, (str, DataType)):
        # If DataType has been passed as a positional argument
        # for decorator use it as a returnType
        return_type = f or returnType
        return functools.partial(
            _create_udf, returnType=return_type, evalType=PythonEvalType.SQL_BATCHED_UDF
        )
    else:
        return _create_udf(f=f, returnType=returnType, evalType=PythonEvalType.SQL_BATCHED_UDF)


udf.__doc__ = pysparkfuncs.udf.__doc__


def _test() -> None:
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.functions

    globs = pyspark.sql.connect.functions.__dict__.copy()

    # Spark Connect does not support Spark Context but the test depends on that.
    del pyspark.sql.connect.functions.monotonically_increasing_id.__doc__

    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.functions tests")
        .remote("local[4]")
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.functions,
        globs=globs,
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL,
    )

    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
