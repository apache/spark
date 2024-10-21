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

import decimal
import inspect
import warnings
import functools
from typing import (
    Any,
    Mapping,
    TYPE_CHECKING,
    Union,
    Sequence,
    List,
    overload,
    Optional,
    Tuple,
    Type,
    Callable,
    ValuesView,
    cast,
)
import random
import sys

import numpy as np

from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.sql.dataframe import DataFrame as ParentDataFrame
from pyspark.sql import Column
from pyspark.sql.connect.expressions import (
    CaseWhen,
    SortOrder,
    Expression,
    LiteralExpression,
    ColumnReference,
    UnresolvedFunction,
    UnresolvedStar,
    SQLExpression,
    LambdaFunction,
    UnresolvedNamedLambdaVariable,
    CallFunction,
)
from pyspark.sql.connect.udf import _create_py_udf
from pyspark.sql.connect.udtf import AnalyzeArgument, AnalyzeResult  # noqa: F401
from pyspark.sql.connect.udtf import _create_py_udtf
from pyspark.sql import functions as pysparkfuncs
from pyspark.sql.types import (
    _from_numpy_type,
    DataType,
    StructType,
    ArrayType,
    StringType,
)
from pyspark.sql.utils import enum_to_value as _enum_to_value

# The implementation of pandas_udf is embedded in pyspark.sql.function.pandas_udf
# for code reuse.
from pyspark.sql.functions import pandas_udf  # noqa: F401


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import (
        ColumnOrName,
        DataTypeOrString,
        UserDefinedFunctionLike,
    )
    from pyspark.sql.connect.udtf import UserDefinedTableFunction


def _to_col(col: "ColumnOrName") -> Column:
    assert isinstance(col, (Column, str))
    return col if isinstance(col, Column) else column(col)


def _sort_col(col: "ColumnOrName") -> Column:
    assert isinstance(col, (Column, str))
    if isinstance(col, Column):
        if isinstance(col._expr, SortOrder):
            return col
        else:
            return col.asc()
    else:
        return column(col).asc()


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
    from pyspark.sql.connect.column import Column as ConnectColumn

    expressions: List[Expression] = []
    for arg in args:
        assert isinstance(arg, (Column, Expression))
        if isinstance(arg, Column):
            expressions.append(arg._expr)  # type: ignore[arg-type]
        else:
            expressions.append(arg)
    return ConnectColumn(UnresolvedFunction(name, expressions))


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
            errorClass="WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION",
            messageParameters={"func_name": f.__name__, "num_args": str(len(parameters))},
        )

    # Verify that all arguments can be used as positional arguments.
    if not all(p.kind in supported_parameter_types for p in parameters):
        raise PySparkValueError(
            errorClass="UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION",
            messageParameters={"func_name": f.__name__},
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
    from pyspark.sql.connect.column import Column as ConnectColumn

    parameters = _get_lambda_parameters(f)

    arg_names = ["x", "y", "z"][: len(parameters)]
    arg_exprs = [
        UnresolvedNamedLambdaVariable([UnresolvedNamedLambdaVariable.fresh_var_name(arg_name)])
        for arg_name in arg_names
    ]
    arg_cols = [ConnectColumn(arg_expr) for arg_expr in arg_exprs]

    result = f(*arg_cols)

    if not isinstance(result, Column):
        raise PySparkValueError(
            errorClass="HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN",
            messageParameters={"func_name": f.__name__, "return_type": type(result).__name__},
        )

    return LambdaFunction(result._expr, arg_exprs)  # type: ignore[arg-type]


def _invoke_higher_order_function(
    name: str,
    cols: Sequence["ColumnOrName"],
    funs: Sequence[Callable],
) -> Column:
    """
    Invokes expression identified by name,
    (relative to ```org.apache.spark.sql.catalyst.expressions``)
    and wraps the result with Column (first Scala one, then Python).

    :param name: Name of the expression
    :param cols: a list of columns
    :param funs: a list of (*Column) -> Column functions.

    :return: a Column
    """
    _cols = [_to_col(c) for c in cols]
    _funs = [_create_lambda(f) for f in funs]

    return _invoke_function(name, *_cols, *_funs)


def _options_to_col(options: Mapping[str, Any]) -> Column:
    _options: List[Column] = []
    for k, v in options.items():
        _options.append(lit(str(k)))
        _options.append(lit(str(v)))
    return create_map(*_options)


# Normal Functions


def col(col: str) -> Column:
    from pyspark.sql.connect.column import Column as ConnectColumn

    if col == "*":
        return ConnectColumn(UnresolvedStar(unparsed_target=None))
    elif col.endswith(".*"):
        return ConnectColumn(UnresolvedStar(unparsed_target=col))
    else:
        return ConnectColumn(ColumnReference(unparsed_identifier=col))


col.__doc__ = pysparkfuncs.col.__doc__


column = col


def lit(col: Any) -> Column:
    from pyspark.sql.connect.column import Column as ConnectColumn

    if isinstance(col, Column):
        return col
    elif isinstance(col, list):
        if any(isinstance(c, Column) for c in col):
            raise PySparkValueError(
                errorClass="COLUMN_IN_LIST", messageParameters={"func_name": "lit"}
            )
        return array(*[lit(c) for c in col])
    elif isinstance(col, np.ndarray) and col.ndim == 1:
        if _from_numpy_type(col.dtype) is None:
            raise PySparkTypeError(
                errorClass="UNSUPPORTED_NUMPY_ARRAY_SCALAR",
                messageParameters={"dtype": col.dtype.name},
            )

        # NumpyArrayConverter for Py4J can not support ndarray with int8 values.
        # Actually this is not a problem for Connect, but here still convert it
        # to int16 for compatibility.
        if col.dtype == np.int8:
            col = col.astype(np.int16)

        return array(*[lit(c) for c in col])
    else:
        return ConnectColumn(LiteralExpression._from_value(col))


lit.__doc__ = pysparkfuncs.lit.__doc__


def bitwiseNOT(col: "ColumnOrName") -> Column:
    warnings.warn("Deprecated in 3.4, use bitwise_not instead.", FutureWarning)
    return bitwise_not(col)


bitwiseNOT.__doc__ = pysparkfuncs.bitwiseNOT.__doc__


def bitwise_not(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("~", col)


bitwise_not.__doc__ = pysparkfuncs.bitwise_not.__doc__


def bit_count(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bit_count", col)


bit_count.__doc__ = pysparkfuncs.bit_count.__doc__


def bit_get(col: "ColumnOrName", pos: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bit_get", col, pos)


bit_get.__doc__ = pysparkfuncs.bit_get.__doc__


def getbit(col: "ColumnOrName", pos: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("getbit", col, pos)


getbit.__doc__ = pysparkfuncs.getbit.__doc__


def broadcast(df: "ParentDataFrame") -> "ParentDataFrame":
    from pyspark.sql.connect.dataframe import DataFrame

    if not isinstance(df, DataFrame):
        raise PySparkTypeError(
            errorClass="NOT_DATAFRAME",
            messageParameters={"arg_name": "df", "arg_type": type(df).__name__},
        )
    return df.hint("broadcast")


broadcast.__doc__ = pysparkfuncs.broadcast.__doc__


def coalesce(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("coalesce", *cols)


coalesce.__doc__ = pysparkfuncs.coalesce.__doc__


def expr(str: str) -> Column:
    from pyspark.sql.connect.column import Column as ConnectColumn

    return ConnectColumn(SQLExpression(str))


expr.__doc__ = pysparkfuncs.expr.__doc__


def greatest(*cols: "ColumnOrName") -> Column:
    if len(cols) < 2:
        raise PySparkValueError(
            errorClass="WRONG_NUM_COLUMNS",
            messageParameters={"func_name": "greatest", "num_cols": "2"},
        )
    return _invoke_function_over_columns("greatest", *cols)


greatest.__doc__ = pysparkfuncs.greatest.__doc__


def input_file_name() -> Column:
    return _invoke_function("input_file_name")


input_file_name.__doc__ = pysparkfuncs.input_file_name.__doc__


def least(*cols: "ColumnOrName") -> Column:
    if len(cols) < 2:
        raise PySparkValueError(
            errorClass="WRONG_NUM_COLUMNS",
            messageParameters={"func_name": "least", "num_cols": "2"},
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
        return _invoke_function("rand", lit(random.randint(0, sys.maxsize)))


rand.__doc__ = pysparkfuncs.rand.__doc__


def randn(seed: Optional[int] = None) -> Column:
    if seed is not None:
        return _invoke_function("randn", lit(seed))
    else:
        return _invoke_function("randn", lit(random.randint(0, sys.maxsize)))


randn.__doc__ = pysparkfuncs.randn.__doc__


def spark_partition_id() -> Column:
    return _invoke_function("spark_partition_id")


spark_partition_id.__doc__ = pysparkfuncs.spark_partition_id.__doc__


def when(condition: Column, value: Any) -> Column:
    from pyspark.sql.connect.column import Column as ConnectColumn

    # Explicitly not using ColumnOrName type here to make reading condition less opaque
    if not isinstance(condition, Column):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN",
            messageParameters={"arg_name": "condition", "arg_type": type(condition).__name__},
        )

    value = _enum_to_value(value)
    value_col = value if isinstance(value, Column) else lit(value)

    return ConnectColumn(
        CaseWhen(
            branches=[(condition._expr, value_col._expr)],  # type: ignore[list-item]
            else_value=None,
        )
    )


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


def bround(col: "ColumnOrName", scale: Optional[Union[Column, int]] = None) -> Column:
    if scale is None:
        return _invoke_function_over_columns("bround", col)
    else:
        scale = _enum_to_value(scale)
        scale = lit(scale) if isinstance(scale, int) else scale
        return _invoke_function_over_columns("bround", col, scale)  # type: ignore[arg-type]


bround.__doc__ = pysparkfuncs.bround.__doc__


def cbrt(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("cbrt", col)


cbrt.__doc__ = pysparkfuncs.cbrt.__doc__


def ceil(col: "ColumnOrName", scale: Optional[Union[Column, int]] = None) -> Column:
    if scale is None:
        return _invoke_function_over_columns("ceil", col)
    else:
        scale = _enum_to_value(scale)
        scale = lit(scale) if isinstance(scale, int) else scale
        return _invoke_function_over_columns("ceil", col, scale)  # type: ignore[arg-type]


ceil.__doc__ = pysparkfuncs.ceil.__doc__


def ceiling(col: "ColumnOrName", scale: Optional[Union[Column, int]] = None) -> Column:
    if scale is None:
        return _invoke_function_over_columns("ceiling", col)
    else:
        scale = _enum_to_value(scale)
        scale = lit(scale) if isinstance(scale, int) else scale
        return _invoke_function_over_columns("ceiling", col, scale)  # type: ignore[arg-type]


ceiling.__doc__ = pysparkfuncs.ceiling.__doc__


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


def e() -> Column:
    return _invoke_function("e")


e.__doc__ = pysparkfuncs.e.__doc__


def exp(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("exp", col)


exp.__doc__ = pysparkfuncs.exp.__doc__


def expm1(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("expm1", col)


expm1.__doc__ = pysparkfuncs.expm1.__doc__


def factorial(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("factorial", col)


factorial.__doc__ = pysparkfuncs.factorial.__doc__


def floor(col: "ColumnOrName", scale: Optional[Union[Column, int]] = None) -> Column:
    if scale is None:
        return _invoke_function_over_columns("floor", col)
    else:
        scale = _enum_to_value(scale)
        scale = lit(scale) if isinstance(scale, int) else scale
        return _invoke_function_over_columns("floor", col, scale)  # type: ignore[arg-type]


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


def ln(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("ln", col)


ln.__doc__ = pysparkfuncs.ln.__doc__


def log2(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("log2", col)


log2.__doc__ = pysparkfuncs.log2.__doc__


def negative(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("negative", col)


negative.__doc__ = pysparkfuncs.negative.__doc__


negate = negative


def pi() -> Column:
    return _invoke_function("pi")


pi.__doc__ = pysparkfuncs.pi.__doc__


def positive(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("positive", col)


positive.__doc__ = pysparkfuncs.positive.__doc__


def pmod(dividend: Union["ColumnOrName", float], divisor: Union["ColumnOrName", float]) -> Column:
    return _invoke_binary_math_function("pmod", dividend, divisor)


pmod.__doc__ = pysparkfuncs.pmod.__doc__


def width_bucket(
    v: "ColumnOrName",
    min: "ColumnOrName",
    max: "ColumnOrName",
    numBucket: Union["ColumnOrName", int],
) -> Column:
    numBucket = _enum_to_value(numBucket)
    numBucket = lit(numBucket) if isinstance(numBucket, int) else numBucket
    return _invoke_function_over_columns("width_bucket", v, min, max, numBucket)


width_bucket.__doc__ = pysparkfuncs.width_bucket.__doc__


def pow(col1: Union["ColumnOrName", float], col2: Union["ColumnOrName", float]) -> Column:
    return _invoke_binary_math_function("power", col1, col2)


pow.__doc__ = pysparkfuncs.pow.__doc__


power = pow


def radians(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("radians", col)


radians.__doc__ = pysparkfuncs.radians.__doc__


def rint(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("rint", col)


rint.__doc__ = pysparkfuncs.rint.__doc__


def round(col: "ColumnOrName", scale: Optional[Union[Column, int]] = None) -> Column:
    if scale is None:
        return _invoke_function_over_columns("round", col)
    else:
        scale = _enum_to_value(scale)
        scale = lit(scale) if isinstance(scale, int) else scale
        return _invoke_function_over_columns("round", col, scale)  # type: ignore[arg-type]


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


def sign(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("sign", col)


sign.__doc__ = pysparkfuncs.sign.__doc__


def sin(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("sin", col)


sin.__doc__ = pysparkfuncs.sin.__doc__


def sinh(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("sinh", col)


sinh.__doc__ = pysparkfuncs.sinh.__doc__


def sqrt(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("sqrt", col)


sqrt.__doc__ = pysparkfuncs.sqrt.__doc__


def try_add(left: "ColumnOrName", right: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_add", left, right)


try_add.__doc__ = pysparkfuncs.try_add.__doc__


def try_avg(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_avg", col)


try_avg.__doc__ = pysparkfuncs.try_avg.__doc__


def try_divide(left: "ColumnOrName", right: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_divide", left, right)


try_divide.__doc__ = pysparkfuncs.try_divide.__doc__


def try_mod(left: "ColumnOrName", right: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_mod", left, right)


try_mod.__doc__ = pysparkfuncs.try_mod.__doc__


def try_multiply(left: "ColumnOrName", right: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_multiply", left, right)


try_multiply.__doc__ = pysparkfuncs.try_multiply.__doc__


def try_subtract(left: "ColumnOrName", right: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_subtract", left, right)


try_subtract.__doc__ = pysparkfuncs.try_subtract.__doc__


def try_sum(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_sum", col)


try_sum.__doc__ = pysparkfuncs.try_sum.__doc__


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


def uniform(
    min: Union[Column, int, float],
    max: Union[Column, int, float],
    seed: Optional[Union[Column, int]] = None,
) -> Column:
    if seed is None:
        return _invoke_function_over_columns(
            "uniform", lit(min), lit(max), lit(random.randint(0, sys.maxsize))
        )
    else:
        return _invoke_function_over_columns("uniform", lit(min), lit(max), lit(seed))


uniform.__doc__ = pysparkfuncs.uniform.__doc__


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


def array_agg(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("array_agg", col)


array_agg.__doc__ = pysparkfuncs.array_agg.__doc__


def collect_set(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("collect_set", col)


collect_set.__doc__ = pysparkfuncs.collect_set.__doc__


def corr(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("corr", col1, col2)


corr.__doc__ = pysparkfuncs.corr.__doc__


def count(col: "ColumnOrName") -> Column:
    if isinstance(col, Column) and isinstance(col._expr, UnresolvedStar):
        col = lit(1)
    return _invoke_function_over_columns("count", col)


count.__doc__ = pysparkfuncs.count.__doc__


def countDistinct(col: "ColumnOrName", *cols: "ColumnOrName") -> Column:
    return count_distinct(col, *cols)


countDistinct.__doc__ = pysparkfuncs.countDistinct.__doc__


def count_distinct(col: "ColumnOrName", *cols: "ColumnOrName") -> Column:
    from pyspark.sql.connect.column import Column as ConnectColumn

    _exprs = [_to_col(c)._expr for c in [col] + list(cols)]
    return ConnectColumn(
        UnresolvedFunction("count", _exprs, is_distinct=True)  # type: ignore[arg-type]
    )


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


def count_min_sketch(
    col: "ColumnOrName",
    eps: Union[Column, float],
    confidence: Union[Column, float],
    seed: Optional[Union[Column, int]] = None,
) -> Column:
    _seed = lit(random.randint(0, sys.maxsize)) if seed is None else lit(seed)
    return _invoke_function_over_columns("count_min_sketch", col, lit(eps), lit(confidence), _seed)


count_min_sketch.__doc__ = pysparkfuncs.count_min_sketch.__doc__


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


def mode(col: "ColumnOrName", deterministic: bool = False) -> Column:
    return _invoke_function("mode", _to_col(col), lit(deterministic))


mode.__doc__ = pysparkfuncs.mode.__doc__


def percentile(
    col: "ColumnOrName",
    percentage: Union[Column, float, Sequence[float], Tuple[float]],
    frequency: Union[Column, int] = 1,
) -> Column:
    if not isinstance(frequency, (int, Column)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_INT",
            messageParameters={
                "arg_name": "frequency",
                "arg_type": type(frequency).__name__,
            },
        )

    percentage = lit(list(percentage)) if isinstance(percentage, (list, tuple)) else lit(percentage)
    return _invoke_function_over_columns("percentile", col, percentage, lit(frequency))


percentile.__doc__ = pysparkfuncs.percentile.__doc__


def percentile_approx(
    col: "ColumnOrName",
    percentage: Union[Column, float, Sequence[float], Tuple[float]],
    accuracy: Union[Column, int] = 10000,
) -> Column:
    percentage = lit(list(percentage)) if isinstance(percentage, (list, tuple)) else lit(percentage)
    return _invoke_function_over_columns("percentile_approx", col, percentage, lit(accuracy))


percentile_approx.__doc__ = pysparkfuncs.percentile_approx.__doc__


def approx_percentile(
    col: "ColumnOrName",
    percentage: Union[Column, float, Sequence[float], Tuple[float]],
    accuracy: Union[Column, int] = 10000,
) -> Column:
    percentage = lit(list(percentage)) if isinstance(percentage, (list, tuple)) else lit(percentage)
    return _invoke_function_over_columns("approx_percentile", col, percentage, lit(accuracy))


approx_percentile.__doc__ = pysparkfuncs.approx_percentile.__doc__


def product(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("product", col)


product.__doc__ = pysparkfuncs.product.__doc__


def skewness(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("skewness", col)


skewness.__doc__ = pysparkfuncs.skewness.__doc__


def stddev(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("stddev", col)


stddev.__doc__ = pysparkfuncs.stddev.__doc__


def std(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("std", col)


std.__doc__ = pysparkfuncs.std.__doc__


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
    from pyspark.sql.connect.column import Column as ConnectColumn

    return ConnectColumn(
        UnresolvedFunction("sum", [_to_col(col)._expr], is_distinct=True)  # type: ignore[list-item]
    )


sum_distinct.__doc__ = pysparkfuncs.sum_distinct.__doc__


def var_pop(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("var_pop", col)


var_pop.__doc__ = pysparkfuncs.var_pop.__doc__


def regr_avgx(y: "ColumnOrName", x: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regr_avgx", y, x)


regr_avgx.__doc__ = pysparkfuncs.regr_avgx.__doc__


def regr_avgy(y: "ColumnOrName", x: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regr_avgy", y, x)


regr_avgy.__doc__ = pysparkfuncs.regr_avgy.__doc__


def regr_count(y: "ColumnOrName", x: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regr_count", y, x)


regr_count.__doc__ = pysparkfuncs.regr_count.__doc__


def regr_intercept(y: "ColumnOrName", x: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regr_intercept", y, x)


regr_intercept.__doc__ = pysparkfuncs.regr_intercept.__doc__


def regr_r2(y: "ColumnOrName", x: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regr_r2", y, x)


regr_r2.__doc__ = pysparkfuncs.regr_r2.__doc__


def regr_slope(y: "ColumnOrName", x: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regr_slope", y, x)


regr_slope.__doc__ = pysparkfuncs.regr_slope.__doc__


def regr_sxx(y: "ColumnOrName", x: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regr_sxx", y, x)


regr_sxx.__doc__ = pysparkfuncs.regr_sxx.__doc__


def regr_sxy(y: "ColumnOrName", x: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regr_sxy", y, x)


regr_sxy.__doc__ = pysparkfuncs.regr_sxy.__doc__


def regr_syy(y: "ColumnOrName", x: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regr_syy", y, x)


regr_syy.__doc__ = pysparkfuncs.regr_syy.__doc__


def var_samp(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("var_samp", col)


var_samp.__doc__ = pysparkfuncs.var_samp.__doc__


def variance(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("variance", col)


variance.__doc__ = pysparkfuncs.variance.__doc__


def every(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("every", col)


every.__doc__ = pysparkfuncs.every.__doc__


def bool_and(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bool_and", col)


bool_and.__doc__ = pysparkfuncs.bool_and.__doc__


def some(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("some", col)


some.__doc__ = pysparkfuncs.some.__doc__


def bool_or(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bool_or", col)


bool_or.__doc__ = pysparkfuncs.bool_or.__doc__


def bit_and(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bit_and", col)


bit_and.__doc__ = pysparkfuncs.bit_and.__doc__


def bit_or(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bit_or", col)


bit_or.__doc__ = pysparkfuncs.bit_or.__doc__


def bit_xor(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bit_xor", col)


bit_xor.__doc__ = pysparkfuncs.bit_xor.__doc__


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


def nth_value(col: "ColumnOrName", offset: int, ignoreNulls: Optional[bool] = False) -> Column:
    if ignoreNulls is None:
        return _invoke_function("nth_value", _to_col(col), lit(offset))
    else:
        return _invoke_function("nth_value", _to_col(col), lit(offset), lit(ignoreNulls))


nth_value.__doc__ = pysparkfuncs.nth_value.__doc__


def any_value(col: "ColumnOrName", ignoreNulls: Optional[Union[bool, Column]] = None) -> Column:
    if ignoreNulls is None:
        return _invoke_function_over_columns("any_value", col)
    else:
        ignoreNulls = _enum_to_value(ignoreNulls)
        ignoreNulls = lit(ignoreNulls) if isinstance(ignoreNulls, bool) else ignoreNulls
        return _invoke_function_over_columns(
            "any_value", col, ignoreNulls  # type: ignore[arg-type]
        )


any_value.__doc__ = pysparkfuncs.any_value.__doc__


def first_value(col: "ColumnOrName", ignoreNulls: Optional[Union[bool, Column]] = None) -> Column:
    if ignoreNulls is None:
        return _invoke_function_over_columns("first_value", col)
    else:
        ignoreNulls = _enum_to_value(ignoreNulls)
        ignoreNulls = lit(ignoreNulls) if isinstance(ignoreNulls, bool) else ignoreNulls
        return _invoke_function_over_columns(
            "first_value", col, ignoreNulls  # type: ignore[arg-type]
        )


first_value.__doc__ = pysparkfuncs.first_value.__doc__


def last_value(col: "ColumnOrName", ignoreNulls: Optional[Union[bool, Column]] = None) -> Column:
    if ignoreNulls is None:
        return _invoke_function_over_columns("last_value", col)
    else:
        ignoreNulls = _enum_to_value(ignoreNulls)
        ignoreNulls = lit(ignoreNulls) if isinstance(ignoreNulls, bool) else ignoreNulls
        return _invoke_function_over_columns(
            "last_value", col, ignoreNulls  # type: ignore[arg-type]
        )


last_value.__doc__ = pysparkfuncs.last_value.__doc__


def count_if(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("count_if", col)


count_if.__doc__ = pysparkfuncs.count_if.__doc__


def histogram_numeric(col: "ColumnOrName", nBins: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("histogram_numeric", col, nBins)


histogram_numeric.__doc__ = pysparkfuncs.histogram_numeric.__doc__


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


def reduce(
    col: "ColumnOrName",
    initialValue: "ColumnOrName",
    merge: Callable[[Column, Column], Column],
    finish: Optional[Callable[[Column], Column]] = None,
) -> Column:
    if finish is not None:
        return _invoke_higher_order_function("reduce", [col, initialValue], [merge, finish])

    else:
        return _invoke_higher_order_function("reduce", [col, initialValue], [merge])


reduce.__doc__ = pysparkfuncs.reduce.__doc__


def array(
    *cols: Union["ColumnOrName", Sequence["ColumnOrName"], Tuple["ColumnOrName", ...]]
) -> Column:
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
    pos = _enum_to_value(pos)
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


def array_size(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("array_size", col)


array_size.__doc__ = pysparkfuncs.array_size.__doc__


def cardinality(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("cardinality", col)


cardinality.__doc__ = pysparkfuncs.cardinality.__doc__


def array_position(col: "ColumnOrName", value: Any) -> Column:
    return _invoke_function("array_position", _to_col(col), lit(value))


array_position.__doc__ = pysparkfuncs.array_position.__doc__


def array_prepend(col: "ColumnOrName", value: Any) -> Column:
    return _invoke_function("array_prepend", _to_col(col), lit(value))


array_prepend.__doc__ = pysparkfuncs.array_prepend.__doc__


def array_remove(col: "ColumnOrName", element: Any) -> Column:
    return _invoke_function("array_remove", _to_col(col), lit(element))


array_remove.__doc__ = pysparkfuncs.array_remove.__doc__


def array_repeat(col: "ColumnOrName", count: Union["ColumnOrName", int]) -> Column:
    count = _enum_to_value(count)
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
    *cols: Union["ColumnOrName", Sequence["ColumnOrName"], Tuple["ColumnOrName", ...]]
) -> Column:
    if len(cols) == 1 and isinstance(cols[0], (list, set, tuple)):
        cols = cols[0]  # type: ignore[assignment]
    return _invoke_function_over_columns("map", *cols)  # type: ignore[arg-type]


create_map.__doc__ = pysparkfuncs.create_map.__doc__


def element_at(col: "ColumnOrName", extraction: Any) -> Column:
    return _invoke_function("element_at", _to_col(col), lit(extraction))


element_at.__doc__ = pysparkfuncs.element_at.__doc__


def try_element_at(col: "ColumnOrName", extraction: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_element_at", col, extraction)


try_element_at.__doc__ = pysparkfuncs.try_element_at.__doc__


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


def from_csv(
    col: "ColumnOrName",
    schema: Union[Column, str],
    options: Optional[Mapping[str, str]] = None,
) -> Column:
    if isinstance(schema, Column):
        _schema = schema
    elif isinstance(schema, str):
        _schema = lit(schema)
    else:
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "schema", "arg_type": type(schema).__name__},
        )

    if options is None:
        return _invoke_function("from_csv", _to_col(col), _schema)
    else:
        return _invoke_function("from_csv", _to_col(col), _schema, _options_to_col(options))


from_csv.__doc__ = pysparkfuncs.from_csv.__doc__


def from_json(
    col: "ColumnOrName",
    schema: Union[ArrayType, StructType, Column, str],
    options: Optional[Mapping[str, str]] = None,
) -> Column:
    if isinstance(schema, (str, Column)):
        _schema = lit(schema)
    elif isinstance(schema, DataType):
        _schema = lit(schema.json())
    else:
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_DATATYPE_OR_STR",
            messageParameters={"arg_name": "schema", "arg_type": type(schema).__name__},
        )

    if options is None:
        return _invoke_function("from_json", _to_col(col), _schema)
    else:
        return _invoke_function("from_json", _to_col(col), _schema, _options_to_col(options))


from_json.__doc__ = pysparkfuncs.from_json.__doc__


def from_xml(
    col: "ColumnOrName",
    schema: Union[StructType, Column, str],
    options: Optional[Mapping[str, str]] = None,
) -> Column:
    if isinstance(schema, (str, Column)):
        _schema = lit(schema)
    elif isinstance(schema, StructType):
        _schema = lit(schema.json())
    else:
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR_OR_STRUCT",
            messageParameters={"arg_name": "schema", "arg_type": type(schema).__name__},
        )

    if options is None:
        return _invoke_function("from_xml", _to_col(col), _schema)
    else:
        return _invoke_function("from_xml", _to_col(col), _schema, _options_to_col(options))


from_xml.__doc__ = pysparkfuncs.from_xml.__doc__


def get(col: "ColumnOrName", index: Union["ColumnOrName", int]) -> Column:
    index = _enum_to_value(index)
    index = lit(index) if isinstance(index, int) else index

    return _invoke_function_over_columns("get", col, index)


get.__doc__ = pysparkfuncs.get.__doc__


def get_json_object(col: "ColumnOrName", path: str) -> Column:
    return _invoke_function("get_json_object", _to_col(col), lit(path))


get_json_object.__doc__ = pysparkfuncs.get_json_object.__doc__


def json_array_length(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("json_array_length", col)


json_array_length.__doc__ = pysparkfuncs.json_array_length.__doc__


def json_object_keys(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("json_object_keys", col)


json_object_keys.__doc__ = pysparkfuncs.json_object_keys.__doc__


def inline(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("inline", col)


inline.__doc__ = pysparkfuncs.inline.__doc__


def inline_outer(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("inline_outer", col)


inline_outer.__doc__ = pysparkfuncs.inline_outer.__doc__


def json_tuple(col: "ColumnOrName", *fields: str) -> Column:
    if len(fields) == 0:
        raise PySparkValueError(
            errorClass="CANNOT_BE_EMPTY",
            messageParameters={"item": "field"},
        )
    return _invoke_function("json_tuple", _to_col(col), *[lit(field) for field in fields])


json_tuple.__doc__ = pysparkfuncs.json_tuple.__doc__


def map_concat(
    *cols: Union["ColumnOrName", Sequence["ColumnOrName"], Tuple["ColumnOrName", ...]]
) -> Column:
    if len(cols) == 1 and isinstance(cols[0], (list, set, tuple)):
        cols = cols[0]  # type: ignore[assignment]
    return _invoke_function_over_columns("map_concat", *cols)  # type: ignore[arg-type]


map_concat.__doc__ = pysparkfuncs.map_concat.__doc__


def map_contains_key(col: "ColumnOrName", value: Any) -> Column:
    return _invoke_function("map_contains_key", _to_col(col), lit(value))


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


def str_to_map(
    text: "ColumnOrName",
    pairDelim: Optional["ColumnOrName"] = None,
    keyValueDelim: Optional["ColumnOrName"] = None,
) -> Column:
    _pairDelim = lit(",") if pairDelim is None else _to_col(pairDelim)
    _keyValueDelim = lit(":") if keyValueDelim is None else _to_col(keyValueDelim)

    return _invoke_function("str_to_map", _to_col(text), _pairDelim, _keyValueDelim)


str_to_map.__doc__ = pysparkfuncs.str_to_map.__doc__


def try_parse_json(col: "ColumnOrName") -> Column:
    return _invoke_function("try_parse_json", _to_col(col))


try_parse_json.__doc__ = pysparkfuncs.try_parse_json.__doc__


def to_variant_object(col: "ColumnOrName") -> Column:
    return _invoke_function("to_variant_object", _to_col(col))


to_variant_object.__doc__ = pysparkfuncs.to_variant_object.__doc__


def parse_json(col: "ColumnOrName") -> Column:
    return _invoke_function("parse_json", _to_col(col))


parse_json.__doc__ = pysparkfuncs.parse_json.__doc__


def is_variant_null(v: "ColumnOrName") -> Column:
    return _invoke_function("is_variant_null", _to_col(v))


is_variant_null.__doc__ = pysparkfuncs.is_variant_null.__doc__


def variant_get(v: "ColumnOrName", path: str, targetType: str) -> Column:
    return _invoke_function("variant_get", _to_col(v), lit(path), lit(targetType))


variant_get.__doc__ = pysparkfuncs.variant_get.__doc__


def try_variant_get(v: "ColumnOrName", path: str, targetType: str) -> Column:
    return _invoke_function("try_variant_get", _to_col(v), lit(path), lit(targetType))


try_variant_get.__doc__ = pysparkfuncs.try_variant_get.__doc__


def schema_of_variant(v: "ColumnOrName") -> Column:
    return _invoke_function("schema_of_variant", _to_col(v))


schema_of_variant.__doc__ = pysparkfuncs.schema_of_variant.__doc__


def schema_of_variant_agg(v: "ColumnOrName") -> Column:
    return _invoke_function("schema_of_variant_agg", _to_col(v))


schema_of_variant_agg.__doc__ = pysparkfuncs.schema_of_variant_agg.__doc__


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


def schema_of_csv(csv: Union[str, Column], options: Optional[Mapping[str, str]] = None) -> Column:
    csv = _enum_to_value(csv)
    if not isinstance(csv, (str, Column)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "csv", "arg_type": type(csv).__name__},
        )

    if options is None:
        return _invoke_function("schema_of_csv", lit(csv))
    else:
        return _invoke_function("schema_of_csv", lit(csv), _options_to_col(options))


schema_of_csv.__doc__ = pysparkfuncs.schema_of_csv.__doc__


def schema_of_json(json: Union[str, Column], options: Optional[Mapping[str, str]] = None) -> Column:
    json = _enum_to_value(json)
    if not isinstance(json, (str, Column)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "json", "arg_type": type(json).__name__},
        )

    if options is None:
        return _invoke_function("schema_of_json", lit(json))
    else:
        return _invoke_function("schema_of_json", lit(json), _options_to_col(options))


schema_of_json.__doc__ = pysparkfuncs.schema_of_json.__doc__


def schema_of_xml(xml: Union[str, Column], options: Optional[Mapping[str, str]] = None) -> Column:
    xml = _enum_to_value(xml)
    if not isinstance(xml, (str, Column)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "xml", "arg_type": type(xml).__name__},
        )

    if options is None:
        return _invoke_function("schema_of_xml", lit(xml))
    else:
        return _invoke_function("schema_of_xml", lit(xml), _options_to_col(options))


schema_of_xml.__doc__ = pysparkfuncs.schema_of_xml.__doc__


def shuffle(col: "ColumnOrName", seed: Optional[Union[Column, int]] = None) -> Column:
    _seed = lit(random.randint(0, sys.maxsize)) if seed is None else lit(seed)
    return _invoke_function("shuffle", _to_col(col), _seed)


shuffle.__doc__ = pysparkfuncs.shuffle.__doc__


def size(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("size", col)


size.__doc__ = pysparkfuncs.size.__doc__


def slice(
    x: "ColumnOrName", start: Union["ColumnOrName", int], length: Union["ColumnOrName", int]
) -> Column:
    start = _enum_to_value(start)
    if isinstance(start, (Column, str)):
        _start = start
    elif isinstance(start, int):
        _start = lit(start)
    else:
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_INT_OR_STR",
            messageParameters={"arg_name": "start", "arg_type": type(start).__name__},
        )

    length = _enum_to_value(length)
    if isinstance(length, (Column, str)):
        _length = length
    elif isinstance(length, int):
        _length = lit(length)
    else:
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_INT_OR_STR",
            messageParameters={"arg_name": "length", "arg_type": type(length).__name__},
        )

    return _invoke_function_over_columns("slice", x, _start, _length)


slice.__doc__ = pysparkfuncs.slice.__doc__


def sort_array(col: "ColumnOrName", asc: bool = True) -> Column:
    return _invoke_function("sort_array", _to_col(col), lit(asc))


sort_array.__doc__ = pysparkfuncs.sort_array.__doc__


def struct(
    *cols: Union["ColumnOrName", Sequence["ColumnOrName"], Tuple["ColumnOrName", ...]]
) -> Column:
    if len(cols) == 1 and isinstance(cols[0], (list, set, tuple)):
        cols = cols[0]  # type: ignore[assignment]
    return _invoke_function_over_columns("struct", *cols)  # type: ignore[arg-type]


struct.__doc__ = pysparkfuncs.struct.__doc__


def named_struct(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("named_struct", *cols)


named_struct.__doc__ = pysparkfuncs.named_struct.__doc__


def to_csv(col: "ColumnOrName", options: Optional[Mapping[str, str]] = None) -> Column:
    if options is None:
        return _invoke_function("to_csv", _to_col(col))
    else:
        return _invoke_function("to_csv", _to_col(col), _options_to_col(options))


to_csv.__doc__ = pysparkfuncs.to_csv.__doc__


def to_json(col: "ColumnOrName", options: Optional[Mapping[str, str]] = None) -> Column:
    if options is None:
        return _invoke_function("to_json", _to_col(col))
    else:
        return _invoke_function("to_json", _to_col(col), _options_to_col(options))


to_json.__doc__ = pysparkfuncs.to_json.__doc__


def to_xml(col: "ColumnOrName", options: Optional[Mapping[str, str]] = None) -> Column:
    if options is None:
        return _invoke_function("to_xml", _to_col(col))
    else:
        return _invoke_function("to_xml", _to_col(col), _options_to_col(options))


to_xml.__doc__ = pysparkfuncs.to_xml.__doc__


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


def ltrim(col: "ColumnOrName", trim: Optional["ColumnOrName"] = None) -> Column:
    if trim is not None:
        return _invoke_function_over_columns("ltrim", trim, col)
    else:
        return _invoke_function_over_columns("ltrim", col)


ltrim.__doc__ = pysparkfuncs.ltrim.__doc__


def rtrim(col: "ColumnOrName", trim: Optional["ColumnOrName"] = None) -> Column:
    if trim is not None:
        return _invoke_function_over_columns("rtrim", trim, col)
    else:
        return _invoke_function_over_columns("rtrim", col)


rtrim.__doc__ = pysparkfuncs.rtrim.__doc__


def trim(col: "ColumnOrName", trim: Optional["ColumnOrName"] = None) -> Column:
    if trim is not None:
        return _invoke_function_over_columns("trim", trim, col)
    else:
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


def is_valid_utf8(str: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("is_valid_utf8", _to_col(str))


is_valid_utf8.__doc__ = pysparkfuncs.is_valid_utf8.__doc__


def make_valid_utf8(str: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("make_valid_utf8", _to_col(str))


make_valid_utf8.__doc__ = pysparkfuncs.make_valid_utf8.__doc__


def validate_utf8(str: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("validate_utf8", _to_col(str))


validate_utf8.__doc__ = pysparkfuncs.validate_utf8.__doc__


def try_validate_utf8(str: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_validate_utf8", _to_col(str))


try_validate_utf8.__doc__ = pysparkfuncs.try_validate_utf8.__doc__


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
    pos = _enum_to_value(pos)
    if not isinstance(pos, (int, str, Column)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_INT_OR_STR",
            messageParameters={"arg_name": "pos", "arg_type": type(pos).__name__},
        )
    len = _enum_to_value(len)
    if len is not None and not isinstance(len, (int, str, Column)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_INT_OR_STR",
            messageParameters={"arg_name": "len", "arg_type": type(len).__name__},
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


def substring(
    str: "ColumnOrName",
    pos: Union["ColumnOrName", int],
    len: Union["ColumnOrName", int],
) -> Column:
    _pos = lit(pos) if isinstance(pos, int) else _to_col(pos)
    _len = lit(len) if isinstance(len, int) else _to_col(len)
    return _invoke_function("substring", _to_col(str), _pos, _len)


substring.__doc__ = pysparkfuncs.substring.__doc__


def substring_index(str: "ColumnOrName", delim: str, count: int) -> Column:
    return _invoke_function("substring_index", _to_col(str), lit(delim), lit(count))


substring_index.__doc__ = pysparkfuncs.substring_index.__doc__


def levenshtein(
    left: "ColumnOrName", right: "ColumnOrName", threshold: Optional[int] = None
) -> Column:
    if threshold is None:
        return _invoke_function_over_columns("levenshtein", left, right)
    else:
        return _invoke_function("levenshtein", _to_col(left), _to_col(right), lit(threshold))


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


def repeat(col: "ColumnOrName", n: Union["ColumnOrName", int]) -> Column:
    n = _enum_to_value(n)
    n = lit(n) if isinstance(n, int) else n
    return _invoke_function("repeat", _to_col(col), _to_col(n))


repeat.__doc__ = pysparkfuncs.repeat.__doc__


def split(
    str: "ColumnOrName",
    pattern: Union[Column, str],
    limit: Union["ColumnOrName", int] = -1,
) -> Column:
    limit = _enum_to_value(limit)
    limit = lit(limit) if isinstance(limit, int) else _to_col(limit)
    return _invoke_function("split", _to_col(str), lit(pattern), limit)


split.__doc__ = pysparkfuncs.split.__doc__


def rlike(str: "ColumnOrName", regexp: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("rlike", str, regexp)


rlike.__doc__ = pysparkfuncs.rlike.__doc__


def regexp(str: "ColumnOrName", regexp: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regexp", str, regexp)


regexp.__doc__ = pysparkfuncs.regexp.__doc__


def regexp_like(str: "ColumnOrName", regexp: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regexp_like", str, regexp)


regexp_like.__doc__ = pysparkfuncs.regexp_like.__doc__


def randstr(length: Union[Column, int], seed: Optional[Union[Column, int]] = None) -> Column:
    if seed is None:
        return _invoke_function_over_columns(
            "randstr", lit(length), lit(random.randint(0, sys.maxsize))
        )
    else:
        return _invoke_function_over_columns("randstr", lit(length), lit(seed))


randstr.__doc__ = pysparkfuncs.randstr.__doc__


def regexp_count(str: "ColumnOrName", regexp: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regexp_count", str, regexp)


regexp_count.__doc__ = pysparkfuncs.regexp_count.__doc__


def regexp_extract(str: "ColumnOrName", pattern: str, idx: int) -> Column:
    return _invoke_function("regexp_extract", _to_col(str), lit(pattern), lit(idx))


regexp_extract.__doc__ = pysparkfuncs.regexp_extract.__doc__


def regexp_extract_all(
    str: "ColumnOrName", regexp: "ColumnOrName", idx: Optional[Union[int, Column]] = None
) -> Column:
    if idx is None:
        return _invoke_function_over_columns("regexp_extract_all", str, regexp)
    else:
        return _invoke_function_over_columns("regexp_extract_all", str, regexp, lit(idx))


regexp_extract_all.__doc__ = pysparkfuncs.regexp_extract_all.__doc__


def regexp_replace(
    string: "ColumnOrName", pattern: Union[str, Column], replacement: Union[str, Column]
) -> Column:
    return _invoke_function_over_columns("regexp_replace", string, lit(pattern), lit(replacement))


regexp_replace.__doc__ = pysparkfuncs.regexp_replace.__doc__


def regexp_substr(str: "ColumnOrName", regexp: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("regexp_substr", str, regexp)


regexp_substr.__doc__ = pysparkfuncs.regexp_substr.__doc__


def regexp_instr(
    str: "ColumnOrName", regexp: "ColumnOrName", idx: Optional[Union[int, Column]] = None
) -> Column:
    if idx is None:
        return _invoke_function_over_columns("regexp_instr", str, regexp)
    else:
        return _invoke_function_over_columns("regexp_instr", str, regexp, lit(idx))


regexp_instr.__doc__ = pysparkfuncs.regexp_instr.__doc__


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


def to_binary(col: "ColumnOrName", format: Optional["ColumnOrName"] = None) -> Column:
    if format is not None:
        return _invoke_function_over_columns("to_binary", col, format)
    else:
        return _invoke_function_over_columns("to_binary", col)


to_binary.__doc__ = pysparkfuncs.to_binary.__doc__


def to_char(col: "ColumnOrName", format: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("to_char", col, format)


to_char.__doc__ = pysparkfuncs.to_char.__doc__


def to_varchar(col: "ColumnOrName", format: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("to_varchar", col, format)


to_varchar.__doc__ = pysparkfuncs.to_varchar.__doc__


def to_number(col: "ColumnOrName", format: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("to_number", col, format)


to_number.__doc__ = pysparkfuncs.to_number.__doc__


def replace(
    src: "ColumnOrName", search: "ColumnOrName", replace: Optional["ColumnOrName"] = None
) -> Column:
    if replace is not None:
        return _invoke_function_over_columns("replace", src, search, replace)
    else:
        return _invoke_function_over_columns("replace", src, search)


replace.__doc__ = pysparkfuncs.replace.__doc__


def split_part(src: "ColumnOrName", delimiter: "ColumnOrName", partNum: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("split_part", src, delimiter, partNum)


split_part.__doc__ = pysparkfuncs.split_part.__doc__


def substr(
    str: "ColumnOrName", pos: "ColumnOrName", len: Optional["ColumnOrName"] = None
) -> Column:
    if len is not None:
        return _invoke_function_over_columns("substr", str, pos, len)
    else:
        return _invoke_function_over_columns("substr", str, pos)


substr.__doc__ = pysparkfuncs.substr.__doc__


def parse_url(
    url: "ColumnOrName", partToExtract: "ColumnOrName", key: Optional["ColumnOrName"] = None
) -> Column:
    if key is not None:
        return _invoke_function_over_columns("parse_url", url, partToExtract, key)
    else:
        return _invoke_function_over_columns("parse_url", url, partToExtract)


parse_url.__doc__ = pysparkfuncs.parse_url.__doc__


def try_parse_url(
    url: "ColumnOrName", partToExtract: "ColumnOrName", key: Optional["ColumnOrName"] = None
) -> Column:
    if key is not None:
        return _invoke_function_over_columns("try_parse_url", url, partToExtract, key)
    else:
        return _invoke_function_over_columns("try_parse_url", url, partToExtract)


try_parse_url.__doc__ = pysparkfuncs.try_parse_url.__doc__


def printf(format: "ColumnOrName", *cols: "ColumnOrName") -> Column:
    return _invoke_function("printf", _to_col(format), *[_to_col(c) for c in cols])


printf.__doc__ = pysparkfuncs.printf.__doc__


def url_decode(str: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("url_decode", str)


url_decode.__doc__ = pysparkfuncs.url_decode.__doc__


def try_url_decode(str: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_url_decode", str)


try_url_decode.__doc__ = pysparkfuncs.try_url_decode.__doc__


def url_encode(str: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("url_encode", str)


url_encode.__doc__ = pysparkfuncs.url_encode.__doc__


def position(
    substr: "ColumnOrName", str: "ColumnOrName", start: Optional["ColumnOrName"] = None
) -> Column:
    if start is not None:
        return _invoke_function_over_columns("position", substr, str, start)
    else:
        return _invoke_function_over_columns("position", substr, str)


position.__doc__ = pysparkfuncs.position.__doc__


def endswith(str: "ColumnOrName", suffix: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("endswith", str, suffix)


endswith.__doc__ = pysparkfuncs.endswith.__doc__


def startswith(str: "ColumnOrName", prefix: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("startswith", str, prefix)


startswith.__doc__ = pysparkfuncs.startswith.__doc__


def char(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("char", col)


char.__doc__ = pysparkfuncs.char.__doc__


def try_to_binary(col: "ColumnOrName", format: Optional["ColumnOrName"] = None) -> Column:
    if format is not None:
        return _invoke_function_over_columns("try_to_binary", col, format)
    else:
        return _invoke_function_over_columns("try_to_binary", col)


try_to_binary.__doc__ = pysparkfuncs.try_to_binary.__doc__


def try_to_number(col: "ColumnOrName", format: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_to_number", col, format)


try_to_number.__doc__ = pysparkfuncs.try_to_number.__doc__


def btrim(str: "ColumnOrName", trim: Optional["ColumnOrName"] = None) -> Column:
    if trim is not None:
        return _invoke_function_over_columns("btrim", str, trim)
    else:
        return _invoke_function_over_columns("btrim", str)


btrim.__doc__ = pysparkfuncs.btrim.__doc__


def char_length(str: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("char_length", str)


char_length.__doc__ = pysparkfuncs.char_length.__doc__


def character_length(str: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("character_length", str)


character_length.__doc__ = pysparkfuncs.character_length.__doc__


def contains(left: "ColumnOrName", right: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("contains", left, right)


contains.__doc__ = pysparkfuncs.contains.__doc__


def elt(*inputs: "ColumnOrName") -> Column:
    return _invoke_function("elt", *[_to_col(input) for input in inputs])


elt.__doc__ = pysparkfuncs.elt.__doc__


def find_in_set(str: "ColumnOrName", str_array: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("find_in_set", str, str_array)


find_in_set.__doc__ = pysparkfuncs.find_in_set.__doc__


def like(
    str: "ColumnOrName", pattern: "ColumnOrName", escapeChar: Optional["Column"] = None
) -> Column:
    if escapeChar is not None:
        return _invoke_function_over_columns("like", str, pattern, escapeChar)
    else:
        return _invoke_function_over_columns("like", str, pattern)


like.__doc__ = pysparkfuncs.like.__doc__


def ilike(
    str: "ColumnOrName", pattern: "ColumnOrName", escapeChar: Optional["Column"] = None
) -> Column:
    if escapeChar is not None:
        return _invoke_function_over_columns("ilike", str, pattern, escapeChar)
    else:
        return _invoke_function_over_columns("ilike", str, pattern)


ilike.__doc__ = pysparkfuncs.ilike.__doc__


def lcase(str: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("lcase", str)


lcase.__doc__ = pysparkfuncs.lcase.__doc__


def ucase(str: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("ucase", str)


ucase.__doc__ = pysparkfuncs.ucase.__doc__


def left(str: "ColumnOrName", len: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("left", str, len)


left.__doc__ = pysparkfuncs.left.__doc__


def right(str: "ColumnOrName", len: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("right", str, len)


right.__doc__ = pysparkfuncs.right.__doc__


def mask(
    col: "ColumnOrName",
    upperChar: Optional["ColumnOrName"] = None,
    lowerChar: Optional["ColumnOrName"] = None,
    digitChar: Optional["ColumnOrName"] = None,
    otherChar: Optional["ColumnOrName"] = None,
) -> Column:
    _upperChar = lit("X") if upperChar is None else upperChar
    _lowerChar = lit("x") if lowerChar is None else lowerChar
    _digitChar = lit("n") if digitChar is None else digitChar
    _otherChar = lit(None) if otherChar is None else otherChar

    return _invoke_function_over_columns(
        "mask", col, _upperChar, _lowerChar, _digitChar, _otherChar
    )


mask.__doc__ = pysparkfuncs.mask.__doc__


def collate(col: "ColumnOrName", collation: str) -> Column:
    return _invoke_function("collate", _to_col(col), lit(collation))


collate.__doc__ = pysparkfuncs.collate.__doc__


def collation(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("collation", col)


collation.__doc__ = pysparkfuncs.collation.__doc__


# Date/Timestamp functions


def curdate() -> Column:
    return _invoke_function("curdate")


curdate.__doc__ = pysparkfuncs.curdate.__doc__


def current_date() -> Column:
    return _invoke_function("current_date")


current_date.__doc__ = pysparkfuncs.current_date.__doc__


def current_timestamp() -> Column:
    return _invoke_function("current_timestamp")


current_timestamp.__doc__ = pysparkfuncs.current_timestamp.__doc__


def now() -> Column:
    return _invoke_function("now")


now.__doc__ = pysparkfuncs.now.__doc__


def current_timezone() -> Column:
    return _invoke_function("current_timezone")


current_timezone.__doc__ = pysparkfuncs.current_timezone.__doc__


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


def day(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("day", col)


day.__doc__ = pysparkfuncs.day.__doc__


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


def weekday(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("weekday", col)


weekday.__doc__ = pysparkfuncs.weekday.__doc__


def monthname(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("monthname", col)


monthname.__doc__ = pysparkfuncs.monthname.__doc__


def dayname(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("dayname", col)


dayname.__doc__ = pysparkfuncs.dayname.__doc__


def extract(field: "ColumnOrName", source: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("extract", field, source)


extract.__doc__ = pysparkfuncs.extract.__doc__


def date_part(field: "ColumnOrName", source: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("date_part", field, source)


extract.__doc__ = pysparkfuncs.extract.__doc__


def datepart(field: "ColumnOrName", source: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("datepart", field, source)


extract.__doc__ = pysparkfuncs.extract.__doc__


def make_date(year: "ColumnOrName", month: "ColumnOrName", day: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("make_date", year, month, day)


make_date.__doc__ = pysparkfuncs.make_date.__doc__


def date_add(start: "ColumnOrName", days: Union["ColumnOrName", int]) -> Column:
    days = _enum_to_value(days)
    days = lit(days) if isinstance(days, int) else days
    return _invoke_function_over_columns("date_add", start, days)


date_add.__doc__ = pysparkfuncs.date_add.__doc__


def dateadd(start: "ColumnOrName", days: Union["ColumnOrName", int]) -> Column:
    days = _enum_to_value(days)
    days = lit(days) if isinstance(days, int) else days
    return _invoke_function_over_columns("dateadd", start, days)


dateadd.__doc__ = pysparkfuncs.dateadd.__doc__


def date_sub(start: "ColumnOrName", days: Union["ColumnOrName", int]) -> Column:
    days = _enum_to_value(days)
    days = lit(days) if isinstance(days, int) else days
    return _invoke_function_over_columns("date_sub", start, days)


date_sub.__doc__ = pysparkfuncs.date_sub.__doc__


def datediff(end: "ColumnOrName", start: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("datediff", end, start)


datediff.__doc__ = pysparkfuncs.datediff.__doc__


def date_diff(end: "ColumnOrName", start: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("date_diff", end, start)


date_diff.__doc__ = pysparkfuncs.date_diff.__doc__


def date_from_unix_date(days: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("date_from_unix_date", days)


date_from_unix_date.__doc__ = pysparkfuncs.date_from_unix_date.__doc__


def add_months(start: "ColumnOrName", months: Union["ColumnOrName", int]) -> Column:
    months = _enum_to_value(months)
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


def unix_date(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("unix_date", col)


unix_date.__doc__ = pysparkfuncs.unix_date.__doc__


def unix_micros(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("unix_micros", col)


unix_micros.__doc__ = pysparkfuncs.unix_micros.__doc__


def unix_millis(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("unix_millis", col)


unix_millis.__doc__ = pysparkfuncs.unix_millis.__doc__


def unix_seconds(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("unix_seconds", col)


unix_seconds.__doc__ = pysparkfuncs.unix_seconds.__doc__


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


def try_to_timestamp(col: "ColumnOrName", format: Optional["ColumnOrName"] = None) -> Column:
    if format is not None:
        return _invoke_function_over_columns("try_to_timestamp", col, format)
    else:
        return _invoke_function_over_columns("try_to_timestamp", col)


try_to_timestamp.__doc__ = pysparkfuncs.try_to_timestamp.__doc__


def xpath(xml: "ColumnOrName", path: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("xpath", xml, path)


xpath.__doc__ = pysparkfuncs.xpath.__doc__


def xpath_boolean(xml: "ColumnOrName", path: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("xpath_boolean", xml, path)


xpath_boolean.__doc__ = pysparkfuncs.xpath_boolean.__doc__


def xpath_double(xml: "ColumnOrName", path: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("xpath_double", xml, path)


xpath_double.__doc__ = pysparkfuncs.xpath_double.__doc__


def xpath_number(xml: "ColumnOrName", path: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("xpath_number", xml, path)


xpath_number.__doc__ = pysparkfuncs.xpath_number.__doc__


def xpath_float(xml: "ColumnOrName", path: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("xpath_float", xml, path)


xpath_float.__doc__ = pysparkfuncs.xpath_float.__doc__


def xpath_int(xml: "ColumnOrName", path: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("xpath_int", xml, path)


xpath_int.__doc__ = pysparkfuncs.xpath_int.__doc__


def xpath_long(xml: "ColumnOrName", path: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("xpath_long", xml, path)


xpath_long.__doc__ = pysparkfuncs.xpath_long.__doc__


def xpath_short(xml: "ColumnOrName", path: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("xpath_short", xml, path)


xpath_short.__doc__ = pysparkfuncs.xpath_short.__doc__


def xpath_string(xml: "ColumnOrName", path: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("xpath_string", xml, path)


xpath_string.__doc__ = pysparkfuncs.xpath_string.__doc__


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


def from_utc_timestamp(timestamp: "ColumnOrName", tz: Union[Column, str]) -> Column:
    return _invoke_function_over_columns("from_utc_timestamp", timestamp, lit(tz))


from_utc_timestamp.__doc__ = pysparkfuncs.from_utc_timestamp.__doc__


def to_utc_timestamp(timestamp: "ColumnOrName", tz: Union[Column, str]) -> Column:
    return _invoke_function_over_columns("to_utc_timestamp", timestamp, lit(tz))


to_utc_timestamp.__doc__ = pysparkfuncs.to_utc_timestamp.__doc__


def timestamp_seconds(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("timestamp_seconds", col)


timestamp_seconds.__doc__ = pysparkfuncs.timestamp_seconds.__doc__


def timestamp_millis(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("timestamp_millis", col)


timestamp_millis.__doc__ = pysparkfuncs.timestamp_millis.__doc__


def timestamp_micros(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("timestamp_micros", col)


timestamp_micros.__doc__ = pysparkfuncs.timestamp_micros.__doc__


def timestamp_diff(unit: str, start: "ColumnOrName", end: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("timestampdiff", lit(unit), start, end)


timestamp_diff.__doc__ = pysparkfuncs.timestamp_diff.__doc__


def timestamp_add(unit: str, quantity: "ColumnOrName", ts: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("timestampadd", lit(unit), quantity, ts)


timestamp_add.__doc__ = pysparkfuncs.timestamp_add.__doc__


def window(
    timeColumn: "ColumnOrName",
    windowDuration: str,
    slideDuration: Optional[str] = None,
    startTime: Optional[str] = None,
) -> Column:
    if windowDuration is None or not isinstance(windowDuration, str):
        raise PySparkTypeError(
            errorClass="NOT_STR",
            messageParameters={
                "arg_name": "windowDuration",
                "arg_type": type(windowDuration).__name__,
            },
        )
    if slideDuration is not None and not isinstance(slideDuration, str):
        raise PySparkTypeError(
            errorClass="NOT_STR",
            messageParameters={
                "arg_name": "slideDuration",
                "arg_type": type(slideDuration).__name__,
            },
        )
    if startTime is not None and not isinstance(startTime, str):
        raise PySparkTypeError(
            errorClass="NOT_STR",
            messageParameters={"arg_name": "startTime", "arg_type": type(startTime).__name__},
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
    gapDuration = _enum_to_value(gapDuration)
    if gapDuration is None or not isinstance(gapDuration, (Column, str)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "gapDuration", "arg_type": type(gapDuration).__name__},
        )

    time_col = _to_col(timeColumn)

    if isinstance(gapDuration, Column):
        return _invoke_function("session_window", time_col, gapDuration)
    else:
        return _invoke_function("session_window", time_col, lit(gapDuration))


session_window.__doc__ = pysparkfuncs.session_window.__doc__


def to_unix_timestamp(
    timestamp: "ColumnOrName",
    format: Optional["ColumnOrName"] = None,
) -> Column:
    if format is not None:
        return _invoke_function_over_columns("to_unix_timestamp", timestamp, format)
    else:
        return _invoke_function_over_columns("to_unix_timestamp", timestamp)


to_unix_timestamp.__doc__ = pysparkfuncs.to_unix_timestamp.__doc__


def to_timestamp_ltz(
    timestamp: "ColumnOrName",
    format: Optional["ColumnOrName"] = None,
) -> Column:
    if format is not None:
        return _invoke_function_over_columns("to_timestamp_ltz", timestamp, format)
    else:
        return _invoke_function_over_columns("to_timestamp_ltz", timestamp)


to_timestamp_ltz.__doc__ = pysparkfuncs.to_timestamp_ltz.__doc__


def to_timestamp_ntz(
    timestamp: "ColumnOrName",
    format: Optional["ColumnOrName"] = None,
) -> Column:
    if format is not None:
        return _invoke_function_over_columns("to_timestamp_ntz", timestamp, format)
    else:
        return _invoke_function_over_columns("to_timestamp_ntz", timestamp)


to_timestamp_ntz.__doc__ = pysparkfuncs.to_timestamp_ntz.__doc__


# Partition Transformation Functions


def bucket(numBuckets: Union[Column, int], col: "ColumnOrName") -> Column:
    warnings.warn("Deprecated in 4.0.0, use partitioning.bucket instead.", FutureWarning)
    from pyspark.sql.connect.functions import partitioning

    return partitioning.bucket(numBuckets, col)


bucket.__doc__ = pysparkfuncs.bucket.__doc__


def years(col: "ColumnOrName") -> Column:
    warnings.warn("Deprecated in 4.0.0, use partitioning.years instead.", FutureWarning)
    from pyspark.sql.connect.functions import partitioning

    return partitioning.years(col)


years.__doc__ = pysparkfuncs.years.__doc__


def months(col: "ColumnOrName") -> Column:
    warnings.warn("Deprecated in 4.0.0, use partitioning.months instead.", FutureWarning)
    from pyspark.sql.connect.functions import partitioning

    return partitioning.months(col)


months.__doc__ = pysparkfuncs.months.__doc__


def days(col: "ColumnOrName") -> Column:
    warnings.warn("Deprecated in 4.0.0, use partitioning.days instead.", FutureWarning)
    from pyspark.sql.connect.functions import partitioning

    return partitioning.days(col)


days.__doc__ = pysparkfuncs.days.__doc__


def hours(col: "ColumnOrName") -> Column:
    warnings.warn("Deprecated in 4.0.0, use partitioning.hours instead.", FutureWarning)
    from pyspark.sql.connect.functions import partitioning

    return partitioning.hours(col)


hours.__doc__ = pysparkfuncs.hours.__doc__


def convert_timezone(
    sourceTz: Optional[Column], targetTz: Column, sourceTs: "ColumnOrName"
) -> Column:
    if sourceTz is None:
        return _invoke_function_over_columns("convert_timezone", targetTz, sourceTs)
    else:
        return _invoke_function_over_columns("convert_timezone", sourceTz, targetTz, sourceTs)


convert_timezone.__doc__ = pysparkfuncs.convert_timezone.__doc__


def make_dt_interval(
    days: Optional["ColumnOrName"] = None,
    hours: Optional["ColumnOrName"] = None,
    mins: Optional["ColumnOrName"] = None,
    secs: Optional["ColumnOrName"] = None,
) -> Column:
    _days = lit(0) if days is None else _to_col(days)
    _hours = lit(0) if hours is None else _to_col(hours)
    _mins = lit(0) if mins is None else _to_col(mins)
    _secs = lit(decimal.Decimal(0)) if secs is None else _to_col(secs)

    return _invoke_function_over_columns("make_dt_interval", _days, _hours, _mins, _secs)


make_dt_interval.__doc__ = pysparkfuncs.make_dt_interval.__doc__


def make_interval(
    years: Optional["ColumnOrName"] = None,
    months: Optional["ColumnOrName"] = None,
    weeks: Optional["ColumnOrName"] = None,
    days: Optional["ColumnOrName"] = None,
    hours: Optional["ColumnOrName"] = None,
    mins: Optional["ColumnOrName"] = None,
    secs: Optional["ColumnOrName"] = None,
) -> Column:
    _years = lit(0) if years is None else _to_col(years)
    _months = lit(0) if months is None else _to_col(months)
    _weeks = lit(0) if weeks is None else _to_col(weeks)
    _days = lit(0) if days is None else _to_col(days)
    _hours = lit(0) if hours is None else _to_col(hours)
    _mins = lit(0) if mins is None else _to_col(mins)
    _secs = lit(decimal.Decimal(0)) if secs is None else _to_col(secs)

    return _invoke_function_over_columns(
        "make_interval", _years, _months, _weeks, _days, _hours, _mins, _secs
    )


make_interval.__doc__ = pysparkfuncs.make_interval.__doc__


def make_timestamp(
    years: "ColumnOrName",
    months: "ColumnOrName",
    days: "ColumnOrName",
    hours: "ColumnOrName",
    mins: "ColumnOrName",
    secs: "ColumnOrName",
    timezone: Optional["ColumnOrName"] = None,
) -> Column:
    if timezone is not None:
        return _invoke_function_over_columns(
            "make_timestamp", years, months, days, hours, mins, secs, timezone
        )
    else:
        return _invoke_function_over_columns(
            "make_timestamp", years, months, days, hours, mins, secs
        )


make_timestamp.__doc__ = pysparkfuncs.make_timestamp.__doc__


def make_timestamp_ltz(
    years: "ColumnOrName",
    months: "ColumnOrName",
    days: "ColumnOrName",
    hours: "ColumnOrName",
    mins: "ColumnOrName",
    secs: "ColumnOrName",
    timezone: Optional["ColumnOrName"] = None,
) -> Column:
    if timezone is not None:
        return _invoke_function_over_columns(
            "make_timestamp_ltz", years, months, days, hours, mins, secs, timezone
        )
    else:
        return _invoke_function_over_columns(
            "make_timestamp_ltz", years, months, days, hours, mins, secs
        )


make_timestamp_ltz.__doc__ = pysparkfuncs.make_timestamp_ltz.__doc__


def make_timestamp_ntz(
    years: "ColumnOrName",
    months: "ColumnOrName",
    days: "ColumnOrName",
    hours: "ColumnOrName",
    mins: "ColumnOrName",
    secs: "ColumnOrName",
) -> Column:
    return _invoke_function_over_columns(
        "make_timestamp_ntz", years, months, days, hours, mins, secs
    )


make_timestamp_ntz.__doc__ = pysparkfuncs.make_timestamp_ntz.__doc__


def make_ym_interval(
    years: Optional["ColumnOrName"] = None,
    months: Optional["ColumnOrName"] = None,
) -> Column:
    _years = lit(0) if years is None else _to_col(years)
    _months = lit(0) if months is None else _to_col(months)
    return _invoke_function_over_columns("make_ym_interval", _years, _months)


make_ym_interval.__doc__ = pysparkfuncs.make_ym_interval.__doc__

# Misc Functions


def current_catalog() -> Column:
    return _invoke_function("current_catalog")


current_catalog.__doc__ = pysparkfuncs.current_catalog.__doc__


def current_database() -> Column:
    return _invoke_function("current_database")


current_database.__doc__ = pysparkfuncs.current_database.__doc__


def current_schema() -> Column:
    return _invoke_function("current_schema")


current_schema.__doc__ = pysparkfuncs.current_schema.__doc__


def current_user() -> Column:
    return _invoke_function("current_user")


current_user.__doc__ = pysparkfuncs.current_user.__doc__


def user() -> Column:
    return _invoke_function("user")


user.__doc__ = pysparkfuncs.user.__doc__


def session_user() -> Column:
    return _invoke_function("session_user")


session_user.__doc__ = pysparkfuncs.session_user.__doc__


def assert_true(col: "ColumnOrName", errMsg: Optional[Union[Column, str]] = None) -> Column:
    errMsg = _enum_to_value(errMsg)
    if errMsg is None:
        return _invoke_function_over_columns("assert_true", col)
    if not isinstance(errMsg, (str, Column)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "errMsg", "arg_type": type(errMsg).__name__},
        )
    return _invoke_function_over_columns("assert_true", col, lit(errMsg))


assert_true.__doc__ = pysparkfuncs.assert_true.__doc__


def raise_error(errMsg: Union[Column, str]) -> Column:
    errMsg = _enum_to_value(errMsg)
    if not isinstance(errMsg, (str, Column)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "errMsg", "arg_type": type(errMsg).__name__},
        )
    return _invoke_function_over_columns("raise_error", lit(errMsg))


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
    if numBits not in [0, 224, 256, 384, 512]:
        raise PySparkValueError(
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "numBits",
                "allowed_values": "[0, 224, 256, 384, 512]",
            },
        )
    return _invoke_function("sha2", _to_col(col), lit(numBits))


sha2.__doc__ = pysparkfuncs.sha2.__doc__


def hll_sketch_agg(
    col: "ColumnOrName",
    lgConfigK: Optional[Union[int, Column]] = None,
) -> Column:
    if lgConfigK is None:
        return _invoke_function_over_columns("hll_sketch_agg", col)
    else:
        return _invoke_function_over_columns("hll_sketch_agg", col, lit(lgConfigK))


hll_sketch_agg.__doc__ = pysparkfuncs.hll_sketch_agg.__doc__


def hll_union_agg(
    col: "ColumnOrName",
    allowDifferentLgConfigK: Optional[Union[bool, Column]] = None,
) -> Column:
    if allowDifferentLgConfigK is None:
        return _invoke_function_over_columns("hll_union_agg", col)
    else:
        return _invoke_function_over_columns("hll_union_agg", col, lit(allowDifferentLgConfigK))


hll_union_agg.__doc__ = pysparkfuncs.hll_union_agg.__doc__


def hll_sketch_estimate(col: "ColumnOrName") -> Column:
    return _invoke_function("hll_sketch_estimate", _to_col(col))


hll_sketch_estimate.__doc__ = pysparkfuncs.hll_sketch_estimate.__doc__


def hll_union(
    col1: "ColumnOrName", col2: "ColumnOrName", allowDifferentLgConfigK: Optional[bool] = None
) -> Column:
    if allowDifferentLgConfigK is not None:
        return _invoke_function(
            "hll_union", _to_col(col1), _to_col(col2), lit(allowDifferentLgConfigK)
        )
    else:
        return _invoke_function("hll_union", _to_col(col1), _to_col(col2))


hll_union.__doc__ = pysparkfuncs.hll_union.__doc__


# Predicates Function


def ifnull(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("ifnull", col1, col2)


ifnull.__doc__ = pysparkfuncs.ifnull.__doc__


def isnotnull(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("isnotnull", col)


isnotnull.__doc__ = pysparkfuncs.isnotnull.__doc__


def equal_null(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("equal_null", col1, col2)


equal_null.__doc__ = pysparkfuncs.equal_null.__doc__


def nullif(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("nullif", col1, col2)


nullif.__doc__ = pysparkfuncs.nullif.__doc__


def nullifzero(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("nullifzero", col)


nullifzero.__doc__ = pysparkfuncs.nullifzero.__doc__


def nvl(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("nvl", col1, col2)


nvl.__doc__ = pysparkfuncs.nvl.__doc__


def nvl2(col1: "ColumnOrName", col2: "ColumnOrName", col3: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("nvl2", col1, col2, col3)


nvl2.__doc__ = pysparkfuncs.nvl2.__doc__


def zeroifnull(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("zeroifnull", col)


zeroifnull.__doc__ = pysparkfuncs.zeroifnull.__doc__


def aes_encrypt(
    input: "ColumnOrName",
    key: "ColumnOrName",
    mode: Optional["ColumnOrName"] = None,
    padding: Optional["ColumnOrName"] = None,
    iv: Optional["ColumnOrName"] = None,
    aad: Optional["ColumnOrName"] = None,
) -> Column:
    _mode = lit("GCM") if mode is None else _to_col(mode)
    _padding = lit("DEFAULT") if padding is None else _to_col(padding)
    _iv = lit("") if iv is None else _to_col(iv)
    _aad = lit("") if aad is None else _to_col(aad)

    return _invoke_function_over_columns("aes_encrypt", input, key, _mode, _padding, _iv, _aad)


aes_encrypt.__doc__ = pysparkfuncs.aes_encrypt.__doc__


def aes_decrypt(
    input: "ColumnOrName",
    key: "ColumnOrName",
    mode: Optional["ColumnOrName"] = None,
    padding: Optional["ColumnOrName"] = None,
    aad: Optional["ColumnOrName"] = None,
) -> Column:
    _mode = lit("GCM") if mode is None else _to_col(mode)
    _padding = lit("DEFAULT") if padding is None else _to_col(padding)
    _aad = lit("") if aad is None else _to_col(aad)

    return _invoke_function_over_columns("aes_decrypt", input, key, _mode, _padding, _aad)


aes_decrypt.__doc__ = pysparkfuncs.aes_decrypt.__doc__


def try_aes_decrypt(
    input: "ColumnOrName",
    key: "ColumnOrName",
    mode: Optional["ColumnOrName"] = None,
    padding: Optional["ColumnOrName"] = None,
    aad: Optional["ColumnOrName"] = None,
) -> Column:
    _mode = lit("GCM") if mode is None else _to_col(mode)
    _padding = lit("DEFAULT") if padding is None else _to_col(padding)
    _aad = lit("") if aad is None else _to_col(aad)

    return _invoke_function_over_columns("try_aes_decrypt", input, key, _mode, _padding, _aad)


try_aes_decrypt.__doc__ = pysparkfuncs.try_aes_decrypt.__doc__


def sha(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("sha", col)


sha.__doc__ = pysparkfuncs.sha.__doc__


def input_file_block_length() -> Column:
    return _invoke_function_over_columns("input_file_block_length")


input_file_block_length.__doc__ = pysparkfuncs.input_file_block_length.__doc__


def input_file_block_start() -> Column:
    return _invoke_function_over_columns("input_file_block_start")


input_file_block_start.__doc__ = pysparkfuncs.input_file_block_start.__doc__


def reflect(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("reflect", *cols)


reflect.__doc__ = pysparkfuncs.reflect.__doc__


def java_method(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("java_method", *cols)


java_method.__doc__ = pysparkfuncs.java_method.__doc__


def try_reflect(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("try_reflect", *cols)


try_reflect.__doc__ = pysparkfuncs.try_reflect.__doc__


def version() -> Column:
    return _invoke_function_over_columns("version")


version.__doc__ = pysparkfuncs.version.__doc__


def typeof(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("typeof", col)


typeof.__doc__ = pysparkfuncs.typeof.__doc__


def stack(*cols: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("stack", *cols)


stack.__doc__ = pysparkfuncs.stack.__doc__


def bitmap_bit_position(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bitmap_bit_position", col)


bitmap_bit_position.__doc__ = pysparkfuncs.bitmap_bit_position.__doc__


def bitmap_bucket_number(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bitmap_bucket_number", col)


bitmap_bucket_number.__doc__ = pysparkfuncs.bitmap_bucket_number.__doc__


def bitmap_construct_agg(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bitmap_construct_agg", col)


bitmap_construct_agg.__doc__ = pysparkfuncs.bitmap_construct_agg.__doc__


def bitmap_count(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bitmap_count", col)


bitmap_count.__doc__ = pysparkfuncs.bitmap_count.__doc__


def bitmap_or_agg(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("bitmap_or_agg", col)


bitmap_or_agg.__doc__ = pysparkfuncs.bitmap_or_agg.__doc__


# Call Functions


def call_udf(udfName: str, *cols: "ColumnOrName") -> Column:
    return _invoke_function(udfName, *[_to_col(c) for c in cols])


call_udf.__doc__ = pysparkfuncs.call_udf.__doc__


def unwrap_udt(col: "ColumnOrName") -> Column:
    return _invoke_function("unwrap_udt", _to_col(col))


unwrap_udt.__doc__ = pysparkfuncs.unwrap_udt.__doc__


def udf(
    f: Optional[Union[Callable[..., Any], "DataTypeOrString"]] = None,
    returnType: "DataTypeOrString" = StringType(),
    *,
    useArrow: Optional[bool] = None,
) -> Union["UserDefinedFunctionLike", Callable[[Callable[..., Any]], "UserDefinedFunctionLike"]]:
    if f is None or isinstance(f, (str, DataType)):
        # If DataType has been passed as a positional argument
        # for decorator use it as a returnType
        return_type = f or returnType
        return functools.partial(
            _create_py_udf,
            returnType=return_type,
            useArrow=useArrow,
        )
    else:
        return _create_py_udf(f=f, returnType=returnType, useArrow=useArrow)


udf.__doc__ = pysparkfuncs.udf.__doc__


def udtf(
    cls: Optional[Type] = None,
    *,
    returnType: Optional[Union[StructType, str]] = None,
    useArrow: Optional[bool] = None,
) -> Union["UserDefinedTableFunction", Callable[[Type], "UserDefinedTableFunction"]]:
    if cls is None:
        return functools.partial(_create_py_udtf, returnType=returnType, useArrow=useArrow)
    else:
        return _create_py_udtf(cls=cls, returnType=returnType, useArrow=useArrow)


udtf.__doc__ = pysparkfuncs.udtf.__doc__


def call_function(funcName: str, *cols: "ColumnOrName") -> Column:
    from pyspark.sql.connect.column import Column as ConnectColumn

    expressions = [_to_col(c)._expr for c in cols]
    return ConnectColumn(CallFunction(funcName, expressions))  # type: ignore[arg-type]


call_function.__doc__ = pysparkfuncs.call_function.__doc__


def _test() -> None:
    import sys
    import os
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.functions.builtin

    globs = pyspark.sql.connect.functions.builtin.__dict__.copy()

    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.functions tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.functions.builtin,
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
