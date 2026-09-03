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
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, no_type_check

import numpy as np

from pyspark.loose_version import LooseVersion
from pyspark.pandas._typing import SeriesOrIndex
from pyspark.pandas.base import IndexOpsMixin
from pyspark.pandas.typedef.typehints import as_spark_type
from pyspark.pandas.utils import _floor_divide_func
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DoubleType,
    FloatType,
    IntegralType,
    NullType,
    NumericType,
    TimestampNTZType,
    TimestampType,
)

unary_np_spark_mappings = {
    "abs": F.abs,
    "absolute": F.abs,
    "arccos": F.acos,
    "arccosh": F.acosh,
    "arcsin": F.asin,
    "arcsinh": F.asinh,
    "arctan": F.atan,
    "arctanh": F.atanh,
    "bitwise_not": F.bitwiseNOT,
    "cbrt": F.cbrt,
    "ceil": F.ceil,
    # It requires complex type which pandas-on-Spark does not support yet
    "conj": lambda _: NotImplemented,
    "conjugate": lambda _: NotImplemented,  # It requires complex type
    "cos": F.cos,
    "cosh": F.cosh,
    "deg2rad": F.radians,
    "degrees": F.degrees,
    "exp": F.exp,
    "exp2": lambda c: F.pow(F.lit(2.0), c),
    "expm1": F.expm1,
    "fabs": lambda c: F.abs(c.cast("double")),
    "floor": F.floor,
    "invert": F.bitwise_not,
    "isfinite": lambda c: F.coalesce(
        ~(F.isnan(c) | (c == float("inf")) | (c == float("-inf"))), F.lit(False)
    ),
    "isinf": lambda c: F.coalesce((c == float("inf")) | (c == float("-inf")), F.lit(False)),
    "isnan": F.isnan,
    "isnat": lambda c: NotImplemented,  # pandas-on-Spark and PySpark does not have Nat concept.
    "log": F.log,
    "log10": F.log10,
    "log1p": F.log1p,
    "log2": lambda c: F.when(c == 0, F.lit(float("-inf"))).otherwise(F.log2(c)),
    "logical_not": lambda c: ~(c.cast(BooleanType())),
    "matmul": lambda _: NotImplemented,  # Can return a NumPy array in pandas.
    "negative": F.negative,
    "positive": F.positive,
    "rad2deg": F.degrees,
    "radians": F.radians,
    "reciprocal": lambda c: F.when(
        # Floating-point and decimal inputs take a true reciprocal; numpy
        # applies it element-wise to Decimal objects as well.
        F.typeof(c).isin("float", "double") | F.typeof(c).startswith("decimal"),
        F.when(c.isNull(), c.cast("double"))
        .when(
            # Cast to double so the zero check also analyzes for the integer and
            # boolean columns that fall through to the otherwise branch (Spark
            # type-checks every branch of the CASE, not just the taken one).
            c.cast("double") == 0,
            F.when(c.cast("string") == "-0.0", F.lit(float("-inf"))).otherwise(F.lit(float("inf"))),
        )
        .otherwise(F.lit(1.0) / c.cast("double")),
    ).otherwise(
        # Integer and boolean inputs: numpy does integer division (truncated
        # toward zero), so only +/-1 survive and every other magnitude -> 0.
        # Dividing by 0 overflows to the width-specific integer minimum for int
        # (int32) and bigint (int64), while narrower widths (tinyint, smallint,
        # and boolean promoted to int8) return 0. Cast through long so boolean
        # and narrower integers can take part in the division.
        F.when(
            c.cast("long") == 0,
            F.when(F.typeof(c) == "int", F.lit(float(np.iinfo(np.int32).min)))
            .when(F.typeof(c) == "bigint", F.lit(float(np.iinfo(np.int64).min)))
            .otherwise(F.lit(0.0)),
        ).otherwise((F.lit(1) / c.cast("long")).cast("long").cast("double"))
    ),
    "rint": lambda c: F.rint(c.cast("double")),
    "sign": F.signum,
    "signbit": lambda c: F.when(
        # A genuine <NA> from a nullable dtype (e.g. Int64) arrives as a non-floating null
        # and must propagate. A NaN from a default (numpy-backed) dtype arrives as a floating
        # null and must map to False (np.signbit(nan) is False); it falls through to
        # otherwise(False) below. Two cases this expression cannot match, seeing only the Spark
        # value and not the pandas dtype: a nullable float dtype's <NA> (Float32 or Float64) is
        # also a floating null, indistinguishable from a NaN after from_pandas, so it reads False
        # instead of propagating; and the sign of a NaN never reaches here (from_pandas nulls a
        # NaN, and a NaN computed in Spark arrives as +NaN), so a negative NaN reads False where
        # np.signbit reports True.
        c.isNull() & ~F.typeof(c).isin("float", "double"),
        F.lit(None).cast("boolean"),
    )
    .when((c < 0) | (c.cast("string") == "-0.0"), True)
    .otherwise(False),
    "sin": F.sin,
    "sinh": F.sinh,
    "spacing": pandas_udf(lambda s: np.spacing(s), DoubleType()),  # type: ignore[call-overload]
    "sqrt": F.sqrt,
    "square": lambda c: c.cast("double") * c,
    "tan": F.tan,
    "tanh": F.tanh,
    "trunc": lambda c: F.when(
        c.cast("double").isNull()
        | F.isnan(c.cast("double"))
        | c.cast("double").isin(float("-inf"), float("inf")),
        c.cast("double"),
    ).otherwise(
        F.signum(c.cast("double"))
        * (F.abs(c.cast("double")) - (F.abs(c.cast("double")) % F.lit(1.0)))
    ),
}


def _copysign_func(c1: Column, c2: Column) -> Column:
    # Sign of y is taken from its IEEE-754 sign bit, so -0.0 counts as negative.
    # c2 < 0 misses -0.0, so detect it via the string cast, the same way the
    # 'reciprocal' mapping distinguishes -0.0 from 0.0. NaN's sign bit is positive
    # and c2 < 0 is already false for NaN, so it correctly falls through to +1.0.
    sign = F.when((c2 < 0) | (c2.cast("string") == "-0.0"), F.lit(-1.0)).otherwise(F.lit(1.0))
    # An integer y column's NULL is a genuine missing value and propagates. A
    # float/double column instead stores its missing value as NaN (surfaced as a
    # Spark NULL by pandas-on-Spark), for which copysign(x, NaN) returns |x|. A
    # nullable Float64 <NA> collapses to that same Spark NULL, so it is likewise
    # treated as NaN and returns |x| rather than propagating; this cannot be
    # distinguished here and matches the prior pandas_udf behavior.
    return F.when(
        c2.isNull() & ~F.typeof(c2).isin("float", "double"), F.lit(None).cast("double")
    ).otherwise(F.abs(c1.cast("double")) * sign)


def _fmod_func(c1: Column, c2: Column) -> Column:
    c1_double = c1.cast("double")
    c2_double = c2.cast("double")
    integral_types = ["tinyint", "smallint", "int", "bigint"]

    return (
        # A null or a nan propagates for every operand type.
        F.when(c1.isNull() | F.isnan(c1), c1_double)
        .when(c2.isNull() | F.isnan(c2), c2_double)
        # Integral operands take the remainder as longs: a double cannot hold 9007199254740993.
        .when(
            F.typeof(c1).isin(integral_types) & F.typeof(c2).isin(integral_types),
            F.when(c2_double == 0, F.lit(0.0)).otherwise(
                F.try_mod(c1.cast("long"), c2.cast("long")).cast("double")
            ),
        )
        # Floating operands, where a zero divisor is nan rather than the 0 above.
        .when(
            F.typeof(c1).isin("float", "double") | F.typeof(c2).isin("float", "double"),
            F.when(c2_double == 0, F.lit(float("nan"))).otherwise(F.try_mod(c1_double, c2_double)),
        )
        # A decimal falls through here, since typeof carries its precision, as in decimal(10,2).
        # np.fmod raises on Decimal objects, so there is no NumPy behavior to match: a zero
        # divisor returns 0 and any other divisor takes the remainder in double.
        .otherwise(F.when(c2_double == 0, F.lit(0.0)).otherwise(F.try_mod(c1_double, c2_double)))
    )


def _logaddexp_func(c1: Column, c2: Column, base2: bool = False) -> Column:
    c1_double = c1.cast("double")
    c2_double = c2.cast("double")
    difference = F.abs(c1_double - c2_double)
    maximum = F.greatest(c1_double, c2_double)
    if base2:
        log_term = F.log1p(F.pow(F.lit(2.0), -difference)) / F.log(F.lit(2.0))
    else:
        log_term = F.log1p(F.exp(-difference))

    return (
        F.when(c1_double.isNull() | F.isnan(c1_double), c1_double)
        .when(c2_double.isNull() | F.isnan(c2_double), c2_double)
        .when((c1_double == float("inf")) | (c2_double == float("inf")), F.lit(float("inf")))
        .when(c1_double == float("-inf"), c2_double + F.lit(0.0))
        .when(c2_double == float("-inf"), c1_double + F.lit(0.0))
        .otherwise(maximum + log_term)
    )


# NumPy 2.3.0 changed how fmax/fmin break a signed-zero tie: for equal operands
# (for example +0.0 and -0.0) it returns the first operand, while older versions
# returned the second. Track the installed NumPy so the result keeps the matching
# sign of zero.
_tie_returns_first_operand = LooseVersion(np.__version__) >= LooseVersion("2.3.0")


# Every branch returns one of the operands, so without a cast the result keeps their
# common type: an integral pair stays exact past 2^53 and the dtype matches NumPy's.
def _fmax_func(c1: Column, c2: Column) -> Column:
    tie = c1 if _tie_returns_first_operand else c2
    return (
        F.when(F.isnan(c1.cast("double")), c2)
        .when(F.isnan(c2.cast("double")), c1)
        .when(c1 == c2, tie)
        .otherwise(F.greatest(c1, c2))
    )


def _fmin_func(c1: Column, c2: Column) -> Column:
    tie = c1 if _tie_returns_first_operand else c2
    return F.when(c1 == c2, tie).otherwise(F.least(c1, c2))


binary_np_spark_mappings = {
    "arctan2": F.atan2,
    "bitwise_and": lambda c1, c2: c1.bitwiseAND(c2),
    "bitwise_or": lambda c1, c2: c1.bitwiseOR(c2),
    "bitwise_xor": lambda c1, c2: c1.bitwiseXOR(c2),
    "copysign": _copysign_func,
    "float_power": lambda c1, c2: F.pow(c1.cast("double"), c2.cast("double")),
    # np.floor_divide dispatches to the pandas-on-Spark floordiv dunder operation
    # before this registry is consulted, so this mapping is not used for that case.
    "floor_divide": _floor_divide_func,
    "fmax": _fmax_func,
    "fmin": _fmin_func,
    "fmod": _fmod_func,
    "gcd": pandas_udf(lambda s1, s2: np.gcd(s1, s2), DoubleType()),  # type: ignore[call-overload]
    "heaviside": lambda c1, c2: F.when(
        c1.isNull() | F.isnan(c1.cast("double")),
        c1.cast("double"),
    )
    .when(c1 < 0, F.lit(0.0))
    .when(c1 == 0, c2.cast("double"))
    .otherwise(F.lit(1.0)),
    "hypot": F.hypot,
    "lcm": pandas_udf(lambda s1, s2: np.lcm(s1, s2), DoubleType()),  # type: ignore[call-overload]
    "ldexp": lambda c1, c2: F.when(
        c1.cast("double").isin(0.0, float("-inf"), float("inf")),
        c1.cast("double"),
    ).otherwise(c1.cast("double") * F.pow(F.lit(2.0), c2)),
    # F.shiftleft accepts literal counts only; call_function also accepts a column.
    # NumPy returns zero for counts outside an int64's bit width, unlike JVM shifts.
    "left_shift": lambda c1, c2: F.when((c2 < 0) | (c2 >= 64), F.lit(0)).otherwise(
        F.call_function("shiftleft", c1, c2)
    ),
    "logaddexp": _logaddexp_func,
    "logaddexp2": lambda c1, c2: _logaddexp_func(c1, c2, base2=True),
    "logical_and": lambda c1, c2: c1.cast(BooleanType()) & c2.cast(BooleanType()),
    "logical_or": lambda c1, c2: c1.cast(BooleanType()) | c2.cast(BooleanType()),
    "logical_xor": lambda c1, c2: (
        # mimics xor by logical operators.
        (c1.cast(BooleanType()) | c2.cast(BooleanType()))
        & (~(c1.cast(BooleanType())) | ~(c2.cast(BooleanType())))
    ),
    "maximum": F.greatest,
    "minimum": F.least,
    "nextafter": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.nextafter(s1, s2), DoubleType()
    ),
    # F.shiftright accepts literal counts only; call_function also accepts a column.
    # NumPy sign-extends counts outside an int64's bit width, unlike JVM shifts.
    "right_shift": lambda c1, c2: F.when(
        (c2 < 0) | (c2 >= 64), F.call_function("shiftright", c1, F.lit(63))
    ).otherwise(F.call_function("shiftright", c1, c2)),
}


def _modf_fractional_func(c: Column) -> Column:
    c_double = c.cast("double")
    # signum * (abs % 1) keeps the fractional magnitude with the sign of the input,
    # including the signed zero of a whole number (for example -2.0 -> -0.0), the same
    # way the "trunc" mapping (reused below for the integral part) relies on signum.
    fractional = F.signum(c_double) * (F.abs(c_double) % F.lit(1.0))
    return (
        F.when(c.isNull() | F.isnan(c_double), c_double)
        # +-inf has no fractional part; numpy returns a zero with the input's sign.
        .when(c_double == float("inf"), F.lit(0.0))
        .when(c_double == float("-inf"), F.lit(-0.0))
        .otherwise(fractional)
    )


def _frexp_scale(c_double: Column, exponent: Column) -> Column:
    # x * 2**-exponent, in two halves because a single 2**exponent is not finite across
    # frexp's range (-1073 for the smallest subnormal, 1024 for the largest double).
    half = F.floor(exponent / F.lit(2.0))
    return c_double / F.pow(F.lit(2.0), half) / F.pow(F.lit(2.0), exponent - half)


def _frexp_finite_exponent(c_double: Column) -> Column:
    # floor(log2|x|) + 1, which the logarithm can put on the wrong side of an integer next
    # to a power of two, but never by more than one: check the scaled magnitude against
    # frexp's [0.5, 1) range and step the estimate where it falls outside.
    estimate = F.floor(F.log2(F.abs(c_double))) + F.lit(1)
    magnitude = F.abs(_frexp_scale(c_double, estimate))
    return (
        F.when(magnitude >= F.lit(1.0), estimate + F.lit(1))
        .when(magnitude < F.lit(0.5), estimate - F.lit(1))
        .otherwise(estimate)
    )


def _frexp_is_special_value(c: Column) -> Column:
    # frexp returns 0, +-inf and nan unchanged as the mantissa, keeping the sign of a zero,
    # and pairs them with a zero exponent. from_pandas delivers a NaN as a null; the sign of a
    # NaN never survives to here, so numpy's -nan mantissa reads +nan.
    c_double = c.cast("double")
    return (
        c.isNull()
        | F.isnan(c_double)
        | (c_double == 0)
        | c_double.isin(float("-inf"), float("inf"))
    )


def _frexp_mantissa_func(c: Column) -> Column:
    c_double = c.cast("double")
    return F.when(_frexp_is_special_value(c), c_double).otherwise(
        _frexp_scale(c_double, _frexp_finite_exponent(c_double))
    )


def _frexp_exponent_func(c: Column) -> Column:
    return (
        F.when(
            # A genuine <NA> from a nullable dtype (e.g. Int64) arrives as a non-floating null
            # and must propagate; a NaN arrives as a floating null and takes the zero branch
            # below. A nullable float dtype's <NA> (Float32 or Float64) is a floating null too,
            # indistinguishable from a NaN after from_pandas, so it reads 0 instead.
            c.isNull() & ~F.typeof(c).isin("float", "double"),
            F.lit(None),
        )
        .when(_frexp_is_special_value(c), F.lit(0))
        .otherwise(_frexp_finite_exponent(c.cast("double")))
        # numpy returns the exponent as an int32.
        .cast("int")
    )


# Every multi-output ufunc numpy ships (modf, frexp) has exactly two outputs, so each entry
# maps to a pair of Column->Column functions applied independently and returned as a 2-tuple
# that numpy's __array_ufunc__ unpacks (for example `fractional, integral = np.modf(series)`).
multi_output_np_spark_mappings = {
    # np.frexp(x) -> (mantissa, exponent) with x == mantissa * 2**exponent, the mantissa
    # keeping x's sign at a magnitude in [0.5, 1).
    "frexp": (_frexp_mantissa_func, _frexp_exponent_func),
    # np.modf(x) -> (fractional part, integral part); the integral part is exactly trunc.
    "modf": (_modf_fractional_func, unary_np_spark_mappings["trunc"]),
}


# NumPy accepts a boolean wherever it accepts an integer.
_INTEGRAL_INPUT_TYPES = (BooleanType, IntegralType)
# Some ufuncs reject a decimal column, so the float types are spelled out rather than using
# NumericType, which includes DecimalType.
_NUMERIC_INPUT_TYPES = (BooleanType, IntegralType, FloatType, DoubleType)
# For the ufuncs that accept a decimal column too.
_NUMERIC_OR_DECIMAL_INPUT_TYPES = (BooleanType, NumericType)
# np.isnan and its neighbours have a datetime64 loop, which reports NaT.
_NUMERIC_OR_TIMESTAMP_INPUT_TYPES = _NUMERIC_INPUT_TYPES + (TimestampType, TimestampNTZType)
# np.sign is the one ufunc with no boolean loop, and NumericType excludes BooleanType.
_SIGN_INPUT_TYPES = (NumericType,)
# pandas reads an all-null column as False for the two-operand bitwise operators.
_BITWISE_INPUT_TYPES = _INTEGRAL_INPUT_TYPES + (NullType,)


# The operand types NumPy accepts, one tuple of Spark types per operand. Without this Spark
# casts the operand instead: np.fmod on a string column answered 1.0. A ufunc is absent when its
# accepted types cannot be listed per operand (np.fmax takes any pair it can compare), when it is
# still a pandas_udf, where NumPy runs per value, or when the entry is unreachable.
_np_spark_accepted_types: Dict[str, Tuple[Tuple[type, ...], ...]] = {
    "absolute": (_NUMERIC_OR_DECIMAL_INPUT_TYPES,),
    "arccos": (_NUMERIC_INPUT_TYPES,),
    "arccosh": (_NUMERIC_INPUT_TYPES,),
    "arcsin": (_NUMERIC_INPUT_TYPES,),
    "arcsinh": (_NUMERIC_INPUT_TYPES,),
    "arctan": (_NUMERIC_INPUT_TYPES,),
    "arctan2": (_NUMERIC_INPUT_TYPES, _NUMERIC_INPUT_TYPES),
    "arctanh": (_NUMERIC_INPUT_TYPES,),
    "bitwise_and": (_BITWISE_INPUT_TYPES, _BITWISE_INPUT_TYPES),
    "bitwise_or": (_BITWISE_INPUT_TYPES, _BITWISE_INPUT_TYPES),
    "bitwise_xor": (_BITWISE_INPUT_TYPES, _BITWISE_INPUT_TYPES),
    "cbrt": (_NUMERIC_INPUT_TYPES,),
    "ceil": (_NUMERIC_OR_DECIMAL_INPUT_TYPES,),
    "copysign": (_NUMERIC_INPUT_TYPES, _NUMERIC_INPUT_TYPES),
    "cos": (_NUMERIC_INPUT_TYPES,),
    "cosh": (_NUMERIC_INPUT_TYPES,),
    "deg2rad": (_NUMERIC_INPUT_TYPES,),
    "degrees": (_NUMERIC_INPUT_TYPES,),
    "exp": (_NUMERIC_OR_DECIMAL_INPUT_TYPES,),
    "exp2": (_NUMERIC_INPUT_TYPES,),
    "expm1": (_NUMERIC_INPUT_TYPES,),
    "fabs": (_NUMERIC_INPUT_TYPES,),
    "float_power": (_NUMERIC_INPUT_TYPES, _NUMERIC_INPUT_TYPES),
    "floor": (_NUMERIC_OR_DECIMAL_INPUT_TYPES,),
    "fmod": (_NUMERIC_INPUT_TYPES, _NUMERIC_INPUT_TYPES),
    "frexp": (_NUMERIC_INPUT_TYPES,),
    "heaviside": (_NUMERIC_INPUT_TYPES, _NUMERIC_INPUT_TYPES),
    "hypot": (_NUMERIC_INPUT_TYPES, _NUMERIC_INPUT_TYPES),
    "invert": (_INTEGRAL_INPUT_TYPES,),
    "isfinite": (_NUMERIC_OR_TIMESTAMP_INPUT_TYPES,),
    "isinf": (_NUMERIC_OR_TIMESTAMP_INPUT_TYPES,),
    "isnan": (_NUMERIC_OR_TIMESTAMP_INPUT_TYPES,),
    # np.ldexp builds x * 2**exp and takes the exponent from an integer loop only.
    "ldexp": (_NUMERIC_INPUT_TYPES, _INTEGRAL_INPUT_TYPES),
    "left_shift": (_INTEGRAL_INPUT_TYPES, _INTEGRAL_INPUT_TYPES),
    "log": (_NUMERIC_INPUT_TYPES,),
    "log10": (_NUMERIC_OR_DECIMAL_INPUT_TYPES,),
    "log1p": (_NUMERIC_INPUT_TYPES,),
    "log2": (_NUMERIC_INPUT_TYPES,),
    "logaddexp": (_NUMERIC_INPUT_TYPES, _NUMERIC_INPUT_TYPES),
    "logaddexp2": (_NUMERIC_INPUT_TYPES, _NUMERIC_INPUT_TYPES),
    "logical_xor": (_NUMERIC_OR_TIMESTAMP_INPUT_TYPES, _NUMERIC_OR_TIMESTAMP_INPUT_TYPES),
    "modf": (_NUMERIC_INPUT_TYPES,),
    "negative": (_NUMERIC_OR_DECIMAL_INPUT_TYPES,),
    "positive": (_NUMERIC_OR_DECIMAL_INPUT_TYPES,),
    "rad2deg": (_NUMERIC_INPUT_TYPES,),
    "radians": (_NUMERIC_INPUT_TYPES,),
    "reciprocal": (_NUMERIC_OR_DECIMAL_INPUT_TYPES,),
    "right_shift": (_INTEGRAL_INPUT_TYPES, _INTEGRAL_INPUT_TYPES),
    "rint": (_NUMERIC_INPUT_TYPES,),
    "sign": (_SIGN_INPUT_TYPES,),
    "signbit": (_NUMERIC_INPUT_TYPES,),
    "sin": (_NUMERIC_INPUT_TYPES,),
    "sinh": (_NUMERIC_INPUT_TYPES,),
    "sqrt": (_NUMERIC_OR_DECIMAL_INPUT_TYPES,),
    "square": (_NUMERIC_OR_DECIMAL_INPUT_TYPES,),
    "tan": (_NUMERIC_INPUT_TYPES,),
    "tanh": (_NUMERIC_INPUT_TYPES,),
    "trunc": (_NUMERIC_OR_DECIMAL_INPUT_TYPES,),
}


def _check_operand_types(op_name: str, inputs: Tuple[Any, ...]) -> None:
    accepted_per_operand = _np_spark_accepted_types.get(op_name)
    if accepted_per_operand is None:
        return

    data_types: List[Optional[DataType]] = []
    for inp in inputs:
        if isinstance(inp, IndexOpsMixin):
            data_types.append(inp.spark.data_type)
        else:
            # A scalar has no Spark type; an unmappable one gives None and goes unchecked.
            data_types.append(as_spark_type(type(inp), raise_error=False))
    for data_type, accepted in zip(data_types, accepted_per_operand):
        if data_type is not None and not isinstance(data_type, accepted):
            raise TypeError(
                "ufunc '%s' is not supported for the input types (%s)."
                % (
                    op_name,
                    ", ".join("unknown" if dt is None else dt.simpleString() for dt in data_types),
                )
            )


# Copied from pandas.
# See also https://docs.scipy.org/doc/numpy/reference/arrays.classes.html#standard-array-subclasses
def maybe_dispatch_ufunc_to_dunder_op(
    ser_or_index: IndexOpsMixin, ufunc: Callable, method: str, *inputs: Any, **kwargs: Any
) -> SeriesOrIndex:
    special = {
        "add",
        "sub",
        "mul",
        "pow",
        "mod",
        "floordiv",
        "truediv",
        "divmod",
        "eq",
        "ne",
        "lt",
        "gt",
        "le",
        "ge",
        "remainder",
        "matmul",
    }
    aliases = {
        "absolute": "abs",
        "multiply": "mul",
        "floor_divide": "floordiv",
        "true_divide": "truediv",
        "power": "pow",
        "remainder": "mod",
        "divide": "truediv",
        "equal": "eq",
        "not_equal": "ne",
        "less": "lt",
        "less_equal": "le",
        "greater": "gt",
        "greater_equal": "ge",
    }

    # For op(., Array) -> Array.__r{op}__
    flipped = {
        "lt": "__gt__",
        "le": "__ge__",
        "gt": "__lt__",
        "ge": "__le__",
        "eq": "__eq__",
        "ne": "__ne__",
    }

    op_name = ufunc.__name__
    op_name = aliases.get(op_name, op_name)

    @no_type_check
    def not_implemented(*args, **kwargs):
        return NotImplemented

    if method == "__call__" and op_name in special and kwargs.get("out") is None:
        if isinstance(inputs[0], type(ser_or_index)):
            name = "__{}__".format(op_name)
            return getattr(ser_or_index, name, not_implemented)(inputs[1])
        else:
            name = flipped.get(op_name, "__r{}__".format(op_name))
            return getattr(ser_or_index, name, not_implemented)(inputs[0])
    else:
        return NotImplemented


# See also https://docs.scipy.org/doc/numpy/reference/arrays.classes.html#standard-array-subclasses
def maybe_dispatch_ufunc_to_spark_func(
    ser_or_index: IndexOpsMixin, ufunc: Callable, method: str, *inputs: Any, **kwargs: Any
) -> Union[SeriesOrIndex, Tuple[SeriesOrIndex, SeriesOrIndex]]:
    from pyspark.pandas.base import column_op

    op_name = ufunc.__name__

    # Check before building the expression, so the error comes from the ufunc call itself.
    if method == "__call__" and kwargs.get("out") is None:
        _check_operand_types(op_name, inputs)

    if (
        method == "__call__"
        and op_name in multi_output_np_spark_mappings
        and kwargs.get("out") is None
    ):
        # These ufuncs are unary in their input, so the single input is always a Series
        # that column_op unwraps to a Column -- no literal wrapping needed. Build one
        # Series per output and return them as a 2-tuple (see the mapping's docstring).
        first_func, second_func = multi_output_np_spark_mappings[op_name]
        return column_op(first_func)(*inputs), column_op(second_func)(*inputs)

    if (
        method == "__call__"
        and (op_name in unary_np_spark_mappings or op_name in binary_np_spark_mappings)
        and kwargs.get("out") is None
    ):
        np_spark_map_func = unary_np_spark_mappings.get(op_name) or binary_np_spark_mappings.get(
            op_name
        )

        @no_type_check
        def convert_arguments(*args):
            args = [F.lit(inp) for inp in args]
            return np_spark_map_func(*args)

        return column_op(convert_arguments)(*inputs)
    else:
        return NotImplemented


def _test() -> None:
    import doctest
    import os
    import sys

    import pyspark.pandas.numpy_compat
    from pyspark.sql import SparkSession

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.numpy_compat.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.numpy_compat tests")
        .getOrCreate()
    )
    failure_count, test_count = doctest.testmod(
        pyspark.pandas.numpy_compat,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
