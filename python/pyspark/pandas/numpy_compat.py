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
from typing import Any, Callable, no_type_check

import numpy as np

from pyspark.loose_version import LooseVersion
from pyspark.sql import Column, functions as F
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import DoubleType, BooleanType
from pyspark.pandas.base import IndexOpsMixin


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
    "frexp": lambda _: NotImplemented,  # 'frexp' output lengths become different
    # and it cannot be supported via pandas UDF.
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
        F.typeof(c).isin("float", "double"),
        F.when(c.isNull(), c.cast("double"))
        .when(
            c == 0,
            F.when(c.cast("string") == "-0.0", F.lit(float("-inf"))).otherwise(F.lit(float("inf"))),
        )
        .otherwise(F.lit(1.0) / c),
    ).otherwise(
        # Integer input: numpy does integer division (truncated toward zero),
        # so casting the float quotient to long reproduces 1 -> 1, -1 -> -1,
        # and every other magnitude -> 0. Dividing by 0 overflows to the int64
        # minimum, matching numpy's behavior on integer arrays.
        F.when(c == 0, F.lit(float(np.iinfo(np.int64).min))).otherwise(
            (F.lit(1) / c).cast("long").cast("double")
        )
    ),
    "rint": lambda c: F.rint(c.cast("double")),
    "sign": F.signum,
    "signbit": lambda c: F.when(
        # A genuine <NA> from a nullable dtype (e.g. Int64) arrives as a non-double
        # null and must propagate. A NaN from a default (numpy-backed) dtype arrives
        # as a double null and must map to False (np.signbit(nan) is False); it falls
        # through to otherwise(False) below. A nullable Float64 <NA> is also a double
        # null, indistinguishable from a NaN after from_pandas, so it cannot propagate.
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


def _fmod_func(c1: Column, c2: Column) -> Column:
    c1_double = c1.cast("double")
    c2_double = c2.cast("double")

    return F.when(
        F.typeof(c1).isin("float", "double") | F.typeof(c2).isin("float", "double"),
        F.when(c1.isNull() | F.isnan(c1), c1_double)
        .when(c2.isNull() | F.isnan(c2), c2_double)
        .when(c2_double == 0, F.lit(float("nan")))
        .otherwise(F.try_mod(c1_double, c2_double)),
    ).otherwise(
        F.when(c1.isNull() | F.isnan(c1), c1_double)
        .when(c2.isNull() | F.isnan(c2), c2_double)
        .when(c2_double == 0, F.lit(0.0))
        .otherwise(F.try_mod(c1_double, c2_double))
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


def _floor_divide_func(c1: Column, c2: Column) -> Column:
    c1_double = c1.cast("double")
    c2_double = c2.cast("double")

    return F.when(
        F.typeof(c1).isin("float", "double") | F.typeof(c2).isin("float", "double"),
        F.when(c1.isNull() | F.isnan(c1), c1_double)
        .when(c2.isNull() | F.isnan(c2), c2_double)
        .when(
            c1_double.isin(float("-inf"), float("inf")),
            F.when(
                c2_double == 0,
                F.when(
                    (c1_double < 0) != (c2_double.cast("string") == "-0.0"),
                    F.lit(float("-inf")),
                ).otherwise(F.lit(float("inf"))),
            ).otherwise(F.lit(float("nan"))),
        )
        .when(
            c2_double.isin(float("-inf"), float("inf")),
            F.when(c1_double == 0, c1_double / c2_double)
            .when((c1_double < 0) != (c2_double < 0), F.lit(-1.0))
            .otherwise(F.lit(0.0)),
        )
        .when(
            c2_double == 0,
            F.when(c1_double == 0, F.lit(float("nan")))
            .when(
                (c1_double < 0) != (c2_double.cast("string") == "-0.0"),
                F.lit(float("-inf")),
            )
            .otherwise(F.lit(float("inf"))),
        )
        .when(c1_double == 0, c1_double / c2_double)
        .otherwise((c1_double / c2_double) - F.pmod(c1_double / c2_double, F.lit(1.0))),
    ).otherwise(
        # np.floor_divide on pandas Series returns IEEE values for an integral zero divisor.
        F.when(c1.isNull() | F.isnan(c1), c1_double)
        .when(c2.isNull() | F.isnan(c2), c2_double)
        .when(
            c2_double == 0,
            F.when(c1_double == 0, F.lit(float("nan")))
            .when(c1_double < 0, F.lit(float("-inf")))
            .otherwise(F.lit(float("inf"))),
        )
        .otherwise((c1_double / c2_double) - F.pmod(c1_double / c2_double, F.lit(1.0)))
    )


# NumPy 2.3.0 changed how fmax/fmin break a signed-zero tie: for equal operands
# (for example +0.0 and -0.0) it returns the first operand, while older versions
# returned the second. Track the installed NumPy so the result keeps the matching
# sign of zero.
_tie_returns_first_operand = LooseVersion(np.__version__) >= LooseVersion("2.3.0")


def _fmax_func(c1: Column, c2: Column) -> Column:
    tie = c1 if _tie_returns_first_operand else c2
    return (
        F.when(F.isnan(c1.cast("double")), c2)
        .when(F.isnan(c2.cast("double")), c1)
        .when(c1 == c2, tie)
        .otherwise(F.greatest(c1, c2))
        .cast("double")
    )


def _fmin_func(c1: Column, c2: Column) -> Column:
    tie = c1 if _tie_returns_first_operand else c2
    return F.when(c1 == c2, tie).otherwise(F.least(c1, c2)).cast("double")


binary_np_spark_mappings = {
    "arctan2": F.atan2,
    "bitwise_and": lambda c1, c2: c1.bitwiseAND(c2),
    "bitwise_or": lambda c1, c2: c1.bitwiseOR(c2),
    "bitwise_xor": lambda c1, c2: c1.bitwiseXOR(c2),
    "copysign": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.copysign(s1, s2), DoubleType()
    ),
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
    "modf": pandas_udf(lambda s1, s2: np.modf(s1, s2), DoubleType()),  # type: ignore[call-overload]
    "nextafter": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.nextafter(s1, s2), DoubleType()
    ),
    # F.shiftright accepts literal counts only; call_function also accepts a column.
    # NumPy sign-extends counts outside an int64's bit width, unlike JVM shifts.
    "right_shift": lambda c1, c2: F.when(
        (c2 < 0) | (c2 >= 64), F.call_function("shiftright", c1, F.lit(63))
    ).otherwise(F.call_function("shiftright", c1, c2)),
}


# Copied from pandas.
# See also https://docs.scipy.org/doc/numpy/reference/arrays.classes.html#standard-array-subclasses
def maybe_dispatch_ufunc_to_dunder_op(
    ser_or_index: IndexOpsMixin, ufunc: Callable, method: str, *inputs: Any, **kwargs: Any
) -> IndexOpsMixin:
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
) -> IndexOpsMixin:
    from pyspark.pandas.base import column_op

    op_name = ufunc.__name__

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
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.numpy_compat

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
