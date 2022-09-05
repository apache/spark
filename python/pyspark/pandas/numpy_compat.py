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
from pyspark.sql import functions as F, Column
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import DoubleType, LongType, BooleanType

from pyspark.pandas.base import IndexOpsMixin
from pyspark.pandas.spark import functions as SF


unary_np_spark_mappings = {
    "abs": F.abs,
    "absolute": F.abs,
    "arccos": F.acos,
    "arccosh": pandas_udf(lambda s: np.arccosh(s), DoubleType()),  # type: ignore[call-overload]
    "arcsin": F.asin,
    "arcsinh": pandas_udf(lambda s: np.arcsinh(s), DoubleType()),  # type: ignore[call-overload]
    "arctan": F.atan,
    "arctanh": pandas_udf(lambda s: np.arctanh(s), DoubleType()),  # type: ignore[call-overload]
    "bitwise_not": F.bitwiseNOT,
    "cbrt": F.cbrt,
    "ceil": F.ceil,
    # It requires complex type which pandas-on-Spark does not support yet
    "conj": lambda _: NotImplemented,
    "conjugate": lambda _: NotImplemented,  # It requires complex type
    "cos": F.cos,
    "cosh": pandas_udf(lambda s: np.cosh(s), DoubleType()),  # type: ignore[call-overload]
    "deg2rad": pandas_udf(lambda s: np.deg2rad(s), DoubleType()),  # type: ignore[call-overload]
    "degrees": F.degrees,
    "exp": F.exp,
    "exp2": pandas_udf(lambda s: np.exp2(s), DoubleType()),  # type: ignore[call-overload]
    "expm1": F.expm1,
    "fabs": pandas_udf(lambda s: np.fabs(s), DoubleType()),  # type: ignore[call-overload]
    "floor": F.floor,
    "frexp": lambda _: NotImplemented,  # 'frexp' output lengths become different
    # and it cannot be supported via pandas UDF.
    "invert": pandas_udf(lambda s: np.invert(s), DoubleType()),  # type: ignore[call-overload]
    "isfinite": lambda c: c != float("inf"),
    "isinf": lambda c: c == float("inf"),
    "isnan": F.isnan,
    "isnat": lambda c: NotImplemented,  # pandas-on-Spark and PySpark does not have Nat concept.
    "log": F.log,
    "log10": F.log10,
    "log1p": F.log1p,
    "log2": pandas_udf(lambda s: np.log2(s), DoubleType()),  # type: ignore[call-overload]
    "logical_not": lambda c: ~(c.cast(BooleanType())),
    "matmul": lambda _: NotImplemented,  # Can return a NumPy array in pandas.
    "negative": lambda c: c * -1,
    "positive": lambda c: c,
    "rad2deg": pandas_udf(lambda s: np.rad2deg(s), DoubleType()),  # type: ignore[call-overload]
    "radians": F.radians,
    "reciprocal": pandas_udf(  # type: ignore[call-overload]
        lambda s: np.reciprocal(s), DoubleType()
    ),
    "rint": pandas_udf(lambda s: np.rint(s), DoubleType()),  # type: ignore[call-overload]
    "sign": lambda c: F.when(c == 0, 0).when(c < 0, -1).otherwise(1),
    "signbit": lambda c: F.when(c < 0, True).otherwise(False),
    "sin": F.sin,
    "sinh": pandas_udf(lambda s: np.sinh(s), DoubleType()),  # type: ignore[call-overload]
    "spacing": pandas_udf(lambda s: np.spacing(s), DoubleType()),  # type: ignore[call-overload]
    "sqrt": F.sqrt,
    "square": pandas_udf(lambda s: np.square(s), DoubleType()),  # type: ignore[call-overload]
    "tan": F.tan,
    "tanh": pandas_udf(lambda s: np.tanh(s), DoubleType()),  # type: ignore[call-overload]
    "trunc": pandas_udf(lambda s: np.trunc(s), DoubleType()),  # type: ignore[call-overload]
}

binary_np_spark_mappings = {
    "arctan2": F.atan2,
    "bitwise_and": lambda c1, c2: c1.bitwiseAND(c2),
    "bitwise_or": lambda c1, c2: c1.bitwiseOR(c2),
    "bitwise_xor": lambda c1, c2: c1.bitwiseXOR(c2),
    "copysign": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.copysign(s1, s2), DoubleType()
    ),
    "float_power": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.float_power(s1, s2), DoubleType()
    ),
    "floor_divide": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.floor_divide(s1, s2), DoubleType()
    ),
    "fmax": pandas_udf(lambda s1, s2: np.fmax(s1, s2), DoubleType()),  # type: ignore[call-overload]
    "fmin": pandas_udf(lambda s1, s2: np.fmin(s1, s2), DoubleType()),  # type: ignore[call-overload]
    "fmod": pandas_udf(lambda s1, s2: np.fmod(s1, s2), DoubleType()),  # type: ignore[call-overload]
    "gcd": pandas_udf(lambda s1, s2: np.gcd(s1, s2), DoubleType()),  # type: ignore[call-overload]
    "heaviside": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.heaviside(s1, s2), DoubleType()
    ),
    "hypot": F.hypot,
    "lcm": pandas_udf(lambda s1, s2: np.lcm(s1, s2), DoubleType()),  # type: ignore[call-overload]
    "ldexp": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.ldexp(s1, s2), DoubleType()
    ),
    "left_shift": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.left_shift(s1, s2), LongType()
    ),
    "logaddexp": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.logaddexp(s1, s2), DoubleType()
    ),
    "logaddexp2": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.logaddexp2(s1, s2), DoubleType()
    ),
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
    "right_shift": pandas_udf(  # type: ignore[call-overload]
        lambda s1, s2: np.right_shift(s1, s2), LongType()
    ),
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
            args = [SF.lit(inp) if not isinstance(inp, Column) else inp for inp in args]
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
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.numpy_compat,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
