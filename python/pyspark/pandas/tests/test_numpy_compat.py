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

import platform
import unittest
from decimal import Decimal

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.loose_version import LooseVersion
from pyspark.pandas import reset_option, set_option
from pyspark.sql import functions as F
from pyspark.testing.pandasutils import PandasOnSparkTestCase

# np.reciprocal(int 0) and the fmax/fmin signed-zero tie are unspecified by C/IEEE, so NumPy's
# own answer varies by CPU architecture and NumPy version. pandas-on-Spark returns one fixed
# value everywhere, which matches NumPy only on the environment it was verified against, so the
# tests comparing the two run only there.
_numpy_matches_spark = (
    platform.system() == "Linux"
    and platform.machine() == "x86_64"
    and LooseVersion(np.__version__) >= LooseVersion("2.3.0")
)
_skip_if_numpy_differs = unittest.skipIf(
    not _numpy_matches_spark,
    "NumPy's reciprocal(int 0) and fmax/fmin signed-zero tie vary by architecture and NumPy "
    "version, while pandas-on-Spark returns one fixed value that matches NumPy only on "
    "Linux x86_64 with NumPy >= 2.3.0",
)


class NumPyCompatTestsMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Some nanosecond->microsecond conversions throw loss of precision errors
        cls.spark.conf.set("spark.sql.execution.pandas.convertToArrowArraySafely", "false")

    blacklist = [
        # Pandas-on-Spark does not currently support
        "conj",
        "conjugate",
        "isnat",
        "matmul",
        # Values are close enough but tests failed.
        "log",  # flaky
        "log10",  # flaky
        "log1p",  # flaky
    ]
    # The sweeps below draw random integers including 0, where reciprocal diverges.
    if not _numpy_matches_spark:
        blacklist = blacklist + ["reciprocal"]

    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_np_add_series(self):
        psdf = self.psdf
        pdf = self.pdf

        self.assert_eq(np.add(psdf.a, psdf.b), np.add(pdf.a, pdf.b))

        psdf = self.psdf
        pdf = self.pdf
        self.assert_eq(np.add(psdf.a, 1), np.add(pdf.a, 1))

    def test_np_add_index(self):
        k_index = self.psdf.index
        p_index = self.pdf.index
        self.assert_eq(np.add(k_index, k_index), np.add(p_index, p_index))

    def test_np_unsupported_series(self):
        psdf = self.psdf
        with self.assertRaisesRegex(NotImplementedError, "pandas.*not.*support.*sqrt.*"):
            np.sqrt(psdf.a, psdf.b)

    def test_np_unsupported_frame(self):
        psdf = self.psdf
        with self.assertRaisesRegex(NotImplementedError, "on-Spark.*not.*support.*sqrt.*"):
            np.sqrt(psdf, psdf)

        psdf1 = ps.DataFrame({"A": [1, 2, 3]})
        psdf2 = ps.DataFrame({("A", "B"): [4, 5, 6]})
        with self.assertRaisesRegex(ValueError, "cannot join with no overlapping index names"):
            np.left_shift(psdf1, psdf2)

    @property
    def operand_type_pdf(self):
        return pd.DataFrame(
            {
                "integer": [7, 8],
                "double": [7.5, 8.5],
                "decimal": [Decimal("7.5"), Decimal("8.5")],
                "string": ["7", "8"],
                "timestamp": pd.to_datetime(["2020-01-01", "2020-01-02"]),
                "boolean": [True, False],
                # All nulls, which Spark types as void.
                "null": [None, None],
            }
        )

    def test_np_unsupported_operand_types(self):
        # Not at module scope: importing numpy_compat builds its pandas_udf entries, which
        # bind the Column class of whichever session mode is active.
        from pyspark.pandas.numpy_compat import (
            _np_spark_accepted_types,
            binary_np_spark_mappings,
            multi_output_np_spark_mappings,
            unary_np_spark_mappings,
        )

        psdf = ps.from_pandas(self.operand_type_pdf)

        # No ufunc accepts a string column, so one loop covers the whole table.
        for op_name, accepted_per_operand in _np_spark_accepted_types.items():
            with self.subTest(name=op_name):
                self.assertTrue(
                    op_name in unary_np_spark_mappings
                    or op_name in binary_np_spark_mappings
                    or op_name in multi_output_np_spark_mappings,
                    "%s has no mapping entry" % op_name,
                )
                with self.assertRaisesRegex(
                    TypeError,
                    "ufunc '%s' is not supported for the input types .*string" % op_name,
                ):
                    getattr(np, op_name)(*[psdf["string"]] * len(accepted_per_operand))

    def test_np_unsupported_operand_types_by_ufunc(self):
        # The types only some ufuncs reject, and which operand carries the rejected one.
        psdf = ps.from_pandas(self.operand_type_pdf)

        for np_func, columns, unsupported in (
            (np.cosh, ["timestamp"], "timestamp"),
            (np.cosh, ["null"], "void"),
            (np.fmod, ["decimal", "decimal"], "decimal"),
            # np.invert and the shifts have integer loops only.
            (np.invert, ["double"], "double"),
            # The rejected operand is the second one here, the first one below.
            (np.left_shift, ["integer", "double"], "double"),
            (np.copysign, ["double", "string"], "string"),
            (np.logaddexp, ["timestamp", "double"], "timestamp"),
            # np.ldexp takes its exponent from an integer loop.
            (np.ldexp, ["double", "double"], "double"),
            # np.sign is the only ufunc here with no boolean loop.
            (np.sign, ["boolean"], "boolean"),
        ):
            with self.subTest(np_func=np_func.__name__, unsupported=unsupported):
                with self.assertRaisesRegex(
                    TypeError,
                    "ufunc '%s' is not supported for the input types .*%s"
                    % (np_func.__name__, unsupported),
                ):
                    np_func(*[psdf[column] for column in columns])

        # An Index reaches the same dispatch as a Series.
        with self.assertRaisesRegex(TypeError, "ufunc 'cosh' is not supported"):
            np.cosh(ps.Index(["7", "8"]))

    def test_np_unsupported_scalar_operand_types(self):
        # A scalar operand is typed from its Python type, not from a Spark column.
        psdf = ps.from_pandas(self.operand_type_pdf)

        for np_func, args, unsupported in (
            (np.fmod, (psdf["integer"], "8"), "string"),
            (np.ldexp, (psdf["double"], 2.5), "double"),
            (np.left_shift, (psdf["integer"], 1.5), "double"),
        ):
            with self.subTest(np_func=np_func.__name__, unsupported=unsupported):
                with self.assertRaisesRegex(
                    TypeError,
                    "ufunc '%s' is not supported for the input types .*%s"
                    % (np_func.__name__, unsupported),
                ):
                    np_func(*args)

    def test_np_supported_operand_types(self):
        pdf = self.operand_type_pdf
        psdf = ps.from_pandas(pdf)

        # The accepted cases the rest of this file does not reach: a decimal column, a scalar
        # operand, and a ufunc with no table entry.
        self.assert_eq(np.square(psdf["decimal"]), np.square(pdf["decimal"]), almost=True)
        self.assert_eq(np.trunc(psdf["decimal"]), np.trunc(pdf["decimal"]), almost=True)
        self.assert_eq(np.sqrt(psdf["decimal"]), np.sqrt(pdf["decimal"]), almost=True)
        self.assert_eq(np.absolute(psdf["decimal"]), np.absolute(pdf["decimal"]), almost=True)
        self.assert_eq(np.sign(psdf["decimal"]), np.sign(pdf["decimal"]), almost=True)
        # pandas reads an all-null column as False for the bitwise operators, so the check must
        # not reject void. The values still differ from pandas, which is a pre-existing gap.
        self.assertIsNotNone(np.bitwise_and(psdf["null"], psdf["null"]))
        self.assert_eq(np.ldexp(psdf["double"], 2), np.ldexp(pdf["double"], 2), almost=True)
        self.assert_eq(np.fmod(psdf["integer"], 2), np.fmod(pdf["integer"], 2), almost=True)
        self.assert_eq(np.left_shift(psdf["integer"], 1), np.left_shift(pdf["integer"], 1))
        self.assert_eq(
            np.fmax(psdf["string"], psdf["string"]), np.fmax(pdf["string"], pdf["string"])
        )

    def test_np_math_functions(self):
        for np_func, values in (
            (np.arccosh, [-np.inf, -1.0, 0.0, 1.0, 2.0, 64.0, np.inf, np.nan]),
            (np.arcsinh, [-np.inf, -64.0, -2.0, 0.0, 2.0, 64.0, np.inf, np.nan]),
            (
                np.arctanh,
                [-np.inf, -64.0, -1.0, 0.0, 1.0, 64.0, np.inf, np.nan],
            ),
            (np.cosh, [-np.inf, -64.0, -2.0, 0.0, 2.0, 64.0, np.inf, np.nan]),
            (np.deg2rad, [-np.inf, -64.0, -180.0, 0.0, 180.0, 64.0, np.inf, np.nan]),
            (np.exp2, [-np.inf, -64.0, -2.0, 0.0, 2.0, 64.0, np.inf, np.nan]),
            (np.fabs, [np.iinfo(np.int64).min, -2, 0, 2]),
            (np.fabs, [-np.inf, -64.0, -2.0, 0.0, 2.0, 64.0, np.inf, np.nan]),
            (np.invert, [np.iinfo(np.int64).min, -2, -1, 0, 1, 2, np.iinfo(np.int64).max]),
            (np.isfinite, [-np.inf, -64.0, -0.0, 0.0, 64.0, np.inf, np.nan]),
            (np.isinf, [-np.inf, -64.0, -0.0, 0.0, 64.0, np.inf, np.nan]),
            (np.log2, [-np.inf, -64.0, -1.0, -0.0, 0.0, 1.0, 2.0, 64.0, np.inf, np.nan]),
            (np.negative, [-np.inf, -64.0, -2.0, 0.0, 2.0, 64.0, np.inf, np.nan]),
            (np.positive, [-np.inf, -64.0, -2.0, 0.0, 2.0, 64.0, np.inf, np.nan]),
            (np.rad2deg, [-np.inf, -64.0, -np.pi, 0.0, np.pi, 64.0, np.inf, np.nan]),
            (np.rint, [-np.inf, -2.5, -1.5, -0.5, -0.0, 0.0, 0.5, 1.5, 2.5, np.inf, np.nan]),
            (
                np.reciprocal,
                [-np.inf, -64.0, -2.0, -1.0, -0.0, 0.0, 1.0, 2.0, 64.0, np.inf, np.nan],
            ),
            (np.sign, [-np.inf, -64.0, -2.0, -0.0, 0.0, 2.0, 64.0, np.inf, np.nan]),
            (np.sinh, [-np.inf, -64.0, -2.0, 0.0, 2.0, 64.0, np.inf, np.nan]),
            (np.square, [-np.inf, -64.0, -2.0, 0.0, 2.0, 64.0, np.inf, np.nan]),
            (np.tanh, [-np.inf, -64.0, -2.0, 0.0, 2.0, 64.0, np.inf, np.nan]),
            (
                np.trunc,
                [
                    -np.inf,
                    -64.0,
                    -2.0,
                    -1.5,
                    -0.5,
                    -0.0,
                    0.0,
                    0.5,
                    1.5,
                    2.0,
                    64.0,
                    np.inf,
                    np.nan,
                ],
            ),
        ):
            with self.subTest(name=np_func.__name__, values=values):
                pdf = pd.DataFrame({"a": values})
                psdf = ps.from_pandas(pdf)

                self.assert_eq(np_func(psdf.a), np_func(pdf.a), almost=True)

    @_skip_if_numpy_differs
    def test_np_reciprocal_integer(self):
        # np.reciprocal on an integer column does integer division (truncated
        # toward zero): 1 -> 1, -1 -> -1, and every other magnitude -> 0. The
        # value 0 overflows to the int64 minimum. Cover positive, negative, and
        # zero inputs to lock in parity with pandas.
        for values in (
            [1, 2, 3, 64, 100],
            [-1, -2, -3, -64, -100],
            [-2, -1, 0, 1, 2],
            [np.iinfo(np.int64).min, -1, 1, np.iinfo(np.int64).max],
        ):
            with self.subTest(values=values):
                pdf = pd.DataFrame({"a": values})
                psdf = ps.from_pandas(pdf)

                self.assert_eq(np.reciprocal(psdf.a), np.reciprocal(pdf.a), almost=True)

    @_skip_if_numpy_differs
    def test_np_reciprocal_non_default_dtypes(self):
        # The non-floating reciprocal branch also serves narrower integers,
        # booleans, and decimals. numpy divides integers (and booleans, as
        # int8) toward zero, so 0 overflows to the width-specific minimum
        # (0 for int8/int16, int32 min for int32), while decimals take a true
        # floating reciprocal. Lock in parity with pandas for each.
        for dtype in ("int8", "int16", "int32"):
            with self.subTest(dtype=dtype):
                pdf = pd.DataFrame({"a": np.array([-2, -1, 0, 1, 2], dtype=dtype)})
                psdf = ps.from_pandas(pdf)

                self.assert_eq(np.reciprocal(psdf.a), np.reciprocal(pdf.a), almost=True)

        # Boolean: numpy promotes to int8 (True -> 1, False -> 0).
        pdf = pd.DataFrame({"a": [True, False, True]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(np.reciprocal(psdf.a), np.reciprocal(pdf.a), almost=True)

        # Decimal: numpy takes a floating reciprocal. 0 is excluded because
        # numpy raises DivisionByZero on Decimal('0').
        pdf = pd.DataFrame({"a": [Decimal("2.5"), Decimal("-4"), Decimal("0.5")]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(np.reciprocal(psdf.a), np.reciprocal(pdf.a), almost=True)

    def test_np_bitwise_shift_functions(self):
        pdf = pd.DataFrame(
            {
                "value": [np.iinfo(np.int64).min, -2, -1, 0, 1, 2, np.iinfo(np.int64).max],
                "bits": [-1, 0, 1, 63, 64, 65, 2],
            }
        )
        psdf = ps.from_pandas(pdf)

        for np_func in (np.left_shift, np.right_shift):
            with self.subTest(name=np_func.__name__):
                self.assert_eq(
                    np_func(psdf.value, psdf.bits), np_func(pdf.value, pdf.bits), almost=True
                )

    def test_np_float_power(self):
        for pdf in (
            pd.DataFrame({"base": [-64, -2, -1, 0, 1, 2, 64], "exponent": [-2, -1, 0, 1, 2, 3, 2]}),
            pd.DataFrame(
                {
                    "base": [-np.inf, -64.0, -2.0, -0.0, 0.0, 2.0, 64.0, np.inf, np.nan],
                    "exponent": [2.0, 3.0, -2.0, -3.0, -3.0, 0.5, -2.0, 2.0, 2.0],
                }
            ),
        ):
            psdf = ps.from_pandas(pdf)
            self.assert_eq(
                np.float_power(psdf.base, psdf.exponent),
                np.float_power(pdf.base, pdf.exponent),
                almost=True,
            )

    def test_np_ldexp(self):
        pdf = pd.DataFrame(
            {
                "x": [
                    -np.inf,
                    -64.0,
                    -2.0,
                    -0.0,
                    0.0,
                    1.0,
                    2.0,
                    64.0,
                    np.inf,
                    np.nan,
                    1.0,
                    1.0,
                    1.0,
                    0.0,
                    -0.0,
                    np.inf,
                    -np.inf,
                ],
                "exp": [
                    2,
                    3,
                    -2,
                    -3,
                    -3,
                    0,
                    -2,
                    2,
                    2,
                    2,
                    -1074,
                    -1075,
                    1024,
                    1024,
                    1024,
                    -1075,
                    -1075,
                ],
            }
        )
        psdf = ps.from_pandas(pdf)

        result = np.ldexp(psdf.x, psdf.exp)
        expected = np.ldexp(pdf.x, pdf.exp)
        self.assert_eq(result, expected, almost=True)
        self.assert_eq(np.signbit(result.to_pandas()), np.signbit(expected))

    def test_np_fmod(self):
        for pdf in (
            pd.DataFrame(
                {
                    "x1": [-64, -2, -1, 0, 1, 2, 64],
                    "x2": [2, 3, -2, -3, -3, 0, 2],
                }
            ),
            pd.DataFrame(
                {
                    "x1": [-np.inf, -64.0, -2.0, -0.0, 0.0, 2.0, 64.0, np.inf, np.nan, 1.0],
                    "x2": [2.0, 3.0, -2.0, -3.0, -3.0, 0.0, -np.inf, np.inf, 2.0, 0.0],
                }
            ),
            pd.DataFrame(
                {
                    "x1": pd.array([1, 2, None, None], dtype="Int64"),
                    "x2": pd.array([2, None, 2, 0], dtype="Int64"),
                }
            ),
        ):
            psdf = ps.from_pandas(pdf)

            self.assert_eq(np.fmod(psdf.x1, psdf.x2), np.fmod(pdf.x1, pdf.x2), almost=True)

        # Integral operands above 2**53, where casting an operand to double would drop its low
        # bits: fmod(9007199254740993, 2) is 1, not 0. Compared exactly, since almost=True would
        # accept an off-by-one at these magnitudes.
        pdf = pd.DataFrame(
            {
                "x1": [9007199254740993, 9007199254740995, -9007199254740993, 4611686018427387905],
                "x2": [2, 4, 2, 1000000000],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(np.fmod(psdf.x1, psdf.x2), np.fmod(pdf.x1, pdf.x2).astype("float64"))

        # almost=True treats -0.0 and 0.0 as equal, so check the sign of zero explicitly:
        # an integral remainder is never negative zero, unlike a double one.
        pdf = pd.DataFrame({"x1": [-64, -2, 0, 64], "x2": [2, 2, 3, 2]})
        psdf = ps.from_pandas(pdf)

        result = np.fmod(psdf.x1, psdf.x2)
        expected = np.fmod(pdf.x1, pdf.x2)
        self.assert_eq(np.signbit(result.to_pandas()), np.signbit(expected))

    def test_np_modf(self):
        # np.modf(x) returns a tuple (fractional part, integral part).
        for pdf in (
            pd.DataFrame({"a": [-64, -2, -1, 0, 1, 2, 64]}),
            pd.DataFrame(
                {"a": [-np.inf, -64.0, -2.0, -0.5, -0.0, 0.0, 0.5, 2.0, 64.0, np.inf, np.nan]}
            ),
            pd.DataFrame({"a": pd.array([1, -2, None], dtype="Int64")}),
        ):
            psdf = ps.from_pandas(pdf)
            ps_fractional, ps_integral = np.modf(psdf.a)
            pd_fractional, pd_integral = np.modf(pdf.a)
            self.assert_eq(ps_fractional, pd_fractional, almost=True)
            self.assert_eq(ps_integral, pd_integral, almost=True)

        # almost=True treats -0.0 and 0.0 as equal, so verify the sign of zero explicitly:
        # the fractional part of a negative whole number (-2.0 -> -0.0) and the fractional
        # part of -inf (-> -0.0) must keep the input's sign, as must the integral part of a
        # value in (-1, 0) (-0.5 -> -0.0).
        pdf = pd.DataFrame({"a": [-2.0, -0.5, -0.0, 0.0, 0.5, 2.0, -np.inf, np.inf]})
        psdf = ps.from_pandas(pdf)
        ps_fractional, ps_integral = np.modf(psdf.a)
        pd_fractional, pd_integral = np.modf(pdf.a)
        self.assert_eq(np.signbit(ps_fractional.to_pandas()), np.signbit(pd_fractional))
        self.assert_eq(np.signbit(ps_integral.to_pandas()), np.signbit(pd_integral))

        # DataFrame input: np.modf returns a tuple of DataFrames, one per output.
        pdf = pd.DataFrame(
            {
                "a": [-3.5, -2.0, -0.5, 0.0, 2.7],
                "b": [1.5, -0.0, np.inf, -np.inf, np.nan],
            }
        )
        psdf = ps.from_pandas(pdf)
        ps_fractional, ps_integral = np.modf(psdf)
        pd_fractional, pd_integral = np.modf(pdf)
        self.assert_eq(ps_fractional, pd_fractional, almost=True)
        self.assert_eq(ps_integral, pd_integral, almost=True)
        self.assert_eq(np.signbit(ps_fractional.to_pandas()), np.signbit(pd_fractional))
        self.assert_eq(np.signbit(ps_integral.to_pandas()), np.signbit(pd_integral))

        # A DataFrame with no columns has no per-column result to inspect, so the number of
        # outputs comes from the ufunc; pandas returns one empty DataFrame per output here too.
        ps_fractional, ps_integral = np.modf(psdf[[]])
        pd_fractional, pd_integral = np.modf(pdf[[]])
        self.assert_eq(ps_fractional, pd_fractional)
        self.assert_eq(ps_integral, pd_integral)

        # Index input: np.modf returns a tuple of Index objects.
        pidx = pd.Index([-3.5, -2.0, -0.5, 0.0, 2.7])
        psidx = ps.from_pandas(pidx)
        ps_fractional, ps_integral = np.modf(psidx)
        pd_fractional, pd_integral = np.modf(pidx)
        self.assert_eq(ps_fractional, pd_fractional, almost=True)
        self.assert_eq(ps_integral, pd_integral, almost=True)

    def test_np_frexp(self):
        # np.frexp(x) returns a tuple (mantissa, exponent), where x == mantissa * 2**exponent.
        for pdf in (
            pd.DataFrame({"a": [-64, -3, -1, 0, 1, 3, 64]}),
            pd.DataFrame(
                {"a": [-np.inf, -64.0, -1.5, -0.5, -0.0, 0.0, 0.5, 1.5, 64.0, np.inf, np.nan]}
            ),
            pd.DataFrame({"a": pd.array([1, -2, None], dtype="Int64")}),
        ):
            psdf = ps.from_pandas(pdf)
            ps_mantissa, ps_exponent = np.frexp(psdf.a)
            pd_mantissa, pd_exponent = np.frexp(pdf.a)
            self.assert_eq(ps_mantissa, pd_mantissa, almost=True)
            self.assert_eq(ps_exponent, pd_exponent, almost=True)

        # Values next to a power of two, where the logarithm behind the exponent needs its
        # correction, and both ends of the double range. Compared exactly, not with almost=True.
        pdf = pd.DataFrame(
            {
                "a": [
                    np.nextafter(2.0, 0.0),
                    2.0,
                    np.nextafter(2.0, np.inf),
                    np.nextafter(-2.0, 0.0),
                    np.finfo(np.float64).max,
                    np.finfo(np.float64).tiny,
                    np.nextafter(0.0, 1.0),  # the smallest subnormal
                ]
            }
        )
        psdf = ps.from_pandas(pdf)
        ps_mantissa, ps_exponent = np.frexp(psdf.a)
        pd_mantissa, pd_exponent = np.frexp(pdf.a)
        self.assert_eq(ps_mantissa, pd_mantissa)
        self.assert_eq(ps_exponent, pd_exponent)

        # almost=True treats -0.0 and 0.0 as equal, so check the sign of zero explicitly:
        # the mantissa of a zero is that zero, and of +-inf that infinity.
        pdf = pd.DataFrame({"a": [-2.0, -0.5, -0.0, 0.0, 0.5, 2.0, -np.inf, np.inf]})
        psdf = ps.from_pandas(pdf)
        ps_mantissa, _ = np.frexp(psdf.a)
        pd_mantissa, _ = np.frexp(pdf.a)
        self.assert_eq(np.signbit(ps_mantissa.to_pandas()), np.signbit(pd_mantissa))

        # DataFrame input: np.frexp returns a tuple of DataFrames, one per output.
        pdf = pd.DataFrame(
            {
                "a": [-3.5, -1.0, -0.5, 0.0, 6.0],
                "b": [1.5, -0.0, np.inf, -np.inf, np.nan],
            }
        )
        psdf = ps.from_pandas(pdf)
        ps_mantissa, ps_exponent = np.frexp(psdf)
        pd_mantissa, pd_exponent = np.frexp(pdf)
        self.assert_eq(ps_mantissa, pd_mantissa, almost=True)
        self.assert_eq(ps_exponent, pd_exponent, almost=True)
        self.assert_eq(np.signbit(ps_mantissa.to_pandas()), np.signbit(pd_mantissa))

        # Index input: np.frexp returns a tuple of Index objects.
        pidx = pd.Index([-3.5, -1.0, -0.5, 0.0, 6.0])
        psidx = ps.from_pandas(pidx)
        ps_mantissa, ps_exponent = np.frexp(psidx)
        pd_mantissa, pd_exponent = np.frexp(pidx)
        self.assert_eq(ps_mantissa, pd_mantissa, almost=True)
        self.assert_eq(ps_exponent, pd_exponent, almost=True)

    def test_floor_divide_func(self):
        from pyspark.pandas.utils import _floor_divide_func

        def floor_divided(pdf):
            psdf = ps.from_pandas(pdf)
            return (
                psdf.spark.frame()
                .select(_floor_divide_func(F.col("x1"), F.col("x2")).alias("result"))
                .toPandas()["result"]
                .rename(None)
            )

        for pdf in (
            pd.DataFrame(
                {
                    "x1": [-64, -2, -1, 0, 1, 2, 64, -1, 0],
                    "x2": [2, 3, -2, -3, -3, 0, 2, 0, 0],
                }
            ),
            pd.DataFrame(
                {
                    "x1": [
                        -np.inf,
                        -64.0,
                        -2.0,
                        -0.0,
                        0.0,
                        2.0,
                        64.0,
                        np.inf,
                        np.nan,
                        1.0,
                        -1.0,
                        np.inf,
                        -np.inf,
                        np.inf,
                        -np.inf,
                        1.0,
                    ],
                    "x2": [
                        2.0,
                        3.0,
                        -2.0,
                        -3.0,
                        -3.0,
                        0.0,
                        -np.inf,
                        np.inf,
                        2.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        -0.0,
                        -0.0,
                        np.nan,
                    ],
                }
            ),
            pd.DataFrame(
                {
                    "x1": pd.array([1, None, None], dtype="Int64"),
                    "x2": pd.array([None, 2, 0], dtype="Int64"),
                }
            ),
        ):
            self.assert_eq(floor_divided(pdf), np.floor_divide(pdf.x1, pdf.x2), almost=True)

        # Divisors binary cannot represent exactly, where the quotient rounds up across an
        # integer: 1.0 / 0.1 rounds to 10.0, so flooring it gives 10 instead of 9. Compared
        # exactly, since almost=True would accept an off-by-one on the large values below.
        pdf = pd.DataFrame(
            {
                "x1": [1.0, 10.0, 2.0, 0.5, 7.0, -1.0, -10.0, 3.0],
                "x2": [0.1, 0.1, 0.2, 0.1, 0.7, 0.1, 0.1, 7.0],
            }
        )
        self.assert_eq(floor_divided(pdf), np.floor_divide(pdf.x1, pdf.x2))

        # Integral operands above 2**53, where casting an operand to double would drop its
        # low bits: -9007199254740993 // 2 is -4503599627370497, not -4503599627370496.
        pdf = pd.DataFrame(
            {
                "x1": [9007199254740993, -9007199254740993, 4611686018427387905, 7, -7],
                "x2": [1, 2, 3, 3, 3],
            }
        )
        self.assert_eq(floor_divided(pdf), np.floor_divide(pdf.x1, pdf.x2).astype("float64"))

        # The most negative long divided by -1, whose quotient a long cannot hold. NumPy wraps
        # around, while Spark's integer division raises.
        pdf = pd.DataFrame({"x1": [-(2**63), -(2**63)], "x2": [-1, 2]})
        self.assert_eq(floor_divided(pdf), np.floor_divide(pdf.x1, pdf.x2).astype("float64"))

        # Finite operands whose quotient overflows to an infinity, which is its own floor.
        pdf = pd.DataFrame(
            {"x1": [1e300, -1e300, 1e300, -1e300], "x2": [1e-300, 1e-300, -1e-300, -1e-300]}
        )
        self.assert_eq(floor_divided(pdf), np.floor_divide(pdf.x1, pdf.x2))

    def test_np_logaddexp(self):
        for pdf in (
            pd.DataFrame(
                {
                    "x1": [-64, -2, -1, 0, 1, 2, 64],
                    "x2": [2, 3, -2, -3, -3, 0, 2],
                }
            ),
            pd.DataFrame(
                {
                    "x1": [
                        -np.inf,
                        -np.inf,
                        -2.0,
                        -2.0,
                        -0.0,
                        0.0,
                        2.0,
                        np.inf,
                        np.inf,
                        np.nan,
                        -1000.0,
                        -np.inf,
                        -0.0,
                    ],
                    "x2": [
                        -np.inf,
                        3.0,
                        -np.inf,
                        2.0,
                        0.0,
                        -0.0,
                        np.inf,
                        2.0,
                        np.inf,
                        2.0,
                        1000.0,
                        -0.0,
                        -np.inf,
                    ],
                }
            ),
        ):
            psdf = ps.from_pandas(pdf)
            for np_func in (np.logaddexp, np.logaddexp2):
                result = np_func(psdf.x1, psdf.x2)
                expected = np_func(pdf.x1, pdf.x2)
                self.assert_eq(result, expected, almost=True)
                self.assert_eq(np.signbit(result.to_pandas()), np.signbit(expected))

    @_skip_if_numpy_differs
    def test_np_fmax_fmin(self):
        for pdf in (
            pd.DataFrame({"x1": [-2, -1, 0, 1, 2], "x2": [2, 1, 0, -1, -2]}),
            pd.DataFrame(
                {
                    "x1": [np.nan, 2.0, np.nan, -np.inf, -2.0, -0.0, 0.0, 2.0, np.inf],
                    "x2": [2.0, np.nan, np.nan, np.inf, -np.inf, 0.0, -0.0, np.inf, -np.inf],
                }
            ),
            pd.DataFrame({"x1": [-0.0, 0.0], "x2": [0.0, -0.0]}),
        ):
            psdf = ps.from_pandas(pdf)
            for np_func in (np.fmax, np.fmin):
                result = np_func(psdf.x1, psdf.x2)
                expected = np_func(pdf.x1, pdf.x2)
                self.assert_eq(result, expected, almost=True)
                # NumPy's vectorized implementation may select either zero operand, whereas
                # its scalar implementation consistently selects the first one.
                expected_signbit = pd.Series(
                    [np.signbit(np_func(x1, x2)) for x1, x2 in zip(pdf.x1, pdf.x2)]
                )
                self.assert_eq(np.signbit(result.to_pandas()), expected_signbit)

    def test_np_fmax_fmin_integer_precision(self):
        # The result keeps the operands' type, so an integral pair stays exact past 2^53,
        # where a double result rounds to the nearest even value. The last row also pins the
        # tie branch, which returns an operand rather than a comparison; it is an equal
        # non-zero pair, so no signed-zero tie arises and the test needs no skip.
        pdf = pd.DataFrame(
            {
                "x1": [2**53 + 1, -(2**53 + 1), 2**53 + 1],
                "x2": [2, -2, 2**53 + 1],
            }
        )
        psdf = ps.from_pandas(pdf)

        for np_func in (np.fmax, np.fmin):
            self.assert_eq(np_func(psdf.x1, psdf.x2), np_func(pdf.x1, pdf.x2))

    def test_np_fmax_fmin_non_default_dtypes(self):
        # Both helpers select an operand, so the result keeps the operands' type for every
        # dtype NumPy also preserves. Tie rows are equal non-zero pairs, since the signed-zero
        # tie is the one case where NumPy's own answer varies; keep them that way so this test
        # needs no skip either.
        for dtype in ("int8", "int16", "int32", "float32"):
            with self.subTest(dtype=dtype):
                pdf = pd.DataFrame(
                    {
                        "x1": np.array([-2, 1, 3], dtype=dtype),
                        "x2": np.array([2, -1, 3], dtype=dtype),
                    }
                )
                psdf = ps.from_pandas(pdf)

                for np_func in (np.fmax, np.fmin):
                    self.assert_eq(np_func(psdf.x1, psdf.x2), np_func(pdf.x1, pdf.x2))

        for pdf in (
            pd.DataFrame({"x1": [True, False, True], "x2": [False, False, True]}),
            pd.DataFrame(
                {
                    "x1": [Decimal("7.5"), Decimal("-2.5"), Decimal("3.0")],
                    "x2": [Decimal("2.0"), Decimal("-9.0"), Decimal("3.0")],
                }
            ),
            pd.DataFrame(
                {
                    "x1": pd.to_datetime(["2020-01-01", "2021-06-01"]),
                    "x2": pd.to_datetime(["2020-06-01", "2021-01-01"]),
                }
            ),
        ):
            psdf = ps.from_pandas(pdf)

            for np_func in (np.fmax, np.fmin):
                self.assert_eq(np_func(psdf.x1, psdf.x2), np_func(pdf.x1, pdf.x2))

    def test_np_copysign(self):
        for pdf in (
            pd.DataFrame(
                {
                    "x1": [-64, -2, -1, 0, 1, 2, 64],
                    "x2": [2, -3, -2, -3, 3, -1, 2],
                }
            ),
            pd.DataFrame(
                {
                    "x1": [-np.inf, -64.0, -2.0, -0.0, 0.0, 2.0, 64.0, np.inf, np.nan, 1.0],
                    "x2": [2.0, -3.0, -2.0, 0.0, -0.0, -1.0, np.inf, -np.inf, 2.0, np.nan],
                }
            ),
            pd.DataFrame(
                {
                    "x1": pd.array([1, -2, 3, None, None], dtype="Int64"),
                    "x2": pd.array([-2, 3, None, 2, None], dtype="Int64"),
                }
            ),
        ):
            psdf = ps.from_pandas(pdf)
            result = np.copysign(psdf.x1, psdf.x2)
            expected = np.copysign(pdf.x1, pdf.x2)
            self.assert_eq(result, expected, almost=True)
            # copysign only differs from |x| in the sign bit, so assert on signbit
            # explicitly -- 0.0 == -0.0 numerically and would hide a wrong sign.
            self.assert_eq(np.signbit(result.to_pandas()), np.signbit(expected))

    def test_np_copysign_signed_zero(self):
        # np.copysign takes the sign from y's IEEE-754 sign bit, not from y < 0:
        # copysign(1.0, -0.0) == -1.0 and copysign(1.0, 0.0) == 1.0.
        pdf = pd.DataFrame(
            {
                "x1": [1.0, 1.0, -0.0, -0.0, 3.0],
                "x2": [0.0, -0.0, 0.0, -0.0, -0.0],
            }
        )
        psdf = ps.from_pandas(pdf)
        result = np.copysign(psdf.x1, psdf.x2).to_pandas()
        expected = np.copysign(pdf.x1, pdf.x2)
        self.assert_eq(result, expected)
        self.assert_eq(np.signbit(result), np.signbit(expected))

    def test_np_heaviside(self):
        for pdf in (
            pd.DataFrame({"x1": [-2, -1, 0, 1, 2], "x2": [-2, -1, 0, 1, 2]}),
            pd.DataFrame(
                {
                    "x1": [-np.inf, -2.0, -0.0, 0.0, 0.0, 2.0, np.inf, np.nan],
                    "x2": [2.0, -2.0, -0.0, 0.5, np.nan, np.nan, -0.0, 2.0],
                }
            ),
        ):
            psdf = ps.from_pandas(pdf)
            self.assert_eq(
                np.heaviside(psdf.x1, psdf.x2), np.heaviside(pdf.x1, pdf.x2), almost=True
            )

    def test_np_signbit(self):
        # np.signbit returns the IEEE-754 sign bit, which differs from (x < 0) only
        # at -0.0: the sign bit is set even though -0.0 is not less than zero. A
        # missing value in a default (numpy-backed) dtype arrives as a NaN and maps
        # to False (np.signbit(nan) is False), whereas a genuine <NA> in a nullable
        # dtype (e.g. Int64) propagates. A nullable Float64 <NA> is indistinguishable
        # from a NaN after from_pandas, so it is deliberately not covered here.
        for pdf in (
            pd.DataFrame({"a": [-0.0, 0.0, -1.0, 1.0, -np.inf, np.inf, np.nan]}),
            pd.DataFrame({"a": [1, -2, None]}),
            pd.DataFrame({"a": pd.array([1, -2, None], dtype="Int64")}),
        ):
            psdf = ps.from_pandas(pdf)
            self.assert_eq(np.signbit(psdf.a), np.signbit(pdf.a))

    def test_np_spark_compat_series(self):
        from pyspark.pandas.numpy_compat import binary_np_spark_mappings, unary_np_spark_mappings

        # Use randomly generated dataFrame
        pdf = pd.DataFrame(
            np.random.randint(-100, 100, size=(np.random.randint(100), 2)), columns=["a", "b"]
        )
        pdf2 = pd.DataFrame(
            np.random.randint(-100, 100, size=(len(pdf), len(pdf.columns))), columns=["a", "b"]
        )
        psdf = ps.from_pandas(pdf)
        psdf2 = ps.from_pandas(pdf2)

        for np_name, spark_func in unary_np_spark_mappings.items():
            np_func = getattr(np, np_name)
            if np_name not in self.blacklist:
                try:
                    # unary ufunc
                    self.assert_eq(np_func(pdf.a), np_func(psdf.a), almost=True)
                except Exception as e:
                    raise AssertionError("Test in '%s' function was failed." % np_name) from e

        for np_name, spark_func in binary_np_spark_mappings.items():
            np_func = getattr(np, np_name)
            if np_name not in self.blacklist:
                try:
                    # binary ufunc
                    self.assert_eq(np_func(pdf.a, pdf.b), np_func(psdf.a, psdf.b), almost=True)
                    self.assert_eq(np_func(pdf.a, 1), np_func(psdf.a, 1), almost=True)
                except Exception as e:
                    raise AssertionError("Test in '%s' function was failed." % np_name) from e

        # Test only top 5 for now. 'compute.ops_on_diff_frames' option increases too much time.
        try:
            set_option("compute.ops_on_diff_frames", True)
            for np_name, spark_func in list(binary_np_spark_mappings.items())[:5]:
                np_func = getattr(np, np_name)
                if np_name not in self.blacklist:
                    try:
                        # binary ufunc
                        self.assert_eq(
                            np_func(pdf.a, pdf2.b).sort_index(),
                            np_func(psdf.a, psdf2.b).sort_index(),
                            almost=True,
                        )
                    except Exception as e:
                        raise AssertionError("Test in '%s' function was failed." % np_name) from e
        finally:
            reset_option("compute.ops_on_diff_frames")

    def test_np_spark_compat_frame(self):
        from pyspark.pandas.numpy_compat import binary_np_spark_mappings, unary_np_spark_mappings

        # Use randomly generated dataFrame
        pdf = pd.DataFrame(
            np.random.randint(-100, 100, size=(np.random.randint(100), 2)), columns=["a", "b"]
        )
        pdf2 = pd.DataFrame(
            np.random.randint(-100, 100, size=(len(pdf), len(pdf.columns))), columns=["a", "b"]
        )
        psdf = ps.from_pandas(pdf)
        psdf2 = ps.from_pandas(pdf2)

        for np_name, spark_func in unary_np_spark_mappings.items():
            np_func = getattr(np, np_name)
            if np_name not in self.blacklist:
                try:
                    # unary ufunc
                    self.assert_eq(np_func(pdf), np_func(psdf), almost=True)
                except Exception as e:
                    raise AssertionError("Test in '%s' function was failed." % np_name) from e

        for np_name, spark_func in binary_np_spark_mappings.items():
            np_func = getattr(np, np_name)
            if np_name not in self.blacklist:
                try:
                    # binary ufunc
                    self.assert_eq(np_func(pdf, pdf), np_func(psdf, psdf), almost=True)
                    self.assert_eq(np_func(pdf, 1), np_func(psdf, 1), almost=True)
                except Exception as e:
                    raise AssertionError("Test in '%s' function was failed." % np_name) from e

        # Test only top 5 for now. 'compute.ops_on_diff_frames' option increases too much time.
        try:
            set_option("compute.ops_on_diff_frames", True)
            for np_name, spark_func in list(binary_np_spark_mappings.items())[:5]:
                np_func = getattr(np, np_name)
                if np_name not in self.blacklist:
                    try:
                        # binary ufunc
                        self.assert_eq(
                            np_func(pdf, pdf2).sort_index(),
                            np_func(psdf, psdf2).sort_index(),
                            almost=True,
                        )

                    except Exception as e:
                        raise AssertionError("Test in '%s' function was failed." % np_name) from e
        finally:
            reset_option("compute.ops_on_diff_frames")


class NumPyCompatTests(
    NumPyCompatTestsMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
