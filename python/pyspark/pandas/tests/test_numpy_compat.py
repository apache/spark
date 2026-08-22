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

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas import set_option, reset_option
from pyspark.sql import functions as F
from pyspark.testing.pandasutils import PandasOnSparkTestCase


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
        "frexp",
        # Values are close enough but tests failed.
        "log",  # flaky
        "log10",  # flaky
        "log1p",  # flaky
        "modf",
    ]

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

    def test_floor_divide_func(self):
        from pyspark.pandas.numpy_compat import _floor_divide_func

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
            psdf = ps.from_pandas(pdf)
            result = (
                psdf.spark.frame()
                .select(_floor_divide_func(F.col("x1"), F.col("x2")).alias("result"))
                .toPandas()["result"]
                .rename(None)
            )
            self.assert_eq(result, np.floor_divide(pdf.x1, pdf.x2), almost=True)

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
        from pyspark.pandas.numpy_compat import unary_np_spark_mappings, binary_np_spark_mappings

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
        from pyspark.pandas.numpy_compat import unary_np_spark_mappings, binary_np_spark_mappings

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
