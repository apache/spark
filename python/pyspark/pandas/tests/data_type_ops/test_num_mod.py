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


import contextlib
import decimal
import io
from decimal import Decimal

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase
from pyspark.sql import functions as F
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class NumModTestsMixin:
    @property
    def float_pser(self):
        return pd.Series([1, 2, 3], dtype=float)

    @property
    def float_psser(self):
        return ps.from_pandas(self.float_pser)

    def test_mod(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            ignore_null = self.ignore_null(col)
            self.assert_eq(pser % pser, psser % psser, check_exact=False, ignore_null=ignore_null)
            self.assert_eq(
                pser % pser.astype(bool),
                psser % psser.astype(bool),
                check_exact=False,
                ignore_null=ignore_null,
            )
            self.assert_eq(pser % True, psser % True, check_exact=False, ignore_null=ignore_null)
            self.assert_eq(pser % 1, psser % 1, check_exact=False, ignore_null=ignore_null)

            if not col.startswith("decimal"):
                self.assert_eq(pser % 0, psser % 0, check_exact=False)
            if col in ["int", "int32"]:
                self.assert_eq(
                    pd.Series([np.nan, np.nan, np.nan], dtype=float, name=col), psser % False
                )
            else:
                self.assert_eq(
                    pd.Series([np.nan, np.nan, np.nan], dtype=pser.dtype, name=col), psser % False
                )

            for n_col in self.non_numeric_df_cols:
                if n_col == "bool":
                    self.assert_eq(pdf["float"] % pdf[n_col], psdf["float"] % psdf[n_col])
                else:
                    self.assertRaises(TypeError, lambda: psser % psdf[n_col])

    def test_mod_sign_of_divisor(self):
        # SPARK-60655: the result takes the sign of the divisor, as in NumPy, including for an
        # infinite divisor.
        pdf = pd.DataFrame(
            {
                "x": [1.0, -1.0, 0.0, 2.5, -2.5, np.inf],
                "y": [np.inf, np.inf, -np.inf, -np.inf, 2.0, 2.0],
            }
        )
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf["x"], psdf["x"]

        for divisor in [np.inf, -np.inf, 2.0, -2.0]:
            with self.subTest(divisor=divisor):
                self.assert_eq(pser % divisor, psser % divisor)
                self.assert_eq(np.mod(pser, divisor), np.mod(psser, divisor))
                self.assert_eq(divisor % pser, divisor % psser)
        self.assert_eq(pdf["x"] % pdf["y"], psdf["x"] % psdf["y"])
        self.assert_eq(1.0 % pdf["y"], 1.0 % psdf["y"])

        # A zero result takes the sign of the divisor, as in pandas.
        pdf = pd.DataFrame({"x": [2.0, -2.0, 0.0, -0.0, 4.0], "y": [-2.0, 2.0, -2.0, 2.0, 2.0]})
        psdf = ps.from_pandas(pdf)
        for expected, actual in [
            (pdf["x"] % 2.0, psdf["x"] % 2.0),
            (pdf["x"] % -2.0, psdf["x"] % -2.0),
            (4.0 % pdf["y"], 4.0 % psdf["y"]),
            (-4.0 % pdf["y"], -4.0 % psdf["y"]),
            (pdf["x"] % pdf["y"], psdf["x"] % psdf["y"]),
        ]:
            expected, actual = expected.to_numpy(), actual.to_pandas().to_numpy()
            zero = expected == 0
            self.assertTrue(zero.any())
            self.assertEqual(np.signbit(actual[zero]).tolist(), np.signbit(expected[zero]).tolist())

        # A small remainder is kept next to a large divisor.
        pser = pd.Series([1e-20, -1e-20])
        self.assert_eq(pser % 1e20, ps.from_pandas(pser) % 1e20)

        # Integral operands near the bounds do not overflow.
        pser = pd.Series([1, -1, 0], dtype="int64")
        psser = ps.from_pandas(pser)
        for divisor in [np.iinfo(np.int64).max, np.iinfo(np.int64).min]:
            with self.subTest(divisor=divisor):
                self.assert_eq(pser % divisor, psser % divisor)

    def test_mod_nondeterministic_operand(self):
        # SPARK-60655: each operand is evaluated once per row.
        psser = ps.Series(range(6)).spark.transform(lambda c: F.monotonically_increasing_id())
        pdf = ps.concat(
            [psser.rename("id"), (psser % 3).rename("mod"), (100 % (psser + 1)).rename("rmod")],
            axis=1,
        ).to_pandas()
        self.assert_eq(pdf["mod"], (pdf["id"] % 3).rename("mod"))
        self.assert_eq(pdf["rmod"], (100 % (pdf["id"] + 1)).rename("rmod"))

    def test_mod_chained(self):
        # SPARK-60655: the plan of chained modulo operations grows linearly.
        def plan_size(psser):
            with contextlib.redirect_stdout(io.StringIO()) as out:
                psser.to_frame().to_spark().explain(extended=True)
            return len(out.getvalue())

        pser = pd.Series([7, -7, 3, 10])
        psser = ps.from_pandas(pser)
        sizes = []
        for divisor in [11, 7, 5, 3, 2]:
            pser, psser = pser % divisor, psser % divisor
            sizes.append(plan_size(psser))
        self.assert_eq(pser, psser)
        self.assertLess(sizes[-1], 2 * len(sizes) * sizes[0])

    def test_mod_decimal(self):
        # SPARK-60655: a decimal result is exact and has the type of
        # ((left % right) + right) % right.
        def python_mod(a, b):
            with decimal.localcontext() as ctx:
                ctx.prec = 100
                r = a % b  # takes the sign of the dividend
                return r + b if r and (r < 0) != (b < 0) else r

        for ltype, rtype, rows in [
            (
                "decimal(38,18)",
                "decimal(38,18)",
                [("0.100000000000000001", "0.300000000000000009"), ("1.000000000000000001", "1")]
                + [("-1.5", "4"), ("7.25", "-2")],
            ),
            ("decimal(18,2)", "decimal(18,2)", [("7.25", "-2"), ("-1.5", "4"), ("6", "-3")]),
            ("decimal(10,2)", "decimal(20,5)", [("7.25", "-2.12345"), ("-1.5", "0.00004")]),
        ]:
            with self.subTest(ltype=ltype, rtype=rtype):
                rows = [(Decimal(a), Decimal(b)) for a, b in rows]
                sdf = self.spark.createDataFrame(rows, f"a {ltype}, b {rtype}")
                expected_type = sdf.select(((sdf.a % sdf.b) + sdf.b) % sdf.b).schema[0].dataType
                psdf = sdf.pandas_api()
                result = psdf["a"] % psdf["b"]
                self.assertEqual(result.spark.data_type, expected_type)
                self.assertEqual(result.to_list(), [python_mod(a, b) for a, b in rows])

        psser = ps.Series([Decimal("-1.5")])
        with self.sql_conf({"spark.sql.legacy.decimal.retainFractionDigitsOnTruncate": True}):
            for ansi in [True, False]:
                with self.subTest(ansi=ansi), self.sql_conf({"spark.sql.ansi.enabled": ansi}):
                    self.assertEqual(
                        (psser % Decimal(10**21)).to_list(), [Decimal("999999999999999999998.5")]
                    )

    def test_mod_numpy_scalar(self):
        # SPARK-60655: a NumPy scalar operand is taken as the Python number it holds.
        pser = pd.Series([5, 7, -7])
        psser = ps.from_pandas(pser)
        for ansi in [True, False]:
            with self.sql_conf({"spark.sql.ansi.enabled": ansi}):
                for scalar, dtype in [
                    (np.uint8(3), np.int64),
                    (np.uint16(3), np.int64),
                    (np.uint32(3), np.int64),
                    (np.uint64(3), np.int64),
                    (np.int64(3), np.int64),
                    (np.float16(2.5), np.float64),
                    (np.float32(2.5), np.float64),
                ]:
                    with self.subTest(ansi=ansi, scalar=repr(scalar)):
                        for expected, actual in [
                            (pser % scalar, psser % scalar),
                            (scalar % pser, scalar % psser),
                        ]:
                            self.assertEqual(actual.dtype, dtype)
                            self.assert_eq(expected.astype(float), actual.astype(float))


class NumModTests(
    NumModTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
