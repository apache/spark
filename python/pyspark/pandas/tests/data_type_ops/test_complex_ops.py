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

import decimal
import datetime

import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase


class ComplexOpsTestsMixin:
    @property
    def pser(self):
        return pd.Series([[1, 2, 3]])

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def numeric_array_pdf(self):
        psers = {
            "int": pd.Series([[1, 2, 3]]),
            "float": pd.Series([[0.1, 0.2, 0.3]]),
            "decimal": pd.Series([[decimal.Decimal(1), decimal.Decimal(2), decimal.Decimal(3)]]),
        }
        return pd.concat(psers, axis=1)

    @property
    def numeric_array_psdf(self):
        return ps.from_pandas(self.numeric_array_pdf)

    @property
    def numeric_array_df_cols(self):
        return self.numeric_array_pdf.columns

    @property
    def non_numeric_array_pdf(self):
        psers = {
            "string": pd.Series([["x", "y", "z"]]),
            "date": pd.Series(
                [[datetime.date(1994, 1, 1), datetime.date(1994, 1, 2), datetime.date(1994, 1, 3)]]
            ),
            "bool": pd.Series([[True, True, False]]),
        }
        return pd.concat(psers, axis=1)

    @property
    def non_numeric_array_psdf(self):
        return ps.from_pandas(self.non_numeric_array_pdf)

    @property
    def non_numeric_array_df_cols(self):
        return self.non_numeric_array_pdf.columns

    @property
    def array_pdf(self):
        return pd.concat([self.numeric_array_pdf, self.non_numeric_array_pdf], axis=1)

    @property
    def array_psdf(self):
        return ps.from_pandas(self.array_pdf)

    @property
    def array_df_cols(self):
        return self.array_pdf.columns

    @property
    def complex_pdf(self):
        psers = {
            "this_array": self.pser,
            "that_array": pd.Series([[2, 3, 4]]),
            "this_struct": pd.Series([("x", 1)]),
            "that_struct": pd.Series([("a", 2)]),
        }
        return pd.concat(psers, axis=1)

    @property
    def complex_psdf(self):
        pssers = {
            "this_array": self.psser,
            "that_array": ps.Series([[2, 3, 4]]),
            "this_struct": ps.Index([("x", 1)]).to_series().reset_index(drop=True),
            "that_struct": ps.Index([("a", 2)]).to_series().reset_index(drop=True),
        }
        return ps.concat(pssers, axis=1)

    def test_add(self):
        pdf, psdf = self.array_pdf, self.array_psdf
        for col in self.array_df_cols:
            self.assert_eq(pdf[col] + pdf[col], psdf[col] + psdf[col], check_exact=False)

        # Numeric array + Numeric array
        for col in self.numeric_array_df_cols:
            pser1, psser1 = pdf[col], psdf[col]
            for other_col in self.numeric_array_df_cols:
                pser2, psser2 = pdf[other_col], psdf[other_col]
                self.assert_eq(
                    (pser1 + pser2).sort_values(),
                    (psser1 + psser2).sort_values(),
                    check_exact=False,
                )

        # Non-numeric array + Non-numeric array
        self.assertRaises(
            TypeError,
            lambda: psdf["string"] + psdf["bool"],
        )
        self.assertRaises(
            TypeError,
            lambda: psdf["string"] + psdf["date"],
        )
        self.assertRaises(
            TypeError,
            lambda: psdf["bool"] + psdf["date"],
        )

        for col in self.non_numeric_array_df_cols:
            pser, psser = pdf[col], psdf[col]
            self.assert_eq(pser + pser, psser + psser, check_exact=False)

        # Numeric array + Non-numeric array
        for numeric_col in self.numeric_array_df_cols:
            for non_numeric_col in self.non_numeric_array_df_cols:
                self.assertRaises(TypeError, lambda: psdf[numeric_col] + psdf[non_numeric_col])

    def test_sub(self):
        self.assertRaises(TypeError, lambda: self.psser - "x")
        self.assertRaises(TypeError, lambda: self.psser - 1)

        psdf = self.array_psdf
        for col in self.array_df_cols:
            for other_col in self.array_df_cols:
                self.assertRaises(TypeError, lambda: psdf[col] - psdf[other_col])

    def test_mul(self):
        self.assertRaises(TypeError, lambda: self.psser * "x")
        self.assertRaises(TypeError, lambda: self.psser * 1)

        psdf = self.array_psdf
        for col in self.array_df_cols:
            for other_col in self.array_df_cols:
                self.assertRaises(TypeError, lambda: psdf[col] * psdf[other_col])

    def test_truediv(self):
        self.assertRaises(TypeError, lambda: self.psser / "x")
        self.assertRaises(TypeError, lambda: self.psser / 1)

        psdf = self.array_psdf
        for col in self.array_df_cols:
            for other_col in self.array_df_cols:
                self.assertRaises(TypeError, lambda: psdf[col] / psdf[other_col])

    def test_floordiv(self):
        self.assertRaises(TypeError, lambda: self.psser // "x")
        self.assertRaises(TypeError, lambda: self.psser // 1)

        psdf = self.array_psdf
        for col in self.array_df_cols:
            for other_col in self.array_df_cols:
                self.assertRaises(TypeError, lambda: psdf[col] // psdf[other_col])

    def test_mod(self):
        self.assertRaises(TypeError, lambda: self.psser % "x")
        self.assertRaises(TypeError, lambda: self.psser % 1)

        psdf = self.array_psdf
        for col in self.array_df_cols:
            for other_col in self.array_df_cols:
                self.assertRaises(TypeError, lambda: psdf[col] % psdf[other_col])

    def test_pow(self):
        self.assertRaises(TypeError, lambda: self.psser ** "x")
        self.assertRaises(TypeError, lambda: self.psser**1)

        psdf = self.array_psdf
        for col in self.array_df_cols:
            for other_col in self.array_df_cols:
                self.assertRaises(TypeError, lambda: psdf[col] ** psdf[other_col])

    def test_radd(self):
        self.assertRaises(TypeError, lambda: "x" + self.psser)
        self.assertRaises(TypeError, lambda: 1 + self.psser)

    def test_rsub(self):
        self.assertRaises(TypeError, lambda: "x" - self.psser)
        self.assertRaises(TypeError, lambda: 1 - self.psser)

    def test_rmul(self):
        self.assertRaises(TypeError, lambda: "x" * self.psser)
        self.assertRaises(TypeError, lambda: 2 * self.psser)

    def test_rtruediv(self):
        self.assertRaises(TypeError, lambda: "x" / self.psser)
        self.assertRaises(TypeError, lambda: 1 / self.psser)

    def test_rfloordiv(self):
        self.assertRaises(TypeError, lambda: "x" // self.psser)
        self.assertRaises(TypeError, lambda: 1 // self.psser)

    def test_rmod(self):
        self.assertRaises(TypeError, lambda: 1 % self.psser)

    def test_rpow(self):
        self.assertRaises(TypeError, lambda: "x" ** self.psser)
        self.assertRaises(TypeError, lambda: 1**self.psser)

    def test_and(self):
        self.assertRaises(TypeError, lambda: self.psser & True)
        self.assertRaises(TypeError, lambda: self.psser & False)
        self.assertRaises(TypeError, lambda: self.psser & self.psser)

    def test_rand(self):
        self.assertRaises(TypeError, lambda: True & self.psser)
        self.assertRaises(TypeError, lambda: False & self.psser)

    def test_or(self):
        self.assertRaises(TypeError, lambda: self.psser | True)
        self.assertRaises(TypeError, lambda: self.psser | False)
        self.assertRaises(TypeError, lambda: self.psser | self.psser)

    def test_ror(self):
        self.assertRaises(TypeError, lambda: True | self.psser)
        self.assertRaises(TypeError, lambda: False | self.psser)

    def test_from_to_pandas(self):
        pdf, psdf = self.array_pdf, self.array_psdf
        for col in self.array_df_cols:
            pser, psser = pdf[col], psdf[col]
            self.assert_eq(pser, psser._to_pandas(), check_exact=False)
            self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        pdf, psdf = self.array_pdf, self.array_psdf
        for col in self.array_df_cols:
            pser, psser = pdf[col], psdf[col]
            self.assert_eq(pser.isnull(), psser.isnull())

    def test_astype(self):
        self.assert_eq(self.pser.astype(str), self.psser.astype(str))

    def test_neg(self):
        self.assertRaises(TypeError, lambda: -self.psser)

    def test_abs(self):
        self.assertRaises(TypeError, lambda: abs(self.psser))

    def test_invert(self):
        self.assertRaises(TypeError, lambda: ~self.psser)

    def test_eq(self):
        pdf, psdf = self.complex_pdf, self.complex_pdf
        self.assert_eq(
            pdf["this_array"] == pdf["that_array"], psdf["this_array"] == psdf["that_array"]
        )
        self.assert_eq(
            pdf["this_struct"] == pdf["that_struct"], psdf["this_struct"] == psdf["that_struct"]
        )
        self.assert_eq(
            pdf["this_array"] == pdf["this_array"], psdf["this_array"] == psdf["this_array"]
        )
        self.assert_eq(
            pdf["this_struct"] == pdf["this_struct"], psdf["this_struct"] == psdf["this_struct"]
        )

    def test_ne(self):
        pdf, psdf = self.complex_pdf, self.complex_pdf
        self.assert_eq(
            pdf["this_array"] != pdf["that_array"], psdf["this_array"] != psdf["that_array"]
        )
        self.assert_eq(
            pdf["this_struct"] != pdf["that_struct"], psdf["this_struct"] != psdf["that_struct"]
        )
        self.assert_eq(
            pdf["this_array"] != pdf["this_array"], psdf["this_array"] != psdf["this_array"]
        )
        self.assert_eq(
            pdf["this_struct"] != pdf["this_struct"], psdf["this_struct"] != psdf["this_struct"]
        )

    def test_lt(self):
        pdf, psdf = self.complex_pdf, self.complex_pdf
        self.assert_eq(
            pdf["this_array"] < pdf["that_array"], psdf["this_array"] < psdf["that_array"]
        )
        self.assert_eq(
            pdf["this_struct"] < pdf["that_struct"], psdf["this_struct"] < psdf["that_struct"]
        )
        self.assert_eq(
            pdf["this_array"] < pdf["this_array"], psdf["this_array"] < psdf["this_array"]
        )
        self.assert_eq(
            pdf["this_struct"] < pdf["this_struct"], psdf["this_struct"] < psdf["this_struct"]
        )

    def test_le(self):
        pdf, psdf = self.complex_pdf, self.complex_pdf
        self.assert_eq(
            pdf["this_array"] <= pdf["that_array"], psdf["this_array"] <= psdf["that_array"]
        )
        self.assert_eq(
            pdf["this_struct"] <= pdf["that_struct"], psdf["this_struct"] <= psdf["that_struct"]
        )
        self.assert_eq(
            pdf["this_array"] <= pdf["this_array"], psdf["this_array"] <= psdf["this_array"]
        )
        self.assert_eq(
            pdf["this_struct"] <= pdf["this_struct"], psdf["this_struct"] <= psdf["this_struct"]
        )

    def test_gt(self):
        pdf, psdf = self.complex_pdf, self.complex_pdf
        self.assert_eq(
            pdf["this_array"] > pdf["that_array"], psdf["this_array"] > psdf["that_array"]
        )
        self.assert_eq(
            pdf["this_struct"] > pdf["that_struct"], psdf["this_struct"] > psdf["that_struct"]
        )
        self.assert_eq(
            pdf["this_array"] > pdf["this_array"], psdf["this_array"] > psdf["this_array"]
        )
        self.assert_eq(
            pdf["this_struct"] > pdf["this_struct"], psdf["this_struct"] > psdf["this_struct"]
        )

    def test_ge(self):
        pdf, psdf = self.complex_pdf, self.complex_pdf
        self.assert_eq(
            pdf["this_array"] >= pdf["that_array"], psdf["this_array"] >= psdf["that_array"]
        )
        self.assert_eq(
            pdf["this_struct"] >= pdf["that_struct"], psdf["this_struct"] >= psdf["that_struct"]
        )
        self.assert_eq(
            pdf["this_array"] >= pdf["this_array"], psdf["this_array"] >= psdf["this_array"]
        )
        self.assert_eq(
            pdf["this_struct"] >= pdf["this_struct"], psdf["this_struct"] >= psdf["this_struct"]
        )


class ComplexOpsTests(
    ComplexOpsTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_complex_ops import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
