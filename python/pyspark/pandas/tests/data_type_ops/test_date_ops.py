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

import datetime

import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase


class DateOpsTestsMixin:
    @property
    def pser(self):
        return pd.Series(
            [datetime.date(1994, 1, 31), datetime.date(1994, 2, 1), datetime.date(1994, 2, 2)]
        )

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def date_pdf(self):
        psers = {
            "this": self.pser,
            "that": pd.Series(
                [datetime.date(2000, 1, 31), datetime.date(1994, 3, 1), datetime.date(1990, 2, 2)]
            ),
        }
        return pd.concat(psers, axis=1)

    @property
    def date_psdf(self):
        return ps.from_pandas(self.date_pdf)

    @property
    def some_date(self):
        return datetime.date(1994, 1, 1)

    def test_add(self):
        self.assertRaises(TypeError, lambda: self.psser + "x")
        self.assertRaises(TypeError, lambda: self.psser + 1)
        self.assertRaises(TypeError, lambda: self.psser + self.some_date)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser + psser)

    def test_sub(self):
        self.assertRaises(TypeError, lambda: self.psser - "x")
        self.assertRaises(TypeError, lambda: self.psser - 1)
        self.assert_eq(
            (self.pser - self.some_date).apply(lambda x: x.days),
            self.psser - self.some_date,
        )
        pdf, psdf = self.pdf, self.psdf
        for col in self.df_cols:
            if col == "date":
                self.assert_eq(
                    (pdf["date"] - pdf[col]).apply(lambda x: x.days), psdf["date"] - psdf[col]
                )
            else:
                self.assertRaises(TypeError, lambda: psdf["date"] - psdf[col])
        pdf, psdf = self.date_pdf, self.date_psdf
        self.assert_eq(
            (pdf["this"] - pdf["that"]).apply(lambda x: x.days), psdf["this"] - psdf["that"]
        )

    def test_mul(self):
        self.assertRaises(TypeError, lambda: self.psser * "x")
        self.assertRaises(TypeError, lambda: self.psser * 1)
        self.assertRaises(TypeError, lambda: self.psser * self.some_date)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser * psser)

    def test_truediv(self):
        self.assertRaises(TypeError, lambda: self.psser / "x")
        self.assertRaises(TypeError, lambda: self.psser / 1)
        self.assertRaises(TypeError, lambda: self.psser / self.some_date)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser / psser)

    def test_floordiv(self):
        self.assertRaises(TypeError, lambda: self.psser // "x")
        self.assertRaises(TypeError, lambda: self.psser // 1)
        self.assertRaises(TypeError, lambda: self.psser // self.some_date)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser // psser)

    def test_mod(self):
        self.assertRaises(TypeError, lambda: self.psser % "x")
        self.assertRaises(TypeError, lambda: self.psser % 1)
        self.assertRaises(TypeError, lambda: self.psser % self.some_date)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser % psser)

    def test_pow(self):
        self.assertRaises(TypeError, lambda: self.psser ** "x")
        self.assertRaises(TypeError, lambda: self.psser**1)
        self.assertRaises(TypeError, lambda: self.psser**self.some_date)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser**psser)

    def test_radd(self):
        self.assertRaises(TypeError, lambda: "x" + self.psser)
        self.assertRaises(TypeError, lambda: 1 + self.psser)
        self.assertRaises(TypeError, lambda: self.some_date + self.psser)

    def test_rsub(self):
        self.assertRaises(TypeError, lambda: "x" - self.psser)
        self.assertRaises(TypeError, lambda: 1 - self.psser)
        self.assert_eq(
            (self.some_date - self.pser).apply(lambda x: x.days),
            self.some_date - self.psser,
        )

    def test_rmul(self):
        self.assertRaises(TypeError, lambda: "x" * self.psser)
        self.assertRaises(TypeError, lambda: 1 * self.psser)
        self.assertRaises(TypeError, lambda: self.some_date * self.psser)

    def test_rtruediv(self):
        self.assertRaises(TypeError, lambda: "x" / self.psser)
        self.assertRaises(TypeError, lambda: 1 / self.psser)
        self.assertRaises(TypeError, lambda: self.some_date / self.psser)

    def test_rfloordiv(self):
        self.assertRaises(TypeError, lambda: "x" // self.psser)
        self.assertRaises(TypeError, lambda: 1 // self.psser)
        self.assertRaises(TypeError, lambda: self.some_date // self.psser)

    def test_rmod(self):
        self.assertRaises(TypeError, lambda: 1 % self.psser)
        self.assertRaises(TypeError, lambda: self.some_date % self.psser)

    def test_rpow(self):
        self.assertRaises(TypeError, lambda: "x" ** self.psser)
        self.assertRaises(TypeError, lambda: 1**self.psser)
        self.assertRaises(TypeError, lambda: self.some_date**self.psser)

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
        data = [datetime.date(1994, 1, 31), datetime.date(1994, 2, 1), datetime.date(1994, 2, 2)]
        pser = pd.Series(data)
        psser = ps.Series(data)
        self.assert_eq(pser, psser._to_pandas())
        self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        self.assert_eq(self.pser.isnull(), self.psser.isnull())

    def test_astype(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser.astype(str), psser.astype(str))
        self.assert_eq(pser.astype(bool), psser.astype(bool))
        cat_type = CategoricalDtype(categories=["a", "b", "c"])
        self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))

    def test_neg(self):
        self.assertRaises(TypeError, lambda: -self.psser)

    def test_abs(self):
        self.assertRaises(TypeError, lambda: abs(self.psser))

    def test_invert(self):
        self.assertRaises(TypeError, lambda: ~self.psser)

    def test_eq(self):
        pdf, psdf = self.date_pdf, self.date_psdf
        self.assert_eq(pdf["this"] == pdf["that"], psdf["this"] == psdf["that"])
        self.assert_eq(pdf["this"] == pdf["this"], psdf["this"] == psdf["this"])

    def test_ne(self):
        pdf, psdf = self.date_pdf, self.date_psdf
        self.assert_eq(pdf["this"] != pdf["that"], psdf["this"] != psdf["that"])
        self.assert_eq(pdf["this"] != pdf["this"], psdf["this"] != psdf["this"])

    def test_lt(self):
        pdf, psdf = self.date_pdf, self.date_psdf
        self.assert_eq(pdf["this"] < pdf["that"], psdf["this"] < psdf["that"])
        self.assert_eq(pdf["this"] < pdf["this"], psdf["this"] < psdf["this"])

    def test_le(self):
        pdf, psdf = self.date_pdf, self.date_psdf
        self.assert_eq(pdf["this"] <= pdf["that"], psdf["this"] <= psdf["that"])
        self.assert_eq(pdf["this"] <= pdf["this"], psdf["this"] <= psdf["this"])

    def test_gt(self):
        pdf, psdf = self.date_pdf, self.date_psdf
        self.assert_eq(pdf["this"] > pdf["that"], psdf["this"] > psdf["that"])
        self.assert_eq(pdf["this"] > pdf["this"], psdf["this"] > psdf["this"])

    def test_ge(self):
        pdf, psdf = self.date_pdf, self.date_psdf
        self.assert_eq(pdf["this"] >= pdf["that"], psdf["this"] >= psdf["that"])
        self.assert_eq(pdf["this"] >= pdf["this"], psdf["this"] >= psdf["this"])


class DateOpsTests(
    DateOpsTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_date_ops import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
