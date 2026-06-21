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


class TimeOpsTestsMixin:
    @property
    def pser(self):
        return pd.Series(
            [datetime.time(10, 0, 0), datetime.time(12, 30, 0), datetime.time(14, 0, 0)]
        )

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def time_pdf(self):
        psers = {
            "this": self.pser,
            "that": pd.Series(
                [datetime.time(11, 0, 0), datetime.time(12, 0, 0), datetime.time(13, 0, 0)]
            ),
        }
        return pd.concat(psers, axis=1)

    @property
    def time_psdf(self):
        return ps.from_pandas(self.time_pdf)

    @property
    def some_time(self):
        return datetime.time(12, 0, 0)

    def test_add(self):
        self.assertRaises(TypeError, lambda: self.psser + "x")
        self.assertRaises(TypeError, lambda: self.psser + 1)
        self.assertRaises(TypeError, lambda: self.psser + self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser + psser)

    def test_sub(self):
        self.assertRaises(TypeError, lambda: self.psser - "x")
        self.assertRaises(TypeError, lambda: self.psser - 1)
        self.assertRaises(TypeError, lambda: self.psser - self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser - psser)

    def test_mul(self):
        self.assertRaises(TypeError, lambda: self.psser * "x")
        self.assertRaises(TypeError, lambda: self.psser * 1)
        self.assertRaises(TypeError, lambda: self.psser * self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser * psser)

    def test_truediv(self):
        self.assertRaises(TypeError, lambda: self.psser / "x")
        self.assertRaises(TypeError, lambda: self.psser / 1)
        self.assertRaises(TypeError, lambda: self.psser / self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser / psser)

    def test_floordiv(self):
        self.assertRaises(TypeError, lambda: self.psser // "x")
        self.assertRaises(TypeError, lambda: self.psser // 1)
        self.assertRaises(TypeError, lambda: self.psser // self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser // psser)

    def test_mod(self):
        self.assertRaises(TypeError, lambda: self.psser % "x")
        self.assertRaises(TypeError, lambda: self.psser % 1)
        self.assertRaises(TypeError, lambda: self.psser % self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser % psser)

    def test_pow(self):
        self.assertRaises(TypeError, lambda: self.psser ** "x")
        self.assertRaises(TypeError, lambda: self.psser**1)
        self.assertRaises(TypeError, lambda: self.psser**self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser**psser)

    def test_radd(self):
        self.assertRaises(TypeError, lambda: "x" + self.psser)
        self.assertRaises(TypeError, lambda: 1 + self.psser)
        self.assertRaises(TypeError, lambda: self.some_time + self.psser)

    def test_rsub(self):
        self.assertRaises(TypeError, lambda: "x" - self.psser)
        self.assertRaises(TypeError, lambda: 1 - self.psser)
        self.assertRaises(TypeError, lambda: self.some_time - self.psser)

    def test_rmul(self):
        self.assertRaises(TypeError, lambda: "x" * self.psser)
        self.assertRaises(TypeError, lambda: 1 * self.psser)
        self.assertRaises(TypeError, lambda: self.some_time * self.psser)

    def test_rtruediv(self):
        self.assertRaises(TypeError, lambda: "x" / self.psser)
        self.assertRaises(TypeError, lambda: 1 / self.psser)
        self.assertRaises(TypeError, lambda: self.some_time / self.psser)

    def test_rfloordiv(self):
        self.assertRaises(TypeError, lambda: "x" // self.psser)
        self.assertRaises(TypeError, lambda: 1 // self.psser)
        self.assertRaises(TypeError, lambda: self.some_time // self.psser)

    def test_rpow(self):
        self.assertRaises(TypeError, lambda: "x" ** self.psser)
        self.assertRaises(TypeError, lambda: 1**self.psser)
        self.assertRaises(TypeError, lambda: self.some_time**self.psser)

    def test_rmod(self):
        self.assertRaises(TypeError, lambda: "x" % self.psser)
        self.assertRaises(TypeError, lambda: 1 % self.psser)
        self.assertRaises(TypeError, lambda: self.some_time % self.psser)

    def test_lt(self):
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] < pdf["that"], psdf["this"] < psdf["that"])
        self.assert_eq(pdf["this"] < pdf["this"], psdf["this"] < psdf["this"])
        self.assertRaises(TypeError, lambda: psdf["this"] < pdf["this"])

    def test_le(self):
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] <= pdf["that"], psdf["this"] <= psdf["that"])
        self.assert_eq(pdf["this"] <= pdf["this"], psdf["this"] <= psdf["this"])
        self.assertRaises(TypeError, lambda: psdf["this"] <= pdf["this"])

    def test_gt(self):
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] > pdf["that"], psdf["this"] > psdf["that"])
        self.assert_eq(pdf["this"] > pdf["this"], psdf["this"] > psdf["this"])
        self.assertRaises(TypeError, lambda: psdf["this"] > pdf["this"])

    def test_ge(self):
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] >= pdf["that"], psdf["this"] >= psdf["that"])
        self.assert_eq(pdf["this"] >= pdf["this"], psdf["this"] >= psdf["this"])
        self.assertRaises(TypeError, lambda: psdf["this"] >= pdf["this"])

    def test_eq(self):
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] == pdf["that"], psdf["this"] == psdf["that"])
        self.assert_eq(pdf["this"] == pdf["this"], psdf["this"] == psdf["this"])
        self.assertRaises(TypeError, lambda: psdf["this"] == pdf["this"])

    def test_ne(self):
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] != pdf["that"], psdf["this"] != psdf["that"])
        self.assert_eq(pdf["this"] != pdf["this"], psdf["this"] != psdf["this"])
        self.assertRaises(TypeError, lambda: psdf["this"] != pdf["this"])

    def test_isnull(self):
        self.assert_eq(self.pser.isnull(), self.psser.isnull())

    def test_from_to_pandas(self):
        data = [datetime.time(10, 0, 0), datetime.time(12, 30, 0), datetime.time(14, 0, 0)]
        pser = pd.Series(data)
        psser = ps.Series(data)
        self.assert_eq(pser, psser._to_pandas())
        self.assert_eq(ps.from_pandas(pser), psser)

    def test_astype(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser.astype(str), psser.astype(str))
        self.assert_eq(pser.astype(bool), psser.astype(bool))
        cat_type = CategoricalDtype(categories=["a", "b", "c"])
        self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))


class TimeOpsTests(
    TimeOpsTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
