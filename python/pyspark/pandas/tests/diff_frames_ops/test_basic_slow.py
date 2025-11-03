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

import pandas as pd
import numpy as np

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class DiffFramesBasicSlowMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_update(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x
        pser.update(pd.Series([4, 5, 6]))
        psser.update(ps.Series([4, 5, 6]))
        self.assert_eq(psser.sort_index(), pser.sort_index())
        self.assert_eq(psdf.sort_index(), pdf.sort_index())

        pser1 = pd.Series([None, 2, 3, 4, 5, 6, 7, 8, None])
        pser2 = pd.Series([None, 5, None, 3, 2, 1, None, 0, 0])
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        pser1.update(pser2)
        psser1.update(psser2)
        self.assert_eq(psser1.sort_index(), pser1)

    def test_where(self):
        pdf1 = pd.DataFrame({"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame({"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.where(pdf2 > 100), psdf1.where(psdf2 > 100).sort_index())

        pdf1 = pd.DataFrame({"A": [-1, -2, -3, -4, -5], "B": [-100, -200, -300, -400, -500]})
        pdf2 = pd.DataFrame({"A": [-10, -20, -30, -40, -50], "B": [-5, -4, -3, -2, -1]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.where(pdf2 < -250), psdf1.where(psdf2 < -250).sort_index())

        # multi-index columns
        pdf1 = pd.DataFrame({("X", "A"): [0, 1, 2, 3, 4], ("X", "B"): [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame(
            {("X", "A"): [0, -1, -2, -3, -4], ("X", "B"): [-100, -200, -300, -400, -500]}
        )
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.where(pdf2 > 100), psdf1.where(psdf2 > 100).sort_index())

    def test_mask(self):
        pdf1 = pd.DataFrame({"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame({"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.mask(pdf2 < 100), psdf1.mask(psdf2 < 100).sort_index())

        pdf1 = pd.DataFrame({"A": [-1, -2, -3, -4, -5], "B": [-100, -200, -300, -400, -500]})
        pdf2 = pd.DataFrame({"A": [-10, -20, -30, -40, -50], "B": [-5, -4, -3, -2, -1]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.mask(pdf2 > -250), psdf1.mask(psdf2 > -250).sort_index())

        # multi-index columns
        pdf1 = pd.DataFrame({("X", "A"): [0, 1, 2, 3, 4], ("X", "B"): [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame(
            {("X", "A"): [0, -1, -2, -3, -4], ("X", "B"): [-100, -200, -300, -400, -500]}
        )
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.mask(pdf2 < 100), psdf1.mask(psdf2 < 100).sort_index())

    def test_to_series_comparison(self):
        psidx1 = ps.Index([1, 2, 3, 4, 5])
        psidx2 = ps.Index([1, 2, 3, 4, 5])

        self.assert_eq((psidx1.to_series() == psidx2.to_series()).all(), True)

        psidx1.name = "koalas"
        psidx2.name = "koalas"

        self.assert_eq((psidx1.to_series() == psidx2.to_series()).all(), True)

    def test_pow_and_rpow(self):
        pser = pd.Series([1, 2, np.nan])
        psser = ps.from_pandas(pser)
        pser_other = pd.Series([np.nan, 2, 3])
        psser_other = ps.from_pandas(pser_other)

        self.assert_eq(pser.pow(pser_other), psser.pow(psser_other).sort_index())
        self.assert_eq(pser**pser_other, (psser**psser_other).sort_index())
        self.assert_eq(pser.rpow(pser_other), psser.rpow(psser_other).sort_index())

    def test_shift(self):
        pdf = pd.DataFrame(
            {
                "Col1": [10, 20, 15, 30, 45],
                "Col2": [13, 23, 18, 33, 48],
                "Col3": [17, 27, 22, 37, 52],
            },
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.shift().loc[pdf["Col1"] == 20].astype(int), psdf.shift().loc[psdf["Col1"] == 20]
        )
        self.assert_eq(
            pdf["Col2"].shift().loc[pdf["Col1"] == 20].astype(int),
            psdf["Col2"].shift().loc[psdf["Col1"] == 20],
        )

    def test_diff(self):
        pdf = pd.DataFrame(
            {
                "Col1": [10, 20, 15, 30, 45],
                "Col2": [13, 23, 18, 33, 48],
                "Col3": [17, 27, 22, 37, 52],
            },
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.diff().loc[pdf["Col1"] == 20].astype(int), psdf.diff().loc[psdf["Col1"] == 20]
        )
        self.assert_eq(
            pdf["Col2"].diff().loc[pdf["Col1"] == 20].astype(int),
            psdf["Col2"].diff().loc[psdf["Col1"] == 20],
        )

    def test_rank(self):
        pdf = pd.DataFrame(
            {
                "Col1": [10, 20, 15, 30, 45],
                "Col2": [13, 23, 18, 33, 48],
                "Col3": [17, 27, 22, 37, 52],
            },
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.rank().loc[pdf["Col1"] == 20], psdf.rank().loc[psdf["Col1"] == 20])
        self.assert_eq(
            pdf["Col2"].rank().loc[pdf["Col1"] == 20], psdf["Col2"].rank().loc[psdf["Col1"] == 20]
        )


class DiffFramesBasicSlowTests(DiffFramesBasicSlowMixin, PandasOnSparkTestCase, SQLTestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_basic_slow import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
