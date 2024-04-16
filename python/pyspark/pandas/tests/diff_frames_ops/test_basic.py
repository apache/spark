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


import unittest

import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class BasicMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    @property
    def pdf1(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 10, 11],
        )

    @property
    def pdf2(self):
        return pd.DataFrame(
            {"a": [9, 8, 7, 6, 5, 4, 3, 2, 1], "b": [0, 0, 0, 4, 5, 6, 1, 2, 3]},
            index=list(range(9)),
        )

    @property
    def pdf3(self):
        return pd.DataFrame(
            {"b": [1, 1, 1, 1, 1, 1, 1, 1, 1], "c": [1, 1, 1, 1, 1, 1, 1, 1, 1]},
            index=list(range(9)),
        )

    @property
    def pdf4(self):
        return pd.DataFrame(
            {"e": [2, 2, 2, 2, 2, 2, 2, 2, 2], "f": [2, 2, 2, 2, 2, 2, 2, 2, 2]},
            index=list(range(9)),
        )

    @property
    def psdf1(self):
        return ps.from_pandas(self.pdf1)

    @property
    def psdf2(self):
        return ps.from_pandas(self.pdf2)

    @property
    def psdf3(self):
        return ps.from_pandas(self.pdf3)

    @property
    def psdf4(self):
        return ps.from_pandas(self.pdf4)

    def test_ranges(self):
        self.assert_eq(
            (ps.range(10) + ps.range(10)).sort_index(),
            (
                ps.DataFrame({"id": list(range(10))}) + ps.DataFrame({"id": list(range(10))})
            ).sort_index(),
        )

    def test_no_matched_index(self):
        with self.assertRaisesRegex(ValueError, "Index names must be exactly matched"):
            ps.DataFrame({"a": [1, 2, 3]}).set_index("a") + ps.DataFrame(
                {"b": [1, 2, 3]}
            ).set_index("b")

    def test_mod(self):
        pser = pd.Series([100, None, -300, None, 500, -700])
        pser_other = pd.Series([-150] * 6)
        psser = ps.from_pandas(pser)
        psser_other = ps.from_pandas(pser_other)

        self.assert_eq(psser.mod(psser_other).sort_index(), pser.mod(pser_other))
        self.assert_eq(psser.mod(psser_other).sort_index(), pser.mod(pser_other))
        self.assert_eq(psser.mod(psser_other).sort_index(), pser.mod(pser_other))

    def test_rmod(self):
        pser = pd.Series([100, None, -300, None, 500, -700])
        pser_other = pd.Series([-150] * 6)
        psser = ps.from_pandas(pser)
        psser_other = ps.from_pandas(pser_other)

        self.assert_eq(psser.rmod(psser_other).sort_index(), pser.rmod(pser_other))
        self.assert_eq(psser.rmod(psser_other).sort_index(), pser.rmod(pser_other))
        self.assert_eq(psser.rmod(psser_other).sort_index(), pser.rmod(pser_other))

    def test_getitem_boolean_series(self):
        pdf1 = pd.DataFrame(
            {"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]}, index=[20, 10, 30, 0, 50]
        )
        pdf2 = pd.DataFrame(
            {"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]},
            index=[0, 30, 10, 20, 50],
        )
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1[pdf2.A > -3].sort_index(), psdf1[psdf2.A > -3].sort_index())

        self.assert_eq(pdf1.A[pdf2.A > -3].sort_index(), psdf1.A[psdf2.A > -3].sort_index())

        self.assert_eq(
            (pdf1.A + 1)[pdf2.A > -3].sort_index(), (psdf1.A + 1)[psdf2.A > -3].sort_index()
        )

    def test_loc_getitem_boolean_series(self):
        pdf1 = pd.DataFrame(
            {"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]}, index=[20, 10, 30, 0, 50]
        )
        pdf2 = pd.DataFrame(
            {"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]},
            index=[20, 10, 30, 0, 50],
        )
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.loc[pdf2.A > -3].sort_index(), psdf1.loc[psdf2.A > -3].sort_index())

        self.assert_eq(pdf1.A.loc[pdf2.A > -3].sort_index(), psdf1.A.loc[psdf2.A > -3].sort_index())

        self.assert_eq(
            (pdf1.A + 1).loc[pdf2.A > -3].sort_index(), (psdf1.A + 1).loc[psdf2.A > -3].sort_index()
        )

        pser = pd.Series([0, 1, 2, 3, 4], index=[20, 10, 30, 0, 50])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.loc[pdf2.A > -3].sort_index(), psser.loc[psdf2.A > -3].sort_index())
        pser.name = psser.name = "B"
        self.assert_eq(pser.loc[pdf2.A > -3].sort_index(), psser.loc[psdf2.A > -3].sort_index())

    def test_insert(self):
        #
        # Basic DataFrame
        #
        pdf = pd.DataFrame([1, 2, 3])
        psdf = ps.from_pandas(pdf)

        pser = pd.Series([4, 5, 6])
        psser = ps.from_pandas(pser)
        psdf.insert(1, "y", psser)
        pdf.insert(1, "y", pser)
        self.assert_eq(psdf.sort_index(), pdf.sort_index())

        #
        # DataFrame with Index different from inserting Series'
        #
        pdf = pd.DataFrame([1, 2, 3], index=[10, 20, 30])
        psdf = ps.from_pandas(pdf)

        pser = pd.Series([4, 5, 6])
        psser = ps.from_pandas(pser)
        psdf.insert(1, "y", psser)
        pdf.insert(1, "y", pser)
        self.assert_eq(psdf.sort_index(), pdf.sort_index())

        #
        # DataFrame with Multi-index columns
        #
        pdf = pd.DataFrame({("x", "a"): [1, 2, 3]})
        psdf = ps.from_pandas(pdf)

        pser = pd.Series([4, 5, 6])
        psser = ps.from_pandas(pser)
        pdf = pd.DataFrame({("x", "a", "b"): [1, 2, 3]})
        psdf = ps.from_pandas(pdf)
        psdf.insert(0, "a", psser)
        pdf.insert(0, "a", pser)
        self.assert_eq(psdf.sort_index(), pdf.sort_index())
        psdf.insert(0, ("b", "c", ""), psser)
        pdf.insert(0, ("b", "c", ""), pser)
        self.assert_eq(psdf.sort_index(), pdf.sort_index())

    def test_different_columns(self):
        psdf1 = self.psdf1
        psdf4 = self.psdf4
        pdf1 = self.pdf1
        pdf4 = self.pdf4

        self.assert_eq((psdf1 + psdf4).sort_index(), (pdf1 + pdf4).sort_index(), almost=True)

        # Multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b")])
        psdf1.columns = columns
        pdf1.columns = columns
        columns = pd.MultiIndex.from_tuples([("z", "e"), ("z", "f")])
        psdf4.columns = columns
        pdf4.columns = columns

        self.assert_eq((psdf1 + psdf4).sort_index(), (pdf1 + pdf4).sort_index(), almost=True)


class BasicTests(
    BasicMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.diff_frames_ops.test_basic import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
