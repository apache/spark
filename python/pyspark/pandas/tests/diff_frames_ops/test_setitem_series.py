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

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class DiffFramesSetItemSeriesMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_series_loc_setitem(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y

        pser_another = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        psser_another = ps.from_pandas(pser_another)

        psser.loc[psser % 2 == 1] = -psser_another
        pser.loc[pser % 2 == 1] = -pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y
        psser.loc[psser_another % 2 == 1] = -psser
        pser.loc[pser_another % 2 == 1] = -pser
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y
        psser.loc[psser_another % 2 == 1] = -psser
        pser.loc[pser_another % 2 == 1] = -pser
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y
        psser.loc[psser_another % 2 == 1] = -psser_another
        pser.loc[pser_another % 2 == 1] = -pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y
        psser.loc[["viper", "sidewinder"]] = -psser_another
        pser.loc[["viper", "sidewinder"]] = -pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y
        psser.loc[psser_another % 2 == 1] = 10
        pser.loc[pser_another % 2 == 1] = 10
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

    def test_series_iloc_setitem(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y

        pser1 = pser + 1
        psser1 = psser + 1

        pser_another = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        psser_another = ps.from_pandas(pser_another)

        psser.iloc[[0, 1, 2]] = -psser_another
        pser.iloc[[0, 1, 2]] = -pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            psser.iloc[[1, 2]] = -psser_another

        psser.iloc[[0, 1, 2]] = 10 * psser_another
        pser.iloc[[0, 1, 2]] = 10 * pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            psser.iloc[[0]] = 10 * psser_another

        psser1.iloc[[0, 1, 2]] = -psser_another
        pser1.iloc[[0, 1, 2]] = -pser_another
        self.assert_eq(psser1, pser1)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            psser1.iloc[[1, 2]] = -psser_another


class DiffFramesSetItemSeriesTests(
    DiffFramesSetItemSeriesMixin, PandasOnSparkTestCase, SQLTestUtils
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_setitem_series import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
