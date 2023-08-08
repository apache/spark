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


class DiffFramesDotSeriesMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_series_dot(self):
        pser = pd.Series([90, 91, 85], index=[2, 4, 1])
        psser = ps.from_pandas(pser)
        pser_other = pd.Series([90, 91, 85], index=[2, 4, 1])
        psser_other = ps.from_pandas(pser_other)

        self.assert_eq(psser.dot(psser_other), pser.dot(pser_other))

        psser_other = ps.Series([90, 91, 85], index=[1, 2, 4])
        pser_other = pd.Series([90, 91, 85], index=[1, 2, 4])

        self.assert_eq(psser.dot(psser_other), pser.dot(pser_other))

        # length of index is different
        psser_other = ps.Series([90, 91, 85, 100], index=[2, 4, 1, 0])
        with self.assertRaisesRegex(ValueError, "matrices are not aligned"):
            psser.dot(psser_other)

        # for MultiIndex
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        psser = ps.from_pandas(pser)
        pser_other = pd.Series([-450, 20, 12, -30, -250, 15, -320, 100, 3], index=midx)
        psser_other = ps.from_pandas(pser_other)
        self.assert_eq(psser.dot(psser_other), pser.dot(pser_other))

        pser = pd.Series([0, 1, 2, 3])
        psser = ps.from_pandas(pser)

        # DataFrame "other" without Index/MultiIndex as columns
        pdf = pd.DataFrame([[0, 1], [-2, 3], [4, -5], [6, 7]])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))

        # DataFrame "other" with Index as columns
        pdf.columns = pd.Index(["x", "y"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))
        pdf.columns = pd.Index(["x", "y"], name="cols_name")
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))

        pdf = pdf.reindex([1, 0, 2, 3])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))

        # DataFrame "other" with MultiIndex as columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))
        pdf.columns = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y")], names=["cols_name1", "cols_name2"]
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))

        psser = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).b
        pser = psser._to_pandas()
        psdf = ps.DataFrame({"c": [7, 8, 9]})
        pdf = psdf._to_pandas()
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))

        # SPARK-36968: ps.Series.dot raise "matrices are not aligned" if index is not same
        pser = pd.Series([90, 91, 85], index=[0, 1, 2])
        psser = ps.from_pandas(pser)
        pser_other = pd.Series([90, 91, 85], index=[0, 1, 3])
        psser_other = ps.from_pandas(pser_other)
        pser_other2 = pd.Series([90, 91, 85, 100], index=[0, 1, 3, 5])
        psser_other2 = ps.from_pandas(pser_other2)

        with self.assertRaisesRegex(ValueError, "matrices are not aligned"):
            psser.dot(psser_other)

        with ps.option_context("compute.eager_check", False), self.assertRaisesRegex(
            ValueError, "matrices are not aligned"
        ):
            psser.dot(psser_other2)

        with ps.option_context("compute.eager_check", True), self.assertRaisesRegex(
            ValueError, "matrices are not aligned"
        ):
            psser.dot(psser_other)

        with ps.option_context("compute.eager_check", False):
            self.assert_eq(psser.dot(psser_other), 16381)


class DiffFramesDotSeriesTests(DiffFramesDotSeriesMixin, PandasOnSparkTestCase, SQLTestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_dot_series import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
