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


class DiffFramesAlignMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_align(self):
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]}, index=[10, 20, 30])
        pdf2 = pd.DataFrame({"a": [4, 5, 6], "c": ["d", "e", "f"]}, index=[10, 11, 12])
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        for join in ["outer", "inner", "left", "right"]:
            for axis in [None, 0]:
                psdf_l, psdf_r = psdf1.align(psdf2, join=join, axis=axis)
                pdf_l, pdf_r = pdf1.align(pdf2, join=join, axis=axis)
                self.assert_eq(psdf_l.sort_index(), pdf_l.sort_index())
                self.assert_eq(psdf_r.sort_index(), pdf_r.sort_index())

        pser1 = pd.Series([7, 8, 9], index=[10, 11, 12])
        pser2 = pd.Series(["g", "h", "i"], index=[10, 20, 30])
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        for join in ["outer", "inner", "left", "right"]:
            psser_l, psser_r = psser1.align(psser2, join=join)
            pser_l, pser_r = pser1.align(pser2, join=join)
            self.assert_eq(psser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(psser_r.sort_index(), pser_r.sort_index())

            psdf_l, psser_r = psdf1.align(psser1, join=join, axis=0)
            pdf_l, pser_r = pdf1.align(pser1, join=join, axis=0)
            self.assert_eq(psdf_l.sort_index(), pdf_l.sort_index())
            self.assert_eq(psser_r.sort_index(), pser_r.sort_index())

            psser_l, psdf_r = psser1.align(psdf1, join=join)
            pser_l, pdf_r = pser1.align(pdf1, join=join)
            self.assert_eq(psser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(psdf_r.sort_index(), pdf_r.sort_index())

        # multi-index columns
        pdf3 = pd.DataFrame(
            {("x", "a"): [4, 5, 6], ("y", "c"): ["d", "e", "f"]}, index=[10, 11, 12]
        )
        psdf3 = ps.from_pandas(pdf3)
        pser3 = pdf3[("y", "c")]
        psser3 = psdf3[("y", "c")]

        for join in ["outer", "inner", "left", "right"]:
            psdf_l, psdf_r = psdf1.align(psdf3, join=join, axis=0)
            pdf_l, pdf_r = pdf1.align(pdf3, join=join, axis=0)
            self.assert_eq(psdf_l.sort_index(), pdf_l.sort_index())
            self.assert_eq(psdf_r.sort_index(), pdf_r.sort_index())

            psser_l, psser_r = psser1.align(psser3, join=join)
            pser_l, pser_r = pser1.align(pser3, join=join)
            self.assert_eq(psser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(psser_r.sort_index(), pser_r.sort_index())

            psdf_l, psser_r = psdf1.align(psser3, join=join, axis=0)
            pdf_l, pser_r = pdf1.align(pser3, join=join, axis=0)
            self.assert_eq(psdf_l.sort_index(), pdf_l.sort_index())
            self.assert_eq(psser_r.sort_index(), pser_r.sort_index())

            psser_l, psdf_r = psser3.align(psdf1, join=join)
            pser_l, pdf_r = pser3.align(pdf1, join=join)
            self.assert_eq(psser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(psdf_r.sort_index(), pdf_r.sort_index())

        self.assertRaises(ValueError, lambda: psdf1.align(psdf3, axis=None))
        self.assertRaises(ValueError, lambda: psdf1.align(psdf3, axis=1))


class DiffFramesAlignTests(DiffFramesAlignMixin, PandasOnSparkTestCase, SQLTestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_align import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
