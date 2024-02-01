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
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class FrameAlignMixin:
    def test_align(self):
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]}, index=[10, 20, 30])
        psdf1 = ps.from_pandas(pdf1)

        for join in ["outer", "inner", "left", "right"]:
            for axis in [None, 0, 1]:
                psdf_l, psdf_r = psdf1.align(psdf1[["b"]], join=join, axis=axis)
                pdf_l, pdf_r = pdf1.align(pdf1[["b"]], join=join, axis=axis)
                self.assert_eq(psdf_l, pdf_l)
                self.assert_eq(psdf_r, pdf_r)

                psdf_l, psdf_r = psdf1[["a"]].align(psdf1[["b", "a"]], join=join, axis=axis)
                pdf_l, pdf_r = pdf1[["a"]].align(pdf1[["b", "a"]], join=join, axis=axis)
                self.assert_eq(psdf_l, pdf_l)
                self.assert_eq(psdf_r, pdf_r)

                psdf_l, psdf_r = psdf1[["b", "a"]].align(psdf1[["a"]], join=join, axis=axis)
                pdf_l, pdf_r = pdf1[["b", "a"]].align(pdf1[["a"]], join=join, axis=axis)
                self.assert_eq(psdf_l, pdf_l)
                self.assert_eq(psdf_r, pdf_r)

        psdf_l, psdf_r = psdf1.align(psdf1["b"], axis=0)
        pdf_l, pdf_r = pdf1.align(pdf1["b"], axis=0)
        self.assert_eq(psdf_l, pdf_l)
        self.assert_eq(psdf_r, pdf_r)

        psdf_l, psser_b = psdf1[["a"]].align(psdf1["b"], axis=0)
        pdf_l, pser_b = pdf1[["a"]].align(pdf1["b"], axis=0)
        self.assert_eq(psdf_l, pdf_l)
        self.assert_eq(psser_b, pser_b)

        self.assertRaises(ValueError, lambda: psdf1.align(psdf1, join="unknown"))
        self.assertRaises(ValueError, lambda: psdf1.align(psdf1["b"]))
        self.assertRaises(TypeError, lambda: psdf1.align(["b"]))
        self.assertRaises(NotImplementedError, lambda: psdf1.align(psdf1["b"], axis=1))

        pdf2 = pd.DataFrame({"a": [4, 5, 6], "d": ["d", "e", "f"]}, index=[10, 11, 12])
        psdf2 = ps.from_pandas(pdf2)

        for join in ["outer", "inner", "left", "right"]:
            psdf_l, psdf_r = psdf1.align(psdf2, join=join, axis=1)
            pdf_l, pdf_r = pdf1.align(pdf2, join=join, axis=1)
            self.assert_eq(psdf_l.sort_index(), pdf_l.sort_index())
            self.assert_eq(psdf_r.sort_index(), pdf_r.sort_index())


class FrameAlignTests(
    FrameAlignMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_align import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
