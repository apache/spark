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


class FrameEvalMixin:
    def test_eval(self):
        pdf = pd.DataFrame({"A": range(1, 6), "B": range(10, 0, -2)})
        psdf = ps.from_pandas(pdf)

        # operation between columns (returns Series)
        self.assert_eq(pdf.eval("A + B"), psdf.eval("A + B"))
        self.assert_eq(pdf.eval("A + A"), psdf.eval("A + A"))
        # assignment (returns DataFrame)
        self.assert_eq(pdf.eval("C = A + B"), psdf.eval("C = A + B"))
        self.assert_eq(pdf.eval("A = A + A"), psdf.eval("A = A + A"))
        # operation between scalars (returns scalar)
        self.assert_eq(pdf.eval("1 + 1"), psdf.eval("1 + 1"))
        # complicated operations with assignment
        self.assert_eq(
            pdf.eval("B = A + B // (100 + 200) * (500 - B) - 10.5"),
            psdf.eval("B = A + B // (100 + 200) * (500 - B) - 10.5"),
        )

        # inplace=True (only support for assignment)
        pdf.eval("C = A + B", inplace=True)
        psdf.eval("C = A + B", inplace=True)
        self.assert_eq(pdf, psdf)
        pser = pdf.A
        psser = psdf.A
        pdf.eval("A = B + C", inplace=True)
        psdf.eval("A = B + C", inplace=True)
        self.assert_eq(pdf, psdf)
        self.assert_eq(pser, psser)

        # doesn't support for multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b"), ("z", "c")])
        psdf.columns = columns
        self.assertRaises(TypeError, lambda: psdf.eval("x.a + y.b"))


class FrameEvalTests(
    FrameEvalMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_eval import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
