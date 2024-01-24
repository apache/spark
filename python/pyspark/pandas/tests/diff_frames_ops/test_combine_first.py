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


class CombineFirstMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_combine_first(self):
        pser1 = pd.Series({"falcon": 330.0, "eagle": 160.0})
        pser2 = pd.Series({"falcon": 345.0, "eagle": 200.0, "duck": 30.0})
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        self.assert_eq(
            psser1.combine_first(psser2).sort_index(), pser1.combine_first(pser2).sort_index()
        )
        with self.assertRaisesRegex(
            TypeError, "`combine_first` only allows `Series` for parameter `other`"
        ):
            psser1.combine_first(50)

        psser1.name = ("X", "A")
        psser2.name = ("Y", "B")
        pser1.name = ("X", "A")
        pser2.name = ("Y", "B")
        self.assert_eq(
            psser1.combine_first(psser2).sort_index(), pser1.combine_first(pser2).sort_index()
        )

        # MultiIndex
        midx1 = pd.MultiIndex(
            [["lama", "cow", "falcon", "koala"], ["speed", "weight", "length", "power"]],
            [[0, 3, 1, 1, 1, 2, 2, 2], [0, 2, 0, 3, 2, 0, 1, 3]],
        )
        midx2 = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser1 = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1], index=midx1)
        pser2 = pd.Series([-45, 200, -1.2, 30, -250, 1.5, 320, 1, -0.3], index=midx2)
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        self.assert_eq(
            psser1.combine_first(psser2).sort_index(), pser1.combine_first(pser2).sort_index()
        )

        # DataFrame
        pdf1 = pd.DataFrame({"A": [None, 0], "B": [4, None]})
        psdf1 = ps.from_pandas(pdf1)
        pdf2 = pd.DataFrame({"C": [3, 3], "B": [1, 1]})
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.combine_first(pdf2), psdf1.combine_first(psdf2).sort_index())

        pdf1.columns = pd.MultiIndex.from_tuples([("A", "willow"), ("B", "pine")])
        psdf1 = ps.from_pandas(pdf1)
        pdf2.columns = pd.MultiIndex.from_tuples([("C", "oak"), ("B", "pine")])
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.combine_first(pdf2), psdf1.combine_first(psdf2).sort_index())


class CombineFirstTests(
    CombineFirstMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_combine_first import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
