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


class DiffFramesIndexMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_index_ops(self):
        pidx1 = pd.Index([1, 2, 3, 4, 5], name="x")
        pidx2 = pd.Index([6, 7, 8, 9, 10], name="x")
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)

        self.assert_eq(psidx1 * 10 + psidx2, pidx1 * 10 + pidx2)
        self.assert_eq(psidx1.rename(None) * 10 + psidx2, pidx1.rename(None) * 10 + pidx2)
        self.assert_eq(psidx1 * 10 + psidx2.rename(None), pidx1 * 10 + pidx2.rename(None))

        pidx3 = pd.Index([11, 12, 13])
        psidx3 = ps.from_pandas(pidx3)

        with self.assertRaisesRegex(
            ValueError, "operands could not be broadcast together with shapes"
        ):
            psidx1 + psidx3

        pidx1 = pd.Index([1, 2, 3, 4, 5], name="a")
        pidx2 = pd.Index([6, 7, 8, 9, 10], name="a")
        pidx3 = pd.Index([11, 12, 13, 14, 15], name="x")
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)
        psidx3 = ps.from_pandas(pidx3)

        self.assert_eq(psidx1 * 10 + psidx2, pidx1 * 10 + pidx2)
        self.assert_eq(psidx1 * 10 + psidx3, pidx1 * 10 + pidx3)

    def test_multi_index_column_assignment_frame(self):
        pdf = pd.DataFrame({"a": [1, 2, 3, 2], "b": [4.0, 2.0, 3.0, 1.0]})
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        psdf = ps.DataFrame(pdf)

        psdf["c"] = ps.Series([10, 20, 30, 20])
        pdf["c"] = pd.Series([10, 20, 30, 20])

        psdf[("d", "x")] = ps.Series([100, 200, 300, 200], name="1")
        pdf[("d", "x")] = pd.Series([100, 200, 300, 200], name="1")

        psdf[("d", "y")] = ps.Series([1000, 2000, 3000, 2000], name=("1", "2"))
        pdf[("d", "y")] = pd.Series([1000, 2000, 3000, 2000], name=("1", "2"))

        psdf["e"] = ps.Series([10000, 20000, 30000, 20000], name=("1", "2", "3"))
        pdf["e"] = pd.Series([10000, 20000, 30000, 20000], name=("1", "2", "3"))

        psdf[[("f", "x"), ("f", "y")]] = ps.DataFrame(
            {"1": [100000, 200000, 300000, 200000], "2": [1000000, 2000000, 3000000, 2000000]}
        )
        pdf[[("f", "x"), ("f", "y")]] = pd.DataFrame(
            {"1": [100000, 200000, 300000, 200000], "2": [1000000, 2000000, 3000000, 2000000]}
        )

        self.assert_eq(repr(psdf.sort_index()), repr(pdf))

        with self.assertRaisesRegex(KeyError, "Key length \\(3\\) exceeds index depth \\(2\\)"):
            psdf[("1", "2", "3")] = ps.Series([100, 200, 300, 200])


class DiffFramesIndexTests(DiffFramesIndexMixin, PandasOnSparkTestCase, SQLTestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_index import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
