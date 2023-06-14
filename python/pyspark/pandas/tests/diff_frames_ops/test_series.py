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


class DiffFramesSeriesMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_series_repeat(self):
        pser1 = pd.Series(["a", "b", "c"], name="a")
        pser2 = pd.Series([10, 20, 30], name="rep")
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        self.assert_eq(psser1.repeat(psser2).sort_index(), pser1.repeat(pser2).sort_index())

    def test_series_ops(self):
        pser1 = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x", index=[11, 12, 13, 14, 15, 16, 17])
        pser2 = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x", index=[11, 12, 13, 14, 15, 16, 17])
        pidx1 = pd.Index([10, 11, 12, 13, 14, 15, 16], name="x")
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)
        psidx1 = ps.from_pandas(pidx1)

        self.assert_eq(
            (psser1 + 1 + 10 * psser2).sort_index(), (pser1 + 1 + 10 * pser2).sort_index()
        )
        self.assert_eq(
            (psser1 + 1 + 10 * psser2.rename()).sort_index(),
            (pser1 + 1 + 10 * pser2.rename()).sort_index(),
        )
        self.assert_eq(
            (psser1.rename() + 1 + 10 * psser2).sort_index(),
            (pser1.rename() + 1 + 10 * pser2).sort_index(),
        )
        self.assert_eq(
            (psser1.rename() + 1 + 10 * psser2.rename()).sort_index(),
            (pser1.rename() + 1 + 10 * pser2.rename()).sort_index(),
        )

        self.assert_eq(psser1 + 1 + 10 * psidx1, pser1 + 1 + 10 * pidx1)
        self.assert_eq(psser1.rename() + 1 + 10 * psidx1, pser1.rename() + 1 + 10 * pidx1)
        self.assert_eq(psser1 + 1 + 10 * psidx1.rename(None), pser1 + 1 + 10 * pidx1.rename(None))
        self.assert_eq(
            psser1.rename() + 1 + 10 * psidx1.rename(None),
            pser1.rename() + 1 + 10 * pidx1.rename(None),
        )

        self.assert_eq(psidx1 + 1 + 10 * psser1, pidx1 + 1 + 10 * pser1)
        self.assert_eq(psidx1 + 1 + 10 * psser1.rename(), pidx1 + 1 + 10 * pser1.rename())
        self.assert_eq(psidx1.rename(None) + 1 + 10 * psser1, pidx1.rename(None) + 1 + 10 * pser1)
        self.assert_eq(
            psidx1.rename(None) + 1 + 10 * psser1.rename(),
            pidx1.rename(None) + 1 + 10 * pser1.rename(),
        )

        pidx2 = pd.Index([11, 12, 13])
        psidx2 = ps.from_pandas(pidx2)

        with self.assertRaisesRegex(
            ValueError, "operands could not be broadcast together with shapes"
        ):
            psser1 + psidx2

        with self.assertRaisesRegex(
            ValueError, "operands could not be broadcast together with shapes"
        ):
            psidx2 + psser1

    def test_series_eq(self):
        pser = pd.Series([1, 2, 3, 4, 5, 6], name="x")
        psser = ps.from_pandas(pser)

        # other = Series
        pandas_other = pd.Series([np.nan, 1, 3, 4, np.nan, 6], name="x")
        pandas_on_spark_other = ps.from_pandas(pandas_other)
        self.assert_eq(pser.eq(pandas_other), psser.eq(pandas_on_spark_other).sort_index())
        self.assert_eq(pser == pandas_other, (psser == pandas_on_spark_other).sort_index())

        # other = Series with different Index
        pandas_other = pd.Series(
            [np.nan, 1, 3, 4, np.nan, 6], index=[10, 20, 30, 40, 50, 60], name="x"
        )
        pandas_on_spark_other = ps.from_pandas(pandas_other)
        self.assert_eq(pser.eq(pandas_other), psser.eq(pandas_on_spark_other).sort_index())

        # other = Index
        pandas_other = pd.Index([np.nan, 1, 3, 4, np.nan, 6], name="x")
        pandas_on_spark_other = ps.from_pandas(pandas_other)
        self.assert_eq(pser.eq(pandas_other), psser.eq(pandas_on_spark_other).sort_index())
        self.assert_eq(pser == pandas_other, (psser == pandas_on_spark_other).sort_index())


class DiffFramesSeriesTests(DiffFramesSeriesMixin, PandasOnSparkTestCase, SQLTestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_series import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
