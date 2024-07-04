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

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class CompareSeriesMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_compare(self):
        pser1 = pd.Series(["b", "c", np.nan, "g", np.nan])
        pser2 = pd.Series(["a", "c", np.nan, np.nan, "h"])
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)
        self.assert_eq(
            pser1.compare(pser2).sort_index(),
            psser1.compare(psser2).sort_index(),
        )

        # `keep_shape=True`
        self.assert_eq(
            pser1.compare(pser2, keep_shape=True).sort_index(),
            psser1.compare(psser2, keep_shape=True).sort_index(),
        )
        # `keep_equal=True`
        self.assert_eq(
            pser1.compare(pser2, keep_equal=True).sort_index(),
            psser1.compare(psser2, keep_equal=True).sort_index(),
        )
        # `keep_shape=True` and `keep_equal=True`
        self.assert_eq(
            pser1.compare(pser2, keep_shape=True, keep_equal=True).sort_index(),
            psser1.compare(psser2, keep_shape=True, keep_equal=True).sort_index(),
        )

        # MultiIndex
        pser1.index = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y"), ("c", "z"), ("x", "k"), ("q", "l")]
        )
        pser2.index = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y"), ("c", "z"), ("x", "k"), ("q", "l")]
        )
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)
        self.assert_eq(
            pser1.compare(pser2).sort_index(),
            psser1.compare(psser2).sort_index(),
        )

        # `keep_shape=True` with MultiIndex
        self.assert_eq(
            pser1.compare(pser2, keep_shape=True).sort_index(),
            psser1.compare(psser2, keep_shape=True).sort_index(),
        )
        # `keep_equal=True` with MultiIndex
        self.assert_eq(
            pser1.compare(pser2, keep_equal=True).sort_index(),
            psser1.compare(psser2, keep_equal=True).sort_index(),
        )
        # `keep_shape=True` and `keep_equal=True` with MultiIndex
        self.assert_eq(
            pser1.compare(pser2, keep_shape=True, keep_equal=True).sort_index(),
            psser1.compare(psser2, keep_shape=True, keep_equal=True).sort_index(),
        )

        # Different Index
        with self.assertRaisesRegex(
            ValueError, "Can only compare identically-labeled Series objects"
        ):
            psser1 = ps.Series(
                [1, 2, 3, 4, 5],
                index=pd.Index([1, 2, 3, 4, 5]),
            )
            psser2 = ps.Series(
                [2, 2, 3, 4, 1],
                index=pd.Index([5, 4, 3, 2, 1]),
            )
            psser1.compare(psser2)
        # Different MultiIndex
        with self.assertRaisesRegex(
            ValueError, "Can only compare identically-labeled Series objects"
        ):
            psser1 = ps.Series(
                [1, 2, 3, 4, 5],
                index=pd.MultiIndex.from_tuples(
                    [("a", "x"), ("b", "y"), ("c", "z"), ("x", "k"), ("q", "l")]
                ),
            )
            psser2 = ps.Series(
                [2, 2, 3, 4, 1],
                index=pd.MultiIndex.from_tuples(
                    [("a", "x"), ("b", "y"), ("c", "a"), ("x", "k"), ("q", "l")]
                ),
            )
            psser1.compare(psser2)
        # SPARK-37495: Skip identical index checking of Series.compare when config
        # 'compute.eager_check' is disabled
        psser1 = ps.Series([1, 2, 3, 4, 5], index=pd.Index([1, 2, 3, 4, 5]))
        psser2 = ps.Series([1, 2, 3, 4, 5, 6], index=pd.Index([1, 2, 4, 3, 6, 7]))
        expected = ps.DataFrame(
            {"self": [3, 4, 5, np.nan, np.nan], "other": [4, 3, np.nan, 5.0, 6.0]},
            index=[3, 4, 5, 6, 7],
        )

        with ps.option_context("compute.eager_check", False):
            self.assert_eq(expected, psser1.compare(psser2).sort_index())


class CompareSeriesTests(
    CompareSeriesMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_compare_series import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
