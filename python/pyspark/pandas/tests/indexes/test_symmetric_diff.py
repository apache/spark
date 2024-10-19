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

import pyspark.pandas as ps
from pyspark.loose_version import LooseVersion
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class IndexesSymmetricDiffMixin:
    def test_index_symmetric_difference(self):
        pidx1 = pd.Index([1, 2, 3, 4])
        pidx2 = pd.Index([2, 3, 4, 5])
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)

        self.assert_eq(
            psidx1.symmetric_difference(psidx2).sort_values(),
            pidx1.symmetric_difference(pidx2).sort_values(),
        )
        self.assert_eq(
            (psidx1 + 1).symmetric_difference(psidx2).sort_values(),
            (pidx1 + 1).symmetric_difference(pidx2).sort_values(),
        )
        # No longer supported from pandas 2.0.0.
        if LooseVersion(pd.__version__) >= LooseVersion("2.0.0"):
            self.assert_eq(
                (psidx1 ^ psidx2).sort_values(),
                ps.Index([1, 5], dtype="int64"),
            )
        else:
            self.assert_eq(
                (psidx1 ^ psidx2).sort_values(),
                (pidx1 ^ pidx2).sort_values(),
            )
        self.assert_eq(
            psidx1.symmetric_difference(psidx2, result_name="result").sort_values(),
            pidx1.symmetric_difference(pidx2, result_name="result").sort_values(),
        )

        pmidx1 = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 0, 0, 0, 1, 2, 0, 1, 2]],
        )
        pmidx2 = pd.MultiIndex(
            [["koalas", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 0, 0, 0, 1, 2, 0, 1, 2]],
        )
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)

        self.assert_eq(
            psmidx1.symmetric_difference(psmidx2).sort_values(),
            pmidx1.symmetric_difference(pmidx2).sort_values(),
        )

        # Pandas has a bug that raise TypeError when setting `result_name` for MultiIndex.
        pandas_result = pmidx1.symmetric_difference(pmidx2)
        pandas_result.names = ["a", "b"]
        self.assert_eq(
            psmidx1.symmetric_difference(psmidx2, result_name=["a", "b"]).sort_values(),
            pandas_result,
        )

        # Pandas sort the result by default, so doesn't provide the `True` for sort.
        self.assert_eq(
            psmidx1.symmetric_difference(psmidx2, sort=True),
            pmidx1.symmetric_difference(pmidx2),
        )

        idx = ps.Index(["a", "b", "c"])
        midx = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])

        with self.assertRaisesRegex(NotImplementedError, "Doesn't support*"):
            idx.symmetric_difference(midx)

    def test_multi_index_symmetric_difference(self):
        idx = ps.Index(["a", "b", "c"])
        midx = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        midx_ = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])

        self.assert_eq(
            midx.symmetric_difference(midx_),
            midx._to_pandas().symmetric_difference(midx_._to_pandas()),
        )

        with self.assertRaisesRegex(NotImplementedError, "Doesn't support*"):
            midx.symmetric_difference(idx)


class IndexesSymmetricDiffTests(
    IndexesSymmetricDiffMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_symmetric_diff import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
