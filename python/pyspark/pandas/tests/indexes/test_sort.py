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
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class IndexesSortMixin:
    def _test_sort_values(self, pidx, psidx):
        self.assert_eq(pidx.sort_values(), psidx.sort_values())
        # Parameter ascending
        self.assert_eq(pidx.sort_values(ascending=False), psidx.sort_values(ascending=False))
        # Parameter return_indexer
        p_sorted, p_indexer = pidx.sort_values(return_indexer=True)
        ps_sorted, ps_indexer = psidx.sort_values(return_indexer=True)
        self.assert_eq(p_sorted, ps_sorted)
        self.assert_eq(p_indexer, ps_indexer.to_list())
        self.assert_eq(
            pidx.sort_values(return_indexer=False), psidx.sort_values(return_indexer=False)
        )
        # Parameter return_indexer and ascending
        p_sorted, p_indexer = pidx.sort_values(return_indexer=True, ascending=False)
        ps_sorted, ps_indexer = psidx.sort_values(return_indexer=True, ascending=False)
        self.assert_eq(p_sorted, ps_sorted)
        self.assert_eq(p_indexer, ps_indexer.to_list())
        self.assert_eq(
            pidx.sort_values(return_indexer=False, ascending=False),
            psidx.sort_values(return_indexer=False, ascending=False),
        )

    def test_sort_values(self):
        pidx = pd.Index([-10, -100, 200, 100])
        psidx = ps.from_pandas(pidx)

        self._test_sort_values(pidx, psidx)

        pidx.name = "koalas"
        psidx.name = "koalas"

        self._test_sort_values(pidx, psidx)

        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.from_pandas(pidx)

        pidx.names = ["hello", "koalas", "goodbye"]
        psidx.names = ["hello", "koalas", "goodbye"]

        self._test_sort_values(pidx, psidx)

    def test_index_sort(self):
        idx = ps.Index([1, 2, 3, 4, 5])
        midx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])

        with self.assertRaisesRegex(
            TypeError, "cannot sort an Index object in-place, use sort_values instead"
        ):
            idx.sort()
        with self.assertRaisesRegex(
            TypeError, "cannot sort an Index object in-place, use sort_values instead"
        ):
            midx.sort()


class IndexesSortTests(
    IndexesSortMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_sort import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
