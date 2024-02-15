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

import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class IndexesDiffMixin:
    def test_difference(self):
        # Index
        pidx1 = pd.Index([1, 2, 3, 4], name="koalas")
        pidx2 = pd.Index([3, 4, 5, 6], name="koalas")
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)
        # Series
        pser = pd.Series([3, 4, 5, 6], name="koalas")
        psser = ps.from_pandas(pser)

        self.assert_eq(
            psidx1.difference(psidx2).sort_values(), pidx1.difference(pidx2).sort_values()
        )
        self.assert_eq(psidx1.difference(psser).sort_values(), pidx1.difference(pser).sort_values())
        self.assert_eq(
            psidx1.difference([3, 4, 5, 6]).sort_values(),
            pidx1.difference([3, 4, 5, 6]).sort_values(),
        )
        self.assert_eq(
            psidx1.difference((3, 4, 5, 6)).sort_values(),
            pidx1.difference((3, 4, 5, 6)).sort_values(),
        )
        self.assert_eq(
            psidx1.difference({3, 4, 5, 6}).sort_values(),
            pidx1.difference({3, 4, 5, 6}).sort_values(),
        )
        self.assert_eq(
            psidx1.difference({3: 1, 4: 2, 5: 3, 6: 4}).sort_values(),
            pidx1.difference({3: 1, 4: 2, 5: 3, 6: 4}).sort_values(),
        )

        # Exceptions for Index
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx1.difference("1234")
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx1.difference(1234)
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx1.difference(12.34)
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx1.difference(None)
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx1.difference(np.nan)
        with self.assertRaisesRegex(
            ValueError, "The 'sort' keyword only takes the values of None or True; 1 was passed."
        ):
            psidx1.difference(psidx2, sort=1)

        # MultiIndex
        pmidx1 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2), ("c", "z", 3)], names=["hello", "koalas", "world"]
        )
        pmidx2 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "z", 2), ("k", "z", 3)], names=["hello", "koalas", "world"]
        )
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)

        self.assert_eq(
            psmidx1.difference(psmidx2).sort_values(), pmidx1.difference(pmidx2).sort_values()
        )
        self.assert_eq(
            psmidx1.difference(psidx1).sort_values(), pmidx1.difference(pidx1).sort_values()
        )
        self.assert_eq(
            psidx1.difference(psmidx1).sort_values(), pidx1.difference(pmidx1).sort_values()
        )
        self.assert_eq(psidx1.difference(psser).sort_values(), pidx1.difference(pser).sort_values())
        self.assert_eq(
            psmidx1.difference({("a", "x", 1)}).sort_values(),
            pmidx1.difference({("a", "x", 1)}).sort_values(),
        )
        self.assert_eq(
            psmidx1.difference({("a", "x", 1): [1, 2, 3]}).sort_values(),
            pmidx1.difference({("a", "x", 1): [1, 2, 3]}).sort_values(),
        )

        # Exceptions for MultiIndex
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            psmidx1.difference(["b", "z", "2"])


class IndexesDiffTests(
    IndexesDiffMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_diff import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
