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


class UnionMixin:
    def test_union(self):
        # Index
        pidx1 = pd.Index([1, 2, 3, 4])
        pidx2 = pd.Index([3, 4, 5, 6])
        pidx3 = pd.Index([7.0, 8.0, 9.0, 10.0])
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)
        psidx3 = ps.from_pandas(pidx3)

        self.assert_eq(psidx1.union(psidx2), pidx1.union(pidx2))
        self.assert_eq(psidx2.union(psidx1), pidx2.union(pidx1))
        self.assert_eq(psidx1.union(psidx3), pidx1.union(pidx3))

        self.assert_eq(psidx1.union([3, 4, 5, 6]), pidx1.union([3, 4, 5, 6]), almost=True)
        self.assert_eq(psidx2.union([1, 2, 3, 4]), pidx2.union([1, 2, 3, 4]), almost=True)
        self.assert_eq(
            psidx1.union(ps.Series([3, 4, 5, 6])), pidx1.union(pd.Series([3, 4, 5, 6])), almost=True
        )
        self.assert_eq(
            psidx2.union(ps.Series([1, 2, 3, 4])), pidx2.union(pd.Series([1, 2, 3, 4])), almost=True
        )

        # Testing if the result is correct after sort=False.
        self.assert_eq(
            psidx1.union(psidx2, sort=False).sort_values(),
            pidx1.union(pidx2, sort=False).sort_values(),
        )
        self.assert_eq(
            psidx2.union(psidx1, sort=False).sort_values(),
            pidx2.union(pidx1, sort=False).sort_values(),
        )
        self.assert_eq(
            psidx1.union([3, 4, 5, 6], sort=False).sort_values(),
            pidx1.union([3, 4, 5, 6], sort=False).sort_values(),
            almost=True,
        )
        self.assert_eq(
            psidx2.union([1, 2, 3, 4], sort=False).sort_values(),
            pidx2.union([1, 2, 3, 4], sort=False).sort_values(),
            almost=True,
        )
        self.assert_eq(
            psidx1.union(ps.Series([3, 4, 5, 6]), sort=False).sort_values(),
            pidx1.union(pd.Series([3, 4, 5, 6]), sort=False).sort_values(),
            almost=True,
        )
        self.assert_eq(
            psidx2.union(ps.Series([1, 2, 3, 4]), sort=False).sort_values(),
            pidx2.union(pd.Series([1, 2, 3, 4]), sort=False).sort_values(),
            almost=True,
        )

        pidx1 = pd.Index([1, 2, 3, 4, 3, 4, 3, 4])
        pidx2 = pd.Index([3, 4, 3, 4, 5, 6])
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)

        self.assert_eq(psidx1.union(psidx2), pidx1.union(pidx2))
        self.assert_eq(
            psidx1.union([3, 4, 3, 3, 5, 6]), pidx1.union([3, 4, 3, 4, 5, 6]), almost=True
        )
        self.assert_eq(
            psidx1.union(ps.Series([3, 4, 3, 3, 5, 6])),
            pidx1.union(pd.Series([3, 4, 3, 4, 5, 6])),
            almost=True,
        )

        # Manually create the expected result here since there is a bug in Index.union
        # dropping duplicated values in pandas < 1.3.
        expected = pd.Index([1, 2, 3, 3, 3, 4, 4, 4, 5, 6])
        self.assert_eq(psidx2.union(psidx1), expected)
        self.assert_eq(
            psidx2.union([1, 2, 3, 4, 3, 4, 3, 4]),
            expected,
            almost=True,
        )
        self.assert_eq(
            psidx2.union(ps.Series([1, 2, 3, 4, 3, 4, 3, 4])),
            expected,
            almost=True,
        )

        # MultiIndex
        pmidx1 = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")])
        pmidx2 = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")])
        pmidx3 = pd.MultiIndex.from_tuples([(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)])
        pmidx4 = pd.MultiIndex.from_tuples([(1, 3), (1, 4), (1, 5), (1, 6)])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        psmidx3 = ps.from_pandas(pmidx3)
        psmidx4 = ps.from_pandas(pmidx4)

        # Manually create the expected result here since there is a bug in MultiIndex.union
        # dropping duplicated values in pandas < 1.3.
        expected = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "a"), ("x", "b"), ("x", "b"), ("x", "c"), ("x", "d")]
        )
        self.assert_eq(psmidx1.union(psmidx2), expected)
        self.assert_eq(psmidx2.union(psmidx1), expected)
        self.assert_eq(
            psmidx1.union([("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")]),
            expected,
        )
        self.assert_eq(
            psmidx2.union([("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")]),
            expected,
        )

        expected = pd.MultiIndex.from_tuples(
            [(1, 1), (1, 2), (1, 3), (1, 3), (1, 4), (1, 4), (1, 5), (1, 6)]
        )
        self.assert_eq(psmidx3.union(psmidx4), expected)
        self.assert_eq(psmidx4.union(psmidx3), expected)
        self.assert_eq(
            psmidx3.union([(1, 3), (1, 4), (1, 5), (1, 6)]),
            expected,
        )
        self.assert_eq(
            psmidx4.union([(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)]),
            expected,
        )

        # Testing if the result is correct after sort=False.
        # Manually create the expected result here since there is a bug in MultiIndex.union
        # dropping duplicated values in pandas < 1.3.
        expected = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "a"), ("x", "b"), ("x", "b"), ("x", "c"), ("x", "d")]
        )
        self.assert_eq(psmidx1.union(psmidx2, sort=False).sort_values(), expected)
        self.assert_eq(psmidx2.union(psmidx1, sort=False).sort_values(), expected)
        self.assert_eq(
            psmidx1.union(
                [("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")], sort=False
            ).sort_values(),
            expected,
        )
        self.assert_eq(
            psmidx2.union(
                [("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")], sort=False
            ).sort_values(),
            expected,
        )

        expected = pd.MultiIndex.from_tuples(
            [(1, 1), (1, 2), (1, 3), (1, 3), (1, 4), (1, 4), (1, 5), (1, 6)]
        )
        self.assert_eq(psmidx3.union(psmidx4, sort=False).sort_values(), expected)
        self.assert_eq(psmidx4.union(psmidx3, sort=False).sort_values(), expected)
        self.assert_eq(
            psmidx3.union([(1, 3), (1, 4), (1, 5), (1, 6)], sort=False).sort_values(), expected
        )
        self.assert_eq(
            psmidx4.union(
                [(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)], sort=False
            ).sort_values(),
            expected,
        )

        self.assertRaises(NotImplementedError, lambda: psidx1.union(psmidx1))
        self.assertRaises(TypeError, lambda: psmidx1.union(psidx1))
        self.assertRaises(TypeError, lambda: psmidx1.union(["x", "a"]))
        self.assertRaises(ValueError, lambda: psidx1.union(ps.range(2)))


class UnionTests(
    UnionMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_union import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
