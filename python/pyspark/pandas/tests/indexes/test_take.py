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


class IndexesTakeMixin:
    def test_take(self):
        # Index
        pidx = pd.Index([100, 200, 300, 400, 500], name="Koalas")
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.take([0, 2, 4]).sort_values(), pidx.take([0, 2, 4]).sort_values())
        self.assert_eq(
            psidx.take(range(0, 5, 2)).sort_values(), pidx.take(range(0, 5, 2)).sort_values()
        )
        self.assert_eq(psidx.take([-4, -2, 0]).sort_values(), pidx.take([-4, -2, 0]).sort_values())
        self.assert_eq(
            psidx.take(range(-4, 1, 2)).sort_values(), pidx.take(range(-4, 1, 2)).sort_values()
        )

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "b"), ("x", "c")], names=["hello", "Koalas"]
        )
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psmidx.take([0, 2]).sort_values(), pmidx.take([0, 2]).sort_values())
        self.assert_eq(
            psmidx.take(range(0, 4, 2)).sort_values(), pmidx.take(range(0, 4, 2)).sort_values()
        )
        self.assert_eq(psmidx.take([-2, 0]).sort_values(), pmidx.take([-2, 0]).sort_values())
        self.assert_eq(
            psmidx.take(range(-2, 1, 2)).sort_values(), pmidx.take(range(-2, 1, 2)).sort_values()
        )

        # Checking the type of indices.
        self.assertRaises(TypeError, lambda: psidx.take(1))
        self.assertRaises(TypeError, lambda: psidx.take("1"))
        self.assertRaises(TypeError, lambda: psidx.take({1, 2}))
        self.assertRaises(TypeError, lambda: psidx.take({1: None, 2: None}))
        self.assertRaises(TypeError, lambda: psmidx.take(1))
        self.assertRaises(TypeError, lambda: psmidx.take("1"))
        self.assertRaises(TypeError, lambda: psmidx.take({1, 2}))
        self.assertRaises(TypeError, lambda: psmidx.take({1: None, 2: None}))


class IndexesTakeTests(
    IndexesTakeMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_take import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
