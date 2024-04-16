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


class IndexesInsertMixin:
    def test_insert(self):
        # Integer
        pidx = pd.Index([1, 2, 3], name="Koalas")
        psidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.insert(1, 100).sort_values(),
            psidx.insert(1, 100).sort_values(),
        )
        self.assert_eq(
            pidx.insert(-1, 100).sort_values(),
            psidx.insert(-1, 100).sort_values(),
        )
        err_msg = "index 100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(100, 100)
        err_msg = "index -100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(-100, 100)

        # Floating
        pidx = pd.Index([1.0, 2.0, 3.0], name="Koalas")
        psidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.insert(1, 100.0).sort_values(),
            psidx.insert(1, 100.0).sort_values(),
        )
        self.assert_eq(
            pidx.insert(-1, 100.0).sort_values(),
            psidx.insert(-1, 100.0).sort_values(),
        )
        err_msg = "index 100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(100, 100)
        err_msg = "index -100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(-100, 100)

        # String
        pidx = pd.Index(["a", "b", "c"], name="Koalas")
        psidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.insert(1, "x").sort_values(),
            psidx.insert(1, "x").sort_values(),
        )
        self.assert_eq(
            pidx.insert(-1, "x").sort_values(),
            psidx.insert(-1, "x").sort_values(),
        )
        err_msg = "index 100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(100, "x")
        err_msg = "index -100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(-100, "x")

        # Boolean
        pidx = pd.Index([True, False, True, False], name="Koalas")
        psidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.insert(1, True).sort_values(),
            psidx.insert(1, True).sort_values(),
        )
        self.assert_eq(
            pidx.insert(-1, True).sort_values(),
            psidx.insert(-1, True).sort_values(),
        )
        err_msg = "index 100 is out of bounds for axis 0 with size 4"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(100, True)
        err_msg = "index -100 is out of bounds for axis 0 with size 4"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(-100, True)

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y"), ("c", "z")], names=["Hello", "Koalas"]
        )
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(
            pmidx.insert(2, ("h", "j")).sort_values(),
            psmidx.insert(2, ("h", "j")).sort_values(),
        )
        self.assert_eq(
            pmidx.insert(-1, ("h", "j")).sort_values(),
            psmidx.insert(-1, ("h", "j")).sort_values(),
        )

        err_msg = "index 4 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psmidx.insert(4, ("b", "y"))

        err_msg = "index -4 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psmidx.insert(-4, ("b", "y"))


class IndexesInsertTests(
    IndexesInsertMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_insert import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
