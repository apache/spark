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


class IndexesDeleteMixin:
    def test_delete(self):
        pidx = pd.Index([10, 9, 8, 7, 6, 7, 8, 9, 10])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.delete(8).sort_values(), psidx.delete(8).sort_values())
        self.assert_eq(pidx.delete(-9).sort_values(), psidx.delete(-9).sort_values())
        self.assert_eq(
            pidx.delete([-9, 0, 8]).sort_values(), psidx.delete([-9, 0, 8]).sort_values()
        )

        with self.assertRaisesRegex(IndexError, "index 9 is out of bounds for axis 0 with size 9"):
            psidx.delete([0, 9])
        with self.assertRaisesRegex(
            IndexError, "index -10 is out of bounds for axis 0 with size 9"
        ):
            psidx.delete([-10, 0])
        with self.assertRaisesRegex(IndexError, "index 9 is out of bounds for axis 0 with size 9"):
            psidx.delete(9)
        with self.assertRaisesRegex(
            IndexError, "index -10 is out of bounds for axis 0 with size 9"
        ):
            psidx.delete(-10)

        # MultiIndex
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])

        self.assert_eq(pidx.delete(2).sort_values(), psidx.delete(2).sort_values())
        self.assert_eq(pidx.delete(-3).sort_values(), psidx.delete(-3).sort_values())
        self.assert_eq(
            pidx.delete([-3, 0, 2]).sort_values(), psidx.delete([-3, 0, 2]).sort_values()
        )

        with self.assertRaisesRegex(IndexError, "index 3 is out of bounds for axis 0 with size 3"):
            psidx.delete([0, 3])
        with self.assertRaisesRegex(IndexError, "index -4 is out of bounds for axis 0 with size 3"):
            psidx.delete([-4, 0])
        with self.assertRaisesRegex(IndexError, "index 3 is out of bounds for axis 0 with size 3"):
            psidx.delete(3)
        with self.assertRaisesRegex(IndexError, "index -4 is out of bounds for axis 0 with size 3"):
            psidx.delete(-4)


class IndexesDeleteTests(
    IndexesDeleteMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_delete import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
