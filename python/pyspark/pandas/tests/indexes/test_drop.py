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


class IndexesDropMixin:
    def test_drop_duplicates(self):
        pidx = pd.Index([4, 2, 4, 1, 4, 3])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.drop_duplicates(), pidx.drop_duplicates())
        self.assert_eq((psidx + 1).drop_duplicates(), (pidx + 1).drop_duplicates())

        self.assert_eq(psidx.drop_duplicates(keep="first"), pidx.drop_duplicates(keep="first"))
        self.assert_eq(psidx.drop_duplicates(keep="last"), pidx.drop_duplicates(keep="last"))
        self.assert_eq(psidx.drop_duplicates(keep=False), pidx.drop_duplicates(keep=False))

        arrays = [[1, 2, 3, 1, 2], ["red", "blue", "black", "red", "blue"]]
        pmidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(psmidx.drop_duplicates(), pmidx.drop_duplicates())
        self.assert_eq(psmidx.drop_duplicates(keep="first"), pmidx.drop_duplicates(keep="first"))
        self.assert_eq(psmidx.drop_duplicates(keep="last"), pmidx.drop_duplicates(keep="last"))
        self.assert_eq(psmidx.drop_duplicates(keep=False), pmidx.drop_duplicates(keep=False))

    def test_dropna(self):
        pidx = pd.Index([np.nan, 2, 4, 1, None, 3])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.dropna(), pidx.dropna())
        self.assert_eq((psidx + 1).dropna(), (pidx + 1).dropna())

        self.assert_eq(psidx.dropna(how="any"), pidx.dropna(how="any"))
        self.assert_eq(psidx.dropna(how="all"), pidx.dropna(how="all"))

        pmidx = pd.MultiIndex.from_tuples(
            [(np.nan, 1.0), (2.0, 2.0), (np.nan, None), (3.0, np.nan)]
        )
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(psmidx.dropna(), pmidx.dropna())
        self.assert_eq(psmidx.dropna(how="any"), pmidx.dropna(how="any"))
        self.assert_eq(psmidx.dropna(how="all"), pmidx.dropna(how="all"))

        invalid_how = "none"
        with self.assertRaisesRegex(ValueError, "invalid how option: %s" % invalid_how):
            psmidx.dropna(invalid_how)

    def test_drop_level(self):
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "green")]
        pmidx = pd.MultiIndex.from_tuples(tuples)
        psmidx = ps.from_pandas(pmidx)

        with self.assertRaisesRegex(
            IndexError, "Too many levels: Index has only 2 levels, -3 is not a valid level number"
        ):
            psmidx.droplevel(-3)

    def test_multiindex_droplevel(self):
        pidx = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2)], names=["level1", "level2", "level3"]
        )
        psidx = ps.from_pandas(pidx)
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only 3 levels, not 5"):
            psidx.droplevel(4)

        with self.assertRaisesRegex(KeyError, "Level level4 not found"):
            psidx.droplevel("level4")

        with self.assertRaisesRegex(KeyError, "Level.*level3.*level4.*not found"):
            psidx.droplevel([("level3", "level4")])

        with self.assertRaisesRegex(
            ValueError,
            "Cannot remove 4 levels from an index with 3 levels: at least one "
            "level must be left.",
        ):
            psidx.droplevel([0, 0, 1, 2])

        with self.assertRaisesRegex(
            ValueError,
            "Cannot remove 3 levels from an index with 3 levels: at least one "
            "level must be left.",
        ):
            psidx.droplevel([0, 1, 2])

        self.assert_eq(pidx.droplevel(0), psidx.droplevel(0))
        self.assert_eq(pidx.droplevel([0, 1]), psidx.droplevel([0, 1]))
        self.assert_eq(pidx.droplevel((0, 1)), psidx.droplevel((0, 1)))
        self.assert_eq(pidx.droplevel([0, "level2"]), psidx.droplevel([0, "level2"]))
        self.assert_eq(pidx.droplevel((0, "level2")), psidx.droplevel((0, "level2")))

        # non-string names
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)], names=[1.0, 2.0, 3.0])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.droplevel(1.0), psidx.droplevel(1.0))
        self.assert_eq(pidx.droplevel([0, 2.0]), psidx.droplevel([0, 2.0]))

    def test_index_drop(self):
        pidx = pd.Index([1, 2, 3])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.drop(1), psidx.drop(1))
        self.assert_eq(pidx.drop([1, 2]), psidx.drop([1, 2]))
        self.assert_eq((pidx + 1).drop([2, 3]), (psidx + 1).drop([2, 3]))

    def test_multiindex_drop(self):
        pidx = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y"), ("c", "z")], names=["level1", "level2"]
        )
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.drop("a"), psidx.drop("a"))
        self.assert_eq(pidx.drop(["a", "b"]), psidx.drop(["a", "b"]))
        self.assert_eq(pidx.drop(["x", "y"], level=1), psidx.drop(["x", "y"], level=1))
        self.assert_eq(
            pidx.drop(["x", "y"], level="level2"), psidx.drop(["x", "y"], level="level2")
        )

        pidx.names = ["lv1", "lv2"]
        psidx.names = ["lv1", "lv2"]
        self.assert_eq(pidx.drop(["x", "y"], level="lv2"), psidx.drop(["x", "y"], level="lv2"))

        self.assertRaises(IndexError, lambda: psidx.drop(["a", "b"], level=2))
        self.assertRaises(KeyError, lambda: psidx.drop(["a", "b"], level="level"))

        psidx.names = ["lv", "lv"]
        self.assertRaises(ValueError, lambda: psidx.drop(["x", "y"], level="lv"))

    def test_index_drop_duplicates(self):
        pidx = pd.Index([1, 1, 2])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.drop_duplicates().sort_values(), psidx.drop_duplicates().sort_values())

        pidx = pd.MultiIndex.from_tuples([(1, 1), (1, 1), (2, 2)], names=["level1", "level2"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.drop_duplicates().sort_values(), psidx.drop_duplicates().sort_values())


class IndexesDropTests(
    IndexesDropMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_drop import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
