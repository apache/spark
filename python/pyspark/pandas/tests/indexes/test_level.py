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


class LevelMixin:
    def test_multi_index_levshape(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        psidx = ps.from_pandas(pidx)
        self.assertEqual(pidx.levshape, psidx.levshape)

    def test_index_nlevels(self):
        pdf = pd.DataFrame({"a": [1, 2, 3]}, index=pd.Index(["a", "b", "c"]))
        psdf = ps.from_pandas(pdf)

        self.assertEqual(psdf.index.nlevels, 1)

    def test_multiindex_nlevel(self):
        pdf = pd.DataFrame({"a": [1, 2, 3]}, index=[list("abc"), list("def")])
        psdf = ps.from_pandas(pdf)

        self.assertEqual(psdf.index.nlevels, 2)

    def test_multiindex_swaplevel(self):
        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(0, 1), psidx.swaplevel(0, 1))

        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]], names=["word", "number"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(0, 1), psidx.swaplevel(0, 1))

        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]], names=["word", None])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(-2, -1), psidx.swaplevel(-2, -1))
        self.assert_eq(pidx.swaplevel(0, 1), psidx.swaplevel(0, 1))
        self.assert_eq(pidx.swaplevel("word", 1), psidx.swaplevel("word", 1))

        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            psidx.swaplevel(-3, "word")
        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            psidx.swaplevel(0, 2)
        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            psidx.swaplevel(0, -3)
        with self.assertRaisesRegex(KeyError, "Level work not found"):
            psidx.swaplevel(0, "work")

    def test_index_get_level_values(self):
        pidx = pd.Index([1, 2, 3], name="ks")
        psidx = ps.from_pandas(pidx)

        for level in [0, "ks"]:
            self.assert_eq(psidx.get_level_values(level), pidx.get_level_values(level))

    def test_multiindex_get_level_values(self):
        pmidx = pd.MultiIndex.from_tuples([("a", "d"), ("b", "e"), ("c", "f")])
        pmidx.names = ["level_1", "level_2"]
        psmidx = ps.from_pandas(pmidx)

        for level in [0, 1, "level_1", "level_2"]:
            self.assert_eq(psmidx.get_level_values(level), pmidx.get_level_values(level))

    def test_index_get_level_number(self):
        # name of two levels are the same, which is None
        psdf = ps.DataFrame({"a": [1, 2, 3]}, index=[list("aac"), list("ddf")])
        with self.assertRaisesRegex(
            ValueError, "The name None occurs multiple times, use a level number"
        ):
            psdf.index._get_level_number(None)

        mi = pd.MultiIndex.from_arrays((list("abc"), list("def")))
        mi.names = ["level_1", "level_2"]
        psdf = ps.DataFrame({"a": [1, 2, 3]}, index=mi)

        # level is not int and not in the level name list
        with self.assertRaisesRegex(KeyError, "Level lv_3 not found"):
            psdf.index._get_level_number("lv_3")

        # level is int, but an invalid negative number
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only"):
            psdf.index._get_level_number(-3)

        # level is int, but an invalid positive number
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only"):
            psdf.index._get_level_number(3)

        # Correct and valid inputs in numbers
        level_number = [-2, -1, 0, 1]
        outputs = [0, 1, 0, 1]

        for lv, output in zip(level_number, outputs):
            self.assertEqual(output, psdf.index._get_level_number(lv))

        # Valid inputs as level names
        level_names = ["level_1", "level_2"]
        outputs = [0, 1]

        for lv, output in zip(level_names, outputs):
            self.assertEqual(output, psdf.index._get_level_number(lv))

    def test_multiindex_equal_levels(self):
        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pmidx2 = pd.MultiIndex.from_tuples([("b", "y"), ("a", "x"), ("c", "z")])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "j")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("a", "x")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "y"), ("b", "x"), ("c", "z")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z"), ("a", "y")])
        pmidx2 = pd.MultiIndex.from_tuples([("a", "y"), ("b", "x"), ("c", "z"), ("c", "x")])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pmidx2 = pd.MultiIndex.from_tuples([("a", "x", "q"), ("b", "y", "w"), ("c", "z", "e")])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))


class LevelTests(
    LevelMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_level import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
