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


class UniqueMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_index_unique(self):
        psidx = self.psdf.index

        # here the output is different than pandas in terms of order
        expected = [0, 1, 3, 5, 6, 8, 9]

        self.assert_eq(expected, sorted(psidx.unique()._to_pandas()))
        self.assert_eq(expected, sorted(psidx.unique(level=0)._to_pandas()))

        expected = [1, 2, 4, 6, 7, 9, 10]
        self.assert_eq(expected, sorted((psidx + 1).unique()._to_pandas()))

        with self.assertRaisesRegex(IndexError, "Too many levels*"):
            psidx.unique(level=1)

        with self.assertRaisesRegex(KeyError, "Requested level (hi)*"):
            psidx.unique(level="hi")

    def test_unique(self):
        pidx = pd.Index(["a", "b", "a"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.unique().sort_values(), pidx.unique().sort_values())
        self.assert_eq(psidx.unique().sort_values(), pidx.unique().sort_values())

        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "a")])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psmidx.unique().sort_values(), pmidx.unique().sort_values())
        self.assert_eq(psmidx.unique().sort_values(), pmidx.unique().sort_values())

        with self.assertRaisesRegex(
            IndexError, "Too many levels: Index has only 1 level, -2 is not a valid level number"
        ):
            psidx.unique(level=-2)

    def test_index_is_unique(self):
        indexes = [("a", "b", "c"), ("a", "a", "c"), (1, 3, 3), (1, 2, 3)]
        names = [None, "ks", "ks", None]
        is_uniq = [True, False, False, True]

        for idx, name, expected in zip(indexes, names, is_uniq):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=pd.Index(idx, name=name))
            psdf = ps.from_pandas(pdf)

            self.assertEqual(psdf.index.is_unique, expected)

    def test_multiindex_is_unique(self):
        indexes = [
            [list("abc"), list("edf")],
            [list("aac"), list("edf")],
            [list("aac"), list("eef")],
            [[1, 4, 4], [4, 6, 6]],
        ]
        is_uniq = [True, True, False, False]

        for idx, expected in zip(indexes, is_uniq):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=idx)
            psdf = ps.from_pandas(pdf)

            self.assertEqual(psdf.index.is_unique, expected)

    def test_index_nunique(self):
        pidx = pd.Index([1, 1, 2, None])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.nunique(), psidx.nunique())
        self.assert_eq(pidx.nunique(dropna=True), psidx.nunique(dropna=True))

    def test_multiindex_nunique(self):
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        with self.assertRaisesRegex(NotImplementedError, "notna is not defined for MultiIndex"):
            psidx.notnull()

    def test_multi_index_nunique(self):
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "green")]
        pmidx = pd.MultiIndex.from_tuples(tuples)
        psmidx = ps.from_pandas(pmidx)

        with self.assertRaisesRegex(NotImplementedError, "nunique is not defined for MultiIndex"):
            psmidx.nunique()

    def test_index_has_duplicates(self):
        indexes = [("a", "b", "c"), ("a", "a", "c"), (1, 3, 3), (1, 2, 3)]
        names = [None, "ks", "ks", None]
        has_dup = [False, True, True, False]

        for idx, name, expected in zip(indexes, names, has_dup):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=pd.Index(idx, name=name))
            psdf = ps.from_pandas(pdf)

            self.assertEqual(psdf.index.has_duplicates, expected)

    def test_multiindex_has_duplicates(self):
        indexes = [
            [list("abc"), list("edf")],
            [list("aac"), list("edf")],
            [list("aac"), list("eef")],
            [[1, 4, 4], [4, 6, 6]],
        ]
        has_dup = [False, False, True, True]

        for idx, expected in zip(indexes, has_dup):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=idx)
            psdf = ps.from_pandas(pdf)

            self.assertEqual(psdf.index.has_duplicates, expected)


class UniqueTests(
    UniqueMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_unique import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
