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


class IndexingLocMultiIdxMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_loc_multiindex(self):
        psdf = self.psdf
        psdf = psdf.set_index("b", append=True)
        pdf = self.pdf
        pdf = pdf.set_index("b", append=True)

        self.assert_eq(psdf.loc[:], pdf.loc[:])
        self.assert_eq(psdf.loc[5:5], pdf.loc[5:5])
        self.assert_eq(psdf.loc[5:9], pdf.loc[5:9])

        self.assert_eq(psdf.loc[5], pdf.loc[5])
        self.assert_eq(psdf.loc[9], pdf.loc[9])
        # TODO: self.assert_eq(psdf.loc[(5, 3)], pdf.loc[(5, 3)])
        # TODO: self.assert_eq(psdf.loc[(9, 0)], pdf.loc[(9, 0)])
        self.assert_eq(psdf.a.loc[5], pdf.a.loc[5])
        self.assert_eq(psdf.a.loc[9], pdf.a.loc[9])
        self.assertTrue((psdf.a.loc[(5, 3)] == pdf.a.loc[(5, 3)]).all())
        self.assert_eq(psdf.a.loc[(9, 0)], pdf.a.loc[(9, 0)])

        # monotonically increasing index test
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5]},
            index=pd.MultiIndex.from_tuples(
                [("x", "a"), ("x", "b"), ("y", "c"), ("y", "d"), ("z", "e")]
            ),
        )
        psdf = ps.from_pandas(pdf)

        for rows_sel in [
            slice(None),
            slice("y", None),
            slice(None, "y"),
            slice(("x", "b"), None),
            slice(None, ("y", "c")),
            slice(("x", "b"), ("y", "c")),
            slice("x", ("y", "c")),
            slice(("x", "b"), "y"),
        ]:
            with self.subTest("monotonically increasing", rows_sel=rows_sel):
                self.assert_eq(psdf.loc[rows_sel], pdf.loc[rows_sel])
                self.assert_eq(psdf.a.loc[rows_sel], pdf.a.loc[rows_sel])

        # monotonically increasing first index test
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5]},
            index=pd.MultiIndex.from_tuples(
                [("x", "a"), ("x", "b"), ("y", "c"), ("y", "a"), ("z", "e")]
            ),
        )
        psdf = ps.from_pandas(pdf)

        for rows_sel in [
            slice(None),
            slice("y", None),
            slice(None, "y"),
        ]:
            with self.subTest("monotonically increasing first index", rows_sel=rows_sel):
                self.assert_eq(psdf.loc[rows_sel], pdf.loc[rows_sel])
                self.assert_eq(psdf.a.loc[rows_sel], pdf.a.loc[rows_sel])

        for rows_sel in [
            slice(("x", "b"), None),
            slice(None, ("y", "c")),
            slice(("x", "b"), ("y", "c")),
            slice("x", ("y", "c")),
            slice(("x", "b"), "y"),
        ]:
            with self.subTest("monotonically increasing first index", rows_sel=rows_sel):
                self.assertRaises(KeyError, lambda: psdf.loc[rows_sel])
                self.assertRaises(KeyError, lambda: psdf.a.loc[rows_sel])

        # not monotonically increasing index test
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5]},
            index=pd.MultiIndex.from_tuples(
                [("z", "e"), ("y", "d"), ("y", "c"), ("x", "b"), ("x", "a")]
            ),
        )
        psdf = ps.from_pandas(pdf)

        for rows_sel in [
            slice("y", None),
            slice(None, "y"),
            slice(("x", "b"), None),
            slice(None, ("y", "c")),
            slice(("x", "b"), ("y", "c")),
            slice("x", ("y", "c")),
            slice(("x", "b"), "y"),
        ]:
            with self.subTest("monotonically decreasing", rows_sel=rows_sel):
                self.assertRaises(KeyError, lambda: psdf.loc[rows_sel])
                self.assertRaises(KeyError, lambda: psdf.a.loc[rows_sel])


class IndexingLocMultiIdxTests(
    IndexingLocMultiIdxMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_indexing_loc_multi_idx import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
