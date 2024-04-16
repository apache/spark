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

from pyspark import pandas as ps
from pyspark.pandas.exceptions import SparkPandasIndexingError, SparkPandasNotImplementedError
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class IndexingLoc2DMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def pdf2(self):
        return pd.DataFrame(
            {0: [1, 2, 3, 4, 5, 6, 7, 8, 9], 1: [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    @property
    def psdf2(self):
        return ps.from_pandas(self.pdf2)

    def test_loc2d_multiindex(self):
        psdf = self.psdf
        psdf = psdf.set_index("b", append=True)
        pdf = self.pdf
        pdf = pdf.set_index("b", append=True)

        self.assert_eq(psdf.loc[:, :], pdf.loc[:, :])
        self.assert_eq(psdf.loc[:, "a"], pdf.loc[:, "a"])
        self.assert_eq(psdf.loc[5:5, "a"], pdf.loc[5:5, "a"])

        self.assert_eq(psdf.loc[:, "a":"a"], pdf.loc[:, "a":"a"])
        self.assert_eq(psdf.loc[:, "a":"c"], pdf.loc[:, "a":"c"])
        self.assert_eq(psdf.loc[:, "b":"c"], pdf.loc[:, "b":"c"])

    def test_loc2d(self):
        psdf = self.psdf
        pdf = self.pdf

        # index indexer is always regarded as slice for duplicated values
        self.assert_eq(psdf.loc[5:5, "a"], pdf.loc[5:5, "a"])
        self.assert_eq(psdf.loc[[5], "a"], pdf.loc[[5], "a"])
        self.assert_eq(psdf.loc[5:5, ["a"]], pdf.loc[5:5, ["a"]])
        self.assert_eq(psdf.loc[[5], ["a"]], pdf.loc[[5], ["a"]])
        self.assert_eq(psdf.loc[:, :], pdf.loc[:, :])

        self.assert_eq(psdf.loc[3:8, "a"], pdf.loc[3:8, "a"])
        self.assert_eq(psdf.loc[:8, "a"], pdf.loc[:8, "a"])
        self.assert_eq(psdf.loc[3:, "a"], pdf.loc[3:, "a"])
        self.assert_eq(psdf.loc[[8], "a"], pdf.loc[[8], "a"])

        self.assert_eq(psdf.loc[3:8, ["a"]], pdf.loc[3:8, ["a"]])
        self.assert_eq(psdf.loc[:8, ["a"]], pdf.loc[:8, ["a"]])
        self.assert_eq(psdf.loc[3:, ["a"]], pdf.loc[3:, ["a"]])
        # TODO?: self.assert_eq(psdf.loc[[3, 4, 3], ['a']], pdf.loc[[3, 4, 3], ['a']])

        self.assertRaises(SparkPandasIndexingError, lambda: psdf.loc[3, 3, 3])
        self.assertRaises(SparkPandasIndexingError, lambda: psdf.a.loc[3, 3])
        self.assertRaises(SparkPandasIndexingError, lambda: psdf.a.loc[3:, 3])
        self.assertRaises(SparkPandasIndexingError, lambda: psdf.a.loc[psdf.a % 2 == 0, 3])

        self.assert_eq(psdf.loc[5, "a"], pdf.loc[5, "a"])
        self.assert_eq(psdf.loc[9, "a"], pdf.loc[9, "a"])
        self.assert_eq(psdf.loc[5, ["a"]], pdf.loc[5, ["a"]])
        self.assert_eq(psdf.loc[9, ["a"]], pdf.loc[9, ["a"]])

        self.assert_eq(psdf.loc[:, "a":"a"], pdf.loc[:, "a":"a"])
        self.assert_eq(psdf.loc[:, "a":"d"], pdf.loc[:, "a":"d"])
        self.assert_eq(psdf.loc[:, "c":"d"], pdf.loc[:, "c":"d"])

        # bool list-like column select
        bool_list = [True, False]
        self.assert_eq(psdf.loc[:, bool_list], pdf.loc[:, bool_list])
        self.assert_eq(psdf.loc[:, np.array(bool_list)], pdf.loc[:, np.array(bool_list)])

        pser = pd.Series(bool_list, index=pdf.columns)
        self.assert_eq(psdf.loc[:, pser], pdf.loc[:, pser])
        pser = pd.Series(list(reversed(bool_list)), index=list(reversed(pdf.columns)))
        self.assert_eq(psdf.loc[:, pser], pdf.loc[:, pser])

        self.assertRaises(IndexError, lambda: psdf.loc[:, bool_list[:-1]])
        self.assertRaises(IndexError, lambda: psdf.loc[:, np.array(bool_list + [True])])
        self.assertRaises(SparkPandasIndexingError, lambda: psdf.loc[:, pd.Series(bool_list)])

        # non-string column names
        psdf = self.psdf2
        pdf = self.pdf2

        self.assert_eq(psdf.loc[5:5, 0], pdf.loc[5:5, 0])
        self.assert_eq(psdf.loc[5:5, [0]], pdf.loc[5:5, [0]])
        self.assert_eq(psdf.loc[3:8, 0], pdf.loc[3:8, 0])
        self.assert_eq(psdf.loc[3:8, [0]], pdf.loc[3:8, [0]])

        self.assert_eq(psdf.loc[:, 0:0], pdf.loc[:, 0:0])
        self.assert_eq(psdf.loc[:, 0:3], pdf.loc[:, 0:3])
        self.assert_eq(psdf.loc[:, 2:3], pdf.loc[:, 2:3])

    def test_loc2d_multiindex_columns(self):
        arrays = [np.array(["bar", "bar", "baz", "baz"]), np.array(["one", "two", "one", "two"])]

        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "B", "C"], columns=arrays)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.loc["B":"B", "bar"], pdf.loc["B":"B", "bar"])
        self.assert_eq(psdf.loc["B":"B", ["bar"]], pdf.loc["B":"B", ["bar"]])

        self.assert_eq(psdf.loc[:, "bar":"bar"], pdf.loc[:, "bar":"bar"])
        self.assert_eq(psdf.loc[:, "bar":("baz", "one")], pdf.loc[:, "bar":("baz", "one")])
        self.assert_eq(
            psdf.loc[:, ("bar", "two"):("baz", "one")], pdf.loc[:, ("bar", "two"):("baz", "one")]
        )
        self.assert_eq(psdf.loc[:, ("bar", "two"):"bar"], pdf.loc[:, ("bar", "two"):"bar"])
        self.assert_eq(psdf.loc[:, "a":"bax"], pdf.loc[:, "a":"bax"])
        self.assert_eq(
            psdf.loc[:, ("bar", "x"):("baz", "a")],
            pdf.loc[:, ("bar", "x"):("baz", "a")],
            almost=True,
        )

        pdf = pd.DataFrame(
            np.random.randn(3, 4),
            index=["A", "B", "C"],
            columns=pd.MultiIndex.from_tuples(
                [("bar", "two"), ("bar", "one"), ("baz", "one"), ("baz", "two")]
            ),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.loc[:, "bar":"baz"], pdf.loc[:, "bar":"baz"])

        self.assertRaises(KeyError, lambda: psdf.loc[:, "bar":("baz", "one")])
        self.assertRaises(KeyError, lambda: psdf.loc[:, ("bar", "two"):"bar"])

        # bool list-like column select
        bool_list = [True, False, True, False]
        self.assert_eq(psdf.loc[:, bool_list], pdf.loc[:, bool_list])
        self.assert_eq(psdf.loc[:, np.array(bool_list)], pdf.loc[:, np.array(bool_list)])

        pser = pd.Series(bool_list, index=pdf.columns)
        self.assert_eq(psdf.loc[:, pser], pdf.loc[:, pser])

        pser = pd.Series(list(reversed(bool_list)), index=list(reversed(pdf.columns)))
        self.assert_eq(psdf.loc[:, pser], pdf.loc[:, pser])

        # non-string column names
        arrays = [np.array([0, 0, 1, 1]), np.array([1, 2, 1, 2])]

        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "B", "C"], columns=arrays)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.loc["B":"B", 0], pdf.loc["B":"B", 0])
        self.assert_eq(psdf.loc["B":"B", [0]], pdf.loc["B":"B", [0]])
        self.assert_eq(psdf.loc[:, 0:0], pdf.loc[:, 0:0])
        self.assert_eq(psdf.loc[:, 0:(1, 1)], pdf.loc[:, 0:(1, 1)])
        self.assert_eq(psdf.loc[:, (0, 2):(1, 1)], pdf.loc[:, (0, 2):(1, 1)])
        self.assert_eq(psdf.loc[:, (0, 2):0], pdf.loc[:, (0, 2):0])
        self.assert_eq(psdf.loc[:, -1:2], pdf.loc[:, -1:2])

    def test_loc2d_with_known_divisions(self):
        pdf = pd.DataFrame(
            np.random.randn(20, 5), index=list("abcdefghijklmnopqrst"), columns=list("ABCDE")
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.loc[["a"], "A"], pdf.loc[["a"], "A"])
        self.assert_eq(psdf.loc[["a"], ["A"]], pdf.loc[["a"], ["A"]])
        self.assert_eq(psdf.loc["a":"o", "A"], pdf.loc["a":"o", "A"])
        self.assert_eq(psdf.loc["a":"o", ["A"]], pdf.loc["a":"o", ["A"]])
        self.assert_eq(psdf.loc[["n"], ["A"]], pdf.loc[["n"], ["A"]])
        self.assert_eq(psdf.loc[["a", "c", "n"], ["A"]], pdf.loc[["a", "c", "n"], ["A"]])
        # TODO?: self.assert_eq(psdf.loc[['t', 'b'], ['A']], pdf.loc[['t', 'b'], ['A']])
        # TODO?: self.assert_eq(psdf.loc[['r', 'r', 'c', 'g', 'h'], ['A']],
        # TODO?:                pdf.loc[['r', 'r', 'c', 'g', 'h'], ['A']])

    @unittest.skip("TODO: should handle duplicated columns properly")
    def test_loc2d_duplicated_columns(self):
        pdf = pd.DataFrame(
            np.random.randn(20, 5), index=list("abcdefghijklmnopqrst"), columns=list("AABCD")
        )
        psdf = ps.from_pandas(pdf)

        # TODO?: self.assert_eq(psdf.loc[['a'], 'A'], pdf.loc[['a'], 'A'])
        # TODO?: self.assert_eq(psdf.loc[['a'], ['A']], pdf.loc[['a'], ['A']])
        self.assert_eq(psdf.loc[["j"], "B"], pdf.loc[["j"], "B"])
        self.assert_eq(psdf.loc[["j"], ["B"]], pdf.loc[["j"], ["B"]])

        # TODO?: self.assert_eq(psdf.loc['a':'o', 'A'], pdf.loc['a':'o', 'A'])
        # TODO?: self.assert_eq(psdf.loc['a':'o', ['A']], pdf.loc['a':'o', ['A']])
        self.assert_eq(psdf.loc["j":"q", "B"], pdf.loc["j":"q", "B"])
        self.assert_eq(psdf.loc["j":"q", ["B"]], pdf.loc["j":"q", ["B"]])

        # TODO?: self.assert_eq(psdf.loc['a':'o', 'B':'D'], pdf.loc['a':'o', 'B':'D'])
        # TODO?: self.assert_eq(psdf.loc['a':'o', 'B':'D'], pdf.loc['a':'o', 'B':'D'])
        # TODO?: self.assert_eq(psdf.loc['j':'q', 'B':'A'], pdf.loc['j':'q', 'B':'A'])
        # TODO?: self.assert_eq(psdf.loc['j':'q', 'B':'A'], pdf.loc['j':'q', 'B':'A'])

        self.assert_eq(psdf.loc[psdf.B > 0, "B"], pdf.loc[pdf.B > 0, "B"])
        # TODO?: self.assert_eq(psdf.loc[psdf.B > 0, ['A', 'C']], pdf.loc[pdf.B > 0, ['A', 'C']])


class IndexingLoc2DTests(
    IndexingLoc2DMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_indexing_loc_2d import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
