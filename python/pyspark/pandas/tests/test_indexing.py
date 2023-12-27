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

import datetime
import unittest

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.exceptions import SparkPandasIndexingError, SparkPandasNotImplementedError
from pyspark.testing.pandasutils import ComparisonTestBase, compare_both


class IndexingTest(ComparisonTestBase):
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
    def psdf2(self):
        return ps.from_pandas(self.pdf2)

    def test_at(self):
        pdf = self.pdf
        psdf = self.psdf
        # Create the equivalent of pdf.loc[3] as a Koalas Series
        # This is necessary because .loc[n] does not currently work with Koalas DataFrames (#383)
        test_series = ps.Series([3, 6], index=["a", "b"], name="3")

        # Assert invalided signatures raise TypeError
        with self.assertRaises(TypeError, msg="Use DataFrame.at like .at[row_index, column_name]"):
            psdf.at[3]
        with self.assertRaises(TypeError, msg="Use DataFrame.at like .at[row_index, column_name]"):
            psdf.at["ab"]  # 'ab' is of length 2 but str type instead of tuple
        with self.assertRaises(TypeError, msg="Use Series.at like .at[column_name]"):
            test_series.at[3, "b"]

        # Assert .at for DataFrames
        self.assertEqual(psdf.at[3, "b"], 6)
        self.assertEqual(psdf.at[3, "b"], pdf.at[3, "b"])
        self.assert_eq(psdf.at[9, "b"], np.array([0, 0, 0]))
        self.assert_eq(psdf.at[9, "b"], pdf.at[9, "b"])

        # Assert .at for Series
        self.assertEqual(test_series.at["b"], 6)
        self.assertEqual(test_series.at["b"], pdf.loc[3].at["b"])

        # Assert multi-character indices
        self.assertEqual(
            ps.Series([0, 1], index=["ab", "cd"]).at["ab"],
            pd.Series([0, 1], index=["ab", "cd"]).at["ab"],
        )

        # Assert invalid column or index names result in a KeyError like with pandas
        with self.assertRaises(KeyError, msg="x"):
            psdf.at[3, "x"]
        with self.assertRaises(KeyError, msg=99):
            psdf.at[99, "b"]

        with self.assertRaises(ValueError):
            psdf.at[(3, 6), "b"]
        with self.assertRaises(KeyError):
            psdf.at[3, ("x", "b")]

        # Assert setting values fails
        with self.assertRaises(TypeError):
            psdf.at[3, "b"] = 10

        # non-string column names
        pdf = self.pdf2
        psdf = self.psdf2

        # Assert .at for DataFrames
        self.assertEqual(psdf.at[3, 1], 6)
        self.assertEqual(psdf.at[3, 1], pdf.at[3, 1])
        self.assert_eq(psdf.at[9, 1], np.array([0, 0, 0]))
        self.assert_eq(psdf.at[9, 1], pdf.at[9, 1])

    def test_at_multiindex(self):
        psdf = self.psdf.set_index("b", append=True)

        self.assert_eq(psdf.at[(3, 6), "a"], 3)
        self.assert_eq(psdf.at[(3,), "a"], np.array([3]))
        self.assert_eq(list(psdf.at[(9, 0), "a"]), [7, 8, 9])
        self.assert_eq(list(psdf.at[(9,), "a"]), [7, 8, 9])

        with self.assertRaises(ValueError):
            psdf.at[3, "a"]

    def test_at_multiindex_columns(self):
        arrays = [np.array(["bar", "bar", "baz", "baz"]), np.array(["one", "two", "one", "two"])]

        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "B", "C"], columns=arrays)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.at["B", ("bar", "one")], pdf.at["B", ("bar", "one")])

        with self.assertRaises(KeyError):
            psdf.at["B", "bar"]

        # non-string column names
        arrays = [np.array([0, 0, 1, 1]), np.array([1, 2, 1, 2])]

        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "B", "C"], columns=arrays)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.at["B", (0, 1)], pdf.at["B", (0, 1)])

    def test_iat(self):
        pdf = self.pdf
        psdf = self.psdf
        # Create the equivalent of pdf.loc[3] as a Koalas Series
        # This is necessary because .loc[n] does not currently work with Koalas DataFrames (#383)
        test_series = ps.Series([3, 6], index=["a", "b"], name="3")

        # Assert invalided signatures raise TypeError
        with self.assertRaises(
            TypeError,
            msg="Use DataFrame.at like .iat[row_interget_position, column_integer_position]",
        ):
            psdf.iat[3]
        with self.assertRaises(
            ValueError, msg="iAt based indexing on multi-index can only have tuple values"
        ):
            psdf.iat[3, "b"]  # 'ab' is of length 2 but str type instead of tuple
        with self.assertRaises(TypeError, msg="Use Series.iat like .iat[row_integer_position]"):
            test_series.iat[3, "b"]

        # Assert .iat for DataFrames
        self.assertEqual(psdf.iat[7, 0], 8)
        self.assertEqual(psdf.iat[7, 0], pdf.iat[7, 0])

        # Assert .iat for Series
        self.assertEqual(test_series.iat[1], 6)
        self.assertEqual(test_series.iat[1], pdf.loc[3].iat[1])

        # Assert invalid column or integer position result in a KeyError like with pandas
        with self.assertRaises(KeyError, msg=99):
            psdf.iat[0, 99]
        with self.assertRaises(KeyError, msg=99):
            psdf.iat[99, 0]

        with self.assertRaises(ValueError):
            psdf.iat[(1, 1), 1]
        with self.assertRaises(ValueError):
            psdf.iat[1, (1, 1)]

        # Assert setting values fails
        with self.assertRaises(TypeError):
            psdf.iat[4, 1] = 10

    def test_iat_multiindex(self):
        pdf = self.pdf.set_index("b", append=True)
        psdf = self.psdf.set_index("b", append=True)

        self.assert_eq(psdf.iat[7, 0], pdf.iat[7, 0])

        with self.assertRaises(ValueError):
            psdf.iat[3, "a"]

    def test_iat_multiindex_columns(self):
        arrays = [np.array(["bar", "bar", "baz", "baz"]), np.array(["one", "two", "one", "two"])]

        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "B", "C"], columns=arrays)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.iat[1, 3], pdf.iat[1, 3])

        with self.assertRaises(KeyError):
            psdf.iat[0, 99]
        with self.assertRaises(KeyError):
            psdf.iat[99, 0]

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

    def test_getitem(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "B": [9, 8, 7, 6, 5, 4, 3, 2, 1],
                "C": [True, False, True] * 3,
            },
            columns=list("ABC"),
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf["A"], pdf["A"])

        self.assert_eq(psdf[["A", "B"]], pdf[["A", "B"]])

        self.assert_eq(psdf[psdf.C], pdf[pdf.C])

        self.assertRaises(KeyError, lambda: psdf["X"])
        self.assertRaises(KeyError, lambda: psdf[["A", "X"]])
        self.assertRaises(AttributeError, lambda: psdf.X)

        # not str/unicode
        pdf = pd.DataFrame(np.random.randn(10, 5))
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf[0], pdf[0])
        self.assert_eq(psdf[[1, 2]], pdf[[1, 2]])

        self.assertRaises(KeyError, lambda: pdf[8])
        self.assertRaises(KeyError, lambda: pdf[[1, 8]])

        # non-string column names
        pdf = pd.DataFrame(
            {
                10: [1, 2, 3, 4, 5, 6, 7, 8, 9],
                20: [9, 8, 7, 6, 5, 4, 3, 2, 1],
                30: [True, False, True] * 3,
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf[10], pdf[10])
        self.assert_eq(psdf[[10, 20]], pdf[[10, 20]])

    def test_getitem_slice(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "B": [9, 8, 7, 6, 5, 4, 3, 2, 1],
                "C": [True, False, True] * 3,
            },
            index=list("abcdefghi"),
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf["a":"e"], pdf["a":"e"])
        self.assert_eq(psdf["a":"b"], pdf["a":"b"])
        self.assert_eq(psdf["f":], pdf["f":])

    @unittest.skip("TODO?: the behavior of slice for datetime")
    def test_getitem_timestamp_str(self):
        pdf = pd.DataFrame(
            {"A": np.random.randn(100), "B": np.random.randn(100)},
            index=pd.date_range("2011-01-01", freq="H", periods=100),
        )
        psdf = ps.from_pandas(pdf)

        # partial string slice
        # TODO?: self.assert_eq(pdf['2011-01-02'],
        # TODO?:                psdf['2011-01-02'])
        self.assert_eq(pdf["2011-01-02":"2011-01-05"], psdf["2011-01-02":"2011-01-05"])

        pdf = pd.DataFrame(
            {"A": np.random.randn(100), "B": np.random.randn(100)},
            index=pd.date_range("2011-01-01", freq="M", periods=100),
        )
        psdf = ps.from_pandas(pdf)

        # TODO?: self.assert_eq(pdf['2011-01'], psdf['2011-01'])
        # TODO?: self.assert_eq(pdf['2011'], psdf['2011'])

        self.assert_eq(pdf["2011-01":"2012-05"], psdf["2011-01":"2012-05"])
        self.assert_eq(pdf["2011":"2015"], psdf["2011":"2015"])

    @unittest.skip("TODO?: period index can't convert to DataFrame correctly")
    def test_getitem_period_str(self):
        pdf = pd.DataFrame(
            {"A": np.random.randn(100), "B": np.random.randn(100)},
            index=pd.period_range("2011-01-01", freq="H", periods=100),
        )
        psdf = ps.from_pandas(pdf)

        # partial string slice
        # TODO?: self.assert_eq(pdf['2011-01-02'],
        # TODO?:                psdf['2011-01-02'])
        self.assert_eq(pdf["2011-01-02":"2011-01-05"], psdf["2011-01-02":"2011-01-05"])

        pdf = pd.DataFrame(
            {"A": np.random.randn(100), "B": np.random.randn(100)},
            index=pd.period_range("2011-01-01", freq="M", periods=100),
        )
        psdf = ps.from_pandas(pdf)

        # TODO?: self.assert_eq(pdf['2011-01'], psdf['2011-01'])
        # TODO?: self.assert_eq(pdf['2011'], psdf['2011'])

        self.assert_eq(pdf["2011-01":"2012-05"], psdf["2011-01":"2012-05"])
        self.assert_eq(pdf["2011":"2015"], psdf["2011":"2015"])

    def test_iloc(self):
        pdf = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.iloc[0, 0], pdf.iloc[0, 0])
        for indexer in [0, [0], [0, 1], [1, 0], [False, True, True], slice(0, 1)]:
            self.assert_eq(psdf.iloc[:, indexer], pdf.iloc[:, indexer])
            self.assert_eq(psdf.iloc[:1, indexer], pdf.iloc[:1, indexer])
            self.assert_eq(psdf.iloc[:-1, indexer], pdf.iloc[:-1, indexer])
            # self.assert_eq(psdf.iloc[psdf.index == 2, indexer], pdf.iloc[pdf.index == 2, indexer])

        self.assertRaisesRegex(
            SparkPandasNotImplementedError,
            ".iloc requires numeric slice, conditional boolean",
            lambda: ps.range(10).iloc["a", :],
        )

    def test_iloc_multiindex_columns(self):
        arrays = [np.array(["bar", "bar", "baz", "baz"]), np.array(["one", "two", "one", "two"])]

        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "B", "C"], columns=arrays)
        psdf = ps.from_pandas(pdf)

        for indexer in [0, [0], [0, 1], [1, 0], [False, True, True, True], slice(0, 1)]:
            self.assert_eq(psdf.iloc[:, indexer], pdf.iloc[:, indexer])
            self.assert_eq(psdf.iloc[:1, indexer], pdf.iloc[:1, indexer])
            self.assert_eq(psdf.iloc[:-1, indexer], pdf.iloc[:-1, indexer])
            # self.assert_eq(psdf.iloc[psdf.index == "B", indexer],
            #                pdf.iloc[pdf.index == "B", indexer])

    def test_iloc_series(self):
        pser = pd.Series([1, 2, 3])
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.iloc[0], pser.iloc[0])
        self.assert_eq(psser.iloc[:], pser.iloc[:])
        self.assert_eq(psser.iloc[:1], pser.iloc[:1])
        self.assert_eq(psser.iloc[:-1], pser.iloc[:-1])

        self.assert_eq((psser + 1).iloc[0], (pser + 1).iloc[0])
        self.assert_eq((psser + 1).iloc[:], (pser + 1).iloc[:])
        self.assert_eq((psser + 1).iloc[:1], (pser + 1).iloc[:1])
        self.assert_eq((psser + 1).iloc[:-1], (pser + 1).iloc[:-1])

    def test_iloc_slice_rows_sel(self):
        pdf = pd.DataFrame({"A": [1, 2] * 5, "B": [3, 4] * 5, "C": [5, 6] * 5})
        psdf = ps.from_pandas(pdf)

        for rows_sel in [
            slice(None),
            slice(0, 1),
            slice(1, 2),
            slice(-3, None),
            slice(None, -3),
            slice(None, 0),
            slice(None, None, 3),
            slice(3, 8, 2),
            slice(None, None, -2),
            slice(8, 3, -2),
            slice(8, None, -2),
            slice(None, 3, -2),
        ]:
            with self.subTest(rows_sel=rows_sel):
                self.assert_eq(psdf.iloc[rows_sel].sort_index(), pdf.iloc[rows_sel].sort_index())
                self.assert_eq(
                    psdf.A.iloc[rows_sel].sort_index(), pdf.A.iloc[rows_sel].sort_index()
                )
                self.assert_eq(
                    (psdf.A + 1).iloc[rows_sel].sort_index(),
                    (pdf.A + 1).iloc[rows_sel].sort_index(),
                )

    def test_iloc_iterable_rows_sel(self):
        pdf = pd.DataFrame({"A": [1, 2] * 5, "B": [3, 4] * 5, "C": [5, 6] * 5})
        psdf = ps.from_pandas(pdf)

        for rows_sel in [
            [],
            np.array([0, 1]),
            [1, 2],
            np.array([-3]),
            [3],
            np.array([-2]),
            [8, 3, -5],
        ]:
            with self.subTest(rows_sel=rows_sel):
                self.assert_eq(psdf.iloc[rows_sel].sort_index(), pdf.iloc[rows_sel].sort_index())
                self.assert_eq(
                    psdf.A.iloc[rows_sel].sort_index(), pdf.A.iloc[rows_sel].sort_index()
                )
                self.assert_eq(
                    (psdf.A + 1).iloc[rows_sel].sort_index(),
                    (pdf.A + 1).iloc[rows_sel].sort_index(),
                )

            with self.subTest(rows_sel=rows_sel):
                self.assert_eq(
                    psdf.iloc[rows_sel, :].sort_index(), pdf.iloc[rows_sel, :].sort_index()
                )

            with self.subTest(rows_sel=rows_sel):
                self.assert_eq(
                    psdf.iloc[rows_sel, :1].sort_index(), pdf.iloc[rows_sel, :1].sort_index()
                )

    def test_frame_iloc_setitem(self):
        pdf = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        psdf = ps.from_pandas(pdf)

        pdf.iloc[[1, 2], [1, 0]] = 10
        psdf.iloc[[1, 2], [1, 0]] = 10
        self.assert_eq(psdf, pdf)

        pdf.iloc[0, 1] = 50
        psdf.iloc[0, 1] = 50
        self.assert_eq(psdf, pdf)

        with self.assertRaisesRegex(ValueError, "setting an array element with a sequence."):
            psdf.iloc[0, 0] = -psdf.max_speed
        with self.assertRaisesRegex(ValueError, "shape mismatch"):
            psdf.iloc[:, [1, 0]] = -psdf.max_speed
        with self.assertRaisesRegex(ValueError, "Only a dataframe with one column can be assigned"):
            psdf.iloc[:, 0] = psdf

        pdf = pd.DataFrame(
            [[1], [4], [7]], index=["cobra", "viper", "sidewinder"], columns=["max_speed"]
        )
        psdf = ps.from_pandas(pdf)

        pdf.iloc[:, 0] = pdf
        psdf.iloc[:, 0] = psdf
        self.assert_eq(psdf, pdf)

    def test_series_iloc_setitem(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y

        piloc = pser.iloc
        kiloc = psser.iloc

        pser1 = pser + 1
        psser1 = psser + 1

        for key, value in [
            ([1, 2], 10),
            (1, 50),
            (slice(None), 10),
            (slice(None, 1), 20),
            (slice(1, None), 30),
        ]:
            with self.subTest(key=key, value=value):
                pser.iloc[key] = value
                psser.iloc[key] = value
                self.assert_eq(psser, pser)
                self.assert_eq(psdf, pdf)
                self.assert_eq(pssery, psery)

                piloc[key] = -value
                kiloc[key] = -value
                self.assert_eq(psser, pser)
                self.assert_eq(psdf, pdf)
                self.assert_eq(pssery, psery)

                pser1.iloc[key] = value
                psser1.iloc[key] = value
                self.assert_eq(psser1, pser1)
                self.assert_eq(psdf, pdf)
                self.assert_eq(pssery, psery)

        with self.assertRaises(ValueError):
            psser.iloc[1] = -psser

        pser = pd.Index([1, 2, 3]).to_series()
        psser = ps.Index([1, 2, 3]).to_series()

        pser1 = pser + 1
        psser1 = psser + 1

        pser.iloc[0] = 10
        psser.iloc[0] = 10
        self.assert_eq(psser, pser)

        pser1.iloc[0] = 20
        psser1.iloc[0] = 20
        self.assert_eq(psser1, pser1)

        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        psdf = ps.from_pandas(pdf)

        pser = pdf.a
        psser = psdf.a

        pser.iloc[[0, 1, 2]] = -pdf.b
        psser.iloc[[0, 1, 2]] = -psdf.b
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        with self.assertRaisesRegex(ValueError, "setting an array element with a sequence."):
            psser.iloc[1] = psdf[["b"]]

    def test_iloc_raises(self):
        pdf = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})
        psdf = ps.from_pandas(pdf)

        with self.assertRaisesRegex(SparkPandasIndexingError, "Only accepts pairs of candidates"):
            psdf.iloc[[0, 1], [0, 1], [1, 2]]

        with self.assertRaisesRegex(SparkPandasIndexingError, "Too many indexers"):
            psdf.A.iloc[[0, 1], [0, 1]]

        with self.assertRaisesRegex(TypeError, "cannot do slice indexing with these indexers"):
            psdf.iloc[:"b", :]

        with self.assertRaisesRegex(TypeError, "cannot do slice indexing with these indexers"):
            psdf.iloc[:, :"b"]

        with self.assertRaisesRegex(TypeError, "cannot perform reduce with flexible type"):
            psdf.iloc[:, ["A"]]

        with self.assertRaisesRegex(ValueError, "Location based indexing can only have"):
            psdf.iloc[:, "A"]

        with self.assertRaisesRegex(IndexError, "out of range"):
            psdf.iloc[:, [5, 6]]

    def test_index_operator_datetime(self):
        dates = pd.date_range("20130101", periods=6)
        pdf = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list("ABCD"))
        psdf = ps.from_pandas(pdf)

        # Positional iloc search
        self.assert_eq(psdf[:4], pdf[:4], almost=True)
        self.assert_eq(psdf[:3], pdf[:3], almost=True)
        self.assert_eq(psdf[3:], pdf[3:], almost=True)
        self.assert_eq(psdf[2:], pdf[2:], almost=True)
        self.assert_eq(psdf[2:3], pdf[2:3], almost=True)
        self.assert_eq(psdf[2:-1], pdf[2:-1], almost=True)
        self.assert_eq(psdf[10:3], pdf[10:3], almost=True)

        # Index loc search
        self.assert_eq(psdf.A[4], pdf.A[4])
        self.assert_eq(psdf.A[3], pdf.A[3])

        # Positional iloc search
        self.assert_eq(psdf.A[:4], pdf.A[:4], almost=True)
        self.assert_eq(psdf.A[:3], pdf.A[:3], almost=True)
        self.assert_eq(psdf.A[3:], pdf.A[3:], almost=True)
        self.assert_eq(psdf.A[2:], pdf.A[2:], almost=True)
        self.assert_eq(psdf.A[2:3], pdf.A[2:3], almost=True)
        self.assert_eq(psdf.A[2:-1], pdf.A[2:-1], almost=True)
        self.assert_eq(psdf.A[10:3], pdf.A[10:3], almost=True)

        dt1 = datetime.datetime.strptime("2013-01-02", "%Y-%m-%d")
        dt2 = datetime.datetime.strptime("2013-01-04", "%Y-%m-%d")

        # Index loc search
        self.assert_eq(psdf[:dt2], pdf[:dt2], almost=True)
        self.assert_eq(psdf[dt1:], pdf[dt1:], almost=True)
        self.assert_eq(psdf[dt1:dt2], pdf[dt1:dt2], almost=True)
        self.assert_eq(psdf.A[dt2], pdf.A[dt2], almost=True)
        self.assert_eq(psdf.A[:dt2], pdf.A[:dt2], almost=True)
        self.assert_eq(psdf.A[dt1:], pdf.A[dt1:], almost=True)
        self.assert_eq(psdf.A[dt1:dt2], pdf.A[dt1:dt2], almost=True)

    def test_index_operator_int(self):
        pdf = pd.DataFrame(np.random.randn(6, 4), index=[1, 3, 5, 7, 9, 11], columns=list("ABCD"))
        psdf = ps.from_pandas(pdf)

        # Positional iloc search
        self.assert_eq(psdf[:4], pdf[:4])
        self.assert_eq(psdf[:3], pdf[:3])
        self.assert_eq(psdf[3:], pdf[3:])
        self.assert_eq(psdf[2:], pdf[2:])
        self.assert_eq(psdf[2:3], pdf[2:3])
        self.assert_eq(psdf[2:-1], pdf[2:-1])
        self.assert_eq(psdf[10:3], pdf[10:3])

        # Index loc search
        self.assert_eq(psdf.A[5], pdf.A[5])
        self.assert_eq(psdf.A[3], pdf.A[3])
        with self.assertRaisesRegex(
            NotImplementedError, "Duplicated row selection is not currently supported"
        ):
            psdf.iloc[[1, 1]]


if __name__ == "__main__":
    from pyspark.pandas.tests.test_indexing import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
