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
from pyspark.pandas.exceptions import SparkPandasNotImplementedError
from pyspark.testing.pandasutils import PandasOnSparkTestCase, compare_both
from pyspark.testing.sqlutils import SQLTestUtils


class IndexingAdvMixin:
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


class IndexingAdvTests(
    IndexingAdvMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_indexing_adv import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
