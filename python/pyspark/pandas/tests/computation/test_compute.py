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

from pyspark.sql import functions as sf
from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


# This file contains test cases for 'Computations / Descriptive Stats'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#computations-descriptive-stats
class FrameComputeMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def df_pair(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)
        return pdf, psdf

    def test_abs(self):
        pdf = pd.DataFrame({"a": [-2, -1, 0, 1]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(abs(psdf), abs(pdf))
        self.assert_eq(np.abs(psdf), np.abs(pdf))

    def test_clip(self):
        pdf = pd.DataFrame(
            {"A": [0, 2, 4], "B": [4, 2, 0], "X": [-1, 10, 0]}, index=np.random.rand(3)
        )
        psdf = ps.from_pandas(pdf)

        # Assert list-like values are not accepted for 'lower' and 'upper'
        msg = "List-like value are not supported for 'lower' and 'upper' at the moment"
        with self.assertRaises(TypeError, msg=msg):
            psdf.clip(lower=[1])
        with self.assertRaises(TypeError, msg=msg):
            psdf.clip(upper=[1])

        # Assert no lower or upper
        self.assert_eq(psdf.clip(), pdf.clip())
        # Assert lower only
        self.assert_eq(psdf.clip(1), pdf.clip(1))
        # Assert upper only
        self.assert_eq(psdf.clip(upper=3), pdf.clip(upper=3))
        # Assert lower and upper
        self.assert_eq(psdf.clip(1, 3), pdf.clip(1, 3))

        pdf["clip"] = pdf.A.clip(lower=1, upper=3)
        psdf["clip"] = psdf.A.clip(lower=1, upper=3)
        self.assert_eq(psdf, pdf)

        # Assert behavior on string values
        str_psdf = ps.DataFrame({"A": ["a", "b", "c"]}, index=np.random.rand(3))
        self.assert_eq(str_psdf.clip(1, 3), str_psdf)

    def test_mode(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 2, None, 4, 5, 4, 2],
                "B": [-0.1, 0.2, -0.3, np.nan, 0.5, -0.1, -0.1],
                "C": ["d", "b", "c", "c", "e", "a", "a"],
                "D": [np.nan, np.nan, np.nan, np.nan, 0.1, -0.1, -0.1],
                "E": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.mode(), pdf.mode())
        self.assert_eq(psdf.mode(numeric_only=True), pdf.mode(numeric_only=True))
        self.assert_eq(psdf.mode(dropna=False), pdf.mode(dropna=False))

        # dataframe with single column
        for c in ["A", "B", "C", "D", "E"]:
            self.assert_eq(psdf[[c]].mode(), pdf[[c]].mode())

        with self.assertRaises(ValueError):
            psdf.mode(axis=2)

        def func(iterator):
            for pdf in iterator:
                if len(pdf) > 0:
                    if pdf["partition"][0] == 3:
                        yield pd.DataFrame(
                            {
                                "num": [
                                    "3",
                                    "3",
                                    "3",
                                    "3",
                                    "4",
                                ]
                            }
                        )
                    else:
                        yield pd.DataFrame(
                            {
                                "num": [
                                    "0",
                                    "1",
                                    "2",
                                    "3",
                                    "4",
                                ]
                            }
                        )

        df = (
            self.spark.range(0, 4, 1, 4)
            .select(sf.spark_partition_id().alias("partition"))
            .mapInPandas(func, "num string")
        )

        psdf = df.pandas_api()
        self.assert_eq(psdf.mode(), psdf._to_pandas().mode())

    def test_round(self):
        pdf = pd.DataFrame(
            {
                "A": [0.028208, 0.038683, 0.877076],
                "B": [0.992815, 0.645646, 0.149370],
                "C": [0.173891, 0.577595, 0.491027],
            },
            columns=["A", "B", "C"],
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)

        pser = pd.Series([1, 0, 2], index=["A", "B", "C"])
        psser = ps.Series([1, 0, 2], index=["A", "B", "C"])
        self.assert_eq(pdf.round(2), psdf.round(2))
        self.assert_eq(pdf.round({"A": 1, "C": 2}), psdf.round({"A": 1, "C": 2}))
        self.assert_eq(pdf.round({"A": 1, "D": 2}), psdf.round({"A": 1, "D": 2}))
        self.assert_eq(pdf.round(pser), psdf.round(psser))
        msg = "decimals must be an integer, a dict-like or a Series"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.round(1.5)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
        pdf.columns = columns
        psdf.columns = columns
        pser = pd.Series([1, 0, 2], index=columns)
        psser = ps.Series([1, 0, 2], index=columns)
        self.assert_eq(pdf.round(2), psdf.round(2))
        self.assert_eq(
            pdf.round({("X", "A"): 1, ("Y", "C"): 2}), psdf.round({("X", "A"): 1, ("Y", "C"): 2})
        )
        self.assert_eq(pdf.round({("X", "A"): 1, "Y": 2}), psdf.round({("X", "A"): 1, "Y": 2}))
        self.assert_eq(pdf.round(pser), psdf.round(psser))

        # non-string names
        pdf = pd.DataFrame(
            {
                10: [0.028208, 0.038683, 0.877076],
                20: [0.992815, 0.645646, 0.149370],
                30: [0.173891, 0.577595, 0.491027],
            },
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.round({10: 1, 30: 2}), psdf.round({10: 1, 30: 2}))

    def test_diff(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.diff(), psdf.diff())
        self.assert_eq(pdf.diff().diff(-1), psdf.diff().diff(-1))
        self.assert_eq(pdf.diff().sum().astype(int), psdf.diff().sum())

        msg = "should be an int"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.diff(1.5)
        msg = 'axis should be either 0 or "index" currently.'
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.diff(axis=1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "Col1"), ("x", "Col2"), ("y", "Col3")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(pdf.diff(), psdf.diff())

    def test_pct_change(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 2], "b": [4.0, 2.0, 3.0, 1.0], "c": [300, 200, 400, 200]},
            index=np.random.rand(4),
        )
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.pct_change(2), pdf.pct_change(2), check_exact=False)
        self.assert_eq(psdf.pct_change().sum(), pdf.pct_change().sum(), check_exact=False)

    def test_rank(self):
        pdf = pd.DataFrame(
            data={"col1": [1, 2, 3, 1], "col2": [3, 4, 3, 1]},
            columns=["col1", "col2"],
            index=np.random.rand(4),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.rank().sort_index(), psdf.rank().sort_index())
        self.assert_eq(pdf.rank().sum(), psdf.rank().sum())
        self.assert_eq(
            pdf.rank(ascending=False).sort_index(), psdf.rank(ascending=False).sort_index()
        )
        self.assert_eq(pdf.rank(method="min").sort_index(), psdf.rank(method="min").sort_index())
        self.assert_eq(pdf.rank(method="max").sort_index(), psdf.rank(method="max").sort_index())
        self.assert_eq(
            pdf.rank(method="first").sort_index(), psdf.rank(method="first").sort_index()
        )
        self.assert_eq(
            pdf.rank(method="dense").sort_index(), psdf.rank(method="dense").sort_index()
        )

        msg = "method must be one of 'average', 'min', 'max', 'first', 'dense'"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.rank(method="nothing")

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "col1"), ("y", "col2")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.rank().sort_index(), psdf.rank().sort_index())

        # non-numeric columns
        pdf = pd.DataFrame(
            data={"col1": [1, 2, 3, 1], "col2": ["a", "b", "c", "d"]},
            index=np.random.rand(4),
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.rank(numeric_only=True).sort_index(), psdf.rank(numeric_only=True).sort_index()
        )
        self.assert_eq(
            pdf.rank(numeric_only=False).sort_index(), psdf.rank(numeric_only=False).sort_index()
        )
        self.assert_eq(
            pdf.rank(numeric_only=None).sort_index(), psdf.rank(numeric_only=None).sort_index()
        )
        self.assert_eq(
            pdf[["col2"]].rank(numeric_only=True),
            psdf[["col2"]].rank(numeric_only=True),
        )

    def test_nunique(self):
        pdf = pd.DataFrame({"A": [1, 2, 3], "B": [np.nan, 3, np.nan]}, index=np.random.rand(3))
        psdf = ps.from_pandas(pdf)

        # Assert NaNs are dropped by default
        self.assert_eq(psdf.nunique(), pdf.nunique())

        # Assert including NaN values
        self.assert_eq(psdf.nunique(dropna=False), pdf.nunique(dropna=False))

        # Assert approximate counts
        self.assert_eq(
            ps.DataFrame({"A": range(100)}).nunique(approx=True),
            pd.Series([103], index=["A"]),
        )
        self.assert_eq(
            ps.DataFrame({"A": range(100)}).nunique(approx=True, rsd=0.01),
            pd.Series([100], index=["A"]),
        )

        # Assert unsupported axis value yet
        msg = 'axis should be either 0 or "index" currently.'
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.nunique(axis=1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("Y", "B")], names=["1", "2"])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.nunique(), pdf.nunique())
        self.assert_eq(psdf.nunique(dropna=False), pdf.nunique(dropna=False))

    def test_quantile(self):
        pdf, psdf = self.df_pair

        self.assert_eq(psdf.quantile(0.5), pdf.quantile(0.5))
        self.assert_eq(psdf.quantile([0.25, 0.5, 0.75]), pdf.quantile([0.25, 0.5, 0.75]))

        self.assert_eq(psdf.loc[[]].quantile(0.5), pdf.loc[[]].quantile(0.5))
        self.assert_eq(
            psdf.loc[[]].quantile([0.25, 0.5, 0.75]), pdf.loc[[]].quantile([0.25, 0.5, 0.75])
        )

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psdf.quantile(0.5, axis=1)
        with self.assertRaisesRegex(TypeError, "accuracy must be an integer; however"):
            psdf.quantile(accuracy="a")
        with self.assertRaisesRegex(TypeError, "q must be a float or an array of floats;"):
            psdf.quantile(q="a")
        with self.assertRaisesRegex(TypeError, "q must be a float or an array of floats;"):
            psdf.quantile(q=["a"])
        with self.assertRaisesRegex(
            ValueError, r"percentiles should all be in the interval \[0, 1\]"
        ):
            psdf.quantile(q=[1.1])

        self.assert_eq(
            psdf.quantile(0.5, numeric_only=False), pdf.quantile(0.5, numeric_only=False)
        )
        self.assert_eq(
            psdf.quantile([0.25, 0.5, 0.75], numeric_only=False),
            pdf.quantile([0.25, 0.5, 0.75], numeric_only=False),
        )

        # multi-index column
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.quantile(0.5), pdf.quantile(0.5))
        self.assert_eq(psdf.quantile([0.25, 0.5, 0.75]), pdf.quantile([0.25, 0.5, 0.75]))

        pdf = pd.DataFrame({"x": ["a", "b", "c"]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.quantile(0.5, numeric_only=True), pdf.quantile(0.5, numeric_only=True))
        self.assert_eq(
            psdf.quantile([0.25, 0.5, 0.75], numeric_only=True),
            pdf.quantile([0.25, 0.5, 0.75], numeric_only=True),
        )

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            psdf.quantile(0.5, numeric_only=False)
        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            psdf.quantile([0.25, 0.5, 0.75], numeric_only=False)

    def test_product(self):
        pdf = pd.DataFrame(
            {"A": [1, 2, 3, 4, 5], "B": [10, 20, 30, 40, 50], "C": ["a", "b", "c", "d", "e"]}
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index())

        # Named columns
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index())

        # MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index())

        # Named MultiIndex columns
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index())

        # No numeric columns
        pdf = pd.DataFrame({"key": ["a", "b", "c"], "val": ["x", "y", "z"]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index())

        # No numeric named columns
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index(), almost=True)

        # No numeric MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index(), almost=True)

        # No numeric named MultiIndex columns
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index(), almost=True)

        # All NaN columns
        pdf = pd.DataFrame(
            {
                "A": [np.nan, np.nan, np.nan, np.nan, np.nan],
                "B": [10, 20, 30, 40, 50],
                "C": ["a", "b", "c", "d", "e"],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index(), check_exact=False)

        # All NaN named columns
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index(), check_exact=False)

        # All NaN MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index(), check_exact=False)

        # All NaN named MultiIndex columns
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(numeric_only=True), psdf.prod().sort_index(), check_exact=False)


class FrameComputeTests(
    FrameComputeMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_compute import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
