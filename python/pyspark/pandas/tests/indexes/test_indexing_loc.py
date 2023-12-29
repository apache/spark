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
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class IndexingLocMixin:
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

    def test_loc(self):
        psdf = self.psdf
        pdf = self.pdf

        self.assert_eq(psdf.loc[5:5], pdf.loc[5:5])
        self.assert_eq(psdf.loc[3:8], pdf.loc[3:8])
        self.assert_eq(psdf.loc[:8], pdf.loc[:8])
        self.assert_eq(psdf.loc[3:], pdf.loc[3:])
        self.assert_eq(psdf.loc[[5]], pdf.loc[[5]])
        self.assert_eq(psdf.loc[:], pdf.loc[:])

        # TODO?: self.assert_eq(psdf.loc[[3, 4, 1, 8]], pdf.loc[[3, 4, 1, 8]])
        # TODO?: self.assert_eq(psdf.loc[[3, 4, 1, 9]], pdf.loc[[3, 4, 1, 9]])
        # TODO?: self.assert_eq(psdf.loc[np.array([3, 4, 1, 9])], pdf.loc[np.array([3, 4, 1, 9])])

        self.assert_eq(psdf.a.loc[5:5], pdf.a.loc[5:5])
        self.assert_eq(psdf.a.loc[3:8], pdf.a.loc[3:8])
        self.assert_eq(psdf.a.loc[:8], pdf.a.loc[:8])
        self.assert_eq(psdf.a.loc[3:], pdf.a.loc[3:])
        self.assert_eq(psdf.a.loc[[5]], pdf.a.loc[[5]])

        # TODO?: self.assert_eq(psdf.a.loc[[3, 4, 1, 8]], pdf.a.loc[[3, 4, 1, 8]])
        # TODO?: self.assert_eq(psdf.a.loc[[3, 4, 1, 9]], pdf.a.loc[[3, 4, 1, 9]])
        # TODO?: self.assert_eq(psdf.a.loc[np.array([3, 4, 1, 9])],
        #                       pdf.a.loc[np.array([3, 4, 1, 9])])

        self.assert_eq(psdf.a.loc[[]], pdf.a.loc[[]])
        self.assert_eq(psdf.a.loc[np.array([])], pdf.a.loc[np.array([])])

        self.assert_eq(psdf.loc[1000:], pdf.loc[1000:])
        self.assert_eq(psdf.loc[-2000:-1000], pdf.loc[-2000:-1000])

        self.assert_eq(psdf.loc[5], pdf.loc[5])
        self.assert_eq(psdf.loc[9], pdf.loc[9])
        self.assert_eq(psdf.a.loc[5], pdf.a.loc[5])
        self.assert_eq(psdf.a.loc[9], pdf.a.loc[9])

        self.assertRaises(KeyError, lambda: psdf.loc[10])
        self.assertRaises(KeyError, lambda: psdf.a.loc[10])

        # monotonically increasing index test
        pdf = pd.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9]}, index=[0, 1, 1, 2, 2, 2, 4, 5, 6])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.loc[:2], pdf.loc[:2])
        self.assert_eq(psdf.loc[:3], pdf.loc[:3])
        self.assert_eq(psdf.loc[3:], pdf.loc[3:])
        self.assert_eq(psdf.loc[4:], pdf.loc[4:])
        self.assert_eq(psdf.loc[3:2], pdf.loc[3:2])
        self.assert_eq(psdf.loc[-1:2], pdf.loc[-1:2])
        self.assert_eq(psdf.loc[3:10], pdf.loc[3:10])

        # monotonically decreasing index test
        pdf = pd.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9]}, index=[6, 5, 5, 4, 4, 4, 2, 1, 0])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.loc[:4], pdf.loc[:4])
        self.assert_eq(psdf.loc[:3], pdf.loc[:3])
        self.assert_eq(psdf.loc[3:], pdf.loc[3:])
        self.assert_eq(psdf.loc[2:], pdf.loc[2:])
        self.assert_eq(psdf.loc[2:3], pdf.loc[2:3])
        self.assert_eq(psdf.loc[2:-1], pdf.loc[2:-1])
        self.assert_eq(psdf.loc[10:3], pdf.loc[10:3])

        # test when type of key is string and given value is not included in key
        pdf = pd.DataFrame({"a": [1, 2, 3]}, index=["a", "b", "d"])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.loc["a":"z"], pdf.loc["a":"z"])

        # KeyError when index is not monotonic increasing or decreasing
        # and specified values don't exist in index
        psdf = ps.DataFrame([[1, 2], [4, 5], [7, 8]], index=["cobra", "viper", "sidewinder"])

        self.assertRaises(KeyError, lambda: psdf.loc["cobra":"koalas"])
        self.assertRaises(KeyError, lambda: psdf.loc["koalas":"viper"])

        psdf = ps.DataFrame([[1, 2], [4, 5], [7, 8]], index=[10, 30, 20])

        self.assertRaises(KeyError, lambda: psdf.loc[0:30])
        self.assertRaises(KeyError, lambda: psdf.loc[10:100])

    def test_loc_getitem_boolean_series(self):
        pdf = pd.DataFrame(
            {"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]}, index=[20, 10, 30, 0, 50]
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.A.loc[pdf.B > 200], psdf.A.loc[psdf.B > 200])
        self.assert_eq(pdf.B.loc[pdf.B > 200], psdf.B.loc[psdf.B > 200])
        self.assert_eq(pdf.loc[pdf.B > 200], psdf.loc[psdf.B > 200])

    def test_loc_non_informative_index(self):
        pdf = pd.DataFrame({"x": [1, 2, 3, 4]}, index=[10, 20, 30, 40])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.loc[20:30], pdf.loc[20:30])

        pdf = pd.DataFrame({"x": [1, 2, 3, 4]}, index=[10, 20, 20, 40])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.loc[20:20], pdf.loc[20:20])

    def test_loc_with_series(self):
        psdf = self.psdf
        pdf = self.pdf

        self.assert_eq(psdf.loc[psdf.a % 2 == 0], pdf.loc[pdf.a % 2 == 0])
        self.assert_eq(psdf.loc[psdf.a % 2 == 0, "a"], pdf.loc[pdf.a % 2 == 0, "a"])
        self.assert_eq(psdf.loc[psdf.a % 2 == 0, ["a"]], pdf.loc[pdf.a % 2 == 0, ["a"]])
        self.assert_eq(psdf.a.loc[psdf.a % 2 == 0], pdf.a.loc[pdf.a % 2 == 0])

        self.assert_eq(psdf.loc[psdf.copy().a % 2 == 0], pdf.loc[pdf.copy().a % 2 == 0])
        self.assert_eq(psdf.loc[psdf.copy().a % 2 == 0, "a"], pdf.loc[pdf.copy().a % 2 == 0, "a"])
        self.assert_eq(
            psdf.loc[psdf.copy().a % 2 == 0, ["a"]], pdf.loc[pdf.copy().a % 2 == 0, ["a"]]
        )
        self.assert_eq(psdf.a.loc[psdf.copy().a % 2 == 0], pdf.a.loc[pdf.copy().a % 2 == 0])

    def test_loc_noindex(self):
        psdf = self.psdf
        psdf = psdf.reset_index()
        pdf = self.pdf
        pdf = pdf.reset_index()

        self.assert_eq(psdf[["a"]], pdf[["a"]])

        self.assert_eq(psdf.loc[:], pdf.loc[:])
        self.assert_eq(psdf.loc[5:5], pdf.loc[5:5])

    def test_loc_on_numpy_datetimes(self):
        pdf = pd.DataFrame(
            {"x": [1, 2, 3]}, index=list(map(np.datetime64, ["2014", "2015", "2016"]))
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.loc["2014":"2015"], pdf.loc["2014":"2015"])

    def test_loc_on_pandas_datetimes(self):
        pdf = pd.DataFrame(
            {"x": [1, 2, 3]}, index=list(map(pd.Timestamp, ["2014", "2015", "2016"]))
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.loc["2014":"2015"], pdf.loc["2014":"2015"])

    @unittest.skip("TODO?: the behavior of slice for datetime")
    def test_loc_datetime_no_freq(self):
        datetime_index = pd.date_range("2016-01-01", "2016-01-31", freq="12h")
        datetime_index.freq = None  # FORGET FREQUENCY
        pdf = pd.DataFrame({"num": range(len(datetime_index))}, index=datetime_index)
        psdf = ps.from_pandas(pdf)

        slice_ = slice("2016-01-03", "2016-01-05")
        result = psdf.loc[slice_, :]
        expected = pdf.loc[slice_, :]
        self.assert_eq(result, expected)

    @unittest.skip("TODO?: the behavior of slice for datetime")
    def test_loc_timestamp_str(self):
        pdf = pd.DataFrame(
            {"A": np.random.randn(100), "B": np.random.randn(100)},
            index=pd.date_range("2011-01-01", freq="H", periods=100),
        )
        psdf = ps.from_pandas(pdf)

        # partial string slice
        # TODO?: self.assert_eq(pdf.loc['2011-01-02'],
        # TODO?:                psdf.loc['2011-01-02'])
        self.assert_eq(pdf.loc["2011-01-02":"2011-01-05"], psdf.loc["2011-01-02":"2011-01-05"])

        # series
        # TODO?: self.assert_eq(pdf.A.loc['2011-01-02'],
        # TODO?:                psdf.A.loc['2011-01-02'])
        self.assert_eq(pdf.A.loc["2011-01-02":"2011-01-05"], psdf.A.loc["2011-01-02":"2011-01-05"])

        pdf = pd.DataFrame(
            {"A": np.random.randn(100), "B": np.random.randn(100)},
            index=pd.date_range("2011-01-01", freq="M", periods=100),
        )
        psdf = ps.from_pandas(pdf)
        # TODO?: self.assert_eq(pdf.loc['2011-01'], psdf.loc['2011-01'])
        # TODO?: self.assert_eq(pdf.loc['2011'], psdf.loc['2011'])

        self.assert_eq(pdf.loc["2011-01":"2012-05"], psdf.loc["2011-01":"2012-05"])
        self.assert_eq(pdf.loc["2011":"2015"], psdf.loc["2011":"2015"])

        # series
        # TODO?: self.assert_eq(pdf.B.loc['2011-01'], psdf.B.loc['2011-01'])
        # TODO?: self.assert_eq(pdf.B.loc['2011'], psdf.B.loc['2011'])

        self.assert_eq(pdf.B.loc["2011-01":"2012-05"], psdf.B.loc["2011-01":"2012-05"])
        self.assert_eq(pdf.B.loc["2011":"2015"], psdf.B.loc["2011":"2015"])

    def test_frame_loc_setitem(self):
        pdf = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        psdf = ps.from_pandas(pdf)

        pser1 = pdf.max_speed
        pser2 = pdf.shield
        psser1 = psdf.max_speed
        psser2 = psdf.shield

        pdf.loc[["viper", "sidewinder"], ["shield", "max_speed"]] = 10
        psdf.loc[["viper", "sidewinder"], ["shield", "max_speed"]] = 10
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser1, pser1)
        self.assert_eq(psser2, pser2)

        pdf.loc[["viper", "sidewinder"], "shield"] = 50
        psdf.loc[["viper", "sidewinder"], "shield"] = 50
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser1, pser1)
        self.assert_eq(psser2, pser2)

        pdf.loc["cobra", "max_speed"] = 30
        psdf.loc["cobra", "max_speed"] = 30
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser1, pser1)
        self.assert_eq(psser2, pser2)

        pdf.loc[pdf.max_speed < 5, "max_speed"] = -pdf.max_speed
        psdf.loc[psdf.max_speed < 5, "max_speed"] = -psdf.max_speed
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser1, pser1)
        self.assert_eq(psser2, pser2)

        pdf.loc[pdf.max_speed < 2, "max_speed"] = -pdf.max_speed
        psdf.loc[psdf.max_speed < 2, "max_speed"] = -psdf.max_speed
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser1, pser1)
        self.assert_eq(psser2, pser2)

        pdf.loc[:, "min_speed"] = 0
        psdf.loc[:, "min_speed"] = 0
        self.assert_eq(psdf, pdf, almost=True)
        self.assert_eq(psser1, pser1)
        self.assert_eq(psser2, pser2)

        with self.assertRaisesRegex(ValueError, "Incompatible indexer with Series"):
            psdf.loc["cobra", "max_speed"] = -psdf.max_speed
        with self.assertRaisesRegex(ValueError, "shape mismatch"):
            psdf.loc[:, ["shield", "max_speed"]] = -psdf.max_speed
        with self.assertRaisesRegex(ValueError, "Only a dataframe with one column can be assigned"):
            psdf.loc[:, "max_speed"] = psdf

        # multi-index columns
        columns = pd.MultiIndex.from_tuples(
            [("x", "max_speed"), ("x", "shield"), ("y", "min_speed")]
        )
        pdf.columns = columns
        psdf.columns = columns

        pdf.loc[:, ("y", "shield")] = -pdf[("x", "shield")]
        psdf.loc[:, ("y", "shield")] = -psdf[("x", "shield")]
        self.assert_eq(psdf, pdf, almost=True)
        self.assert_eq(psser1, pser1)
        self.assert_eq(psser2, pser2)

        pdf.loc[:, "z"] = 100
        psdf.loc[:, "z"] = 100
        self.assert_eq(psdf, pdf, almost=True)
        self.assert_eq(psser1, pser1)
        self.assert_eq(psser2, pser2)

        with self.assertRaisesRegex(KeyError, "Key length \\(3\\) exceeds index depth \\(2\\)"):
            psdf.loc[:, [("x", "max_speed", "foo")]] = -psdf[("x", "shield")]

        pdf = pd.DataFrame(
            [[1], [4], [7]], index=["cobra", "viper", "sidewinder"], columns=["max_speed"]
        )
        psdf = ps.from_pandas(pdf)

        pdf.loc[:, "max_speed"] = pdf
        psdf.loc[:, "max_speed"] = psdf
        self.assert_eq(psdf, pdf)

    def test_series_loc_setitem(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y

        pser.loc[pser % 2 == 1] = -pser
        psser.loc[psser % 2 == 1] = -psser
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        for key, value in [
            (["viper", "sidewinder"], 10),
            ("viper", 50),
            (slice(None), 10),
            (slice(None, "viper"), 20),
            (slice("viper", None), 30),
        ]:
            with self.subTest(key=key, value=value):
                pser.loc[key] = value
                psser.loc[key] = value
                self.assert_eq(psser, pser)
                self.assert_eq(psdf, pdf)
                self.assert_eq(pssery, psery)

        with self.assertRaises(ValueError):
            psser.loc["viper"] = -psser

        # multiindex
        pser = pd.Series(
            [1, 2, 3],
            index=pd.MultiIndex.from_tuples([("x", "cobra"), ("x", "viper"), ("y", "sidewinder")]),
        )
        psser = ps.from_pandas(pser)

        pser.loc["x"] = pser * 10
        psser.loc["x"] = psser * 10
        self.assert_eq(psser, pser)

        pser.loc["y"] = pser * 10
        psser.loc["y"] = psser * 10
        self.assert_eq(psser, pser)


class IndexingLocTests(
    IndexingLocMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_indexing_loc import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
