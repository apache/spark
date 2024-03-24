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
from datetime import datetime
import unittest

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


# This file contains test cases for 'Time series-related'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#time-series-related
class FrameTimeSeriesMixin:
    def test_shift(self):
        pdf = pd.DataFrame(
            {
                "Col1": [10, 20, 15, 30, 45],
                "Col2": [13, 23, 18, 33, 48],
                "Col3": [17, 27, 22, 37, 52],
            },
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.shift(3), psdf.shift(3))
        self.assert_eq(pdf.shift().shift(-1), psdf.shift().shift(-1))
        self.assert_eq(pdf.shift().sum().astype(int), psdf.shift().sum())

        # Need the expected result since pandas 0.23 does not support `fill_value` argument.
        pdf1 = pd.DataFrame(
            {"Col1": [0, 0, 0, 10, 20], "Col2": [0, 0, 0, 13, 23], "Col3": [0, 0, 0, 17, 27]},
            index=pdf.index,
        )
        self.assert_eq(pdf1, psdf.shift(periods=3, fill_value=0))
        msg = "should be an int"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.shift(1.5)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "Col1"), ("x", "Col2"), ("y", "Col3")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.shift(3), psdf.shift(3))
        self.assert_eq(pdf.shift().shift(-1), psdf.shift().shift(-1))
        self.assert_eq(pdf.shift(0), psdf.shift(0))

    def test_first_valid_index(self):
        pdf = pd.DataFrame(
            {"a": [None, 2, 3, 2], "b": [None, 2.0, 3.0, 1.0], "c": [None, 200, 400, 200]},
            index=["Q", "W", "E", "R"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.first_valid_index(), psdf.first_valid_index())
        self.assert_eq(pdf[[]].first_valid_index(), psdf[[]].first_valid_index())

        # MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.first_valid_index(), psdf.first_valid_index())

        # Empty DataFrame
        pdf = pd.Series([]).to_frame()
        psdf = ps.Series([]).to_frame()
        self.assert_eq(pdf.first_valid_index(), psdf.first_valid_index())

        pdf = pd.DataFrame(
            {"a": [None, 2, 3, 2], "b": [None, 2.0, 3.0, 1.0], "c": [None, 200, 400, 200]},
            index=[
                datetime(2021, 1, 1),
                datetime(2021, 2, 1),
                datetime(2021, 3, 1),
                datetime(2021, 4, 1),
            ],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.first_valid_index(), psdf.first_valid_index())

    def test_last_valid_index(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, None], "b": [1.0, 2.0, 3.0, None], "c": [100, 200, 400, None]},
            index=["Q", "W", "E", "R"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.last_valid_index(), psdf.last_valid_index())
        self.assert_eq(pdf[[]].last_valid_index(), psdf[[]].last_valid_index())

        # MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.last_valid_index(), psdf.last_valid_index())

        # Empty DataFrame
        pdf = pd.Series([]).to_frame()
        psdf = ps.Series([]).to_frame()
        self.assert_eq(pdf.last_valid_index(), psdf.last_valid_index())

    def test_to_datetime(self):
        pdf = pd.DataFrame(
            {"year": [2015, 2016], "month": [2, 3], "day": [4, 5]}, index=np.random.rand(2)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))


class FrameTimeSeriesTests(
    FrameTimeSeriesMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_time_series import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
