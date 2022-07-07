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
import inspect
import datetime
import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError, DataError
from pyspark.pandas.missing.resample import (
    MissingPandasLikeDataFrameResampler,
    MissingPandasLikeSeriesResampler,
)
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class ResampleTest(PandasOnSparkTestCase, TestUtils):
    @property
    def pdf1(self):
        np.random.seed(11)
        dates = [
            pd.NaT,
            datetime.datetime(2011, 12, 31),
            datetime.datetime(2011, 12, 31, 0, 0, 1),
            datetime.datetime(2011, 12, 31, 23, 59, 59),
            datetime.datetime(2012, 1, 1),
            datetime.datetime(2012, 1, 1, 0, 0, 1),
            pd.NaT,
            datetime.datetime(2012, 1, 1, 23, 59, 59),
            datetime.datetime(2012, 1, 2),
            pd.NaT,
            datetime.datetime(2012, 1, 30, 23, 59, 59),
            datetime.datetime(2012, 1, 31),
            datetime.datetime(2012, 1, 31, 0, 0, 1),
            datetime.datetime(2012, 3, 31),
            datetime.datetime(2013, 5, 3),
            datetime.datetime(2022, 5, 3),
        ]
        return pd.DataFrame(
            np.random.rand(len(dates), 2), index=pd.DatetimeIndex(dates), columns=list("AB")
        )

    @property
    def pdf2(self):
        np.random.seed(22)
        dates = [
            datetime.datetime(2022, 5, 1, 4, 5, 6),
            datetime.datetime(2022, 5, 3),
            datetime.datetime(2022, 5, 3, 23, 59, 59),
            datetime.datetime(2022, 5, 4),
            pd.NaT,
            datetime.datetime(2022, 5, 4, 0, 0, 1),
            datetime.datetime(2022, 5, 11),
        ]
        return pd.DataFrame(
            np.random.rand(len(dates), 2), index=pd.DatetimeIndex(dates), columns=list("AB")
        )

    @property
    def pdf3(self):
        np.random.seed(22)
        index = pd.date_range(start="2011-01-02", end="2022-05-01", freq="1D")
        return pd.DataFrame(np.random.rand(len(index), 2), index=index, columns=list("AB"))

    @property
    def pdf4(self):
        np.random.seed(33)
        index = pd.date_range(start="2020-12-12", end="2022-05-01", freq="1H")
        return pd.DataFrame(np.random.rand(len(index), 2), index=index, columns=list("AB"))

    @property
    def pdf5(self):
        np.random.seed(44)
        index = pd.date_range(start="2021-12-30 03:04:05", end="2022-01-02 06:07:08", freq="1T")
        return pd.DataFrame(np.random.rand(len(index), 2), index=index, columns=list("AB"))

    @property
    def pdf6(self):
        np.random.seed(55)
        index = pd.date_range(start="2022-05-02 03:04:05", end="2022-05-02 06:07:08", freq="1S")
        return pd.DataFrame(np.random.rand(len(index), 2), index=index, columns=list("AB"))

    @property
    def psdf1(self):
        return ps.from_pandas(self.pdf1)

    @property
    def psdf2(self):
        return ps.from_pandas(self.pdf2)

    @property
    def psdf3(self):
        return ps.from_pandas(self.pdf3)

    @property
    def psdf4(self):
        return ps.from_pandas(self.pdf4)

    @property
    def psdf5(self):
        return ps.from_pandas(self.pdf5)

    @property
    def psdf6(self):
        return ps.from_pandas(self.pdf6)

    def test_resample_error(self):
        psdf = ps.range(10)

        with self.assertRaisesRegex(
            NotImplementedError, "resample currently works only for DatetimeIndex"
        ):
            psdf.resample("3Y").sum()

        dates = [
            datetime.datetime(2012, 1, 2),
            datetime.datetime(2012, 5, 3),
            datetime.datetime(2022, 5, 3),
            pd.NaT,
        ]
        pdf = pd.DataFrame(np.ones(len(dates)), index=pd.DatetimeIndex(dates), columns=["A"])
        psdf = ps.from_pandas(pdf)

        with self.assertRaisesRegex(ValueError, "rule code W-SUN is not supported"):
            psdf.A.resample("3W").sum()

        with self.assertRaisesRegex(ValueError, "rule offset must be positive"):
            psdf.A.resample("0Y").sum()

        with self.assertRaisesRegex(ValueError, "invalid closed: 'middle'"):
            psdf.A.resample("3Y", closed="middle").sum()

        with self.assertRaisesRegex(ValueError, "invalid label: 'both'"):
            psdf.A.resample("3Y", label="both").sum()

    def test_missing(self):
        pdf_r = self.psdf1.resample("3Y")
        pser_r = self.psdf1.A.resample("3Y")

        # DataFrameResampler functions
        missing_functions = inspect.getmembers(
            MissingPandasLikeDataFrameResampler, inspect.isfunction
        )
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Resampler.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(pdf_r, name)()

        # SeriesResampler functions
        missing_functions = inspect.getmembers(MissingPandasLikeSeriesResampler, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Resampler.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(pser_r, name)()

        # DataFrameResampler properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeDataFrameResampler, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Resampler.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(pdf_r, name)

        # SeriesResampler properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeSeriesResampler, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Resampler.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(pser_r, name)

    def _test_resample(self, pobj, psobj, rules, funcs):
        for rule in rules:
            for func in funcs:
                for closed in [None, "left", "right"]:
                    for label in [None, "left", "right"]:
                        p_resample = pobj.resample(rule=rule, closed=closed, label=label)
                        ps_resample = psobj.resample(rule=rule, closed=closed, label=label)
                        self.assert_eq(
                            getattr(p_resample, func)().sort_index(),
                            getattr(ps_resample, func)().sort_index(),
                            almost=True,
                        )

    def test_dataframe_resample(self):
        self._test_resample(
            self.pdf1,
            self.psdf1,
            ["3Y", "9M", "17D"],
            ["min", "max", "sum", "mean", "std", "var"],
        )
        self._test_resample(self.pdf2, self.psdf2, ["3A", "11M", "D"], ["sum"])
        self._test_resample(self.pdf3, self.psdf3, ["2D", "1M"], ["sum"])
        self._test_resample(self.pdf4, self.psdf4, ["1H", "2D"], ["sum"])
        self._test_resample(self.pdf5, self.psdf5, ["11T", "55MIN", "2H", "D"], ["sum"])
        self._test_resample(self.pdf6, self.psdf6, ["29S", "10MIN", "3H"], ["sum"])

    def test_series_resample(self):
        self._test_resample(self.pdf1.A, self.psdf1.A, ["4Y"], ["sum"])
        self._test_resample(self.pdf2.A, self.psdf2.A, ["13M"], ["sum"])
        self._test_resample(self.pdf3.A, self.psdf3.A, ["18H"], ["sum"])
        self._test_resample(self.pdf4.A, self.psdf4.A, ["6D"], ["sum"])
        self._test_resample(self.pdf5.A, self.psdf5.A, ["47T"], ["sum"])
        self._test_resample(self.pdf6.A, self.psdf6.A, ["111S"], ["sum"])

    def test_resample_on(self):
        np.random.seed(77)
        dates = [
            datetime.datetime(2022, 5, 1, 4, 5, 6),
            datetime.datetime(2022, 5, 3),
            datetime.datetime(2022, 5, 3, 23, 59, 59),
            datetime.datetime(2022, 5, 4),
            pd.NaT,
            datetime.datetime(2022, 5, 4, 0, 0, 1),
            datetime.datetime(2022, 5, 11),
        ]
        pdf = pd.DataFrame(
            np.random.rand(len(dates), 3), index=pd.DatetimeIndex(dates), columns=list("ABC")
        )
        pdf["X"] = pd.DatetimeIndex(dates)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.resample("2D", on="X").sum().sort_index(),
            psdf.resample("2D", on=psdf.X).sum().sort_index(),
            almost=True,
        )


if __name__ == "__main__":
    from pyspark.pandas.tests.test_resample import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
