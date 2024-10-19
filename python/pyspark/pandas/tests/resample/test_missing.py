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
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.resample import (
    MissingPandasLikeDataFrameResampler,
    MissingPandasLikeSeriesResampler,
)
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class ResampleMissingMixin:
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
    def psdf1(self):
        return ps.from_pandas(self.pdf1)

    def test_missing(self):
        pdf_r = self.psdf1.resample("3D")
        pser_r = self.psdf1.A.resample("3D")

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


class ResampleMissingTests(ResampleMissingMixin, PandasOnSparkTestCase, TestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.resample.test_missing import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
