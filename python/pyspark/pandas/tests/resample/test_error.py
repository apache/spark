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
import datetime

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class ResampleErrorMixin:
    def test_resample_error(self):
        psdf = ps.range(10)

        with self.assertRaisesRegex(
            NotImplementedError, "resample currently works only for DatetimeIndex"
        ):
            psdf.resample("3Y").sum()

        with self.assertRaisesRegex(
            NotImplementedError, "resample currently works only for DatetimeIndex"
        ):
            psdf.id.resample("3Y").sum()

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
            psdf.A.resample("0D").sum()

        with self.assertRaisesRegex(ValueError, "rule code YE-DEC is not supported"):
            psdf.A.resample("0Y").sum()

        with self.assertRaisesRegex(ValueError, "invalid closed: 'middle'"):
            psdf.A.resample("3D", closed="middle").sum()

        with self.assertRaisesRegex(ValueError, "rule code YE-DEC is not supported"):
            psdf.A.resample("3Y", closed="middle").sum()

        with self.assertRaisesRegex(ValueError, "invalid label: 'both'"):
            psdf.A.resample("3D", label="both").sum()

        with self.assertRaisesRegex(ValueError, "rule code YE-DEC is not supported"):
            psdf.A.resample("3Y", label="both").sum()

        with self.assertRaisesRegex(
            NotImplementedError, "`on` currently works only for TimestampType"
        ):
            psdf.A.resample("2D", on=psdf.A).sum()

        with self.assertRaisesRegex(
            NotImplementedError, "`on` currently works only for TimestampType"
        ):
            psdf[["A"]].resample("2D", on=psdf.A).sum()

        psdf["B"] = ["a", "b", "c", "d"]
        with self.assertRaisesRegex(ValueError, "No available aggregation columns!"):
            psdf.B.resample("2D").sum()

        with self.assertRaisesRegex(ValueError, "No available aggregation columns!"):
            psdf[[]].resample("2D").sum()


class ResampleErrorTests(ResampleErrorMixin, PandasOnSparkTestCase, TestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.resample.test_error import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
