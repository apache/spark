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

import pyspark.pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class AsOfMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [10.0, 20.0, 30.0, 40.0, 50.0], "b": [None, None, None, None, 500]},
            index=pd.DatetimeIndex(
                [
                    "2018-02-27 09:01:00",
                    "2018-02-27 09:02:00",
                    "2018-02-27 09:03:00",
                    "2018-02-27 09:04:00",
                    "2018-02-27 09:05:00",
                ]
            ),
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_disabled(self):
        with self.assertRaises(PandasNotImplementedError):
            self.psdf.asof(pd.DatetimeIndex(["2018-02-27 09:03:30", "2018-02-27 09:04:30"]))

    def test_fallback(self):
        ps.set_option("compute.pandas_fallback", True)

        self.assert_eq(
            self.pdf.asof(pd.DatetimeIndex(["2018-02-27 09:03:30", "2018-02-27 09:04:30"])),
            self.psdf.asof(pd.DatetimeIndex(["2018-02-27 09:03:30", "2018-02-27 09:04:30"])),
        )
        self.assert_eq(
            self.pdf.asof(
                pd.DatetimeIndex(["2018-02-27 09:03:30", "2018-02-27 09:04:30"]),
                subset=["a"],
            ),
            self.psdf.asof(
                pd.DatetimeIndex(["2018-02-27 09:03:30", "2018-02-27 09:04:30"]),
                subset=["a"],
            ),
        )

        # test with schema infered from partial dataset, len(pdf)==5
        ps.set_option("compute.shortcut_limit", 2)
        self.assert_eq(
            self.pdf.asof(pd.DatetimeIndex(["2018-02-27 09:03:30", "2018-02-27 09:04:30"])),
            self.psdf.asof(pd.DatetimeIndex(["2018-02-27 09:03:30", "2018-02-27 09:04:30"])),
        )
        self.assert_eq(
            self.pdf.asof(
                pd.DatetimeIndex(["2018-02-27 09:03:30", "2018-02-27 09:04:30"]),
                subset=["a"],
            ),
            self.psdf.asof(
                pd.DatetimeIndex(["2018-02-27 09:03:30", "2018-02-27 09:04:30"]),
                subset=["a"],
            ),
        )

        ps.reset_option("compute.shortcut_limit")
        ps.reset_option("compute.pandas_fallback")


class AsFreqTests(
    AsOfMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_asof import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
