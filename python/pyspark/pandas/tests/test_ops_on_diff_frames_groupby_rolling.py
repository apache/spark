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
from distutils.version import LooseVersion

import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class OpsOnDiffFramesGroupByRollingTest(PandasOnSparkTestCase, TestUtils):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def _test_groupby_rolling_func(self, f):
        pser = pd.Series([1, 2, 3], name="a")
        pkey = pd.Series([1, 2, 3], name="a")
        psser = ps.from_pandas(pser)
        kkey = ps.from_pandas(pkey)

        self.assert_eq(
            getattr(psser.groupby(kkey).rolling(2), f)().sort_index(),
            getattr(pser.groupby(pkey).rolling(2), f)().sort_index(),
        )

        pdf = pd.DataFrame({"a": [1, 2, 3, 2], "b": [4.0, 2.0, 3.0, 1.0]})
        pkey = pd.Series([1, 2, 3, 2], name="a")
        psdf = ps.from_pandas(pdf)
        kkey = ps.from_pandas(pkey)

        # The behavior of GroupBy.rolling is changed from pandas 1.3.
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            self.assert_eq(
                getattr(psdf.groupby(kkey).rolling(2), f)().sort_index(),
                getattr(pdf.groupby(pkey).rolling(2), f)().sort_index(),
            )
        else:
            self.assert_eq(
                getattr(psdf.groupby(kkey).rolling(2), f)().sort_index(),
                getattr(pdf.groupby(pkey).rolling(2), f)().drop("a", axis=1).sort_index(),
            )

        self.assert_eq(
            getattr(psdf.groupby(kkey)["b"].rolling(2), f)().sort_index(),
            getattr(pdf.groupby(pkey)["b"].rolling(2), f)().sort_index(),
        )
        self.assert_eq(
            getattr(psdf.groupby(kkey)[["b"]].rolling(2), f)().sort_index(),
            getattr(pdf.groupby(pkey)[["b"]].rolling(2), f)().sort_index(),
        )

    def test_groupby_rolling_count(self):
        self._test_groupby_rolling_func("count")

    def test_groupby_rolling_min(self):
        self._test_groupby_rolling_func("min")

    def test_groupby_rolling_max(self):
        self._test_groupby_rolling_func("max")

    def test_groupby_rolling_mean(self):
        self._test_groupby_rolling_func("mean")

    def test_groupby_rolling_sum(self):
        self._test_groupby_rolling_func("sum")

    def test_groupby_rolling_std(self):
        # TODO: `std` now raise error in pandas 1.0.0
        self._test_groupby_rolling_func("std")

    def test_groupby_rolling_var(self):
        self._test_groupby_rolling_func("var")


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_ops_on_diff_frames_groupby_rolling import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
