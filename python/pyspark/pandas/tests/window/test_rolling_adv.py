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

import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.window.test_rolling import RollingTestingFuncMixin


class RollingAdvMixin(RollingTestingFuncMixin):
    def test_rolling_median(self):
        self._test_rolling_func("median", lambda x: x.quantile(0.5, "lower"))

    def test_rolling_quantile(self):
        self._test_rolling_func(lambda x: x.quantile(0.5), lambda x: x.quantile(0.5, "lower"))

    def test_rolling_std(self):
        self._test_rolling_func("std")

    def test_rolling_var(self):
        self._test_rolling_func("var")

    def test_rolling_sem(self):
        self._test_rolling_func("sem")
        self._test_rolling_func(lambda x: x.sem(ddof=0), lambda x: x.sem(ddof=0))
        # A single-element window with ddof=0: the population std of one element is 0, so
        # pandas 3 returns 0.0 (not null); pandas < 3 returns nan. Both are matched here.
        # Guards against a var_samp-based sem, which is null at count == 1.
        pser = pd.Series([5.0, 6.0, 7.0])
        psser = ps.from_pandas(pser)
        self.assert_eq(
            psser.rolling(1, min_periods=1).sem(ddof=0),
            pser.rolling(1, min_periods=1).sem(ddof=0),
        )

    def test_rolling_skew(self):
        self._test_rolling_func("skew")

    def test_rolling_kurt(self):
        self._test_rolling_func("kurt")


class RollingAdvTests(
    RollingAdvMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
