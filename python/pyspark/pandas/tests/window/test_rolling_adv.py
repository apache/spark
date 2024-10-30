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

from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils
from pyspark.pandas.tests.window.test_rolling import RollingTestingFuncMixin


class RollingAdvMixin(RollingTestingFuncMixin):
    def test_rolling_quantile(self):
        self._test_rolling_func(lambda x: x.quantile(0.5), lambda x: x.quantile(0.5, "lower"))

    def test_rolling_std(self):
        self._test_rolling_func("std")

    def test_rolling_var(self):
        self._test_rolling_func("var")

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
    import unittest
    from pyspark.pandas.tests.window.test_rolling_adv import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
