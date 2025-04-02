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

import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class ExpandingTestingFuncMixin:
    def _test_expanding_func(self, ps_func, pd_func=None):
        if not pd_func:
            pd_func = ps_func
        if isinstance(pd_func, str):
            pd_func = self.convert_str_to_lambda(pd_func)
        if isinstance(ps_func, str):
            ps_func = self.convert_str_to_lambda(ps_func)
        pser = pd.Series([1, 2, 3, 7, 9, 8], index=np.random.rand(6), name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(ps_func(psser.expanding(2)), pd_func(pser.expanding(2)), almost=True)
        self.assert_eq(ps_func(psser.expanding(2)), pd_func(pser.expanding(2)), almost=True)

        # Multiindex
        pser = pd.Series(
            [1, 2, 3], index=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z")])
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(ps_func(psser.expanding(2)), pd_func(pser.expanding(2)))

        pdf = pd.DataFrame(
            {"a": [1.0, 2.0, 3.0, 2.0], "b": [4.0, 2.0, 3.0, 1.0]}, index=np.random.rand(4)
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(ps_func(psdf.expanding(2)), pd_func(pdf.expanding(2)))
        self.assert_eq(ps_func(psdf.expanding(2)).sum(), pd_func(pdf.expanding(2)).sum())

        # Multiindex column
        columns = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(ps_func(psdf.expanding(2)), pd_func(pdf.expanding(2)))


class ExpandingMixin(ExpandingTestingFuncMixin):
    def test_expanding_repr(self):
        self.assertEqual(repr(ps.range(10).expanding(5)), "Expanding [min_periods=5]")

    def test_expanding_count(self):
        self._test_expanding_func("count")

    def test_expanding_min(self):
        self._test_expanding_func("min")

    def test_expanding_max(self):
        self._test_expanding_func("max")

    def test_expanding_mean(self):
        self._test_expanding_func("mean")

    def test_expanding_sum(self):
        self._test_expanding_func("sum")


class ExpandingTests(
    ExpandingMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.window.test_expanding import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
