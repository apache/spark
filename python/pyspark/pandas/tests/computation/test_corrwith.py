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

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class FrameCorrwithMixin:
    def test_corrwith(self):
        df1 = ps.DataFrame(
            {"A": [1, np.nan, 7, 8], "B": [False, True, True, False], "C": [10, 4, 9, 3]}
        )
        df2 = df1[["A", "C"]]
        df3 = df1[["B", "C"]]
        self._test_corrwith(df1, df2)
        self._test_corrwith(df1, df3)
        self._test_corrwith((df1 + 1), df2.A)
        self._test_corrwith((df1 + 1), df3.B)
        self._test_corrwith((df1 + 1), (df2.C + 2))
        self._test_corrwith((df1 + 1), (df3.B + 2))

        with self.assertRaisesRegex(TypeError, "unsupported type"):
            df1.corrwith(123)
        with self.assertRaisesRegex(NotImplementedError, "only works for axis=0"):
            df1.corrwith(df1.A, axis=1)
        with self.assertRaisesRegex(ValueError, "Invalid method"):
            df1.corrwith(df1.A, method="cov")

        df_bool = ps.DataFrame({"A": [True, True, False, False], "B": [True, False, False, True]})
        self._test_corrwith(df_bool, df_bool.A)
        self._test_corrwith(df_bool, df_bool.B)

    def _test_corrwith(self, psdf, psobj):
        pdf = psdf._to_pandas()
        pobj = psobj._to_pandas()
        # There was a regression in pandas 1.5.0
        # when other is Series and method is "pearson" or "spearman", and fixed in pandas 1.5.1
        # Therefore, we only test the pandas 1.5.0 in different way.
        # See https://github.com/pandas-dev/pandas/issues/48826 for the reported issue,
        # and https://github.com/pandas-dev/pandas/pull/46174 for the initial PR that causes.
        methods = ["pearson", "spearman", "kendall"]
        for method in methods:
            for drop in [True, False]:
                p_corr = pdf.corrwith(pobj, drop=drop, method=method)
                ps_corr = psdf.corrwith(psobj, drop=drop, method=method)
                self.assert_eq(p_corr.sort_index(), ps_corr.sort_index(), almost=True)


class FrameCorrwithTests(
    FrameCorrwithMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_corrwith import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
