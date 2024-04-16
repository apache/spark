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
import numpy as np

from pyspark import pandas as ps
from pyspark.loose_version import LooseVersion
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class DiffFramesCorrWithMixin:
    @property
    def pdf1(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 10, 11],
        )

    @property
    def pdf2(self):
        return pd.DataFrame(
            {"a": [9, 8, 7, 6, 5, 4, 3, 2, 1], "b": [0, 0, 0, 4, 5, 6, 1, 2, 3]},
            index=list(range(9)),
        )

    @property
    def pdf3(self):
        return pd.DataFrame(
            {"b": [1, 1, 1, 1, 1, 1, 1, 1, 1], "c": [1, 1, 1, 1, 1, 1, 1, 1, 1]},
            index=list(range(9)),
        )

    @property
    def pdf4(self):
        return pd.DataFrame(
            {"e": [2, 2, 2, 2, 2, 2, 2, 2, 2], "f": [2, 2, 2, 2, 2, 2, 2, 2, 2]},
            index=list(range(9)),
        )

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

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_corrwith(self):
        df1 = ps.DataFrame({"A": [1, np.nan, 7, 8], "X": [5, 8, np.nan, 3], "C": [10, 4, 9, 3]})
        df2 = ps.DataFrame({"A": [5, 3, 6, 4], "B": [11, 2, 4, 3], "C": [4, 3, 8, np.nan]})
        self._test_corrwith(df1, df2)
        self._test_corrwith((df1 + 1), df2.B)
        self._test_corrwith((df1 + 1), (df2.B + 2))

        # There was a regression in pandas 1.5.0, and fixed in pandas 1.5.1.
        # Therefore, we only test the pandas 1.5.0 in different way.
        # See https://github.com/pandas-dev/pandas/issues/49141 for the reported issue,
        # and https://github.com/pandas-dev/pandas/pull/46174 for the initial PR that causes.
        df_bool = ps.DataFrame({"A": [True, True, False, False], "B": [True, False, False, True]})
        ser_bool = ps.Series([True, True, False, True])
        if LooseVersion(pd.__version__) == LooseVersion("1.5.0"):
            expected = ps.Series([0.5773502691896257, 0.5773502691896257], index=["B", "A"])
            self.assert_eq(df_bool.corrwith(ser_bool), expected, almost=True)
        else:
            self._test_corrwith(df_bool, ser_bool)

        self._test_corrwith(self.psdf1, self.psdf1)
        self._test_corrwith(self.psdf1, self.psdf2)
        self._test_corrwith(self.psdf2, self.psdf3)
        self._test_corrwith(self.psdf3, self.psdf4)

        self._test_corrwith(self.psdf1, self.psdf1.a)
        # There was a regression in pandas 1.5.0, and fixed in pandas 1.5.1.
        # Therefore, we only test the pandas 1.5.0 in different way.
        # See https://github.com/pandas-dev/pandas/issues/49141 for the reported issue,
        # and https://github.com/pandas-dev/pandas/pull/46174 for the initial PR that causes.
        if LooseVersion(pd.__version__) == LooseVersion("1.5.0"):
            expected = ps.Series([-0.08827348295047496, 0.4413674147523748], index=["b", "a"])
            self.assert_eq(self.psdf1.corrwith(self.psdf2.b), expected, almost=True)
        else:
            self._test_corrwith(self.psdf1, self.psdf2.b)

        self._test_corrwith(self.psdf2, self.psdf3.c)
        self._test_corrwith(self.psdf3, self.psdf4.f)

    def _test_corrwith(self, psdf, psobj):
        pdf = psdf._to_pandas()
        pobj = psobj._to_pandas()
        for drop in [True, False]:
            p_corr = pdf.corrwith(pobj, drop=drop)
            ps_corr = psdf.corrwith(psobj, drop=drop)
            self.assert_eq(p_corr.sort_index(), ps_corr.sort_index(), almost=True)


class DiffFramesCorrWithTests(
    DiffFramesCorrWithMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_corrwith import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
