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
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class GroupbyStatTestingFuncMixin:
    # TODO: All statistical functions should leverage this utility
    def _test_stat_func(self, func, check_exact=True):
        pdf, psdf = self.pdf, self.psdf
        for p_groupby_obj, ps_groupby_obj in [
            # Against DataFrameGroupBy
            (pdf.groupby("A"), psdf.groupby("A")),
            # Against DataFrameGroupBy with an aggregation column of string type
            (pdf.groupby("A")[["C"]], psdf.groupby("A")[["C"]]),
            # Against SeriesGroupBy
            (pdf.groupby("A")["B"], psdf.groupby("A")["B"]),
        ]:
            self.assert_eq(
                func(p_groupby_obj).sort_index(),
                func(ps_groupby_obj).sort_index(),
                check_exact=check_exact,
            )


class GroupbyStatMixin(GroupbyStatTestingFuncMixin):
    @property
    def pdf(self):
        return pd.DataFrame(
            {
                "A": [1, 2, 1, 2],
                "B": [3.1, 4.1, 4.1, 3.1],
                "C": ["a", "b", "b", "a"],
                "D": [True, False, False, True],
            }
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_mean(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.mean(numeric_only=True))
        psdf = self.psdf
        with self.assertRaises(TypeError):
            psdf.groupby("A")["C"].mean()

    def test_min(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.min())
        self._test_stat_func(lambda groupby_obj: groupby_obj.min(min_count=2))
        self._test_stat_func(lambda groupby_obj: groupby_obj.min(numeric_only=None))
        self._test_stat_func(lambda groupby_obj: groupby_obj.min(numeric_only=True))
        self._test_stat_func(lambda groupby_obj: groupby_obj.min(numeric_only=True, min_count=2))

    def test_max(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.max())
        self._test_stat_func(lambda groupby_obj: groupby_obj.max(min_count=2))
        self._test_stat_func(lambda groupby_obj: groupby_obj.max(numeric_only=None))
        self._test_stat_func(lambda groupby_obj: groupby_obj.max(numeric_only=True))
        self._test_stat_func(lambda groupby_obj: groupby_obj.max(numeric_only=True, min_count=2))

    def test_sum(self):
        pdf = pd.DataFrame(
            {
                "A": ["a", "a", "b", "a"],
                "B": [1, 2, 1, 2],
                "C": [-1.5, np.nan, -3.2, 0.1],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.groupby("A").sum().sort_index(), psdf.groupby("A").sum().sort_index())
        self.assert_eq(
            pdf.groupby("A").sum(min_count=2).sort_index(),
            psdf.groupby("A").sum(min_count=2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("A").sum(min_count=3).sort_index(),
            psdf.groupby("A").sum(min_count=3).sort_index(),
        )

    def test_median(self):
        psdf = ps.DataFrame(
            {
                "a": [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0],
                "b": [2.0, 3.0, 1.0, 4.0, 6.0, 9.0, 8.0, 10.0, 7.0, 5.0],
                "c": [3.0, 5.0, 2.0, 5.0, 1.0, 2.0, 6.0, 4.0, 3.0, 6.0],
            },
            columns=["a", "b", "c"],
            index=[7, 2, 4, 1, 3, 4, 9, 10, 5, 6],
        )
        # DataFrame
        expected_result = ps.DataFrame(
            {"b": [2.0, 8.0, 7.0], "c": [3.0, 2.0, 4.0]}, index=pd.Index([1.0, 2.0, 3.0], name="a")
        )
        self.assert_eq(expected_result, psdf.groupby("a").median().sort_index())
        # Series
        expected_result = ps.Series(
            [2.0, 8.0, 7.0], name="b", index=pd.Index([1.0, 2.0, 3.0], name="a")
        )
        self.assert_eq(expected_result, psdf.groupby("a")["b"].median().sort_index())

        with self.assertRaisesRegex(TypeError, "accuracy must be an integer; however"):
            psdf.groupby("a").median(accuracy="a")


class GroupbyStatTests(
    GroupbyStatMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_stat import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
