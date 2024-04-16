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

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.groupby.test_stat import GroupbyStatTestingFuncMixin


class FuncTestsMixin(GroupbyStatTestingFuncMixin):
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

    def test_basic_stat_funcs(self):
        self._test_stat_func(
            lambda groupby_obj: groupby_obj.var(numeric_only=True), check_exact=False
        )

        pdf, psdf = self.pdf, self.psdf

        # Unlike pandas', the median in pandas-on-Spark is an approximated median based upon
        # approximate percentile computation because computing median across a large dataset
        # is extremely expensive.
        expected = ps.DataFrame({"B": [3.1, 3.1], "D": [0, 0]}, index=pd.Index([1, 2], name="A"))
        self.assert_eq(
            psdf.groupby("A").median().sort_index(),
            expected,
        )
        self.assert_eq(
            psdf.groupby("A").median(numeric_only=None).sort_index(),
            expected,
        )
        self.assert_eq(
            psdf.groupby("A").median(numeric_only=False).sort_index(),
            expected,
        )
        self.assert_eq(
            psdf.groupby("A")["B"].median().sort_index(),
            expected.B,
        )
        with self.assertRaises(TypeError):
            psdf.groupby("A")["C"].mean()

        with self.assertRaisesRegex(
            TypeError, "Unaccepted data types of aggregation columns; numeric or bool expected."
        ):
            psdf.groupby("A")[["C"]].std()

        with self.assertRaisesRegex(
            TypeError, "Unaccepted data types of aggregation columns; numeric or bool expected."
        ):
            psdf.groupby("A")[["C"]].sem()

        self.assert_eq(
            psdf.groupby("A").std().sort_index(),
            pdf.groupby("A").std(numeric_only=True).sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby("A").sem().sort_index(),
            pdf.groupby("A").sem(numeric_only=True).sort_index(),
            check_exact=False,
        )

        self._test_stat_func(lambda groupby_obj: groupby_obj.sum(), check_exact=False)
        self.assert_eq(
            psdf.groupby("A").sum().sort_index(),
            pdf.groupby("A").sum().sort_index(),
            check_exact=False,
        )


class FuncTests(
    FuncTestsMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_stat_func import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
