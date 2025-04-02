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
from pyspark.testing.sqlutils import SQLTestUtils


class CorrMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {
                "A": [0, 0, 0, 1, 1, 2],
                "B": [-1, 2, 3, 5, 6, 0],
                "C": [4, 6, 5, 1, 3, 0],
            },
            columns=["A", "B", "C"],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_corr(self):
        for c in ["A", "B", "C"]:
            self.assert_eq(
                self.pdf.groupby(c).corr().sort_index(),
                self.psdf.groupby(c).corr().sort_index(),
                almost=True,
            )

    def test_method(self):
        for m in ["pearson", "spearman", "kendall"]:
            self.assert_eq(
                self.pdf.groupby("A").corr(method=m).sort_index(),
                self.psdf.groupby("A").corr(method=m).sort_index(),
                almost=True,
            )

    def test_min_periods(self):
        for m in [1, 2, 3]:
            self.assert_eq(
                self.pdf.groupby("A").corr(min_periods=m).sort_index(),
                self.psdf.groupby("A").corr(min_periods=m).sort_index(),
                almost=True,
            )


class CorrTests(
    CorrMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_corr import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
