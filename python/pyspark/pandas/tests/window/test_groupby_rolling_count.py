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
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class GroupByRollingCountMixin:
    def test_groupby_rolling_count(self):
        pser = pd.Series([1, 2, 3, 2], index=np.random.rand(4), name="a")
        psser = ps.from_pandas(pser)
        # TODO(SPARK-43432): Fix `min_periods` for Rolling.count() to work same as pandas
        self.assert_eq(
            psser.groupby(psser).rolling(2).count().sort_index(),
            pser.groupby(pser).rolling(2, min_periods=1).count().sort_index(),
        )
        self.assert_eq(
            psser.groupby(psser).rolling(2).count().sum(),
            pser.groupby(pser).rolling(2, min_periods=1).count().sum(),
        )

        # Multiindex
        pser = pd.Series(
            [1, 2, 3, 2],
            index=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z"), ("c", "z")]),
            name="a",
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(
            psser.groupby(psser).rolling(2).count().sort_index(),
            pser.groupby(pser).rolling(2, min_periods=1).count().sort_index(),
        )

        pdf = pd.DataFrame({"a": [1.0, 2.0, 3.0, 2.0], "b": [4.0, 2.0, 3.0, 1.0]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby(psdf.a).rolling(2).count().sort_index(),
            pdf.groupby(pdf.a).rolling(2, min_periods=1).count().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.a).rolling(2).count().sum(),
            pdf.groupby(pdf.a).rolling(2, min_periods=1).count().sum(),
        )
        self.assert_eq(
            psdf.groupby(psdf.a + 1).rolling(2).count().sort_index(),
            pdf.groupby(pdf.a + 1).rolling(2, min_periods=1).count().sort_index(),
        )

        self.assert_eq(
            psdf.b.groupby(psdf.a).rolling(2).count().sort_index(),
            pdf.b.groupby(pdf.a).rolling(2, min_periods=1).count().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.a)["b"].rolling(2).count().sort_index(),
            pdf.groupby(pdf.a)["b"].rolling(2, min_periods=1).count().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.a)[["b"]].rolling(2).count().sort_index(),
            pdf.groupby(pdf.a)[["b"]].rolling(2, min_periods=1).count().sort_index(),
        )

        # Multiindex column
        columns = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("a", "x")).rolling(2).count().sort_index(),
            pdf.groupby(("a", "x")).rolling(2, min_periods=1).count().sort_index(),
        )

        self.assert_eq(
            psdf.groupby([("a", "x"), ("a", "y")]).rolling(2).count().sort_index(),
            pdf.groupby([("a", "x"), ("a", "y")]).rolling(2, min_periods=1).count().sort_index(),
        )


class GroupByRollingCountTests(
    GroupByRollingCountMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.window.test_groupby_rolling_count import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
