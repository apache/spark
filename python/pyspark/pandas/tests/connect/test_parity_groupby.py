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

from pyspark.pandas.tests.test_groupby import GroupByTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils, TestUtils


class GroupByParityTests(
    GroupByTestsMixin, PandasOnSparkTestUtils, TestUtils, ReusedConnectTestCase
):
    @unittest.skip("TODO(SPARK-43628): Enable SparkContext with Spark Connect.")
    def test_apply_with_side_effect(self):
        super().test_apply_with_side_effect()

    @unittest.skip(
        "TODO(SPARK-43622): Enable pyspark.pandas.spark.functions.covar in Spark Connect."
    )
    def test_basic_stat_funcs(self):
        super().test_basic_stat_funcs()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_bfill(self):
        super().test_bfill()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_cumcount(self):
        super().test_cumcount()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_cummax(self):
        super().test_cummax()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_cummin(self):
        super().test_cummin()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_cumprod(self):
        super().test_cumprod()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_cumsum(self):
        super().test_cumsum()

    @unittest.skip(
        "TODO(SPARK-43645): Enable pyspark.pandas.spark.functions.stddev in Spark Connect."
    )
    def test_ddof(self):
        super().test_ddof()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_ffill(self):
        super().test_ffill()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_fillna(self):
        super().test_fillna()

    @unittest.skip(
        "TODO(SPARK-43617): Enable pyspark.pandas.spark.functions.product in Spark Connect."
    )
    def test_prod(self):
        super().test_prod()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_shift(self):
        super().test_shift()


if __name__ == "__main__":
    from pyspark.pandas.tests.connect.test_parity_groupby import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
