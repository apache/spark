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

from pyspark.pandas.tests.test_ops_on_diff_frames_groupby_rolling import (
    OpsOnDiffFramesGroupByRollingTestsMixin,
)
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils, TestUtils


class OpsOnDiffFramesGroupByRollingParityTests(
    OpsOnDiffFramesGroupByRollingTestsMixin,
    PandasOnSparkTestUtils,
    TestUtils,
    ReusedConnectTestCase,
):
    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_groupby_rolling_count(self):
        super().test_groupby_rolling_count()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_groupby_rolling_min(self):
        super().test_groupby_rolling_min()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_groupby_rolling_max(self):
        super().test_groupby_rolling_max()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_groupby_rolling_mean(self):
        super().test_groupby_rolling_mean()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_groupby_rolling_sum(self):
        super().test_groupby_rolling_sum()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_groupby_rolling_std(self):
        super().test_groupby_rolling_std()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_groupby_rolling_var(self):
        super().test_groupby_rolling_var()


if __name__ == "__main__":
    from pyspark.pandas.tests.connect.test_parity_ops_on_diff_frames_groupby_rolling import *

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
