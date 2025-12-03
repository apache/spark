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

import os
import unittest
from typing import cast

from pyspark import SparkConf
from pyspark.testing.sqlutils import (
    have_pyarrow,
    pyarrow_requirement_message,
    ReusedSQLTestCase,
)

from pyspark.sql.tests.pandas.streaming.test_pandas_transform_with_state_state_variable import (
    TransformWithStateStateVariableTestsMixin,
)


@unittest.skipIf(
    not have_pyarrow or os.environ.get("PYTHON_GIL", "?") == "0",
    cast(str, pyarrow_requirement_message or "Not supported in no-GIL mode"),
)
class TransformWithStateInPySparkStateVariableTestsMixin(TransformWithStateStateVariableTestsMixin):
    @classmethod
    def use_pandas(cls) -> bool:
        return False

    @classmethod
    def conf(cls):
        cfg = SparkConf()
        cfg.set("spark.sql.shuffle.partitions", "5")
        cfg.set(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        cfg.set("spark.sql.execution.arrow.transformWithStateInPandas.maxRecordsPerBatch", "2")
        cfg.set("spark.sql.session.timeZone", "UTC")
        # TODO SPARK-49046 this config is to stop query from FEB sink gracefully
        cfg.set("spark.sql.streaming.noDataMicroBatches.enabled", "false")
        return cfg


class TransformWithStateInPySparkStateVariableTests(
    TransformWithStateInPySparkStateVariableTestsMixin, ReusedSQLTestCase
):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.streaming.test_transform_with_state_state_variable import *  # noqa: F401,E501

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
