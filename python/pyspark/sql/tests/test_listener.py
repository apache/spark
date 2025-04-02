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

from pyspark.sql import SparkSession
from pyspark.testing.sqlutils import (
    SQLTestUtils,
    have_pyarrow,
    have_pandas,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


class QueryExecutionListenerTests(
    unittest.TestCase,
    SQLTestUtils,
):
    # These tests are separate because it uses 'spark.sql.queryExecutionListeners' which is
    # static and immutable. This can't be set or unset, for example, via `spark.conf`.

    @classmethod
    def setUpClass(cls):
        import glob
        from pyspark.find_spark_home import _find_spark_home

        SPARK_HOME = _find_spark_home()
        filename_pattern = (
            "sql/core/target/scala-*/test-classes/org/apache/spark/sql/"
            "TestQueryExecutionListener.class"
        )
        cls.has_listener = bool(glob.glob(os.path.join(SPARK_HOME, filename_pattern)))

        if cls.has_listener:
            # Note that 'spark.sql.queryExecutionListeners' is a static immutable configuration.
            cls.spark = (
                SparkSession.builder.master("local[4]")
                .appName(cls.__name__)
                .config(
                    "spark.sql.queryExecutionListeners",
                    "org.apache.spark.sql.TestQueryExecutionListener",
                )
                .getOrCreate()
            )

    def setUp(self):
        if not self.has_listener:
            raise self.skipTest(
                "'org.apache.spark.sql.TestQueryExecutionListener' is not "
                "available. Will skip the related tests."
            )

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def tearDown(self):
        self.spark._jvm.OnSuccessCall.clear()

    def test_query_execution_listener_on_collect(self):
        self.assertFalse(
            self.spark._jvm.OnSuccessCall.isCalled(),
            "The callback from the query execution listener should not be called before 'collect'",
        )
        self.spark.sql("SELECT * FROM range(1)").collect()
        self.spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(10000)
        self.assertTrue(
            self.spark._jvm.OnSuccessCall.isCalled(),
            "The callback from the query execution listener should be called after 'collect'",
        )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_query_execution_listener_on_collect_with_arrow(self):
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": True}):
            self.assertFalse(
                self.spark._jvm.OnSuccessCall.isCalled(),
                "The callback from the query execution listener should not be "
                "called before 'toPandas'",
            )
            self.spark.sql("SELECT * FROM range(1)").toPandas()
            self.spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(10000)
            self.assertTrue(
                self.spark._jvm.OnSuccessCall.isCalled(),
                "The callback from the query execution listener should be called after 'toPandas'",
            )


if __name__ == "__main__":
    from pyspark.sql.tests.test_listener import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
