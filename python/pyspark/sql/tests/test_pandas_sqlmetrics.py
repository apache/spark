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

from pyspark.sql.functions import pandas_udf
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pandas, have_pyarrow, \
    pandas_requirement_message, pyarrow_requirement_message


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)  # type: ignore[arg-type]
class PandasSQLMetrics(ReusedSQLTestCase):

    def test_pandas_sql_metrics_basic(self):

        PythonSQLMetrics = [
            "time spent executing",
            "time spent sending data",
            "time spent sending code",
            "bytes of code sent",
            "bytes of data returned",
            "bytes of data sent",
            "number of batches returned",
            "number of batches processed",
            "number of rows returned",
            "number of rows processed"
        ]

        @pandas_udf("long")
        def test_pandas(col1):
            return col1 * col1

        res = self.spark.range(10).select(test_pandas("id")).collect()

        statusStore = self.spark._jsparkSession.sharedState().statusStore()
        executionMetrics = statusStore.execution(0).get().metrics().mkString()

        for metric in PythonSQLMetrics:
            self.assertIn(metric, executionMetrics)


if __name__ == "__main__":
    from pyspark.sql.tests.test_pandas_sqlmetrics import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
