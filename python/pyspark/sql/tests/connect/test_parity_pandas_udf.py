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

from pyspark.sql.tests.pandas.test_pandas_udf import PandasUDFTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.errors.exceptions import SparkConnectGrpcException
from pyspark.sql.connect.functions import udf
from pyspark.sql.functions import pandas_udf, PandasUDFType


class PandasUDFParityTests(PandasUDFTestsMixin, ReusedConnectTestCase):
    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_udf_wrong_arg(self):
        super().test_udf_wrong_arg()

    @unittest.skip("Spark Connect does not support spark.conf but the test depends on it.")
    def test_pandas_udf_timestamp_ntz(self):
        super().test_pandas_udf_timestamp_ntz()

    @unittest.skip("Spark Connect does not support spark.conf but the test depends on it.")
    def test_pandas_udf_detect_unsafe_type_conversion(self):
        super().test_pandas_udf_detect_unsafe_type_conversion()

    @unittest.skip("Spark Connect does not support spark.conf but the test depends on it.")
    def test_pandas_udf_arrow_overflow(self):
        super().test_pandas_udf_arrow_overflow()

    # TODO(SPARK-42247): standardize `returnType` attribute of UDF
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_pandas_udf_decorator(self):
        super().test_pandas_udf_decorator()

    # TODO(SPARK-42247): standardize `returnType` attribute of UDF
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_pandas_udf_basic(self):
        super().test_pandas_udf_basic()

    def test_stopiteration_in_udf(self):
        # The vanilla PySpark throws PythonException instead.
        def foo(x):
            raise StopIteration()

        exc_message = "Caught StopIteration thrown from user's code; failing the task"
        df = self.spark.range(0, 100)

        self.assertRaisesRegex(
            SparkConnectGrpcException, exc_message, df.withColumn("v", udf(foo)("id")).collect
        )

        # pandas scalar udf
        self.assertRaisesRegex(
            SparkConnectGrpcException,
            exc_message,
            df.withColumn("v", pandas_udf(foo, "double", PandasUDFType.SCALAR)("id")).collect,
        )


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_pandas_udf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
