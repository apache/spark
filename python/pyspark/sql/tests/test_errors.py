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

from pyspark.errors import (
    ArithmeticException,
    ArrayIndexOutOfBoundsException,
    DateTimeException,
    NumberFormatException,
    SparkRuntimeException,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase


class ErrorsTestsMixin:
    def test_arithmetic_exception(self):
        with self.assertRaises(ArithmeticException):
            with self.sql_conf({"spark.sql.ansi.enabled": True}):
                self.spark.sql("select 1/0").show()

    def test_array_index_out_of_bounds_exception(self):
        with self.assertRaises(ArrayIndexOutOfBoundsException):
            with self.sql_conf({"spark.sql.ansi.enabled": True}):
                self.spark.sql("select array(1, 2)[2]").show()

    def test_date_time_exception(self):
        with self.assertRaises(DateTimeException):
            with self.sql_conf({"spark.sql.ansi.enabled": True}):
                self.spark.sql("select unix_timestamp('2023-01-01', 'dd-MM-yyyy')").show()

    def test_number_format_exception(self):
        with self.assertRaises(NumberFormatException):
            with self.sql_conf({"spark.sql.ansi.enabled": True}):
                self.spark.sql("select cast('abc' as double)").show()

    def test_spark_runtime_exception(self):
        with self.assertRaises(SparkRuntimeException):
            with self.sql_conf({"spark.sql.ansi.enabled": True}):
                self.spark.sql("select cast('abc' as boolean)").show()


class ErrorsTests(ReusedSQLTestCase, ErrorsTestsMixin):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_errors import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
