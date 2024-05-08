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
from pyspark.testing.connectutils import should_test_connect

if should_test_connect:
    from pyspark import sql
    from pyspark.sql.connect.udtf import UserDefinedTableFunction

    sql.udtf.UserDefinedTableFunction = UserDefinedTableFunction

from pyspark.sql.connect.functions import lit, udtf
from pyspark.sql.tests.test_udtf import BaseUDTFTestsMixin, UDTFArrowTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.errors.exceptions.connect import SparkConnectGrpcException


class UDTFParityTests(BaseUDTFTestsMixin, ReusedConnectTestCase):
    @classmethod
    def setUpClass(cls):
        super(UDTFParityTests, cls).setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDTF.arrow.enabled", "false")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.execution.pythonUDTF.arrow.enabled")
        finally:
            super(UDTFParityTests, cls).tearDownClass()

    # TODO: use PySpark error classes instead of SparkConnectGrpcException

    def test_struct_output_type_casting_row(self):
        self.check_struct_output_type_casting_row(SparkConnectGrpcException)

    def test_udtf_with_invalid_return_type(self):
        @udtf(returnType="int")
        class TestUDTF:
            def eval(self, a: int):
                yield a + 1,

        with self.assertRaisesRegex(
            SparkConnectGrpcException, "Invalid Python user-defined table function return type."
        ):
            TestUDTF(lit(1)).collect()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_udtf_init_with_additional_args(self):
        super(UDTFParityTests, self).test_udtf_init_with_additional_args()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_udtf_with_wrong_num_input(self):
        super(UDTFParityTests, self).test_udtf_with_wrong_num_input()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_array_output_type_casting(self):
        super(UDTFParityTests, self).test_array_output_type_casting()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_map_output_type_casting(self):
        super(UDTFParityTests, self).test_map_output_type_casting()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_numeric_output_type_casting(self):
        super(UDTFParityTests, self).test_numeric_output_type_casting()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_numeric_output_type_casting(self):
        super(UDTFParityTests, self).test_numeric_output_type_casting()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_numeric_string_output_type_casting(self):
        super(UDTFParityTests, self).test_numeric_string_output_type_casting()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_string_output_type_casting(self):
        super(UDTFParityTests, self).test_string_output_type_casting()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_string_output_type_casting(self):
        super(UDTFParityTests, self).test_string_output_type_casting()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_struct_output_type_casting_dict(self):
        super(UDTFParityTests, self).test_struct_output_type_casting_dict()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_udtf_init_with_additional_args(self):
        super(UDTFParityTests, self).test_udtf_init_with_additional_args()

    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_udtf_with_wrong_num_input(self):
        super(UDTFParityTests, self).test_udtf_with_wrong_num_input()


class ArrowUDTFParityTests(UDTFArrowTestsMixin, UDTFParityTests):
    @classmethod
    def setUpClass(cls):
        super(ArrowUDTFParityTests, cls).setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDTF.arrow.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.execution.pythonUDTF.arrow.enabled")
        finally:
            super(ArrowUDTFParityTests, cls).tearDownClass()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_udtf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
