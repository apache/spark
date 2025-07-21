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

from pyspark.errors import PySparkException
from pyspark.sql.connect.conf import RuntimeConf
from pyspark.sql.connect.catalog import Catalog
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.udf import UDFRegistration
from pyspark.sql.types import StringType
from pyspark.testing.connectutils import (
    ReusedConnectTestCase,
    should_test_connect,
    connect_requirement_message,
)

from pyspark.pipelines.block_imperative_construct import block_imperative_construct, BLOCKED_METHODS


@unittest.skipIf(not should_test_connect, connect_requirement_message or "Connect not available")
class BlockImperativeConfSetConnectTests(ReusedConnectTestCase):
    
    def _get_test_cases_for_method(self, method_info):
        """Get appropriate test cases for different method types."""
        method_name = method_info["method"]
        
        if method_name == "set":
            return [
                ("spark.test.string", "string_value"),
                ("spark.test.int", 42),
                ("spark.test.bool", True),
                ("spark.test.float", 3.14),
            ]
        elif "catalog" in method_name.lower():
            return ["test_catalog", "spark_catalog", "hive_metastore"]
        elif "database" in method_name.lower():
            return ["test_db", "default", "my_database"]
        elif "global" in method_name.lower():
            return ["global_test_view", "global_temp_view", "my_global_view"]
        elif method_name in ["register", "registerJavaFunction", "registerJavaUDAF"]:
            # UDF registration methods - return test function/class names
            return [
                ("test_udf", lambda x: x + 1),
                ("another_udf", lambda x: x * 2),
            ]
        else:  # temp view methods
            return ["test_view", "temp_view", "my_view"]

    def _get_target_object(self, method_info):
        """Get the appropriate object to call the method on."""
        cls = method_info["class"]
        if cls == RuntimeConf:
            return self.spark.conf
        elif cls == Catalog:
            return self.spark.catalog
        elif cls == DataFrame:
            return self.spark.range(1)
        elif cls == UDFRegistration:
            return self.spark.udf
        
    def test_blocks_all_methods(self):
        """Test that all configured methods are properly blocked."""
        for method_info in BLOCKED_METHODS:
            method_name = method_info["method"]
            test_cases = self._get_test_cases_for_method(method_info)
            target_obj = self._get_target_object(method_info)
            
            with self.subTest(method=method_name):
                for test_case in test_cases:
                    with self.subTest(test_case=test_case):
                        with block_imperative_construct():
                            with self.assertRaises(PySparkException) as context:
                                if method_name == "set":
                                    # set method takes key, value
                                    key, value = test_case
                                    getattr(target_obj, method_name)(key, value)
                                elif method_name in ["register", "registerJavaFunction", "registerJavaUDAF"]:
                                    # UDF registration methods
                                    udf_name, udf_func = test_case
                                    if method_name == "register":
                                        getattr(target_obj, method_name)(udf_name, udf_func, StringType())
                                    elif method_name == "registerJavaFunction":
                                        getattr(target_obj, method_name)(udf_name, "com.example.TestUDF", StringType())
                                    elif method_name == "registerJavaUDAF":
                                        getattr(target_obj, method_name)(udf_name, "com.example.TestUDAF")
                                else:
                                    # other methods take a single parameter
                                    getattr(target_obj, method_name)(test_case)

                            self.assertEqual(
                                context.exception.getCondition(),
                                "IMPERATIVE_CONSTRUCT_IN_DECLARATIVE_PIPELINE",
                            )

    def test_restores_original_methods_after_context(self):
        """Test that all methods are properly restored after context manager exits."""
        # Store original methods
        original_methods = {}
        for method_info in BLOCKED_METHODS:
            cls = method_info["class"]
            method_name = method_info["method"]
            original_methods[(cls, method_name)] = getattr(cls, method_name)

        # Verify methods are originally set correctly
        for method_info in BLOCKED_METHODS:
            cls = method_info["class"]
            method_name = method_info["method"]
            with self.subTest(class_method=f"{cls.__name__}.{method_name}"):
                self.assertIs(getattr(cls, method_name), original_methods[(cls, method_name)])

        # Verify methods are replaced during context
        with block_imperative_construct():
            for method_info in BLOCKED_METHODS:
                cls = method_info["class"]
                method_name = method_info["method"]
                with self.subTest(class_method=f"{cls.__name__}.{method_name}"):
                    self.assertIsNot(getattr(cls, method_name), original_methods[(cls, method_name)])

        # Verify methods are restored after context
        for method_info in BLOCKED_METHODS:
            cls = method_info["class"]
            method_name = method_info["method"]
            with self.subTest(class_method=f"{cls.__name__}.{method_name}"):
                self.assertIs(getattr(cls, method_name), original_methods[(cls, method_name)])

    def test_restores_methods_even_with_exception(self):
        """Test that methods are properly restored even when exceptions occur."""
        # Store original methods
        original_methods = {}
        for method_info in BLOCKED_METHODS:
            cls = method_info["class"]
            method_name = method_info["method"]
            original_methods[(cls, method_name)] = getattr(cls, method_name)

        # Test with various exception scenarios
        exception_test_cases = [
            # Config exception
            lambda: self.spark.conf.set("spark.test.key", "test_value"),
            # Catalog exceptions
            lambda: self.spark.catalog.setCurrentCatalog("test_catalog"),
            lambda: self.spark.catalog.dropTempView("test_view"),
            # DataFrame exceptions
            lambda: self.spark.range(1).createTempView("test_view"),
            lambda: self.spark.range(1).createOrReplaceGlobalTempView("global_view"),
            # UDF registration exceptions
            lambda: self.spark.udf.register("test_udf", lambda x: x + 1, StringType()),
            lambda: self.spark.udf.registerJavaFunction("java_udf", "com.example.TestUDF", StringType()),
        ]

        for i, exception_case in enumerate(exception_test_cases):
            with self.subTest(exception_case_index=i):
                try:
                    with block_imperative_construct():
                        exception_case()
                except PySparkException:
                    pass

                # Verify all methods are restored after exception
                for method_info in BLOCKED_METHODS:
                    cls = method_info["class"]
                    method_name = method_info["method"]
                    self.assertIs(getattr(cls, method_name), original_methods[(cls, method_name)])



if __name__ == "__main__":
    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
