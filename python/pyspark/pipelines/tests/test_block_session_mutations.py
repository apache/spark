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
from pyspark.sql.types import StringType
from pyspark.testing.connectutils import (
    ReusedConnectTestCase,
    should_test_connect,
    connect_requirement_message,
)

if should_test_connect:
    from pyspark.pipelines.block_session_mutations import (
        block_session_mutations,
        BLOCKED_METHODS,
        ERROR_CLASS,
    )


@unittest.skipIf(not should_test_connect, connect_requirement_message or "Connect not available")
class BlockImperativeConfSetConnectTests(ReusedConnectTestCase):
    def test_blocks_runtime_conf_set(self):
        """Test that spark.conf.set() is blocked."""
        config = self.spark.conf

        test_cases = [
            ("spark.test.string", "string_value"),
            ("spark.test.int", 42),
            ("spark.test.bool", True),
        ]

        for key, value in test_cases:
            with self.subTest(key=key, value=value):
                with block_session_mutations():
                    with self.assertRaises(PySparkException) as context:
                        config.set(key, value)

                    self.assertEqual(
                        context.exception.getCondition(),
                        f"{ERROR_CLASS}.SET_RUNTIME_CONF",
                    )
                    self.assertIn("'RuntimeConf.set'", str(context.exception))

    def test_blocks_catalog_set_current_catalog(self):
        """Test that spark.catalog.setCurrentCatalog() is blocked."""
        catalog = self.spark.catalog

        with block_session_mutations():
            with self.assertRaises(PySparkException) as context:
                catalog.setCurrentCatalog("test_catalog")

            self.assertEqual(
                context.exception.getCondition(),
                f"{ERROR_CLASS}.SET_CURRENT_CATALOG",
            )
            self.assertIn("'Catalog.setCurrentCatalog'", str(context.exception))

    def test_blocks_catalog_set_current_database(self):
        """Test that spark.catalog.setCurrentDatabase() is blocked."""
        catalog = self.spark.catalog

        with block_session_mutations():
            with self.assertRaises(PySparkException) as context:
                catalog.setCurrentDatabase("test_db")

            self.assertEqual(
                context.exception.getCondition(),
                f"{ERROR_CLASS}.SET_CURRENT_DATABASE",
            )
            self.assertIn("'Catalog.setCurrentDatabase'", str(context.exception))

    def test_blocks_catalog_drop_temp_view(self):
        """Test that spark.catalog.dropTempView() is blocked."""
        catalog = self.spark.catalog

        with block_session_mutations():
            with self.assertRaises(PySparkException) as context:
                catalog.dropTempView("test_view")

            self.assertEqual(
                context.exception.getCondition(),
                f"{ERROR_CLASS}.DROP_TEMP_VIEW",
            )
            self.assertIn("'Catalog.dropTempView'", str(context.exception))

    def test_blocks_catalog_drop_global_temp_view(self):
        """Test that spark.catalog.dropGlobalTempView() is blocked."""
        catalog = self.spark.catalog

        with block_session_mutations():
            with self.assertRaises(PySparkException) as context:
                catalog.dropGlobalTempView("test_view")

            self.assertEqual(
                context.exception.getCondition(),
                f"{ERROR_CLASS}.DROP_GLOBAL_TEMP_VIEW",
            )
            self.assertIn("'Catalog.dropGlobalTempView'", str(context.exception))

    def test_blocks_dataframe_create_temp_view(self):
        """Test that DataFrame.createTempView() is blocked."""
        df = self.spark.range(1)

        with block_session_mutations():
            with self.assertRaises(PySparkException) as context:
                df.createTempView("test_view")

            self.assertEqual(
                context.exception.getCondition(),
                f"{ERROR_CLASS}.CREATE_TEMP_VIEW",
            )
            self.assertIn("'DataFrame.createTempView'", str(context.exception))

    def test_blocks_dataframe_create_or_replace_temp_view(self):
        """Test that DataFrame.createOrReplaceTempView() is blocked."""
        df = self.spark.range(1)

        with block_session_mutations():
            with self.assertRaises(PySparkException) as context:
                df.createOrReplaceTempView("test_view")

            self.assertEqual(
                context.exception.getCondition(),
                f"{ERROR_CLASS}.CREATE_OR_REPLACE_TEMP_VIEW",
            )
            self.assertIn("'DataFrame.createOrReplaceTempView'", str(context.exception))

    def test_blocks_dataframe_create_global_temp_view(self):
        """Test that DataFrame.createGlobalTempView() is blocked."""
        df = self.spark.range(1)

        with block_session_mutations():
            with self.assertRaises(PySparkException) as context:
                df.createGlobalTempView("test_view")

            self.assertEqual(
                context.exception.getCondition(),
                f"{ERROR_CLASS}.CREATE_GLOBAL_TEMP_VIEW",
            )
            self.assertIn("'DataFrame.createGlobalTempView'", str(context.exception))

    def test_blocks_dataframe_create_or_replace_global_temp_view(self):
        """Test that DataFrame.createOrReplaceGlobalTempView() is blocked."""
        df = self.spark.range(1)

        with block_session_mutations():
            with self.assertRaises(PySparkException) as context:
                df.createOrReplaceGlobalTempView("test_view")

            self.assertEqual(
                context.exception.getCondition(),
                f"{ERROR_CLASS}.CREATE_OR_REPLACE_GLOBAL_TEMP_VIEW",
            )
            self.assertIn("'DataFrame.createOrReplaceGlobalTempView'", str(context.exception))

    def test_blocks_udf_register(self):
        """Test that spark.udf.register() is blocked."""
        udf_registry = self.spark.udf

        def test_func(x):
            return x + 1

        with block_session_mutations():
            with self.assertRaises(PySparkException) as context:
                udf_registry.register("test_udf", test_func, StringType())

            self.assertEqual(
                context.exception.getCondition(),
                f"{ERROR_CLASS}.REGISTER_UDF",
            )
            self.assertIn("'UDFRegistration.register'", str(context.exception))

    def test_blocks_udf_register_java_function(self):
        """Test that spark.udf.registerJavaFunction() is blocked."""
        udf_registry = self.spark.udf

        with block_session_mutations():
            with self.assertRaises(PySparkException) as context:
                udf_registry.registerJavaFunction(
                    "test_java_udf", "com.example.TestUDF", StringType()
                )

            self.assertEqual(
                context.exception.getCondition(),
                f"{ERROR_CLASS}.REGISTER_JAVA_UDF",
            )
            self.assertIn("'UDFRegistration.registerJavaFunction'", str(context.exception))

    def test_blocks_udf_register_java_udaf(self):
        """Test that spark.udf.registerJavaUDAF() is blocked."""
        udf_registry = self.spark.udf

        with block_session_mutations():
            with self.assertRaises(PySparkException) as context:
                udf_registry.registerJavaUDAF("test_java_udaf", "com.example.TestUDAF")

            self.assertEqual(
                context.exception.getCondition(),
                f"{ERROR_CLASS}.REGISTER_JAVA_UDAF",
            )
            self.assertIn("'UDFRegistration.registerJavaUDAF'", str(context.exception))

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
        with block_session_mutations():
            for method_info in BLOCKED_METHODS:
                cls = method_info["class"]
                method_name = method_info["method"]
                with self.subTest(class_method=f"{cls.__name__}.{method_name}"):
                    self.assertIsNot(
                        getattr(cls, method_name), original_methods[(cls, method_name)]
                    )

        # Verify methods are restored after context
        for method_info in BLOCKED_METHODS:
            cls = method_info["class"]
            method_name = method_info["method"]
            with self.subTest(class_method=f"{cls.__name__}.{method_name}"):
                self.assertIs(getattr(cls, method_name), original_methods[(cls, method_name)])


if __name__ == "__main__":
    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
