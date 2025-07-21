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
from pyspark.testing.connectutils import (
    ReusedConnectTestCase,
    should_test_connect,
    connect_requirement_message,
)

from pyspark.pipelines.block_imperative_construct import block_imperative_construct


@unittest.skipIf(not should_test_connect, connect_requirement_message or "Connect not available")
class BlockImperativeConfSetConnectTests(ReusedConnectTestCase):
    def test_blocks_runtime_config_set(self):
        config = self.spark.conf

        test_cases = [
            ("spark.test.string", "string_value"),
            ("spark.test.int", 42),
            ("spark.test.bool", True),
            ("spark.test.float", 3.14),
        ]

        for key, value in test_cases:
            with self.subTest(key=key, value=value):
                with block_imperative_construct():
                    with self.assertRaises(PySparkException) as context:
                        config.set(key, value)

                    self.assertEqual(
                        context.exception.getCondition(),
                        "IMPERATIVE_CONF_SET_IN_DECLARATIVE_PIPELINE",
                    )

    def test_blocks_catalog_set_current_catalog(self):
        catalog = self.spark.catalog

        test_cases = [
            "test_catalog",
            "spark_catalog",
            "hive_metastore",
        ]

        for catalog_name in test_cases:
            with self.subTest(catalog_name=catalog_name):
                with block_imperative_construct():
                    with self.assertRaises(PySparkException) as context:
                        catalog.setCurrentCatalog(catalog_name)

                    self.assertEqual(
                        context.exception.getCondition(),
                        "IMPERATIVE_CONF_SET_IN_DECLARATIVE_PIPELINE",
                    )
                    self.assertIn("'spark.catalog.setCurrentCatalog'", str(context.exception))

    def test_blocks_catalog_set_current_database(self):
        catalog = self.spark.catalog

        test_cases = [
            "test_db",
            "default",
            "my_database",
        ]

        for db_name in test_cases:
            with self.subTest(db_name=db_name):
                with block_imperative_construct():
                    with self.assertRaises(PySparkException) as context:
                        catalog.setCurrentDatabase(db_name)

                    self.assertEqual(
                        context.exception.getCondition(),
                        "IMPERATIVE_CONF_SET_IN_DECLARATIVE_PIPELINE",
                    )
                    self.assertIn("'spark.catalog.setCurrentDatabase'", str(context.exception))

    def test_restores_original_methods_after_context(self):
        original_set = RuntimeConf.set
        original_set_current_catalog = Catalog.setCurrentCatalog
        original_set_current_database = Catalog.setCurrentDatabase

        self.assertIs(RuntimeConf.set, original_set)
        self.assertIs(Catalog.setCurrentCatalog, original_set_current_catalog)
        self.assertIs(Catalog.setCurrentDatabase, original_set_current_database)

        with block_imperative_construct():
            self.assertIsNot(RuntimeConf.set, original_set)
            self.assertIsNot(Catalog.setCurrentCatalog, original_set_current_catalog)
            self.assertIsNot(Catalog.setCurrentDatabase, original_set_current_database)

        self.assertIs(RuntimeConf.set, original_set)
        self.assertIs(Catalog.setCurrentCatalog, original_set_current_catalog)
        self.assertIs(Catalog.setCurrentDatabase, original_set_current_database)

    def test_restores_methods_even_with_exception(self):
        original_set = RuntimeConf.set
        original_set_current_catalog = Catalog.setCurrentCatalog
        original_set_current_database = Catalog.setCurrentDatabase

        try:
            with block_imperative_construct():
                self.spark.conf.set("spark.test.key", "test_value")
        except PySparkException:
            pass

        self.assertIs(RuntimeConf.set, original_set)
        self.assertIs(Catalog.setCurrentCatalog, original_set_current_catalog)
        self.assertIs(Catalog.setCurrentDatabase, original_set_current_database)

        try:
            with block_imperative_construct():
                self.spark.catalog.setCurrentCatalog("test_catalog")
        except PySparkException:
            pass

        self.assertIs(RuntimeConf.set, original_set)
        self.assertIs(Catalog.setCurrentCatalog, original_set_current_catalog)
        self.assertIs(Catalog.setCurrentDatabase, original_set_current_database)

        try:
            with block_imperative_construct():
                self.spark.catalog.setCurrentDatabase("test_db")
        except PySparkException:
            pass

        self.assertIs(RuntimeConf.set, original_set)
        self.assertIs(Catalog.setCurrentCatalog, original_set_current_catalog)
        self.assertIs(Catalog.setCurrentDatabase, original_set_current_database)



if __name__ == "__main__":
    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
