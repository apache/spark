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

from pyspark.sql.tests.test_cdc import CDCTestsMixin, _find_catalyst_test_jar
from pyspark.testing.connectutils import should_test_connect, ReusedMixedTestCase

_catalyst_test_jar = _find_catalyst_test_jar()


@unittest.skipIf(
    not should_test_connect or _catalyst_test_jar is None,
    "Spark Connect test or catalyst test JAR not available",
)
class CDCParityTests(CDCTestsMixin, ReusedMixedTestCase):
    @classmethod
    def conf(cls):
        conf = super().conf()
        conf.set("spark.driver.extraClassPath", _catalyst_test_jar)
        conf.set(
            f"spark.sql.catalog.{cls.catalog_name}",
            "org.apache.spark.sql.connector.catalog.InMemoryChangelogCatalog",
        )
        return conf

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._classic_spark = cls.spark

    def _jvm(self):
        return self._classic_spark._jvm

    def _j_spark_session(self):
        return self._classic_spark._jsparkSession

    def _gateway(self):
        return self._classic_spark.sparkContext._gateway

    def setUp(self):
        # The Connect server creates an isolated session with its own
        # CatalogManager, so tables must exist on the server's catalog.
        # We create them via SQL through the Connect session, then swap
        # self.spark to Connect so test methods exercise the Connect path.
        # Change data is shared across all catalog instances via the companion
        # object in InMemoryChangelogCatalog, so _add_change_row() works from
        # any catalog instance (including the classic session's).
        ReusedMixedTestCase.setUp(self)

        self.connect.sql(f"DROP TABLE IF EXISTS {self.full_table_name}").collect()
        self.connect.sql(f"CREATE TABLE {self.full_table_name} (id BIGINT, data STRING)").collect()

        catalog = self._catalog()
        ident = self._ident()
        catalog.clearChangeRows(ident)

        self.spark = self.connect

    def tearDown(self):
        self.spark = self._classic_spark
        super().tearDown()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
