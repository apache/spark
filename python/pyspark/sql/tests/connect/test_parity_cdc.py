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

from pyspark import SparkConf
from pyspark.sql import SparkSession as PySparkSession
from pyspark.sql.tests.test_cdc import CDCTestsMixin, _find_catalyst_test_jar
from pyspark.testing.connectutils import should_test_connect, ReusedConnectTestCase

_catalyst_test_jar = _find_catalyst_test_jar()


@unittest.skipIf(
    not should_test_connect or _catalyst_test_jar is None,
    "Spark Connect test or catalyst test JAR not available",
)
class CDCParityTests(CDCTestsMixin, ReusedConnectTestCase):

    @classmethod
    def conf(cls):
        conf = super().conf()
        conf.set("spark.driver.extraClassPath", _catalyst_test_jar)
        conf.set(
            f"spark.sql.catalog.{cls.catalog_name}",
            "org.apache.spark.sql.connector.catalog.InMemoryChangelogCatalog",
        )
        return conf

    def _jvm(self):
        return PySparkSession._instantiatedSession._jvm

    def _j_spark_session(self):
        return PySparkSession._instantiatedSession._jsparkSession

    def _gateway(self):
        return PySparkSession._instantiatedSession.sparkContext._gateway


if __name__ == "__main__":
    from pyspark.testing import main

    main()
