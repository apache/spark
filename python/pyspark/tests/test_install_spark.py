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
import tempfile
import unittest

from pyspark.install import install_spark, DEFAULT_HADOOP, DEFAULT_HIVE, \
    UNSUPPORTED_COMBINATIONS, checked_versions, checked_package_name


class SparkInstallationTestCase(unittest.TestCase):

    def test_install_spark(self):
        # Test only one case. Testing this is expensive because it needs to download
        # the Spark distribution.
        spark_version, hadoop_version, hive_version = checked_versions("3.0.1", "3.2", "2.3")

        with tempfile.TemporaryDirectory() as tmp_dir:
            install_spark(
                dest=tmp_dir,
                spark_version=spark_version,
                hadoop_version=hadoop_version,
                hive_version=hive_version)

            self.assertTrue(os.path.isdir("%s/jars" % tmp_dir))
            self.assertTrue(os.path.exists("%s/bin/spark-submit" % tmp_dir))
            self.assertTrue(os.path.exists("%s/RELEASE" % tmp_dir))

    def test_package_name(self):
        self.assertEqual(
            "spark-3.0.0-bin-hadoop3.2",
            checked_package_name("spark-3.0.0", "hadoop3.2", "hive2.3"))

    def test_checked_versions(self):
        test_version = "3.0.1"  # Just pick one version to test.

        # Positive test cases
        self.assertEqual(
            ("spark-3.0.0", "hadoop2.7", "hive2.3"),
            checked_versions("spark-3.0.0", "hadoop2.7", "hive2.3"))

        self.assertEqual(
            ("spark-3.0.0", "hadoop2.7", "hive2.3"),
            checked_versions("3.0.0", "2.7", "2.3"))

        self.assertEqual(
            ("spark-2.4.1", "without-hadoop", "hive2.3"),
            checked_versions("2.4.1", "without", "2.3"))

        self.assertEqual(
            ("spark-3.0.1", "without-hadoop", "hive2.3"),
            checked_versions("spark-3.0.1", "without-hadoop", "hive2.3"))

        # Negative test cases
        for (hadoop_version, hive_version) in UNSUPPORTED_COMBINATIONS:
            with self.assertRaisesRegex(RuntimeError, 'Hive.*should.*Hadoop'):
                checked_versions(
                    spark_version=test_version,
                    hadoop_version=hadoop_version,
                    hive_version=hive_version)

        with self.assertRaisesRegex(RuntimeError, "Spark version should start with 'spark-'"):
            checked_versions(
                spark_version="malformed",
                hadoop_version=DEFAULT_HADOOP,
                hive_version=DEFAULT_HIVE)

        with self.assertRaisesRegex(RuntimeError, "Spark distribution.*malformed.*"):
            checked_versions(
                spark_version=test_version,
                hadoop_version="malformed",
                hive_version=DEFAULT_HIVE)

        with self.assertRaisesRegex(RuntimeError, "Spark distribution.*malformed.*"):
            checked_versions(
                spark_version=test_version,
                hadoop_version=DEFAULT_HADOOP,
                hive_version="malformed")

        with self.assertRaisesRegex(RuntimeError, "Spark distribution of hive1.2 is not supported"):
            checked_versions(
                spark_version=test_version,
                hadoop_version="hadoop3.2",
                hive_version="hive1.2")


if __name__ == "__main__":
    from pyspark.tests.test_install_spark import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
