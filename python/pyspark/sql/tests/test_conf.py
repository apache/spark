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
from decimal import Decimal

from pyspark.errors import IllegalArgumentException, PySparkTypeError
from pyspark.testing.sqlutils import ReusedSQLTestCase


class ConfTestsMixin:
    def test_conf(self):
        spark = self.spark
        spark.conf.set("bogo", "sipeo")
        self.assertEqual(spark.conf.get("bogo"), "sipeo")
        spark.conf.set("bogo", "ta")
        self.assertEqual(spark.conf.get("bogo"), "ta")
        self.assertEqual(spark.conf.get("bogo", "not.read"), "ta")
        self.assertEqual(spark.conf.get("not.set", "ta"), "ta")
        self.assertRaisesRegex(Exception, "not.set", lambda: spark.conf.get("not.set"))
        spark.conf.unset("bogo")
        self.assertEqual(spark.conf.get("bogo", "colombia"), "colombia")

        self.assertEqual(spark.conf.get("hyukjin", None), None)

        # This returns 'STATIC' because it's the default value of
        # 'spark.sql.sources.partitionOverwriteMode', and `defaultValue` in
        # `spark.conf.get` is unset.
        self.assertEqual(spark.conf.get("spark.sql.sources.partitionOverwriteMode"), "STATIC")

        # This returns None because 'spark.sql.sources.partitionOverwriteMode' is unset, but
        # `defaultValue` in `spark.conf.get` is set to None.
        self.assertEqual(spark.conf.get("spark.sql.sources.partitionOverwriteMode", None), None)

        self.assertTrue(spark.conf.isModifiable("spark.sql.execution.arrow.maxRecordsPerBatch"))
        self.assertFalse(spark.conf.isModifiable("spark.sql.warehouse.dir"))

    def test_conf_with_python_objects(self):
        spark = self.spark

        try:
            for value, expected in [(True, "true"), (False, "false")]:
                spark.conf.set("foo", value)
                self.assertEqual(spark.conf.get("foo"), expected)

            spark.conf.set("foo", 1)
            self.assertEqual(spark.conf.get("foo"), "1")

            with self.assertRaises(IllegalArgumentException):
                spark.conf.set("foo", None)

            with self.assertRaises(Exception):
                spark.conf.set("foo", Decimal(1))

            with self.assertRaises(PySparkTypeError) as pe:
                spark.conf.get(123)

            self.check_error(
                exception=pe.exception,
                errorClass="NOT_STR",
                messageParameters={
                    "arg_name": "key",
                    "arg_type": "int",
                },
            )
        finally:
            spark.conf.unset("foo")

    def test_get_all(self):
        spark = self.spark
        all_confs = spark.conf.getAll

        self.assertTrue(len(all_confs) > 0)
        self.assertNotIn("foo", all_confs)

        try:
            spark.conf.set("foo", "bar")
            updated = spark.conf.getAll

            self.assertEqual(len(updated), len(all_confs) + 1)
            self.assertIn("foo", updated)
        finally:
            spark.conf.unset("foo")


class ConfTests(ConfTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_conf import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
