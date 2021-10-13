# -*- encoding: utf-8 -*-
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

from pyspark.sql.functions import sha2
from pyspark.sql.utils import AnalysisException, ParseException, IllegalArgumentException
from pyspark.testing.sqlutils import ReusedSQLTestCase


class UtilsTests(ReusedSQLTestCase):

    def test_capture_analysis_exception(self):
        self.assertRaises(AnalysisException, lambda: self.spark.sql("select abc"))
        self.assertRaises(AnalysisException, lambda: self.df.selectExpr("a + b"))

    def test_capture_user_friendly_exception(self):
        try:
            self.spark.sql("select `中文字段`")
        except AnalysisException as e:
            self.assertRegex(str(e), "Column '`中文字段`' does not exist")

    def test_capture_parse_exception(self):
        self.assertRaises(ParseException, lambda: self.spark.sql("abc"))

    def test_capture_illegalargument_exception(self):
        self.assertRaisesRegex(IllegalArgumentException, "Setting negative mapred.reduce.tasks",
                               lambda: self.spark.sql("SET mapred.reduce.tasks=-1"))
        df = self.spark.createDataFrame([(1, 2)], ["a", "b"])
        self.assertRaisesRegex(IllegalArgumentException, "1024 is not in the permitted values",
                               lambda: df.select(sha2(df.a, 1024)).collect())
        try:
            df.select(sha2(df.a, 1024)).collect()
        except IllegalArgumentException as e:
            self.assertRegex(e.desc, "1024 is not in the permitted values")
            self.assertRegex(e.stackTrace, "org.apache.spark.sql.functions")

    def test_get_error_class_state(self):
        # SPARK-36953: test CapturedException.getErrorClass and getSqlState (from SparkThrowable)
        try:
            self.spark.sql("""SELECT a""")
        except AnalysisException as e:
            self.assertEquals(e.getErrorClass(), "MISSING_COLUMN")
            self.assertEquals(e.getSqlState(), "42000")


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_utils import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
