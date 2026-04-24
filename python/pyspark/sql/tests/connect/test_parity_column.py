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

from pyspark.sql.tests.test_column import ColumnTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class ColumnParityTests(ColumnTestsMixin, ReusedConnectTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.analyzer.strictDataFrameColumnResolution", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.analyzer.strictDataFrameColumnResolution")
        finally:
            super().tearDownClass()

    @unittest.skip("Requires JVM access.")
    def test_validate_column_types(self):
        super().test_validate_column_types()

    def test_df_col_resolution_mode(self):
        self.assertEqual(
            self.spark.conf.get("spark.sql.analyzer.strictDataFrameColumnResolution"),
            "true",
        )


class ColumnParityTestsWithNonStrictDFColResolution(ColumnParityTests):
    """Re-run the Column parity tests with
    `spark.sql.analyzer.strictDataFrameColumnResolution=false` to exercise the
    name-based fallback path for tagged UnresolvedAttributes."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.analyzer.strictDataFrameColumnResolution", "false")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.analyzer.strictDataFrameColumnResolution")
        finally:
            super().tearDownClass()

    def test_df_col_resolution_mode(self):
        self.assertEqual(
            self.spark.conf.get("spark.sql.analyzer.strictDataFrameColumnResolution"),
            "false",
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
