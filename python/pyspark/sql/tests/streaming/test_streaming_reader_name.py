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

import tempfile
import time

from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.testing.sqlutils import ReusedSQLTestCase


class DataStreamReaderNameTests(ReusedSQLTestCase):
    """Test suite for DataStreamReader.name() functionality in PySpark."""

    @classmethod
    def setUpClass(cls):
        super(DataStreamReaderNameTests, cls).setUpClass()
        # Enable streaming source evolution feature
        cls.spark.conf.set("spark.sql.streaming.queryEvolution.enableSourceEvolution", "true")
        cls.spark.conf.set("spark.sql.streaming.offsetLog.formatVersion", "2")

    def test_name_with_valid_names(self):
        """Test that various valid source name patterns work correctly."""
        valid_names = [
            "mySource",
            "my_source",
            "MySource123",
            "_private",
            "source_123_test",
            "123source",
        ]

        for name in valid_names:
            with tempfile.TemporaryDirectory(prefix=f"test_{name}_") as tmpdir:
                self.spark.range(10).write.mode("overwrite").parquet(tmpdir)
                df = (
                    self.spark.readStream.format("parquet")
                    .schema("id LONG")
                    .name(name)
                    .load(tmpdir)
                )
                self.assertTrue(df.isStreaming, f"DataFrame should be streaming for name: {name}")

    def test_name_method_chaining(self):
        """Test that name() returns the reader for method chaining."""
        with tempfile.TemporaryDirectory(prefix="test_chaining_") as tmpdir:
            self.spark.range(10).write.mode("overwrite").parquet(tmpdir)
            df = (
                self.spark.readStream.format("parquet")
                .schema("id LONG")
                .name("my_source")
                .option("maxFilesPerTrigger", "1")
                .load(tmpdir)
            )

            self.assertTrue(df.isStreaming, "DataFrame should be streaming")

    def test_name_before_format(self):
        """Test that order doesn't matter - name can be set before format."""
        with tempfile.TemporaryDirectory(prefix="test_before_format_") as tmpdir:
            self.spark.range(10).write.mode("overwrite").parquet(tmpdir)
            df = (
                self.spark.readStream.name("my_source")
                .format("parquet")
                .schema("id LONG")
                .load(tmpdir)
            )

            self.assertTrue(df.isStreaming, "DataFrame should be streaming")

    def test_invalid_names(self):
        """Test that various invalid source names are rejected."""
        invalid_names = [
            "",  # empty string
            "  ",  # whitespace only
            "my-source",  # hyphen
            "my source",  # space
            "my.source",  # dot
            "my@source",  # special char
            "my$source",  # dollar sign
            "my#source",  # hash
            "my!source",  # exclamation
        ]

        for invalid_name in invalid_names:
            with self.subTest(name=invalid_name):
                with tempfile.TemporaryDirectory(prefix="test_invalid_") as tmpdir:
                    self.spark.range(10).write.mode("overwrite").parquet(tmpdir)
                    with self.assertRaises(PySparkValueError) as context:
                        self.spark.readStream.format("parquet").schema("id LONG").name(
                            invalid_name
                        ).load(tmpdir)

                    # The error message should contain information about invalid name
                    self.assertIn("source", str(context.exception).lower())

    def test_invalid_name_wrong_type(self):
        """Test that None and non-string types are rejected."""
        invalid_types = [None, 123, 45.67, [], {}]

        for invalid_value in invalid_types:
            with self.subTest(value=invalid_value):
                with self.assertRaises(PySparkTypeError):
                    self.spark.readStream.format("rate").name(invalid_value).load()

    def test_name_with_different_formats(self):
        """Test that name() works with different streaming data sources."""
        with tempfile.TemporaryDirectory(prefix="test_name_formats_") as tmpdir:
            # Create test data
            self.spark.range(10).write.mode("overwrite").parquet(tmpdir + "/parquet_data")
            self.spark.range(10).selectExpr("id", "CAST(id AS STRING) as value").write.mode(
                "overwrite"
            ).json(tmpdir + "/json_data")

            # Test with parquet
            parquet_df = (
                self.spark.readStream.format("parquet")
                .name("parquet_source")
                .schema("id LONG")
                .load(tmpdir + "/parquet_data")
            )
            self.assertTrue(parquet_df.isStreaming, "Parquet DataFrame should be streaming")

            # Test with json - specify schema
            json_df = (
                self.spark.readStream.format("json")
                .name("json_source")
                .schema("id LONG, value STRING")
                .load(tmpdir + "/json_data")
            )
            self.assertTrue(json_df.isStreaming, "JSON DataFrame should be streaming")

    def test_name_persists_through_query(self):
        """Test that the name persists when starting a streaming query."""
        with tempfile.TemporaryDirectory(prefix="test_name_query_") as tmpdir:
            data_dir = tmpdir + "/data"
            checkpoint_dir = tmpdir + "/checkpoint"

            # Create test data
            self.spark.range(10).write.mode("overwrite").parquet(data_dir)

            df = (
                self.spark.readStream.format("parquet")
                .schema("id LONG")
                .name("parquet_source_test")
                .load(data_dir)
            )

            query = (
                df.writeStream.format("noop").option("checkpointLocation", checkpoint_dir).start()
            )

            try:
                # Let it run briefly
                time.sleep(1)

                # Verify query is running
                self.assertTrue(query.isActive, "Query should be active")
            finally:
                query.stop()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
