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
"""Tests for the ``MemoryStream`` Python wrapper.

These tests exercise ``MemoryStream`` directly against the JVM bridge,
without depending on the (forthcoming) ``StreamTest`` driver. They verify
that data added from Python flows through the JVM memory source and lands
in a ``writeStream.format("memory")`` sink.
"""

import os
import shutil
import tempfile
import time
from collections import namedtuple

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.streaming import MemoryStream


class MemoryStreamTests(ReusedSQLTestCase):
    """Self-tests for the ``MemoryStream`` Python wrapper."""

    def setUp(self):
        super().setUp()
        self._checkpoint = tempfile.mkdtemp(prefix="memory_stream_test_")
        self._query_name = f"memory_stream_test_{os.getpid()}_{time.monotonic_ns()}"

    def tearDown(self):
        try:
            for q in self.spark.streams.active:
                if q.name == self._query_name:
                    q.stop()
        finally:
            shutil.rmtree(self._checkpoint, ignore_errors=True)
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {self._query_name}")
            except Exception:
                pass
        super().tearDown()

    def _start_passthrough(self, df):
        return (
            df.writeStream.format("memory")
            .queryName(self._query_name)
            .option("checkpointLocation", self._checkpoint)
            .trigger(processingTime="0 seconds")
            .outputMode("append")
            .start()
        )

    def test_simple_int_schema_round_trip(self):
        source = MemoryStream(self.spark, "int")
        self.assertEqual(source.schema.simpleString(), "struct<value:int>")
        offset = source.add_data(1, 2, 3)
        self.assertEqual(offset, 0)

        query = self._start_passthrough(source.to_df())
        try:
            query.processAllAvailable()
            rows = self.spark.sql(f"SELECT value FROM {self._query_name}").collect()
            self.assertEqual(sorted(r.value for r in rows), [1, 2, 3])
        finally:
            query.stop()
            query.awaitTermination(60)

    def test_subsequent_add_data_advances_offset(self):
        source = MemoryStream(self.spark, "int")
        self.assertEqual(source.add_data(1), 0)
        self.assertEqual(source.add_data(2, 3), 1)
        self.assertEqual(source.add_data(4, 5, 6), 2)
        self.assertEqual(source.current_offset, 2)

    def test_empty_add_data_is_noop(self):
        source = MemoryStream(self.spark, "int")
        self.assertEqual(source.add_data(), -1)
        # First real call still gets offset 0.
        self.assertEqual(source.add_data(1), 0)

    def test_struct_schema(self):
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        source = MemoryStream(self.spark, schema)
        source.add_data(Row(name="Alice", age=30), Row(name="Bob", age=25))

        query = self._start_passthrough(source.to_df())
        try:
            query.processAllAvailable()
            rows = self.spark.sql(
                f"SELECT name, age FROM {self._query_name} ORDER BY name"
            ).collect()
            self.assertEqual([(r.name, r.age) for r in rows], [("Alice", 30), ("Bob", 25)])
        finally:
            query.stop()
            query.awaitTermination(60)

    def test_struct_schema_accepts_tuples_and_dicts(self):
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        source = MemoryStream(self.spark, schema)
        # Tuples positional, dicts by name.
        source.add_data(("Alice", 30), {"name": "Bob", "age": 25})
        query = self._start_passthrough(source.to_df())
        try:
            query.processAllAvailable()
            rows = self.spark.sql(
                f"SELECT name, age FROM {self._query_name} ORDER BY name"
            ).collect()
            self.assertEqual([(r.name, r.age) for r in rows], [("Alice", 30), ("Bob", 25)])
        finally:
            query.stop()
            query.awaitTermination(60)

    def test_unsupported_simple_type_rejected(self):
        with self.assertRaises(ValueError):
            MemoryStream(self.spark, "uuid")

    def test_unsupported_schema_arg_type_rejected(self):
        with self.assertRaises(TypeError):
            MemoryStream(self.spark, schema=123)  # type: ignore[arg-type]

    def test_to_df_is_streaming(self):
        source = MemoryStream(self.spark, "int")
        df = source.to_df()
        self.assertTrue(df.isStreaming)
        self.assertEqual(df.schema.simpleString(), "struct<value:int>")

    def test_namedtuple_resolved_by_field_name_not_position(self):
        """Regression: namedtuples must be matched against the schema by
        field name, not positionally. A namedtuple whose fields are in a
        different order than the schema would otherwise silently produce
        mis-mapped Rows."""
        # PT's field order is the *opposite* of the schema's. Positional
        # mapping would put the int "30" into ``name`` and the string
        # "Alice" into ``age``, producing a type-coercion failure or
        # garbled output. Resolving by name yields the correct Row.
        PT = namedtuple("PT", ["age", "name"])
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        source = MemoryStream(self.spark, schema)
        source.add_data(PT(age=30, name="Alice"))

        query = self._start_passthrough(source.to_df())
        try:
            query.processAllAvailable()
            rows = self.spark.sql(f"SELECT name, age FROM {self._query_name}").collect()
            self.assertEqual([(r.name, r.age) for r in rows], [("Alice", 30)])
        finally:
            query.stop()
            query.awaitTermination(60)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
