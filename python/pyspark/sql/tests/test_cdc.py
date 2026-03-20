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

import datetime
import glob
import os
import unittest

from pyspark import SparkConf, SparkContext
from pyspark.errors import AnalysisException
from pyspark.find_spark_home import _find_spark_home
from pyspark.sql import Row, SparkSession
from pyspark.testing.utils import PySparkErrorTestUtils
from pyspark.testing.sqlutils import SQLTestUtils

SPARK_HOME = _find_spark_home()


def _find_catalyst_test_jar():
    """Find the catalyst test JAR needed for InMemoryChangelogCatalog."""
    pattern = os.path.join(
        SPARK_HOME, "sql/catalyst/target/scala-*/spark-catalyst_*-tests.jar"
    )
    jars = glob.glob(pattern)
    return jars[0] if jars else None


class CDCTestsMixin:
    """
    End-to-end tests for PySpark CDC changes() API, mirroring
    ChangelogEndToEndSuite.scala.
    """

    catalog_name = "cdc_py_e2e"
    test_table = "test_table"
    full_table_name = f"{catalog_name}.{test_table}"

    def _jvm(self):
        """Access the JVM gateway. Override in Connect tests."""
        return self.spark._jvm

    def _j_spark_session(self):
        """Access the JVM SparkSession. Override in Connect tests."""
        return self.spark._jsparkSession

    def _gateway(self):
        return self.spark.sparkContext._gateway

    def _catalog(self):
        return (
            self._j_spark_session()
            .sessionState()
            .catalogManager()
            .catalog(self.catalog_name)
        )

    def _ident(self):
        jvm = self._jvm()
        gw = self._gateway()
        empty_ns = gw.new_array(jvm.java.lang.String, 0)
        return jvm.org.apache.spark.sql.connector.catalog.Identifier.of(
            empty_ns, self.test_table
        )

    def _make_change_row(self, id, data, change_type, commit_version, commit_timestamp):
        jvm = self._jvm()
        UTF8String = jvm.org.apache.spark.unsafe.types.UTF8String
        row = jvm.org.apache.spark.sql.catalyst.expressions.GenericInternalRow(5)
        row.setLong(0, id)
        row.update(1, UTF8String.fromString(data))
        row.update(2, UTF8String.fromString(change_type))
        row.setLong(3, commit_version)
        row.setLong(4, commit_timestamp)
        return row

    def _add_change_rows(self, rows):
        jvm = self._jvm()
        catalog = self._catalog()
        ident = self._ident()
        java_list = jvm.java.util.ArrayList()
        for row in rows:
            java_list.add(row)
        scala_seq = (
            jvm.scala.jdk.CollectionConverters.ListHasAsScala(java_list).asScala().toSeq()
        )
        catalog.addChangeRows(ident, scala_seq)

    def setUp(self):
        super().setUp()
        jvm = self._jvm()
        gw = self._gateway()
        catalog = self._catalog()
        ident = self._ident()

        if catalog.tableExists(ident):
            catalog.dropTable(ident)

        Column = jvm.org.apache.spark.sql.connector.catalog.Column
        DataTypes = jvm.org.apache.spark.sql.types.DataTypes
        columns = gw.new_array(jvm.org.apache.spark.sql.connector.catalog.Column, 2)
        columns[0] = Column.create("id", DataTypes.LongType)
        columns[1] = Column.create("data", DataTypes.StringType)

        transforms = gw.new_array(
            jvm.org.apache.spark.sql.connector.expressions.Transform, 0
        )
        props = jvm.java.util.HashMap()
        catalog.createTable(ident, columns, transforms, props)
        catalog.clearChangeRows(ident)

    @staticmethod
    def _ts(epoch_micros):
        """Convert epoch microseconds to local datetime (matching PySpark Timestamp behavior)."""
        return datetime.datetime.fromtimestamp(epoch_micros / 1_000_000)

    # ---------- Batch: basic data retrieval ----------

    def test_changes_returns_data(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "delete", 2, 2000000),
            ]
        )

        expected = [
            Row(id=1, data="a", _change_type="insert", _commit_version=1,
                _commit_timestamp=self._ts(1000000)),
            Row(id=2, data="b", _change_type="delete", _commit_version=2,
                _commit_timestamp=self._ts(2000000)),
        ]

        # DataFrame API
        df = (
            self.spark.read.option("startingVersion", "1")
            .option("endingVersion", "2")
            .changes(self.full_table_name)
        )
        self.assertEqual(sorted(df.collect()), sorted(expected))

        # SQL
        df_sql = self.spark.sql(
            f"SELECT * FROM {self.full_table_name} CHANGES FROM VERSION 1 TO VERSION 2"
        )
        self.assertEqual(sorted(df_sql.collect()), sorted(expected))

    def test_changes_open_ended_version_range(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "insert", 2, 2000000),
                self._make_change_row(3, "c", "insert", 3, 3000000),
            ]
        )

        expected = [
            Row(id=2, data="b", _change_type="insert", _commit_version=2,
                _commit_timestamp=self._ts(2000000)),
            Row(id=3, data="c", _change_type="insert", _commit_version=3,
                _commit_timestamp=self._ts(3000000)),
        ]

        # DataFrame API
        df = self.spark.read.option("startingVersion", "2").changes(self.full_table_name)
        self.assertEqual(sorted(df.collect()), sorted(expected))

        # SQL
        df_sql = self.spark.sql(
            f"SELECT * FROM {self.full_table_name} CHANGES FROM VERSION 2"
        )
        self.assertEqual(sorted(df_sql.collect()), sorted(expected))

    def test_changes_empty_result(self):
        df = (
            self.spark.read.option("startingVersion", "1")
            .option("endingVersion", "5")
            .changes(self.full_table_name)
        )
        self.assertEqual(df.collect(), [])
        self.assertIn("_change_type", df.schema.fieldNames())

    # ---------- Batch: projection, filter, aggregation ----------

    def test_changes_select_cdc_metadata_columns(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "delete", 2, 2000000),
            ]
        )

        expected = [
            Row(id=1, _change_type="insert", _commit_version=1),
            Row(id=2, _change_type="delete", _commit_version=2),
        ]

        # DataFrame API
        df = (
            self.spark.read.option("startingVersion", "1")
            .changes(self.full_table_name)
            .select("id", "_change_type", "_commit_version")
        )
        self.assertEqual(sorted(df.collect()), sorted(expected))

        # SQL
        df_sql = self.spark.sql(
            f"SELECT id, _change_type, _commit_version "
            f"FROM {self.full_table_name} CHANGES FROM VERSION 1"
        )
        self.assertEqual(sorted(df_sql.collect()), sorted(expected))

    def test_changes_with_projection_and_filter(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "insert", 1, 1000000),
                self._make_change_row(1, "a2", "insert", 2, 2000000),
            ]
        )

        expected = [Row(id=1, data="a2")]

        # DataFrame API
        df = (
            self.spark.read.option("startingVersion", "1")
            .changes(self.full_table_name)
            .filter("_commit_version = 2")
            .select("id", "data")
        )
        self.assertEqual(df.collect(), expected)

        # SQL
        df_sql = self.spark.sql(
            f"SELECT id, data FROM {self.full_table_name} "
            "CHANGES FROM VERSION 1 WHERE _commit_version = 2"
        )
        self.assertEqual(df_sql.collect(), expected)

    def test_changes_with_aggregation(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "insert", 1, 1000000),
                self._make_change_row(1, "a", "delete", 2, 2000000),
            ]
        )

        expected = [
            Row(_change_type="delete", count=1),
            Row(_change_type="insert", count=2),
        ]

        # DataFrame API
        df = (
            self.spark.read.option("startingVersion", "1")
            .changes(self.full_table_name)
            .groupBy("_change_type")
            .count()
        )
        self.assertEqual(sorted(df.collect()), sorted(expected))

        # SQL
        df_sql = self.spark.sql(
            f"SELECT _change_type, count(*) AS count "
            f"FROM {self.full_table_name} CHANGES FROM VERSION 1 "
            "GROUP BY _change_type"
        )
        self.assertEqual(sorted(df_sql.collect()), sorted(expected))

    def test_schema_includes_cdc_metadata(self):
        self._add_change_rows([self._make_change_row(1, "a", "insert", 1, 1000000)])

        df = self.spark.read.option("startingVersion", "1").changes(self.full_table_name)
        self.assertEqual(
            df.schema.fieldNames(),
            ["id", "data", "_change_type", "_commit_version", "_commit_timestamp"],
        )

    # ---------- Batch: version range filtering ----------

    def test_changes_version_range_filters(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "insert", 2, 2000000),
                self._make_change_row(3, "c", "insert", 3, 3000000),
                self._make_change_row(4, "d", "insert", 4, 4000000),
            ]
        )

        expected = [
            Row(id=2, data="b", _change_type="insert", _commit_version=2,
                _commit_timestamp=self._ts(2000000)),
            Row(id=3, data="c", _change_type="insert", _commit_version=3,
                _commit_timestamp=self._ts(3000000)),
        ]

        # DataFrame API
        df = (
            self.spark.read.option("startingVersion", "2")
            .option("endingVersion", "3")
            .changes(self.full_table_name)
        )
        self.assertEqual(sorted(df.collect()), sorted(expected))

        # SQL
        df_sql = self.spark.sql(
            f"SELECT * FROM {self.full_table_name} CHANGES FROM VERSION 2 TO VERSION 3"
        )
        self.assertEqual(sorted(df_sql.collect()), sorted(expected))

    # ---------- Batch: bound inclusivity ----------

    def test_changes_default_bounds_inclusive(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "insert", 2, 2000000),
                self._make_change_row(3, "c", "insert", 3, 3000000),
            ]
        )

        # DataFrame API
        df = (
            self.spark.read.option("startingVersion", "1")
            .option("endingVersion", "3")
            .changes(self.full_table_name)
        )
        self.assertEqual(len(df.collect()), 3)

        # SQL
        df_sql = self.spark.sql(
            f"SELECT * FROM {self.full_table_name} CHANGES FROM VERSION 1 TO VERSION 3"
        )
        self.assertEqual(len(df_sql.collect()), 3)

    def test_changes_starting_bound_exclusive(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "insert", 2, 2000000),
                self._make_change_row(3, "c", "insert", 3, 3000000),
            ]
        )

        # DataFrame API — version 1 excluded
        df = (
            self.spark.read.option("startingVersion", "1")
            .option("endingVersion", "3")
            .option("startingBoundInclusive", "false")
            .changes(self.full_table_name)
        )
        result = df.collect()
        self.assertEqual(len(result), 2)
        ids = sorted([r.id for r in result])
        self.assertEqual(ids, [2, 3])

        # SQL
        df_sql = self.spark.sql(
            f"SELECT * FROM {self.full_table_name} "
            "CHANGES FROM VERSION 1 EXCLUSIVE TO VERSION 3"
        )
        self.assertEqual(len(df_sql.collect()), 2)

    def test_changes_ending_bound_exclusive(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "insert", 2, 2000000),
                self._make_change_row(3, "c", "insert", 3, 3000000),
            ]
        )

        # DataFrame API — version 3 excluded
        df = (
            self.spark.read.option("startingVersion", "1")
            .option("endingVersion", "3")
            .option("endingBoundInclusive", "false")
            .changes(self.full_table_name)
        )
        result = df.collect()
        self.assertEqual(len(result), 2)
        ids = sorted([r.id for r in result])
        self.assertEqual(ids, [1, 2])

        # SQL
        df_sql = self.spark.sql(
            f"SELECT * FROM {self.full_table_name} "
            "CHANGES FROM VERSION 1 TO VERSION 3 EXCLUSIVE"
        )
        self.assertEqual(len(df_sql.collect()), 2)

    def test_changes_both_bounds_exclusive(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "insert", 2, 2000000),
                self._make_change_row(3, "c", "insert", 3, 3000000),
            ]
        )

        # DataFrame API — only version 2
        df = (
            self.spark.read.option("startingVersion", "1")
            .option("endingVersion", "3")
            .option("startingBoundInclusive", "false")
            .option("endingBoundInclusive", "false")
            .changes(self.full_table_name)
        )
        result = df.collect()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, 2)

        # SQL
        df_sql = self.spark.sql(
            f"SELECT * FROM {self.full_table_name} "
            "CHANGES FROM VERSION 1 EXCLUSIVE TO VERSION 3 EXCLUSIVE"
        )
        self.assertEqual(len(df_sql.collect()), 1)

    # ---------- Batch: error cases ----------

    def test_changes_rejects_user_schema(self):
        with self.assertRaises(AnalysisException) as ctx:
            self.spark.read.schema("id LONG, data STRING").option(
                "startingVersion", "1"
            ).changes(self.full_table_name)
        self.assertIn("changes", str(ctx.exception))

    # ---------- Streaming ----------

    def test_streaming_changes_returns_data(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "insert", 1, 1000000),
                self._make_change_row(1, "a", "delete", 2, 2000000),
            ]
        )

        expected = [
            Row(id=1, data="a", _change_type="insert", _commit_version=1,
                _commit_timestamp=self._ts(1000000)),
            Row(id=2, data="b", _change_type="insert", _commit_version=1,
                _commit_timestamp=self._ts(1000000)),
            Row(id=1, data="a", _change_type="delete", _commit_version=2,
                _commit_timestamp=self._ts(2000000)),
        ]

        # DataFrame API
        stream_df = (
            self.spark.readStream.option("startingVersion", "1")
            .changes(self.full_table_name)
        )
        q = stream_df.writeStream.format("memory").queryName("cdc_py_stream_df").start()
        try:
            q.processAllAvailable()
            result = self.spark.sql("SELECT * FROM cdc_py_stream_df")
            self.assertEqual(sorted(result.collect()), sorted(expected))
        finally:
            q.stop()

        # SQL
        sql_stream = self.spark.sql(
            f"SELECT * FROM STREAM {self.full_table_name} CHANGES FROM VERSION 1"
        )
        q2 = sql_stream.writeStream.format("memory").queryName("cdc_py_stream_sql").start()
        try:
            q2.processAllAvailable()
            result = self.spark.sql("SELECT * FROM cdc_py_stream_sql")
            self.assertEqual(sorted(result.collect()), sorted(expected))
        finally:
            q2.stop()

    def test_streaming_changes_with_starting_version(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "insert", 1, 1000000),
                self._make_change_row(1, "a", "delete", 2, 2000000),
            ]
        )

        expected = [
            Row(id=1, data="a", _change_type="delete", _commit_version=2,
                _commit_timestamp=self._ts(2000000)),
        ]

        # DataFrame API
        stream_df = (
            self.spark.readStream.option("startingVersion", "2")
            .changes(self.full_table_name)
        )
        q = stream_df.writeStream.format("memory").queryName("cdc_py_stream_v2_df").start()
        try:
            q.processAllAvailable()
            result = self.spark.sql("SELECT * FROM cdc_py_stream_v2_df")
            self.assertEqual(sorted(result.collect()), sorted(expected))
        finally:
            q.stop()

        # SQL
        sql_stream = self.spark.sql(
            f"SELECT * FROM STREAM {self.full_table_name} CHANGES FROM VERSION 2"
        )
        q2 = sql_stream.writeStream.format("memory").queryName("cdc_py_stream_v2_sql").start()
        try:
            q2.processAllAvailable()
            result = self.spark.sql("SELECT * FROM cdc_py_stream_v2_sql")
            self.assertEqual(sorted(result.collect()), sorted(expected))
        finally:
            q2.stop()

    def test_streaming_changes_with_projection_and_filter(self):
        self._add_change_rows(
            [
                self._make_change_row(1, "a", "insert", 1, 1000000),
                self._make_change_row(2, "b", "insert", 1, 1000000),
                self._make_change_row(3, "c", "insert", 2, 2000000),
            ]
        )

        expected = [Row(id=1, data="a"), Row(id=2, data="b")]

        # DataFrame API
        stream_df = (
            self.spark.readStream.option("startingVersion", "1")
            .changes(self.full_table_name)
            .filter("_commit_version = 1")
            .select("id", "data")
        )
        q = stream_df.writeStream.format("memory").queryName("cdc_py_stream_proj_df").start()
        try:
            q.processAllAvailable()
            result = self.spark.sql("SELECT * FROM cdc_py_stream_proj_df")
            self.assertEqual(sorted(result.collect()), sorted(expected))
        finally:
            q.stop()

        # SQL
        sql_stream = self.spark.sql(
            f"SELECT id, data FROM STREAM {self.full_table_name} "
            "CHANGES FROM VERSION 1 WHERE _commit_version = 1"
        )
        q2 = (
            sql_stream.writeStream.format("memory")
            .queryName("cdc_py_stream_proj_sql")
            .start()
        )
        try:
            q2.processAllAvailable()
            result = self.spark.sql("SELECT * FROM cdc_py_stream_proj_sql")
            self.assertEqual(sorted(result.collect()), sorted(expected))
        finally:
            q2.stop()

    def test_streaming_changes_rejects_user_schema(self):
        with self.assertRaises(AnalysisException) as ctx:
            self.spark.readStream.schema("id LONG, data STRING").option(
                "startingVersion", "1"
            ).changes(self.full_table_name)
        self.assertIn("changes", str(ctx.exception))


_catalyst_test_jar = _find_catalyst_test_jar()


@unittest.skipIf(
    _catalyst_test_jar is None,
    "catalyst test JAR not found; run 'build/sbt catalyst/test:package' first",
)
class CDCTests(CDCTestsMixin, SQLTestUtils, PySparkErrorTestUtils, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conf = (
            SparkConf()
            .setMaster("local[4]")
            .setAppName(cls.__name__)
            .set("spark.driver.extraClassPath", _catalyst_test_jar)
            .set(
                f"spark.sql.catalog.{cls.catalog_name}",
                "org.apache.spark.sql.connector.catalog.InMemoryChangelogCatalog",
            )
        )
        cls.sc = SparkContext(conf=conf)
        cls.spark = SparkSession(cls.sc)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
