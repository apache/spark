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
import warnings

from pyspark.sql import SparkSession, SQLContext
from pyspark.testing.sqlutils import ReusedSQLTestCase


class SQLContextTestsMixin:
    """Tests for SQLContext that run in both classic and Connect modes.

    Subclasses must implement :meth:`_make_ctx` to return an :class:`SQLContext`
    appropriate for their mode, and must expose ``self.spark`` as a
    :class:`SparkSession`.
    """

    spark: SparkSession

    def _make_ctx(self) -> SQLContext:
        raise NotImplementedError

    def setUp(self) -> None:
        super().setUp()
        SQLContext._instantiatedContext = None

    def tearDown(self) -> None:
        SQLContext._instantiatedContext = None
        super().tearDown()

    def test_setConf_and_getConf(self) -> None:
        ctx = self._make_ctx()
        ctx.setConf("spark.sql.shuffle.partitions", "42")
        self.assertEqual(ctx.getConf("spark.sql.shuffle.partitions"), "42")
        self.assertEqual(ctx.getConf("spark.sql.shuffle.partitions", "99"), "42")
        ctx.setConf("spark.sql.shuffle.partitions", "200")

    def test_range(self) -> None:
        ctx = self._make_ctx()
        self.assertEqual(ctx.range(3).count(), 3)

    def test_createDataFrame(self) -> None:
        ctx = self._make_ctx()
        df = ctx.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        self.assertEqual(df.count(), 2)

    def test_sql(self) -> None:
        ctx = self._make_ctx()
        self.assertEqual(ctx.sql("SELECT 1 AS n").collect()[0].n, 1)

    def test_registerDataFrameAsTable_and_sql(self) -> None:
        ctx = self._make_ctx()
        df = self.spark.createDataFrame([(1, "r1"), (2, "r2")], ["field1", "field2"])
        ctx.registerDataFrameAsTable(df, "ctx_mixin_tbl")
        try:
            result = ctx.sql("SELECT field1 FROM ctx_mixin_tbl ORDER BY field1").collect()
            self.assertEqual([r.field1 for r in result], [1, 2])
        finally:
            ctx.dropTempTable("ctx_mixin_tbl")

    def test_table(self) -> None:
        ctx = self._make_ctx()
        self.spark.createDataFrame([(42,)], ["v"]).createOrReplaceTempView("ctx_mixin_view")
        try:
            self.assertEqual(ctx.table("ctx_mixin_view").collect()[0].v, 42)
        finally:
            self.spark.catalog.dropTempView("ctx_mixin_view")

    def test_tables_contains_registered_view(self) -> None:
        ctx = self._make_ctx()
        self.spark.createDataFrame([(1,)], ["x"]).createOrReplaceTempView("ctx_mixin_tables_view")
        try:
            names = [r.tableName for r in ctx.tables().collect()]
            self.assertIn("ctx_mixin_tables_view", names)
        finally:
            self.spark.catalog.dropTempView("ctx_mixin_tables_view")

    def test_tables_with_dbName(self) -> None:
        ctx = self._make_ctx()
        self.spark.sql("CREATE DATABASE ctx_mixin_db")
        try:
            self.spark.sql("CREATE TABLE ctx_mixin_db.ctx_mixin_db_tbl (id INT) USING parquet")
            rows = [r for r in ctx.tables("ctx_mixin_db").collect() if not r.isTemporary]
            self.assertEqual(
                [(r.namespace, r.tableName) for r in rows], [("ctx_mixin_db", "ctx_mixin_db_tbl")]
            )
            self.assertIn("ctx_mixin_db_tbl", ctx.tableNames("ctx_mixin_db"))
        finally:
            self.spark.sql("DROP TABLE IF EXISTS ctx_mixin_db.ctx_mixin_db_tbl")
            self.spark.sql("DROP DATABASE IF EXISTS ctx_mixin_db")

    def test_tableNames_contains_registered_view(self) -> None:
        ctx = self._make_ctx()
        self.spark.createDataFrame([(1,)], ["x"]).createOrReplaceTempView(
            "ctx_mixin_tablenames_view"
        )
        try:
            self.assertIn("ctx_mixin_tablenames_view", ctx.tableNames())
        finally:
            self.spark.catalog.dropTempView("ctx_mixin_tablenames_view")

    def test_cacheTable_and_uncacheTable(self) -> None:
        ctx = self._make_ctx()
        self.spark.createDataFrame([(1,)], ["x"]).createOrReplaceTempView("ctx_mixin_cache_view")
        try:
            ctx.cacheTable("ctx_mixin_cache_view")
            ctx.uncacheTable("ctx_mixin_cache_view")
        finally:
            self.spark.catalog.dropTempView("ctx_mixin_cache_view")

    def test_clearCache(self) -> None:
        self._make_ctx().clearCache()

    def test_newSession_returns_distinct_instance(self) -> None:
        ctx = self._make_ctx()
        ctx2 = ctx.newSession()
        self.assertIsNot(ctx, ctx2)

    def test_read_is_available(self) -> None:
        self.assertIsNotNone(self._make_ctx().read)

    def test_readStream_is_available(self) -> None:
        self.assertIsNotNone(self._make_ctx().readStream)

    def test_streams_is_available(self) -> None:
        self.assertIsNotNone(self._make_ctx().streams)

    def test_udf_is_available(self) -> None:
        self.assertIsNotNone(self._make_ctx().udf)

    def test_udtf_is_available(self) -> None:
        self.assertIsNotNone(self._make_ctx().udtf)

    def test_registerFunction(self) -> None:
        from pyspark.sql.types import IntegerType

        ctx = self._make_ctx()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            ctx.registerFunction("ctx_mixin_double", lambda x: x * 2, IntegerType())
        result = ctx.sql("SELECT ctx_mixin_double(3) AS v").collect()[0].v
        self.assertEqual(result, 6)

    def test_createExternalTable(self) -> None:
        ctx = self._make_ctx()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "data")
            self.spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"]).write.parquet(path)
            df = ctx.createExternalTable("ctx_mixin_ext_tbl", path, "parquet")
            try:
                self.assertEqual(df.count(), 2)
            finally:
                self.spark.sql("DROP TABLE IF EXISTS ctx_mixin_ext_tbl")


class SQLContextClassicTests(SQLContextTestsMixin, ReusedSQLTestCase):
    def _make_ctx(self) -> SQLContext:
        # Passing a non-None sparkSession means __init__ does not emit a deprecation
        # warning, so no warnings filter is needed here.
        return SQLContext(self.sc, self.spark)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
