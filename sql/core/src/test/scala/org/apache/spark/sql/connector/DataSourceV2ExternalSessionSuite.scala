/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{BufferedRows, Identifier, InMemoryTable, InMemoryTableCatalog, SharedInMemoryTableCatalog, TableWritePrivilege}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests for cross-session and external-write scenarios where an external
 * [[SparkSession]] or direct table manipulation modifies a DSv2 table
 * while another session holds a stale [[DataFrame]].
 *
 * Uses [[SharedInMemoryTableCatalog]] so both sessions see the same
 * table data via a static map, simulating a real shared metastore.
 */
class DataSourceV2ExternalSessionSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED, true)
    .set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.sharedcat",
      classOf[SharedInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.sharedcat.copyOnLoad", "true")

  after {
    SharedInMemoryTableCatalog.reset()
    spark.sessionState.catalogManager.reset()
  }

  protected def catalog(name: String): InMemoryTableCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog(name)
    catalog.asInstanceOf[InMemoryTableCatalog]
  }

  /**
   * Creates a [[SparkSession]] with a SEPARATE [[SharedState]] (separate
   * [[CacheManager]] and relation cache) but the same [[SparkContext]] and
   * catalog configs. [[SharedInMemoryTableCatalog]] tables are shared
   * via the companion object's static map, so the external session
   * sees the same table data. This simulates a truly external writer
   * whose writes do NOT invalidate this session's [[CacheManager]].
   */
  private def extSession: SparkSession = {
    val savedActive = SparkSession.getActiveSession
    val savedDefault = SparkSession.getDefaultSession
    try {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      SparkSession.builder()
        .sparkContext(spark.sparkContext)
        .create()
    } finally {
      savedDefault.foreach(s =>
        SparkSession.setDefaultSession(s))
      savedActive.foreach(s =>
        SparkSession.setActiveSession(s))
    }
  }

  /**
   * Simulates an external write by directly adding rows to the table,
   * bypassing the SparkSession. This mimics a separate process writing
   * to the same table (e.g., another Spark cluster or ETL job).
   */
  private def externalInsert(
      catalogName: String,
      ident: Identifier,
      values: Seq[Seq[Any]]): Unit = {
    val table = catalog(catalogName)
      .loadTable(ident, java.util.Set.of(TableWritePrivilege.INSERT))
      .asInstanceOf[InMemoryTable]
    val schema = table.schema
    val rows = values.map { vals =>
      InternalRow.fromSeq(vals.zipWithIndex.map { case (v, i) =>
        schema.fields(i).dataType match {
          case StringType => if (v == null) null else UTF8String.fromString(v.toString)
          case _ => v
        }
      })
    }
    val key = Seq.empty[Any]
    val buffered = new BufferedRows(key, schema)
    rows.foreach(buffered.withRow)
    table.withData(Array(buffered))
  }

  // External session tests

  private val T = "sharedcat.ns.tbl"

  test("external write visible via fresh query") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")

      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // external session writes data
      extSession.sql(
        s"INSERT INTO $T VALUES (2, 200)").collect()

      // a fresh query from session1 picks up external write
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("external drop+re-add column detected by column ID") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")

      val df = spark.table(T)

      // external session drops and re-adds column
      val ext = extSession
      ext.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      ext.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()

      // column ID changed, session1 detects it
      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition =
          "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS" +
            ".COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map(
          "tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("external drop+recreate table detected by table ID") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")

      val df = spark.table(T)

      // external session drops and recreates table
      val ext = extSession
      ext.sql(s"DROP TABLE $T").collect()
      ext.sql(
        s"CREATE TABLE $T (id INT, salary INT) USING foo"
      ).collect()

      // table ID changed, session1 detects it
      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition =
          "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS" +
            ".TABLE_ID_MISMATCH",
        matchPVals = true,
        parameters = Map(
          "tableName" -> ".*",
          "capturedTableId" -> ".*",
          "currentTableId" -> ".*"))
    }
  }

  // Design doc scenarios

  // DataFrame.show picks up external writes.
  // Uses testcat2 (no copyOnLoad) so external writes are directly visible.
  test("DataFrame.show picks up external writes") {
    val t = "testcat2.ns.tbl"
    val ident = Identifier.of(Array("ns"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)
      checkAnswer(df, Seq(Row(1, 100)))

      // external writer adds (2, 200)
      externalInsert("testcat2", ident, Seq(Seq(2, 200)))

      // show should reflect the external write
      checkAnswer(df, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // DataFrame picks up external data after column addition.
  // Uses testcat2 (no copyOnLoad) with a fresh query to avoid QueryExecution reuse.
  test("fresh query picks up new data after column addition") {
    val t = "testcat2.ns.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      // add a column and insert new data
      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")
      sql(s"INSERT INTO $t VALUES (2, 200, 50)")

      // a fresh query picks up both old and new data with the full schema
      checkAnswer(
        spark.table(t),
        Seq(Row(1, 100, null), Row(2, 200, 50)))
    }
  }

  // Temp view picks up session writes.
  test("temp view picks up session writes") {
    val t = "testcat2.ns.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (10, 1000)")

      // create temp view with filter
      spark.table(t).filter("salary < 999").createOrReplaceTempView("tmp_view_s1")

      checkAnswer(sql("SELECT * FROM tmp_view_s1"), Seq(Row(1, 100)))

      // session write
      sql(s"INSERT INTO $t VALUES (2, 200)")

      // temp view should pick up the new data
      checkAnswer(
        sql("SELECT * FROM tmp_view_s1"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // Temp view picks up external writes.
  test("temp view picks up external writes") {
    val t = "testcat2.ns.tbl"
    val ident = Identifier.of(Array("ns"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (10, 1000)")

      spark.table(t).filter("salary < 999").createOrReplaceTempView("tmp_view_ext")

      checkAnswer(sql("SELECT * FROM tmp_view_ext"), Seq(Row(1, 100)))

      // external writer adds (2, 200)
      externalInsert("testcat2", ident, Seq(Seq(2, 200)))

      // temp view should pick up external write (connector w/o cache)
      checkAnswer(
        sql("SELECT * FROM tmp_view_ext"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // Temp view picks up new data after column addition.
  test("temp view preserves schema after external column addition") {
    val t = "testcat2.ns.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (10, 1000)")

      spark.table(t).filter("salary < 999").createOrReplaceTempView("tmp_view_s2")
      checkAnswer(sql("SELECT * FROM tmp_view_s2"), Seq(Row(1, 100)))

      // add column and insert new data
      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")
      sql(s"INSERT INTO $t VALUES (2, 200, 50)")

      // temp view should preserve original schema (id, salary) but pick up new data
      checkAnswer(
        sql("SELECT * FROM tmp_view_s2"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // Incrementally constructed join picks up external writes.
  test("join refreshes both sides after external write") {
    val t = "testcat2.ns.tbl"
    val ident = Identifier.of(Array("ns"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df1 = spark.table(t)

      // external writer adds (2, 200)
      externalInsert("testcat2", ident, Seq(Seq(2, 200)))

      val df2 = spark.table(t)

      // join should refresh both sides to the latest version
      val joined = df1.join(df2, df1("id") === df2("id"))
      val result = joined.collect()
      assert(result.length === 2, "Join should see both rows after refresh")
    }
  }
}
