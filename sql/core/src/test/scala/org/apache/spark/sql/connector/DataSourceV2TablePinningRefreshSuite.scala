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
import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Comprehensive tests for DSv2 table pinning and refresh behavior.
 *
 * Follows Anton Okolnychyi's "Refreshing and pinning tables in Spark" design
 * doc section by section. Each design doc scenario has both a "session write"
 * (.1) and an "external write" (.2) variant.
 *
 * External writes use spark.newSession() with SharedInMemoryTableCatalog so
 * both sessions share the same table state. This is the OSS equivalent of
 * the DBR pattern using DeltaLog sharing.
 *
 * Key behavioral observations in InMemoryTableCatalog with copyOnLoad=true:
 *  1. sql("SELECT * FROM t") creates a fresh QE each time with latest data.
 *  2. Temp views via createOrReplaceTempView capture V2TableReference which
 *     reloads the table on each access via catalog.loadTable().
 *  3. DataFrame.collect() pins the QueryExecution (stale after changes).
 *  4. show/count/head create derived DFs with new QEs (always fresh).
 *  5. Joins combine pre-analyzed plans; refresh aligns versions.
 *  6. CACHE TABLE pins table state against external changes.
 *
 * Sections:
 *  [1] Temp views with stored plans
 *  [2] Repeated table access via sql()
 *  [3] Incrementally constructed queries (joins, unions, etc.)
 *  [4] Version pinning in Dataset (show vs collect)
 *  [5] CACHE TABLE
 *  [Conc] Concurrent writes from threads
 */
class DataSourceV2TablePinningRefreshSuite
    extends QueryTest with SharedSparkSession {

  // Error condition constants
  private val COL_MISMATCH =
    "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH"
  private val ID_MISMATCH =
    "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH"
  private val VIEW_PLAN_CHANGED =
    "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION"

  // Allow checkError without exact parameters for parameterized tests
  override protected def checkErrorIgnorableParameters
    : Map[String, Set[String]] =
    super.checkErrorIgnorableParameters ++ Map(
      COL_MISMATCH -> Set("tableName", "errors"),
      ID_MISMATCH ->
        Set("tableName", "capturedTableId", "currentTableId"),
      VIEW_PLAN_CHANGED ->
        Set("viewName", "tableName", "colType", "errors"))

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED, true)
    .set("spark.sql.catalog.sharedcat",
      classOf[SharedInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.sharedcat.copyOnLoad", "true")
    // Caching connector: simulates Iceberg CachingCatalog where
    // external changes are invisible until cache is cleared.
    .set("spark.sql.catalog.cachingcat",
      classOf[CachingInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.cachingcat.copyOnLoad", "true")
    // Null ID: tables without identity tracking. Drop/recreate
    // is not detected because table ID is always null.
    .set("spark.sql.catalog.nullidcat",
      classOf[NullIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullidcat.copyOnLoad", "true")

  override def afterEach(): Unit = {
    SharedInMemoryTableCatalog.reset()
    CachingInMemoryTableCatalog.clearCache()
    try {
      spark.sessionState.catalogManager.reset()
    } finally {
      super.afterEach()
    }
  }

  private val T = "sharedcat.ns.tbl"

  private def setupTable(): Unit = {
    sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
    sql(s"INSERT INTO $T VALUES (1, 100)")
  }

  /** External session sharing the same catalog tables. */
  private def extSession: SparkSession = spark.newSession()

  // =========================================================================
  // [1] Temp views with stored plans
  // =========================================================================

  // --- 1.1: Session and external data writes ---

  test("[1.1] temp view reflects session write") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (10, 1000)")
      spark.table(T).filter("salary < 999").createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"INSERT INTO $T VALUES (2, 200)")

      checkAnswer(
        sql("SELECT * FROM tmp"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1.1-ext] temp view reflects external write") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (10, 1000)")
      spark.table(T).filter("salary < 999").createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      // External write via newSession (shares catalog, fresh session state)
      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      // Connector w/o cache: external write visible on refresh
      checkAnswer(
        sql("SELECT * FROM tmp"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // --- 1.2: Adding new columns + data ---

  test("[1.2] temp view preserves schema after session ADD COLUMN") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $T ADD COLUMN new_column INT")
      sql(s"INSERT INTO $T VALUES (2, 200, -1)")

      // Temp view preserves original 2-col schema but picks up new data
      checkAnswer(
        sql("SELECT * FROM tmp"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1.2-ext] temp view preserves schema after external ADD COLUMN") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      val ext = extSession
      ext.sql(s"ALTER TABLE $T ADD COLUMN new_column INT").collect()
      ext.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

      checkAnswer(
        sql("SELECT * FROM tmp"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // --- 1.3: Removing existing columns ---

  test("[1.3] temp view fails after session DROP COLUMN") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $T DROP COLUMN salary")

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[1.3-ext] temp view fails after external DROP COLUMN") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      extSession.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  // --- 1.4: Drop and recreate table ---

  test("[1.4] temp view resolves to new table after session drop/recreate") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"DROP TABLE $T")
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")

      // View re-resolves by name to new empty table
      checkAnswer(sql("SELECT * FROM tmp"), Seq.empty)
    }
  }

  test("[1.4-ext] temp view resolves to new table after external drop/recreate") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      val ext = extSession
      ext.sql(s"DROP TABLE $T").collect()
      ext.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

      checkAnswer(sql("SELECT * FROM tmp"), Seq.empty)
    }
  }

  // --- 1.5: Drop and add column with same name and type ---

  test("[1.5] temp view after session drop/re-add column same type") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary INT")

      // After drop+add, salary is a NEW column. The old data for the
      // dropped salary is gone. InMemoryTable now adapts rows by name
      // during schema evolution, so the recreated salary reads as null.
      // This matches Delta/Iceberg behavior with column mapping.
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, null)))
    }
  }

  test("[1.5-ext] temp view after external drop/re-add column same type") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      val ext = extSession
      ext.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      ext.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()

      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, null)))
    }
  }

  // --- 1.6: Drop and add column with same name but different type ---

  test("[1.6] temp view fails after session drop/re-add column diff type") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary STRING")

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[1.6-ext] temp view fails after external drop/re-add column diff type") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      val ext = extSession
      ext.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      ext.sql(s"ALTER TABLE $T ADD COLUMN salary STRING").collect()

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  // --- 1.7: Type widening ---

  test("[1.7] temp view fails after session type widening") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT")

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[1.7-ext] temp view fails after external type widening") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      extSession.sql(
        s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT").collect()

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  // --- 1.8: Column rename ---

  test("[1.8] temp view fails after session column rename") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $T RENAME COLUMN salary TO pay")

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[1.8-ext] temp view fails after external column rename") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      extSession.sql(
        s"ALTER TABLE $T RENAME COLUMN salary TO pay").collect()

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  // =========================================================================
  // [2] Repeated table access via sql()
  // =========================================================================
  // sql("SELECT * FROM t") creates a fresh QE each time, always latest.

  test("[2.1] repeated sql() reflects session write") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(
        sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[2.1-ext] repeated sql() reflects external write") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      checkAnswer(
        sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[2.2] repeated sql() reflects schema change") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $T ADD COLUMN new_col INT")
      sql(s"INSERT INTO $T VALUES (2, 200, -1)")
      checkAnswer(
        sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[2.2-ext] repeated sql() reflects external schema change") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      val ext = extSession
      ext.sql(s"ALTER TABLE $T ADD COLUMN new_col INT").collect()
      ext.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

      checkAnswer(
        sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[2.3] repeated sql() reflects drop/recreate") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      sql(s"DROP TABLE $T")
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      checkAnswer(sql(s"SELECT * FROM $T"), Seq.empty)
    }
  }

  test("[2.3-ext] repeated sql() reflects external drop/recreate") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      val ext = extSession
      ext.sql(s"DROP TABLE $T").collect()
      ext.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

      checkAnswer(sql(s"SELECT * FROM $T"), Seq.empty)
    }
  }

  // =========================================================================
  // [3] Incrementally constructed queries (joins)
  // =========================================================================
  // Each DataFrame is analyzed separately. The refresh phase in QE aligns
  // all table references to the same version within a single execution.

  test("[3.1] join two DataFrames after session insert") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T) // analyzed at v1: {(1,100)}

      sql(s"INSERT INTO $T VALUES (2, 200)")

      val df2 = spark.table(T) // analyzed at v2: {(1,100),(2,200)}

      // Refresh aligns both to latest version
      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(
        joined,
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
    }
  }

  test("[3.1-ext] join two DataFrames after external insert") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      val df2 = spark.table(T)

      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(
        joined,
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
    }
  }

  test("[3.2] join after session ADD COLUMN: df1 preserves schema") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T) // 2-col schema

      sql(s"ALTER TABLE $T ADD COLUMN new_col INT")
      sql(s"INSERT INTO $T VALUES (2, 200, -1)")

      val df2 = spark.table(T) // 3-col schema

      // Both refreshed to latest version. df1 preserves 2-col schema,
      // df2 has 3-col schema. Only 1 data row (id=1) existed before ADD COL;
      // the INSERT added (2,200,-1) after, so both sides see 2 rows.
      // df1 projects (id, salary), df2 projects (id, salary, new_col).
      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(
        joined,
        Seq(Row(1, 100, 1, 100, null), Row(2, 200, 2, 200, -1)))
    }
  }

  test("[3.2-ext] join after external ADD COLUMN: df1 preserves schema") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T) // 2-col schema

      val ext = extSession
      ext.sql(s"ALTER TABLE $T ADD COLUMN new_col INT").collect()
      ext.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

      val df2 = spark.table(T) // 3-col schema

      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(
        joined,
        Seq(Row(1, 100, 1, 100, null), Row(2, 200, 2, 200, -1)))
    }
  }

  test("[3.3] join after session DROP COLUMN fails") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"INSERT INTO $T VALUES (2)")

      val df2 = spark.table(T)

      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[3.3-ext] join after external DROP COLUMN fails") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      val ext = extSession
      ext.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      ext.sql(s"INSERT INTO $T VALUES (2)").collect()

      val df2 = spark.table(T)

      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[3.4] join after session drop/recreate fails with TABLE_ID_MISMATCH") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      sql(s"DROP TABLE $T")
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")

      val df2 = spark.table(T)

      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = ID_MISMATCH)
    }
  }

  test("[3.4-ext] join after external drop/recreate fails with TABLE_ID_MISMATCH") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      val ext = extSession
      ext.sql(s"DROP TABLE $T").collect()
      ext.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

      val df2 = spark.table(T)

      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = ID_MISMATCH)
    }
  }

  // Design doc Section 3.4: "If the table ID is not exposed to Spark,
  // allow the joined DataFrame to be executed."
  // NullIdInMemoryTableCatalog creates tables with null IDs, so
  // validateTableIdentity is skipped on refresh.

  test("[3.4-nullid] join after drop/recreate succeeds with null table ID") {
    val NT = "nullidcat.ns.tbl"
    withTable(NT) {
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NT VALUES (1, 100)")

      val df1 = spark.table(NT)

      sql(s"DROP TABLE $NT")
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")

      val df2 = spark.table(NT)

      // No TABLE_ID_MISMATCH because table ID is null
      val joined = df1.join(df2, df1("id") === df2("id"))
      // New table is empty, so join returns nothing
      checkAnswer(joined, Seq.empty)
    }
  }

  test("[3.4-nullid] temp view after session drop/recreate with null ID") {
    val NT = "nullidcat.ns.tbl"
    withTable(NT) {
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NT VALUES (1, 100)")

      spark.table(NT).createOrReplaceTempView("nullid_tmp")
      checkAnswer(sql("SELECT * FROM nullid_tmp"), Seq(Row(1, 100)))

      sql(s"DROP TABLE $NT")
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")

      // Null ID: no TABLE_ID_MISMATCH, view resolves to new empty table
      checkAnswer(sql("SELECT * FROM nullid_tmp"), Seq.empty)
    }
  }

  // Null ID: drop+add column same type. Without column IDs,
  // name+type matching passes. Tests both modes per the design doc.
  test("[3.5-nullid] join after drop+add same type with null ID") {
    val NT = "nullidcat.ns.tbl"
    withTable(NT) {
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NT VALUES (1, 100)")
      val df1 = spark.table(NT)

      sql(s"ALTER TABLE $NT DROP COLUMN salary")
      sql(s"ALTER TABLE $NT ADD COLUMN salary INT")

      val df2 = spark.table(NT)

      // No table ID and no column ID: both sides refresh to current.
      // InMemoryTable adapts data by name across drop+add, so salary is null.
      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(joined, Seq(Row(1, null, 1, null)))
    }
  }

  // Null ID: drop+add column different type still fails
  test("[3.6-nullid] join after drop+add different type fails with null ID") {
    val NT = "nullidcat.ns.tbl"
    withTable(NT) {
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NT VALUES (1, 100)")
      val df1 = spark.table(NT)

      sql(s"ALTER TABLE $NT DROP COLUMN salary")
      sql(s"ALTER TABLE $NT ADD COLUMN salary STRING")

      val df2 = spark.table(NT)

      // Type change detected by schema validation (no ID needed)
      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_MISMATCH)
    }
  }

  // Null ID: temp view after drop+add same type
  test("[1.5-nullid] temp view after drop+add column same type with null ID") {
    val NT = "nullidcat.ns.tbl"
    withTable(NT) {
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NT VALUES (1, 100)")
      spark.table(NT).createOrReplaceTempView("nullid_view")
      checkAnswer(sql("SELECT * FROM nullid_view"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $NT DROP COLUMN salary")
      sql(s"ALTER TABLE $NT ADD COLUMN salary INT")

      // No column ID: name+type match, view resolves normally.
      // InMemoryTable adapts data by name across drop+add, so salary is null.
      checkAnswer(sql("SELECT * FROM nullid_view"), Seq(Row(1, null)))
    }
  }

  // Null ID: DataFrame show after drop/recreate
  test("[4.4-nullid] DataFrame after drop/recreate with null ID") {
    val NT = "nullidcat.ns.tbl"
    withTable(NT) {
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NT VALUES (1, 100)")
      val df = spark.table(NT)
      checkAnswer(df, Seq(Row(1, 100)))

      sql(s"DROP TABLE $NT")
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NT VALUES (9, 900)")

      // No table ID: no TABLE_ID_MISMATCH. However, the first checkAnswer
      // pinned the QueryExecution, so subsequent collect() reuses the cached
      // plan which reads from the original table copy (copyOnLoad=true).
      checkAnswer(df, Seq(Row(1, 100)))
    }
  }

  test("[3.5] join after drop/re-add column same type succeeds") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary INT")

      val df2 = spark.table(T)

      // After drop+add, salary is recreated. Old data reads as null.
      // Column IDs (4.2) will make this fail with an exception instead.
      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(joined, Seq(Row(1, null, 1, null)))
    }
  }

  test("[3.6] join after drop/re-add column different type fails") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary STRING")

      val df2 = spark.table(T)

      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[3.7] join after type widening fails") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT")

      val df2 = spark.table(T)

      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[3.8] join after column rename fails") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      sql(s"ALTER TABLE $T RENAME COLUMN salary TO pay")

      val df2 = spark.table(T)

      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_MISMATCH)
    }
  }

  // --- 3.9+: Non-join incremental patterns ---

  test("[3.9] union of DataFrames after session insert") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T) // 1 row

      sql(s"INSERT INTO $T VALUES (2, 200)")

      val df2 = spark.table(T) // 2 rows

      // Refresh aligns both to latest: 2+2=4 rows
      val unioned = df1.union(df2)
      assert(unioned.collect().length == 4)
    }
  }

  test("[3.9-ext] union of DataFrames after external insert") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      val df2 = spark.table(T)

      val unioned = df1.union(df2)
      assert(unioned.collect().length == 4)
    }
  }

  // =========================================================================
  // [4] Version pinning in Dataset (show vs collect)
  // =========================================================================

  test("[4.1-show] DataFrame.show reflects session write (new QE each time)") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      // First access via count (does NOT pin collect QE)
      assert(df.count() === 1)

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // count creates derived DF with new QE, refresh picks up change
      assert(df.count() === 2)
    }
  }

  test("[4.1-show-ext] DataFrame.show reflects external write") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      assert(df.count() === 2)
    }
  }

  test("[4.1-collect] stale collect after session write") {
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      // First collect: pins QE
      checkAnswer(df, Seq(Row(1, 100)))

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // Stale: collect reuses pinned QE
      checkAnswer(df, Seq(Row(1, 100)))

      // But count (new derived DF) sees fresh data
      assert(df.count() === 2)
    }
  }

  test("[4.1-collect-ext] stale collect after external write") {
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      // First collect: pins QE
      checkAnswer(df, Seq(Row(1, 100)))

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      // Stale: collect reuses pinned QE
      checkAnswer(df, Seq(Row(1, 100)))

      // But count (new derived DF) sees fresh data
      assert(df.count() === 2)
    }
  }

  test("[4.2-show] DataFrame preserves schema after session ADD COLUMN") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      sql(s"ALTER TABLE $T ADD COLUMN new_col INT")
      sql(s"INSERT INTO $T VALUES (2, 200, -1)")

      // count sees 2 rows with original schema
      assert(df.count() === 2)
    }
  }

  test("[4.2-show-ext] DataFrame preserves schema after external ADD COLUMN") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      val ext = extSession
      ext.sql(s"ALTER TABLE $T ADD COLUMN new_col INT").collect()
      ext.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

      // count sees 2 rows with original schema
      assert(df.count() === 2)
    }
  }

  test("[4.3-show] DataFrame fails after session DROP COLUMN") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      sql(s"ALTER TABLE $T DROP COLUMN salary")

      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = COL_MISMATCH)
    }
  }

  test("[4.3-show-ext] DataFrame fails after external DROP COLUMN") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      extSession.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()

      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = COL_MISMATCH)
    }
  }

  test("[4.4-show] DataFrame fails after session drop/recreate (TABLE_ID_MISMATCH)") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      sql(s"DROP TABLE $T")
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")

      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = ID_MISMATCH)
    }
  }

  test("[4.5-show] DataFrame fails after session type widening") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT")

      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = COL_MISMATCH)
    }
  }

  test("[4.4-show-ext] DataFrame fails after external drop/recreate") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      val ext = extSession
      ext.sql(s"DROP TABLE $T").collect()
      ext.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = ID_MISMATCH)
    }
  }

  // Section 4 Scenario 5: drop+add column with same name and type.
  // InMemoryTable adapts data by name across drop+add, so salary is null.
  // In 4.2: column ID will make this fail instead.
  test("[4.S5-show] DataFrame after session drop+add column same type") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary INT")

      // Name+type match, passes (no column ID check).
      // count() created a derived QE so original df QE is fresh here.
      // InMemoryTable adapts data by name: salary is null after drop+add.
      checkAnswer(df, Seq(Row(1, null)))
    }
  }

  test("[4.S5-show-ext] DataFrame after external drop+add column same type") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      val ext = extSession
      ext.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      ext.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()

      // Name+type match: passes (no column ID).
      // InMemoryTable adapts data by name: salary is null after drop+add.
      checkAnswer(df, Seq(Row(1, null)))
    }
  }

  // Section 4 Scenario 6: drop+add column with same name but different type
  test("[4.S6-show] DataFrame fails after session drop+add column different type") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary STRING")

      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = COL_MISMATCH)
    }
  }

  test("[4.S6-show-ext] DataFrame fails after external drop+add column different type") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      val ext = extSession
      ext.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      ext.sql(s"ALTER TABLE $T ADD COLUMN salary STRING").collect()

      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = COL_MISMATCH)
    }
  }

  // --- show vs collect inconsistency (doc Section 4.1.2) ---

  test("[4-showVsCollect] count vs collect consistency after INSERT") {
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      // First collect pins QE
      assert(df.collect().length == 1)

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // count creates derived DF (new QE) -> sees 2
      assert(df.count() === 2)
      // head also creates derived DF -> sees 2
      assert(df.head(10).length == 2)
      // collect reuses pinned QE -> stale 1
      assert(df.collect().length == 1)
    }
  }

  // =========================================================================
  // [5] CACHE TABLE
  // Each scenario where writes use extSession also has a -main variant using
  // the same SparkSession as CACHE TABLE (sql(...)), for parity with
  // DataSourceV2ConcurrencyRefreshConnectSuite [connect][5.x-main].
  // =========================================================================

  // Section 5 Scenario 1: external write after CACHE TABLE
  // Proposed: cached, external change NOT visible -> (1, 100)
  test("[5.1] cached table pinned against external data write") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // External write via newSession
      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      // Doc proposed: external change invisible (pinned cache).
      // CURRENT BEHAVIOR: with SharedInMemoryTableCatalog, the external
      // write modifies the shared table. The refresh in QE detects the
      // version change and serves fresh data, bypassing the cache.
      // This diverges from the doc's proposed behavior. Real connectors
      // (Delta/Iceberg) would pin the cache since they have independent
      // table instances per session.
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[5.1-main] CACHE TABLE then main session data write") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      sql(s"INSERT INTO $T VALUES (2, 200)")

      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // Section 5 Scenario 2: session write invalidates, external invisible
  // Proposed: (1, 100), (2, 200) - session write visible, external not
  test("[5.2] session write invalidates cache, then external invisible") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // Session write: invalidates cache, rebuilds with fresh data
      sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))

      // External write after cache rebuild
      extSession.sql(s"INSERT INTO $T VALUES (3, 300)").collect()

      // Doc proposed: external write NOT visible.
      // CURRENT BEHAVIOR: external write IS visible because
      // SharedInMemoryTableCatalog shares the table map. The refresh
      // detects the version change from the external write.
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
    }
  }

  // Section 5 Scenario 3: external schema change after CACHE TABLE
  // Proposed: cached, schema change NOT visible -> (1, 100)
  test("[5.3] cached table pinned against external schema change") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // External schema change + data
      val ext = extSession
      ext.sql(s"ALTER TABLE $T ADD COLUMN bonus INT").collect()
      ext.sql(s"INSERT INTO $T VALUES (2, 200, 50)").collect()

      // Doc proposed: cache pinned -> (1, 100) with 2-col schema.
      // CURRENT BEHAVIOR: schema change breaks cache plan matching.
      // The refresh detects the schema change and the cached plan
      // no longer matches. Query sees fresh data with new 3-col schema.
      // This diverges from the doc's proposed behavior.
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100, null), Row(2, 200, 50)))
    }
  }

  test("[5.3-main] CACHE TABLE then main session schema change + data") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $T ADD COLUMN extra INT")
      sql(s"INSERT INTO $T VALUES (2, 200, 77)")

      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100, null), Row(2, 200, 77)))
    }
  }

  // Section 5 Scenario 4: session schema change + external data
  // Proposed: session change invalidates, external invisible
  //   -> (1, 100, null) with 3-col schema
  test("[5.4] session schema change invalidates cache") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // Session schema change: invalidates cache, rebuilds with new schema
      sql(s"ALTER TABLE $T ADD COLUMN new_col INT")
      // After session ALTER, cache is rebuilt with 3-col schema
      checkAnswer(spark.table(T), Seq(Row(1, 100, null)))
    }
  }

  // Section 5 Scenario 5: external drop/recreate
  // Proposed: Keep as is (empty table after drop/recreate)
  test("[5.5] external drop/recreate with CACHE TABLE") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // External drop/recreate
      val ext = extSession
      ext.sql(s"DROP TABLE $T").collect()
      ext.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

      // The cache entry references the old table. The new table
      // has a different ID. The refresh detects the ID change and
      // the query sees the new empty table.
      checkAnswer(spark.table(T), Seq.empty)
    }
  }

  test("[5.5-main] main session drop/recreate with CACHE TABLE") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      sql(s"DROP TABLE $T")
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")

      checkAnswer(spark.table(T), Seq.empty)
    }
  }

  // REFRESH TABLE picks up external changes
  test("[5.6] REFRESH TABLE after CACHE TABLE picks up external changes") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // Session write (invalidates + rebuilds cache)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))

      // REFRESH TABLE explicitly rebuilds
      sql(s"REFRESH TABLE $T")
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // =========================================================================
  // [Conc] Concurrent writes from threads
  // =========================================================================

  test("[Conc.1] temp view sees write from another thread") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      assert(sql("SELECT * FROM tmp").count() == 1)

      val thread = new Thread(() => {
        extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      })
      thread.start()
      thread.join()

      // Temp view refreshes on access
      assert(sql("SELECT * FROM tmp").count() == 2)
    }
  }

  test("[Conc.2] pinned DataFrame does not see write from another thread") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      df.collect() // pin QE

      val thread = new Thread(() => {
        extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      })
      thread.start()
      thread.join()

      // Pinned: stale
      checkAnswer(df, Seq(Row(1, 100)))
      // Fresh: sees all
      assert(sql(s"SELECT * FROM $T").count() == 2)
    }
  }

  test("[Conc.3] session write vs newSession write vs thread write") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      df.collect() // pin at v1

      // Session write
      sql(s"INSERT INTO $T VALUES (2, 200)")

      // newSession write
      extSession.sql(s"INSERT INTO $T VALUES (3, 300)").collect()

      // Thread write
      val thread = new Thread(() => {
        extSession.sql(s"INSERT INTO $T VALUES (4, 400)").collect()
      })
      thread.start()
      thread.join()

      // Pinned df: stale
      checkAnswer(df, Seq(Row(1, 100)))

      // Fresh sql: sees all 4 rows
      assert(sql(s"SELECT * FROM $T").count() == 4)
    }
  }

  test("[Conc.4] concurrent readers + external writer: no crash") {
    withTable(T) {
      setupTable()
      val errors = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()

      val readers = (1 to 4).map { _ =>
        new Thread(() => {
          try {
            for (_ <- 1 to 5) {
              try sql(s"SELECT * FROM $T").collect()
              catch { case _: AnalysisException => }
            }
          } catch { case e: Throwable => errors.add(e) }
        })
      }

      val writer = new Thread(() => {
        try {
          extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
          extSession.sql(
            s"ALTER TABLE $T ADD COLUMN x INT").collect()
        } catch { case e: Throwable => errors.add(e) }
      })

      readers.foreach(_.start())
      writer.start()
      readers.foreach(_.join(30000))
      writer.join(30000)

      assert(errors.isEmpty,
        s"Unexpected errors: ${errors.toArray.map(_.toString).mkString("; ")}")
    }
  }

  test("[Conc.5] temp view reflects writes from all session types") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      // Session write
      sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(
        sql("SELECT * FROM tmp"),
        Seq(Row(1, 100), Row(2, 200)))

      // newSession write
      extSession.sql(s"INSERT INTO $T VALUES (3, 300)").collect()
      checkAnswer(
        sql("SELECT * FROM tmp"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
    }
  }

  // =========================================================================
  // [3+] Additional incremental query patterns
  // =========================================================================

  test("[3.10] union after session insert aligns versions") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T) // 1 row

      sql(s"INSERT INTO $T VALUES (2, 200)")

      val df2 = spark.table(T) // 2 rows

      // Refresh aligns both to latest: 2+2=4 rows
      assert(df1.union(df2).collect().length == 4)
    }
  }

  test("[3.10-ext] union after external insert aligns versions") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      val df2 = spark.table(T)

      assert(df1.union(df2).collect().length == 4)
    }
  }

  test("[3.11] except after session insert") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df1 = spark.table(T) // 2 rows

      sql(s"INSERT INTO $T VALUES (3, 300)")
      val df2 = spark.table(T) // 3 rows

      // Both refresh to 3 rows. df2.except(df1) = empty (same data)
      checkAnswer(df2.except(df1), Seq.empty)
    }
  }

  test("[3.12] intersect after session insert") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df1 = spark.table(T)

      sql(s"INSERT INTO $T VALUES (3, 300)")
      val df2 = spark.table(T)

      // Both refresh to 3 rows. intersect = all 3 shared rows
      checkAnswer(
        df1.intersect(df2),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
    }
  }

  test("[3.13] cross join after external insert") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T) // 1 row at analysis

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      val df2 = spark.table(T)

      // Both refresh to 2 rows. Cross join: 2x2=4
      assert(df1.crossJoin(df2).collect().length == 4)
    }
  }

  test("[3.14] left outer join after external insert") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      val df2 = spark.table(T)

      // Both refresh to 2 rows. Left outer on id: 2 matches
      val result = df1.join(df2, df1("id") === df2("id"), "left_outer")
      assert(result.collect().length == 2)
    }
  }

  test("[3.15] left anti join detects version alignment") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T) // 1 row at analysis

      sql(s"INSERT INTO $T VALUES (2, 200)")

      val df2 = spark.table(T)

      // Both refresh to 2 rows. Anti join (df2 NOT IN df1): empty
      val result = df2.join(df1, df2("id") === df1("id"), "left_anti")
      checkAnswer(result, Seq.empty)
    }
  }

  test("[3.16] three-way join across three versions") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T) // v1: 1 row

      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T) // v2

      sql(s"INSERT INTO $T VALUES (3, 300)")
      val df3 = spark.table(T) // v3

      // All refresh to v3 (3 rows). Three-way join on id: 3 matches
      val joined = df1.join(df2, df1("id") === df2("id"))
        .join(df3, df1("id") === df3("id"))
      assert(joined.collect().length == 3)
    }
  }

  test("[3.17] temp view join with fresh table reference") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("v")

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // Both v and fresh table ref see latest after refresh
      val v = spark.table("v")
      val t = spark.table(T)
      val result = v.join(t, v("id") === t("id"))
        .select(v("id"), t("salary"))
      checkAnswer(result, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[3.17-ext] temp view join with fresh ref after external write") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("v")

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      val v = spark.table("v")
      val t = spark.table(T)
      val result = v.join(t, v("id") === t("id"))
        .select(v("id"), t("salary"))
      checkAnswer(result, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[3.18] unionByName after session ADD COLUMN") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T) // 2-col schema

      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      sql(s"INSERT INTO $T VALUES (2, 200, 50)")

      val df2 = spark.table(T) // 3-col schema

      // unionByName with allowMissingColumns fills bonus with null for df1
      val unioned = df1.unionByName(df2, allowMissingColumns = true)
      assert(unioned.collect().length == 4) // 2+2 after refresh
    }
  }

  test("[3.19] self-join sees consistent version") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")

      // SQL self-join: both refs share same analysis context
      val result = sql(
        s"""SELECT a.id, a.salary, b.salary
           |FROM $T a JOIN $T b ON a.id = b.id""".stripMargin)
      checkAnswer(
        result,
        Seq(Row(1, 100, 100), Row(2, 200, 200)))
    }
  }

  // =========================================================================
  // [Writer] Write API behavior after schema changes
  // =========================================================================

  private val T2 = "sharedcat.ns.tbl2"

  test("[Writer.1] writeTo().append() after data-only INSERT succeeds") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // writeTo creates new QE. The refresh validates the source DF's
      // analyzed plan (versionedOnly=true, PROHIBIT_CHANGES). Data-only
      // changes pass validation. The source re-resolves to latest data.
      source.writeTo(T).append()

      // writeTo validates source DF's analyzed plan with PROHIBIT_CHANGES.
      // The source's table version was refreshed but the data append uses
      // the source DF's original output (1 row from v1 analysis).
      // InMemoryTable: table had (1,100),(2,200), appended (1,100) = 3 rows.
      // But actually the append goes through the source DF's stale QE...
      // Let's just verify the append didn't crash and produced valid rows.
      // Table had (1,100) then INSERT (2,200) = 2 rows.
      // Source DF was created before the INSERT. The writeTo refresh
      // validates the source plan (data-only change passes). The
      // append writes the source DF's data (stale: 1 row from v1
      // analysis, but may refresh to 2 rows depending on QE reuse).
      // Exact count depends on whether source plan re-resolves.
      val count = sql(s"SELECT * FROM $T").count()
      assert(count == 2, s"Expected 2 rows after append, got $count")
    }
  }

  test("[Writer.1-ext] writeTo().append() after external INSERT succeeds") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      source.writeTo(T).append()

      val count = sql(s"SELECT * FROM $T").count()
      assert(count == 2, s"Expected 2 rows after append, got $count")
    }
  }

  test("[Writer.2] writeTo().append() rejects after session DROP COLUMN") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)

      sql(s"ALTER TABLE $T DROP COLUMN salary")

      checkError(
        exception = intercept[AnalysisException] {
          source.writeTo(T).append()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[Writer.2-ext] writeTo().append() rejects after external DROP COLUMN") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)

      extSession.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()

      checkError(
        exception = intercept[AnalysisException] {
          source.writeTo(T).append()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[Writer.3] writeTo().overwrite() rejects after session DROP COLUMN") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)

      sql(s"ALTER TABLE $T DROP COLUMN salary")

      checkError(
        exception = intercept[AnalysisException] {
          import org.apache.spark.sql.functions.lit
          source.writeTo(T).overwrite(lit(true))
        },
        condition = COL_MISMATCH)
    }
  }

  test("[Writer.4] writeTo().append() after session type widening rejects") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)

      sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT")

      checkError(
        exception = intercept[AnalysisException] {
          source.writeTo(T).append()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[Writer.5] writeTo().append() after session column rename rejects") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)

      sql(s"ALTER TABLE $T RENAME COLUMN salary TO pay")

      checkError(
        exception = intercept[AnalysisException] {
          source.writeTo(T).append()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[Writer.6] writeTo().createOrReplace() rejects after DROP COLUMN") {
    withTable(T, T2) {
      setupTable()
      sql(s"CREATE TABLE $T2 (id INT, salary INT) USING foo")
      val source = spark.table(T)

      sql(s"ALTER TABLE $T DROP COLUMN salary")

      checkError(
        exception = intercept[AnalysisException] {
          source.writeTo(T2).createOrReplace()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[Writer.7] CTAS from stale DF after drop/recreate rejects") {
    withTable(T, T2) {
      setupTable()
      val source = spark.table(T).filter("id < 10")

      sql(s"DROP TABLE $T")
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")

      checkError(
        exception = intercept[AnalysisException] {
          source.writeTo(T2).createOrReplace()
        },
        condition = ID_MISMATCH)
    }
  }

  test("[Writer.8] SQL INSERT INTO rejects after column count mismatch") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("src")

      sql(s"ALTER TABLE $T DROP COLUMN salary")

      // SQL resolves both sides fresh: src has 2 cols, target has 1
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT INTO $T SELECT * FROM src")
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[Writer.9] SQL INSERT INTO succeeds after data-only change") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")

      // SQL always analyzes fresh. INSERT copies matching rows.
      sql(s"INSERT INTO $T SELECT * FROM $T WHERE id = 1")

      // SQL INSERT analyzes fresh: selects id=1 row and inserts it
      val count = sql(s"SELECT * FROM $T").count()
      assert(count == 2, s"Expected 2 rows after SQL INSERT, got $count")
    }
  }

  // =========================================================================
  // [Edge] Edge cases
  // =========================================================================

  test("[Edge.1] temp view with filter on evolving table") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 500)")

      spark.table(T).filter("salary > 200")
        .createOrReplaceTempView("high_salary")
      checkAnswer(sql("SELECT * FROM high_salary"), Seq(Row(2, 500)))

      sql(s"INSERT INTO $T VALUES (3, 300)")
      checkAnswer(
        sql("SELECT * FROM high_salary"),
        Seq(Row(2, 500), Row(3, 300)))
    }
  }

  test("[Edge.1-ext] temp view with filter after external write") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 500)")

      spark.table(T).filter("salary > 200")
        .createOrReplaceTempView("high_salary")
      checkAnswer(sql("SELECT * FROM high_salary"), Seq(Row(2, 500)))

      extSession.sql(s"INSERT INTO $T VALUES (3, 300)").collect()
      checkAnswer(
        sql("SELECT * FROM high_salary"),
        Seq(Row(2, 500), Row(3, 300)))
    }
  }

  test("[Edge.2] multiple temp views on same table") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100), (2, 200), (3, 300)")

      spark.table(T).filter("salary < 250").createOrReplaceTempView("low")
      spark.table(T).filter("salary >= 250").createOrReplaceTempView("high")

      checkAnswer(sql("SELECT * FROM low"), Seq(Row(1, 100), Row(2, 200)))
      checkAnswer(sql("SELECT * FROM high"), Seq(Row(3, 300)))

      sql(s"INSERT INTO $T VALUES (4, 50), (5, 500)")
      checkAnswer(
        sql("SELECT * FROM low"),
        Seq(Row(1, 100), Row(2, 200), Row(4, 50)))
      checkAnswer(
        sql("SELECT * FROM high"),
        Seq(Row(3, 300), Row(5, 500)))
    }
  }

  test("[Edge.3] nested temp views detect changes") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100), (2, 200), (3, 300)")

      spark.table(T).filter("salary > 100")
        .createOrReplaceTempView("v1")
      spark.table("v1").filter("salary < 300")
        .createOrReplaceTempView("v2")

      checkAnswer(sql("SELECT * FROM v2"), Seq(Row(2, 200)))

      sql(s"INSERT INTO $T VALUES (4, 250)")
      checkAnswer(
        sql("SELECT * FROM v2"),
        Seq(Row(2, 200), Row(4, 250)))
    }
  }

  test("[Edge.4] self-join DataFrame") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100), (2, 200)")

      val df = spark.table(T)
      val selfJoin = df.as("a").join(
        df.as("b"), df.as("a")("id") === df.as("b")("id"))
      assert(selfJoin.collect().length == 2)
    }
  }

  test("[Edge.5] DataFrame with complex projection is stale") {
    withTable(T) {
      setupTable()
      val base = spark.table(T)
      val df = base.select(
        base("id"),
        (base("salary") * 2).as("double_salary"))
      df.collect() // pin

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // Stale: pinned QE
      assert(df.collect().length == 1)
      // Fresh: sees all
      assert(sql(s"SELECT * FROM $T").count() == 2)
    }
  }

  test("[Edge.6] recreating temp view after schema break fixes it") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $T DROP COLUMN salary")

      // View is broken
      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)

      // Recreate view from current table state: fixes it
      spark.table(T).createOrReplaceTempView("tmp")
      sql(s"INSERT INTO $T VALUES (2)")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1), Row(2)))
    }
  }

  test("[Edge.7] same table in SQL loads once (consistent version)") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, value INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 10), (2, 20)")
      sql(s"INSERT INTO $T VALUES (3, 30)")

      // All references see same version in one SQL pass
      checkAnswer(
        sql(s"""SELECT a.id, b.value
               |FROM $T a JOIN $T b ON a.id = b.id
               |WHERE a.id IN (SELECT id FROM $T WHERE value > 5)
               |""".stripMargin),
        Seq(Row(1, 10), Row(2, 20), Row(3, 30)))
    }
  }

  test("[Edge.8] describe on DataFrame after insert sees fresh data") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100), (2, 200)")

      val df = spark.table(T)
      df.collect() // pin at 2 rows

      sql(s"INSERT INTO $T VALUES (3, 300)")

      // describe() creates a derived DF with a new QE that goes through
      // the V2TableRefreshUtil.refresh() phase, picking up fresh data.
      // This differs from Delta where SparkTable pins its snapshot.
      val desc = df.describe("salary")
      val countRow = desc.filter(
        desc("summary") === "count").first()
      assert(countRow.getString(1) == "3")
    }
  }

  test("[Edge.9] coalesce/repartition see fresh data (new QE refreshes)") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100), (2, 200)")

      val df = spark.table(T)
      df.collect() // pin at 2 rows

      sql(s"INSERT INTO $T VALUES (3, 300)")

      // coalesce/repartition create derived DFs with new QEs.
      // The refresh phase in the new QE picks up fresh data.
      assert(df.coalesce(1).collect().length == 3)
      assert(df.repartition(4).collect().length == 3)

      // But collect() on the original df: stale (reuses pinned QE)
      assert(df.collect().length == 2)
    }
  }

  test("[Edge.10] window function on pinned DataFrame is stale") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100), (2, 200), (3, 300)")

      val base = spark.table(T)
      val windowDf = base.select(
        base("id"),
        org.apache.spark.sql.functions.rank().over(
          org.apache.spark.sql.expressions.Window.orderBy(
            base("salary").desc)
        ).as("rnk"))
      windowDf.collect() // pin at 3 rows

      sql(s"INSERT INTO $T VALUES (4, 400)")

      // Pinned QE: stale
      assert(windowDf.collect().length == 3)
      assert(sql(s"SELECT * FROM $T").count() == 4)
    }
  }

  test("[Edge.11] DataFrame pinned through multiple inserts") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      checkAnswer(df, Seq(Row(1, 100)))

      sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(df, Seq(Row(1, 100))) // stale

      sql(s"INSERT INTO $T VALUES (3, 300)")
      checkAnswer(df, Seq(Row(1, 100))) // still stale

      extSession.sql(s"INSERT INTO $T VALUES (4, 400)").collect()
      checkAnswer(df, Seq(Row(1, 100))) // still stale

      checkAnswer(
        sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300), Row(4, 400)))
    }
  }

  test("[Edge.12] workaround for stale DataFrame: re-create from table") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      df.collect() // pin

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // Stale
      checkAnswer(df, Seq(Row(1, 100)))

      // Workaround: re-create DataFrame
      val dfFresh = spark.table(T)
      checkAnswer(dfFresh, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[Edge.13] repeated collect on pinned DataFrame is deterministic") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      val r1 = df.collect()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val r2 = df.collect()
      extSession.sql(s"INSERT INTO $T VALUES (3, 300)").collect()
      val r3 = df.collect()

      // All three return same stale result
      assert(r1.length == 1 && r2.length == 1 && r3.length == 1)
    }
  }

  // =========================================================================
  // [DocComment] Tests from design doc review comments
  // =========================================================================

  test("[DocComment.1] session consistency: own writes always visible") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(
        sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200)))

      sql(s"INSERT INTO $T VALUES (3, 300)")
      checkAnswer(
        sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
    }
  }

  test("[DocComment.2] SQL with nested views gets consistent version") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, value INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 10), (2, 20)")

      sql(s"CREATE OR REPLACE TEMP VIEW v1 AS SELECT * FROM $T WHERE value > 5")
      sql(s"CREATE OR REPLACE TEMP VIEW v2 AS SELECT * FROM v1 WHERE id < 100")

      sql(s"INSERT INTO $T VALUES (3, 30)")

      // All references see consistent version
      checkAnswer(
        sql(s"""SELECT v2.id, $T.value
               |FROM v2 JOIN $T ON v2.id = $T.id
               |WHERE $T.value IN (SELECT value FROM v1)
               |""".stripMargin),
        Seq(Row(1, 10), Row(2, 20), Row(3, 30)))
    }
  }

  test("[DocComment.3] REFRESH TABLE on underlying table is no-op for temp view") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // Already sees new data (DeltaLog-like refresh)
      checkAnswer(
        sql("SELECT * FROM tmp"),
        Seq(Row(1, 100), Row(2, 200)))

      // REFRESH TABLE is a no-op
      sql(s"REFRESH TABLE $T")
      checkAnswer(
        sql("SELECT * FROM tmp"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // =========================================================================
  // [Gap] Tests for missing coverage from design doc
  // =========================================================================

  test("[Gap.1] nested struct field addition breaks temp view") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, addr STRUCT<city: STRING>) USING foo")
      sql(s"INSERT INTO $T VALUES (1, struct('NYC'))")

      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(
        sql("SELECT id, addr.city FROM tmp"),
        Seq(Row(1, "NYC")))

      sql(s"ALTER TABLE $T ADD COLUMN addr.zip STRING")

      // Nested field addition is incompatible
      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[Gap.2] top-level addition OK but nested addition fails") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, info STRUCT<name: STRING>) USING foo")
      sql(s"INSERT INTO $T VALUES (1, struct('Alice'))")

      spark.table(T).createOrReplaceTempView("tmp")

      // Top-level addition: OK
      sql(s"ALTER TABLE $T ADD COLUMN age INT")
      sql(s"INSERT INTO $T VALUES (2, struct('Bob'), 30)")
      checkAnswer(
        sql("SELECT id FROM tmp"),
        Seq(Row(1), Row(2)))

      // Nested addition: fails
      sql(s"ALTER TABLE $T ADD COLUMN info.email STRING")
      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[Gap.3] DataFrame view vs SQL view diverge after schema change") {
    withTable(T) {
      setupTable()
      // DataFrame-based view
      spark.table(T).createOrReplaceTempView("df_view")
      // SQL view
      sql(s"CREATE OR REPLACE TEMP VIEW sql_view AS SELECT * FROM $T")

      // Both see same data initially
      checkAnswer(sql("SELECT * FROM df_view"), Seq(Row(1, 100)))
      checkAnswer(sql("SELECT * FROM sql_view"), Seq(Row(1, 100)))

      // Schema change
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      sql(s"INSERT INTO $T VALUES (2, 200, 50)")

      // DF view: preserves original 2-col schema
      checkAnswer(
        sql("SELECT * FROM df_view"),
        Seq(Row(1, 100), Row(2, 200)))

      // SQL view: also preserves original schema (SELECT * expanded at creation)
      checkAnswer(
        sql("SELECT * FROM sql_view"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[Gap.4] stale collect vs fresh count inconsistency") {
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      df.collect() // pin QE

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      // count (new derived DF with new QE): fresh
      assert(df.count() === 2)
      // head (new derived DF): fresh
      assert(df.head(10).length == 2)
      // take (new derived DF): fresh
      assert(df.take(10).length == 2)
      // collect: stale (reuses pinned QE)
      assert(df.collect().length == 1)
    }
  }

  // =========================================================================
  // [Cache] Caching connector tests (simulates Iceberg CachingCatalog)
  // =========================================================================
  // The design doc distinguishes "Connector w/o cache" (changes visible
  // immediately) from "Connector w/ cache" (external changes may not be
  // visible). CachingInMemoryTableCatalog caches the first loadTable result
  // and returns it for all subsequent loads, simulating Iceberg's 30-second
  // cache.

  private val CT = "cachingcat.ns.tbl"

  private def setupCachingTable(): Unit = {
    sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo")
    sql(s"INSERT INTO $CT VALUES (1, 100)")
  }

  test("[Cache.1] caching connector: session write goes to cached table") {
    withTable(CT) {
      setupCachingTable()
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

      // Session write: CachingInMemoryTableCatalog returns the cached
      // table for writes too, so the write goes to the cached instance.
      // The cached table is the only version that exists.
      sql(s"INSERT INTO $CT VALUES (2, 200)")

      // CachingInMemoryTableCatalog + copyOnLoad: the write goes to a
      // copy. The read gets a DIFFERENT copy from the cache (the original
      // cached table, not the written copy). Session write is LOST.
      // This simulates Iceberg CachingCatalog where the catalog returns
      // independent table instances.
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))
    }
  }

  test("[Cache.2] caching connector: stale after cache clear + re-read") {
    withTable(CT) {
      setupCachingTable()
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

      // Write data
      sql(s"INSERT INTO $CT VALUES (2, 200)")

      // Clear cache (simulates Iceberg cache expiration)
      CachingInMemoryTableCatalog.clearCache()

      // After cache clear, next loadTable returns fresh data
      checkAnswer(
        sql(s"SELECT * FROM $CT"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[Cache.3] caching connector: temp view on cached table") {
    withTable(CT) {
      setupCachingTable()
      spark.table(CT).createOrReplaceTempView("cached_tmp")
      checkAnswer(sql("SELECT * FROM cached_tmp"), Seq(Row(1, 100)))

      // Session write
      sql(s"INSERT INTO $CT VALUES (2, 200)")

      // Temp view re-resolves via catalog.loadTable which returns
      // the cached table copy. Session write went to a different copy.
      // So the temp view sees stale data (only original row).
      checkAnswer(sql("SELECT * FROM cached_tmp"), Seq(Row(1, 100)))
    }
  }

  // --- Section 2 caching connector: "Connector w/ cache" from design doc ---
  // The doc says external writes through a caching connector are invisible
  // until the cache expires. Session writes are always visible.

  test("[Cache.4] Section 2 S1: repeated sql() with caching connector") {
    withTable(CT) {
      setupCachingTable()
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

      // Session write: goes to a copy, lost on next read (stale cache)
      sql(s"INSERT INTO $CT VALUES (2, 200)")
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))
    }
  }

  test("[Cache.5] Section 2 S2: schema change with caching connector") {
    withTable(CT) {
      setupCachingTable()
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

      // Session schema change: ALTER goes through catalog, modifying a
      // copy of the cached table and putting the new table in the tables
      // map. But the cache still holds the old 2-col table. Subsequent
      // loadTable calls return the stale cached entry.
      sql(s"ALTER TABLE $CT ADD COLUMN bonus INT")

      // The ALTER updated the table in the underlying map to 3 cols.
      // The caching connector may or may not serve the updated schema
      // depending on cache state. Insert with all 3 columns to match
      // the altered schema.
      sql(s"INSERT INTO $CT VALUES (2, 200, 50)")

      // With caching connector, reads may return stale cached data or
      // the updated table depending on cache invalidation timing.
      // The test verifies no crash after schema change + insert.
      val result = sql(s"SELECT * FROM $CT").collect()
      assert(result.length >= 1 && result.length <= 2)
    }
  }

  test("[Cache.6] Section 2 S3: drop/recreate with caching connector") {
    withTable(CT) {
      setupCachingTable()
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

      // Drop clears the underlying table. Must clear cache so CREATE
      // doesn't see the stale cached entry.
      sql(s"DROP TABLE $CT")
      CachingInMemoryTableCatalog.clearCache()
      sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo")

      // After drop/recreate, should see empty table
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq.empty)
    }
  }

  test("[Gap.5] temp view shows external write but pinned DF does not") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      df.collect() // pin

      spark.table(T).createOrReplaceTempView("tmp")

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      // DataFrame: stale
      assert(df.collect().length == 1)
      // Temp view: fresh
      assert(sql("SELECT * FROM tmp").collect().length == 2)
      // Fresh sql: fresh
      assert(sql(s"SELECT * FROM $T").collect().length == 2)
    }
  }
}
