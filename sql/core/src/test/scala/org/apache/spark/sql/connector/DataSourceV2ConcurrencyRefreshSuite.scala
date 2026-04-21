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

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Tests for DSv2 column ID validation across access patterns and modifications.
 *
 * Systematically tests column ID detection (drop+add column same/different type)
 * across access patterns: temp view, join, DataFrame, subquery, set operations,
 * phase-locked interleaving, CTAS, and null table ID scenarios.
 *
 * Uses parameterized modification types to verify that column ID mismatch is
 * detected correctly while other modifications (data write, column add/remove,
 * type widening, drop/recreate) produce their expected error conditions.
 */
class DataSourceV2ConcurrencyRefreshSuite
  extends QueryTest with SharedSparkSession {

  // Error condition constants
  private val COL_MISMATCH =
    "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH"
  private val COL_ID_MISMATCH =
    "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH"
  private val ID_MISMATCH =
    "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH"
  private val VIEW_PLAN_CHANGED =
    "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION"
  private val SQL_VIEW_CHANGED =
    "INCOMPATIBLE_VIEW_SCHEMA_CHANGE"

  // In parameterized tests the exact error parameters (table names, column
  // details, view DDL suggestions) vary per mod. Override the ignorable set
  // so checkError validates the condition without requiring exact params.
  // Non-parameterized tests that pass explicit parameters still check them
  // (parameters override the ignorable set per the base implementation).
  override protected def checkErrorIgnorableParameters
    : Map[String, Set[String]] =
    super.checkErrorIgnorableParameters ++ Map(
      COL_MISMATCH ->
        Set("tableName", "errors"),
      COL_ID_MISMATCH ->
        Set("tableName", "errors"),
      ID_MISMATCH ->
        Set("tableName", "capturedTableId", "currentTableId"),
      VIEW_PLAN_CHANGED ->
        Set("viewName", "tableName", "colType", "errors"),
      SQL_VIEW_CHANGED ->
        Set("viewName", "colName", "expectedNum",
          "actualCols", "suggestion"),
      "CANNOT_UP_CAST_DATATYPE" ->
        Set("expression", "sourceType", "targetType",
          "details"),
      "NUM_COLUMNS_MISMATCH" ->
        Set("operator", "firstNumColumns", "secondNumColumns",
          "invalidOrdinalNum", "invalidNumColumns"),
      "UNRESOLVED_COLUMN.WITH_SUGGESTION" ->
        Set("objectName", "proposal"),
      "UNRESOLVED_COLUMN_AMONG_FIELD_NAMES" ->
        Set("colName", "fieldNames"))

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED, true)
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")
    .set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat2.copyOnLoad", "true")
    // Null ID: tables without identity tracking
    .set("spark.sql.catalog.nullidcat",
      classOf[NullIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullidcat.copyOnLoad", "true")

  override def afterEach(): Unit = {
    try {
      spark.sessionState.catalogManager.reset()
    } finally {
      super.afterEach()
    }
  }

  // =====================================================================
  // Infrastructure
  // =====================================================================

  private val T = "testcat.ns1.ns2.tbl"
  private val IDENT = Identifier.of(Array("ns1", "ns2"), "tbl")

  private def cat: InMemoryTableCatalog =
    spark.sessionState.catalogManager.catalog("testcat").asInstanceOf[InMemoryTableCatalog]

  private def setupTable(): Unit = {
    sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
    sql(s"INSERT INTO $T VALUES (1, 100)")
  }

  // =====================================================================
  // Modification Definitions
  // =====================================================================

  // Expected result rows for OK cases. Error conditions for failure cases.
  case class Mod(
      name: String,
      fn: String => Unit,
      tempViewOk: Boolean,
      dfOk: Boolean,
      joinOk: Boolean,
      tempViewRows: Seq[Row] = Nil,
      dfRows: Seq[Row] = Nil,
      joinRows: Seq[Row] = Nil,
      // Error conditions when the modification breaks the access pattern.
      // Defaults cover most schema change failures; override for special
      // cases like drop/recreate (TABLE_ID_MISMATCH).
      dfCondition: String = COL_MISMATCH,
      viewPlanCondition: String = VIEW_PLAN_CHANGED,
      sqlViewCondition: String = SQL_VIEW_CHANGED)

  private val mods: Seq[Mod] = Seq(
    Mod("data write",
      t => sql(s"INSERT INTO $t VALUES (2, 200)"),
      tempViewOk = true, dfOk = true, joinOk = true,
      tempViewRows = Seq(Row(1, 100), Row(2, 200)),
      dfRows = Seq(Row(1, 100), Row(2, 200)),
      joinRows = Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200))),
    Mod("column addition",
      t => sql(s"ALTER TABLE $t ADD COLUMN new_col INT"),
      tempViewOk = true, dfOk = true, joinOk = true,
      tempViewRows = Seq(Row(1, 100)),
      dfRows = Seq(Row(1, 100)),
      joinRows = Seq(Row(1, 100, 1, 100, null))),
    Mod("column removal",
      t => sql(s"ALTER TABLE $t DROP COLUMN salary"),
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("column rename",
      t => sql(s"ALTER TABLE $t RENAME COLUMN salary TO pay"),
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("type widening INT to BIGINT",
      t => sql(s"ALTER TABLE $t ALTER COLUMN salary TYPE BIGINT"),
      tempViewOk = false, dfOk = false, joinOk = false,
      sqlViewCondition = "CANNOT_UP_CAST_DATATYPE"),
    Mod("drop+add column same type",
      t => { sql(s"ALTER TABLE $t DROP COLUMN salary")
             sql(s"ALTER TABLE $t ADD COLUMN salary INT") },
      // Temp views re-resolve via V2TableReference which does not check column IDs.
      // DataFrames/joins use V2TableRefreshUtil.refresh which detects column ID changes.
      // InMemoryTable returns the original value for the re-added column.
      tempViewOk = true, dfOk = false, joinOk = false,
      tempViewRows = Seq(Row(1, 100)),
      dfCondition = COL_ID_MISMATCH),
    Mod("drop+add column different type",
      t => { sql(s"ALTER TABLE $t DROP COLUMN salary")
             sql(s"ALTER TABLE $t ADD COLUMN salary STRING") },
      tempViewOk = false, dfOk = false, joinOk = false,
      // Column ID check runs before schema validation, so
      // COLUMN_ID_MISMATCH is detected before COLUMNS_MISMATCH.
      dfCondition = COL_ID_MISMATCH,
      sqlViewCondition = "CANNOT_UP_CAST_DATATYPE"),
    Mod("drop/recreate table",
      t => { sql(s"DROP TABLE $t")
             sql(s"CREATE TABLE $t (id INT, salary INT) USING foo") },
      tempViewOk = true, dfOk = false, joinOk = false,
      tempViewRows = Seq.empty,
      dfCondition = ID_MISMATCH))

  // External modifications via catalog API (bypass session cache invalidation).
  // These simulate "someone else changed the table" without going through
  // SparkSession SQL, so session cache invalidation is NOT triggered.
  private val extMods: Seq[Mod] = Seq(
    Mod("ext column addition",
      _ => cat.alterTable(IDENT,
        TableChange.addColumn(Array("new_col"), IntegerType, true)),
      tempViewOk = true, dfOk = true, joinOk = true,
      tempViewRows = Seq(Row(1, 100)),
      dfRows = Seq(Row(1, 100)),
      joinRows = Seq(Row(1, 100, 1, 100, null))),
    Mod("ext column removal",
      _ => cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false)),
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("ext column rename",
      _ => cat.alterTable(IDENT,
        TableChange.renameColumn(Array("salary"), "pay")),
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("ext type widening",
      _ => cat.alterTable(IDENT,
        TableChange.updateColumnType(Array("salary"), LongType)),
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("ext data truncation",
      _ => cat.loadTable(IDENT, util.Set.of(TableWritePrivilege.DELETE))
              .asInstanceOf[TruncatableTable].truncateTable(),
      tempViewOk = true, dfOk = true, joinOk = true,
      tempViewRows = Seq.empty,
      dfRows = Seq.empty,
      joinRows = Seq.empty),
    Mod("ext drop+add column same type",
      _ => { cat.alterTable(IDENT,
               TableChange.deleteColumn(Array("salary"), false))
             cat.alterTable(IDENT,
               TableChange.addColumn(
                 Array("salary"), IntegerType, true)) },
      // Temp views re-resolve via V2TableReference (no column ID check).
      // DataFrames/joins detect column ID changes via refresh.
      // InMemoryTable returns the original value for the re-added column.
      tempViewOk = true, dfOk = false, joinOk = false,
      tempViewRows = Seq(Row(1, 100)),
      dfCondition = COL_ID_MISMATCH),
    Mod("ext drop+add column different type",
      _ => { cat.alterTable(IDENT,
               TableChange.deleteColumn(Array("salary"), false))
             cat.alterTable(IDENT,
               TableChange.addColumn(
                 Array("salary"), StringType, true)) },
      tempViewOk = false, dfOk = false, joinOk = false,
      // Column ID check runs before schema validation.
      dfCondition = COL_ID_MISMATCH))

  // =====================================================================
  // Section 6: Subquery Same Table x All Modifications
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S6] subquery same table: ${mod.name}") {
      withTable(T) {
        setupTable()
        val df = spark.sql(
          s"SELECT * FROM $T WHERE id IN (SELECT id FROM $T)")
        mod.fn(T)
        if (mod.dfOk) {
          checkAnswer(df, mod.dfRows)
        } else {
          checkError(
            exception = intercept[AnalysisException] { df.collect() },
            condition = mod.dfCondition)
        }
      }
    }
  }

  // Section 6 external: subquery with external modifications
  extMods.foreach { mod =>
    test(s"[S6-ext] subquery same table: ${mod.name}") {
      withTable(T) {
        setupTable()
        val df = spark.sql(
          s"SELECT * FROM $T WHERE id IN (SELECT id FROM $T)")
        mod.fn(T)
        if (mod.dfOk) {
          checkAnswer(df, mod.dfRows)
        } else {
          checkError(
            exception = intercept[AnalysisException] { df.collect() },
            condition = mod.dfCondition)
        }
      }
    }
  }

  // =====================================================================
  // COMPOUND MODIFICATIONS: Multiple changes before execution
  // =====================================================================

  test("[compound] three successive drops + adds") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, a INT, b INT, c INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 10, 20, 30)")
      val df = spark.table(T)
      sql(s"ALTER TABLE $T DROP COLUMN a")
      sql(s"ALTER TABLE $T ADD COLUMN a STRING") // different type
      // Column ID check runs before type check: the re-added column
      // has a new column ID, so COLUMN_ID_MISMATCH is detected first.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_ID_MISMATCH)
    }
  }

  // =====================================================================
  // Set Operations x All Modifications
  // =====================================================================

  // For set operations, use joinOk EXCEPT column addition
  // which causes numColumnsMismatch (df1=2 cols, df2=3 cols).
  private def setOpOk(mod: Mod): Boolean =
    mod.joinOk && mod.name != "column addition"

  // Set operations (union/except/intersect) throw NUM_COLUMNS_MISMATCH
  // when column counts differ, ID_MISMATCH for drop/recreate, or
  // the mod's dfCondition for type/rename incompatibilities.
  private def setOpCondition(mod: Mod): String = {
    if (mod.name.contains("column addition")) {
      "NUM_COLUMNS_MISMATCH"
    } else if (mod.name.contains("column removal")) {
      // Column removal is detected by the refresh phase before
      // the set operation evaluates
      COL_MISMATCH
    } else {
      mod.dfCondition
    }
  }

  mods.foreach { mod =>
    test(s"[union] df1.union(df2): ${mod.name}") {
      withTable(T) {
        setupTable()
        val df1 = spark.table(T)
        mod.fn(T)
        val df2 = spark.table(T)
        if (setOpOk(mod)) {
          df1.union(df2).collect()
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              df1.union(df2).collect()
            },
            condition = setOpCondition(mod))
        }
      }
    }
  }

  // --- Pattern: Except ---

  mods.foreach { mod =>
    test(s"[except] df1.except(df2): ${mod.name}") {
      withTable(T) {
        setupTable()
        val df1 = spark.table(T)
        mod.fn(T)
        val df2 = spark.table(T)
        if (setOpOk(mod)) {
          df1.except(df2).collect()
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              df1.except(df2).collect()
            },
            condition = setOpCondition(mod))
        }
      }
    }
  }

  // --- Pattern: Intersect ---

  mods.foreach { mod =>
    test(s"[intersect] df1.intersect(df2): ${mod.name}") {
      withTable(T) {
        setupTable()
        val df1 = spark.table(T)
        mod.fn(T)
        val df2 = spark.table(T)
        if (setOpOk(mod)) {
          df1.intersect(df2).collect()
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              df1.intersect(df2).collect()
            },
            condition = setOpCondition(mod))
        }
      }
    }
  }

  // --- Pattern: Self-union (same DF used twice) ---

  mods.foreach { mod =>
    test(s"[self-union] df.union(df): ${mod.name}") {
      withTable(T) {
        setupTable()
        val df = spark.table(T)
        mod.fn(T)
        // same DF object combined with itself
        if (mod.dfOk) {
          df.union(df).collect()
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              df.union(df).collect()
            },
            condition = mod.dfCondition)
        }
      }
    }
  }

  // --- Pattern: Chained transformations across version boundary ---

  mods.foreach { mod =>
    test(s"[chained] filter.groupBy.count: ${mod.name}") {
      withTable(T) {
        setupTable()
        sql(s"INSERT INTO $T VALUES (2, 200)")
        val df = spark.table(T)
        val filtered = df.filter("salary > 50")
        val aggregated = filtered.groupBy("id").count()
        mod.fn(T)
        // Derived DFs share the base DF's plan; refresh
        // propagates through the chain
        if (mod.dfOk) {
          aggregated.collect()
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              aggregated.collect()
            },
            condition = mod.dfCondition)
        }
      }
    }
  }

  // --- Pattern: Cross-table join with shared dependency ---

  mods.foreach { mod =>
    test(s"[cross-table] shared table in two DF trees: ${mod.name}") {
      val t2 = "testcat.ns1.ns2.tbl2"
      withTable(T, t2) {
        setupTable()
        sql(s"CREATE TABLE $t2 (id INT, bonus INT) USING foo")
        sql(s"INSERT INTO $t2 VALUES (1, 50)")

        // df1 reads T directly
        val df1 = spark.table(T).select("id", "salary")
        // df2 joins T with t2 -- T appears in df2's plan too
        val dfT = spark.table(T)
        val dfT2 = spark.table(t2)
        val df2 = dfT.join(dfT2, dfT("id") === dfT2("id"))
          .select(dfT("id"), dfT2("bonus"))

        mod.fn(T)

        // Combine df1 and df2: T appears in both subtrees
        if (mod.dfOk) {
          df1.join(df2, df1("id") === df2("id")).collect()
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              df1.join(df2, df1("id") === df2("id")).collect()
            },
            condition = mod.dfCondition)
        }
      }
    }
  }

  // --- Pattern: Temp view joined with fresh read ---

  mods.foreach { mod =>
    test(s"[view+fresh] temp view join fresh DF: ${mod.name}") {
      withTable(T) {
        setupTable()
        // capture old version in a temp view
        spark.table(T).createOrReplaceTempView("old_v")
        mod.fn(T)
        // fresh read picks up new version
        val fresh = spark.table(T)
        // combine stale view plan with fresh plan
        if (mod.tempViewOk) {
          spark.table("old_v")
            .join(fresh, spark.table("old_v")("id") === fresh("id"))
            .collect()
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              spark.table("old_v")
                .join(
                  fresh,
                  spark.table("old_v")("id") === fresh("id"))
                .collect()
            },
            condition = mod.viewPlanCondition)
        }
      }
    }
  }

  // =====================================================================
  // CTAS from stale source
  // =====================================================================

  mods.filter(!_.dfOk).foreach { mod =>
    test(s"[ctas-stale] CTAS from stale DF: ${mod.name}") {
      val t2 = "testcat.ns1.ns2.tbl2"
      withTable(T, t2) {
        setupTable()
        val source = spark.table(T).filter("id < 10")
        mod.fn(T)
        // CTAS from DF whose source table changed incompatibly.
        // ReplaceTableAsSelectExec refreshes the source query
        // and validates column IDs before writing.
        checkError(
          exception = intercept[AnalysisException] {
            source.writeTo(t2).createOrReplace()
          },
          condition = mod.dfCondition)
      }
    }
  }

  // =====================================================================
  // Null table ID (no identity tracking)
  // Tables without IDs skip validateTableIdentity entirely.
  // Drop/recreate is NOT detected as TABLE_ID_MISMATCH, but column IDs
  // assigned by InMemoryBaseTable still detect column replacement.
  // =====================================================================

  private val NI = "nullidcat.ns1.tbl"

  test("[null-id] drop/recreate detected via COLUMN_ID_MISMATCH") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df = spark.table(NI)
      // Drop and recreate
      sql(s"DROP TABLE $NI")
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (9, 900)")
      // Without table ID, validateTableIdentity is skipped, but
      // InMemoryBaseTable always assigns column IDs. The recreated
      // table has new column IDs, so COLUMN_ID_MISMATCH is detected.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_ID_MISMATCH)
    }
  }

  test("[null-id] join after drop/recreate fails with COLUMN_ID_MISMATCH") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df1 = spark.table(NI)
      sql(s"DROP TABLE $NI")
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (2, 200)")
      val df2 = spark.table(NI)
      // No table ID = no TABLE_ID_MISMATCH, but InMemoryBaseTable
      // assigns column IDs. Recreated table has new column IDs.
      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_ID_MISMATCH)
    }
  }

  // InMemoryBaseTable always assigns column IDs even for nullidcat.
  // Drop+add produces a new column ID, triggering COLUMN_ID_MISMATCH.
  test("[null-id] drop+add column same type fails with COLUMN_ID_MISMATCH") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df = spark.table(NI)
      sql(s"ALTER TABLE $NI DROP COLUMN salary")
      sql(s"ALTER TABLE $NI ADD COLUMN salary INT")
      // Column IDs detect the re-added column is different.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_ID_MISMATCH)
    }
  }

  // drop+add column different type: column ID check runs before
  // schema validation, so COLUMN_ID_MISMATCH is detected first.
  test("[null-id] drop+add column different type detected") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df = spark.table(NI)
      sql(s"ALTER TABLE $NI DROP COLUMN salary")
      sql(s"ALTER TABLE $NI ADD COLUMN salary STRING")
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_ID_MISMATCH)
    }
  }

  test("[null-id] temp view after drop+add column same type") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      spark.table(NI).createOrReplaceTempView("ni_tmp")
      sql(s"ALTER TABLE $NI DROP COLUMN salary")
      sql(s"ALTER TABLE $NI ADD COLUMN salary INT")
      // Temp views re-resolve via V2TableReference (no column ID check).
      // InMemoryTable returns the original value for the re-added column.
      checkAnswer(spark.table("ni_tmp"), Seq(Row(1, 100)))
    }
  }

  test("[null-id] join after drop+add column same type fails with COLUMN_ID_MISMATCH") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df1 = spark.table(NI)
      sql(s"ALTER TABLE $NI DROP COLUMN salary")
      sql(s"ALTER TABLE $NI ADD COLUMN salary INT")
      val df2 = spark.table(NI)
      // Column IDs detect the re-added column is different.
      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_ID_MISMATCH)
    }
  }
}
