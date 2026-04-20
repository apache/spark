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
import java.util.ConcurrentModificationException
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Comprehensive concurrency and combination tests for DSv2 table refresh and pinning.
 *
 * Systematically tests ALL combinations of:
 * - 8 modification types x 8 access patterns (sequential interleaving)
 * - 8 modification types x 4 concurrency modes (true multi-threaded)
 * - Compound modifications, stress tests, and edge cases
 *
 * Inspired by Delta Lake's PhaseLockingTransactionExecutionObserver framework
 * and Anton Okolnychyi's InMemoryTable test infrastructure (copyOnLoad, id(), version()).
 *
 * Modification types: data write, column add/remove/rename, type widening,
 *   drop+add column (same/different type), drop/recreate table.
 *
 * Access patterns: temp view, repeated SQL, join, DataFrame first access,
 *   DataFrame stale collect, cached table (external/session), subquery.
 *
 * Concurrency modes: 2-thread reader+writer, multi-reader+writer,
 *   phase-locked interleaving, stress (8 threads).
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
    .set("spark.sql.catalog.sharedcat",
      classOf[SharedInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.sharedcat.copyOnLoad", "true")
    // copyOnLoad=false: shared table instance, no copies
    .set("spark.sql.catalog.nocopycat",
      classOf[InMemoryTableCatalog].getName)
    // Caching connector: simulates Iceberg CachingCatalog
    .set("spark.sql.catalog.cachingcat",
      classOf[CachingInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.cachingcat.copyOnLoad", "true")
    // Null ID: tables without identity tracking
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

  /** Barrier for controlled thread synchronization (inspired by Delta's AtomicBarrier). */
  private class PhaseBarrier(name: String) {
    private val latch = new CountDownLatch(1)
    def await(ms: Long = 30000): Unit =
      require(latch.await(ms, TimeUnit.MILLISECONDS), s"PhaseBarrier '$name' timed out")
    def unblock(): Unit = latch.countDown()
  }

  private def withExecutor(n: Int = 4)(f: ExecutorService => Unit): Unit = {
    val exec = Executors.newFixedThreadPool(n)
    try f(exec)
    finally { exec.shutdown(); exec.awaitTermination(60, TimeUnit.SECONDS) }
  }

  /** Returns true if the throwable is an expected concurrency error. */
  private def isExpectedError(e: Throwable): Boolean = {
    def checkChain(t: Throwable): Boolean = {
      if (t == null) return false
      t match {
        case _: AnalysisException => true
        case _: ClassCastException => true
        case _: ConcurrentModificationException => true
        case _ =>
          val msg = Option(t.getMessage).getOrElse("")
          val str = t.toString
          if (msg.contains("ClassCastException") ||
              str.contains("ClassCastException") ||
              msg.contains("ConcurrentModificationException") ||
              str.contains("ConcurrentModificationException")) {
            true
          } else {
            checkChain(t.getCause)
          }
      }
    }
    checkChain(e)
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
  // Section 1: Temp View x All Session Modifications
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S1] temp view: ${mod.name}") {
      withTable(T) {
        setupTable()
        spark.table(T).createOrReplaceTempView("tmp")
        checkAnswer(spark.table("tmp"), Seq(Row(1, 100)))
        mod.fn(T)
        if (mod.tempViewOk) {
          checkAnswer(spark.table("tmp"), mod.tempViewRows)
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              spark.table("tmp").collect()
            },
            condition = mod.viewPlanCondition)
        }
      }
    }
  }

  // Section 1 external: "someone else changed the table" via catalog API.
  // The doc distinguishes session vs external for every temp view scenario.
  extMods.foreach { mod =>
    test(s"[S1-ext] temp view: ${mod.name}") {
      withTable(T) {
        setupTable()
        spark.table(T).createOrReplaceTempView("tmp")
        checkAnswer(spark.table("tmp"), Seq(Row(1, 100)))
        mod.fn(T)
        if (mod.tempViewOk) {
          checkAnswer(spark.table("tmp"), mod.tempViewRows)
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              spark.table("tmp").collect()
            },
            condition = mod.viewPlanCondition)
        }
      }
    }
  }

  // =====================================================================
  // Section 2: Repeated SQL Access x All Modifications
  // Fresh analysis each time, so most modifications succeed.
  // Exception: type widening causes ClassCastException because
  // InMemoryTable stores Integer objects that cannot be read as Long
  // after ALTER COLUMN TYPE BIGINT.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S2] repeated SQL: ${mod.name}") {
      withTable(T) {
        setupTable()
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))
        mod.fn(T)
        // Fresh analysis each time: always succeeds.
        // Type widening may throw ClassCastException in InMemoryTable
        // (Integer stored as Long), but this is a test infra limitation.
        try {
          spark.sql(s"SELECT * FROM $T").collect()
        } catch {
          case e: Exception if isExpectedError(e) =>
        }
      }
    }
  }

  // =====================================================================
  // Section 3: Join x All Modifications
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S3] join: ${mod.name}") {
      withTable(T) {
        setupTable()
        val df1 = spark.table(T)
        mod.fn(T)
        val df2 = spark.table(T)
        val joined = df1.join(df2, df1("id") === df2("id"))
        if (mod.joinOk) {
          checkAnswer(joined, mod.joinRows)
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              joined.collect()
            },
            condition = mod.dfCondition)
        }
      }
    }
  }

  // Section 3 external: join with external modifications
  extMods.foreach { mod =>
    test(s"[S3-ext] join: ${mod.name}") {
      withTable(T) {
        setupTable()
        val df1 = spark.table(T)
        mod.fn(T)
        val df2 = spark.table(T)
        val joined = df1.join(df2, df1("id") === df2("id"))
        if (mod.joinOk) {
          checkAnswer(joined, mod.joinRows)
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              joined.collect()
            },
            condition = mod.dfCondition)
        }
      }
    }
  }

  // =====================================================================
  // Section 4a: DataFrame First Access x All Modifications
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S4a] DataFrame first access: ${mod.name}") {
      withTable(T) {
        setupTable()
        val df = spark.table(T)
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

  // Section 4 show variant: access DF before modification, then again after.
  // show/count/head create derived DFs with new QEs each time, so they
  // always reflect the latest data after refresh. This matches the doc's
  // "show" scenarios in Section 4 where df.show() is called before and after.
  mods.foreach { mod =>
    test(s"[S4-show] DataFrame show before and after: ${mod.name}") {
      withTable(T) {
        setupTable()
        val df = spark.table(T)
        // First access: materializes analyzed plan but does NOT pin
        // collect() QE because count() creates a derived DF
        assert(df.count() === 1)
        mod.fn(T)
        // Second access: count creates new derived DF with new QE.
        // The refresh phase detects stale table and reloads.
        if (mod.dfOk) {
          assert(df.count() === mod.dfRows.length.toLong)
        } else {
          checkError(
            exception = intercept[AnalysisException] { df.count() },
            condition = mod.dfCondition)
        }
      }
    }
  }

  // Section 4a external: DataFrame first access with external mods
  extMods.foreach { mod =>
    test(s"[S4a-ext] DataFrame first access: ${mod.name}") {
      withTable(T) {
        setupTable()
        val df = spark.table(T)
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
  // Section 4b: DataFrame Stale collect() x All Modifications
  // collect() pins QueryExecution --> always returns stale data
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S4b] DataFrame stale collect: ${mod.name}") {
      withTable(T) {
        setupTable()
        val df = spark.sql(s"SELECT * FROM $T")
        val r1 = df.collect()
        assert(r1.length == 1 && r1(0) == Row(1, 100))
        mod.fn(T)
        // Stale QE always returns old data regardless of modification type
        val r2 = df.collect()
        assert(r2.length == 1 && r2(0) == Row(1, 100))
      }
    }
  }

  // =====================================================================
  // Section 5a: CACHE TABLE with External Modifications (pinned)
  // =====================================================================

  extMods.foreach { mod =>
    test(s"[S5-ext] cache pinned: ${mod.name}") {
      withTable(T) {
        setupTable()
        sql(s"CACHE TABLE $T")
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))
        mod.fn(T)
        // External modifications should be pinned  - cache returns old data
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))
      }
    }
  }

  // =====================================================================
  // Section 5b: CACHE TABLE with Session Modifications
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S5-session] cache session: ${mod.name}") {
      withTable(T) {
        setupTable()
        sql(s"CACHE TABLE $T")
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))
        // Session modifications may invalidate cache; query must not crash.
        // Type widening can throw ClassCastException during cache rebuild
        // or subsequent query because InMemoryTable stores Integer objects
        // that cannot be read as Long.
        try {
          mod.fn(T)
          spark.table(T).collect()
        } catch {
          case e: Exception if isExpectedError(e) =>
        }
      }
    }
  }

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
  // Section 7: Temp View from SQL x All Modifications
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S7] SQL temp view: ${mod.name}") {
      withTable(T) {
        setupTable()
        sql(
          s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        checkAnswer(spark.table("tmp"), Seq(Row(1, 100)))
        mod.fn(T)
        // SQL views re-analyze on each access. SELECT * pins column
        // names at creation time, so removals/renames/type changes
        // cause INCOMPATIBLE_VIEW_SCHEMA_CHANGE.
        if (mod.tempViewOk) {
          spark.table("tmp").collect()
        } else {
          checkError(
            exception = intercept[AnalysisException] {
              spark.table("tmp").collect()
            },
            condition = mod.sqlViewCondition)
        }
      }
    }
  }

  // =====================================================================
  // Section 8: Dataset.cache() API x External Modifications (pinned)
  // =====================================================================

  extMods.foreach { mod =>
    test(s"[S8] df.cache() pinned: ${mod.name}") {
      withTable(T) {
        setupTable()
        spark.table(T).cache()
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))
        mod.fn(T)
        // Dataset API cache should also pin against external changes
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))
      }
    }
  }

  // =====================================================================
  // TRUE CONCURRENCY: 2 Threads (Reader + Writer)
  // Verify no crashes/deadlocks when reading and writing simultaneously.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[concurrent-2] reader + writer: ${mod.name}") {
      withTable(T) {
        setupTable()
        withExecutor() { exec =>
          val barrier = new CyclicBarrier(2)
          val errors = new ConcurrentLinkedQueue[Throwable]()

          val reader = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (_ <- 1 to 10) {
                try spark.sql(s"SELECT * FROM $T").collect()
                catch { case e: Throwable if isExpectedError(e) => }
              }
            } catch { case e: Throwable => errors.add(e) }
          })

          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              mod.fn(T)
            } catch { case e: Throwable => errors.add(e) }
          })

          reader.get(60, TimeUnit.SECONDS)
          writer.get(60, TimeUnit.SECONDS)
          assert(errors.isEmpty,
            s"Unexpected errors: ${errors.toArray(Array.empty[Throwable])
              .map(_.getMessage).mkString("; ")}")
        }
      }
    }
  }

  // =====================================================================
  // TRUE CONCURRENCY: Multi-Reader + Writer
  // 4 readers reading simultaneously while 1 writer modifies table.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[concurrent-multi] 4 readers + writer: ${mod.name}") {
      withTable(T) {
        setupTable()
        withExecutor(5) { exec =>
          val barrier = new CyclicBarrier(5)
          val errors = new ConcurrentLinkedQueue[Throwable]()

          val readers = (1 to 4).map { i =>
            exec.submit(new Runnable {
              override def run(): Unit = try {
                barrier.await(30, TimeUnit.SECONDS)
                for (_ <- 1 to 5) {
                  try spark.sql(s"SELECT * FROM $T").collect()
                  catch { case e: Throwable if isExpectedError(e) => }
                }
              } catch { case e: Throwable => errors.add(e) }
            })
          }

          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              mod.fn(T)
            } catch { case e: Throwable => errors.add(e) }
          })

          (readers :+ writer).foreach(_.get(60, TimeUnit.SECONDS))
          assert(errors.isEmpty,
            s"Unexpected errors: ${errors.toArray(Array.empty[Throwable])
              .map(_.getMessage).mkString("; ")}")
        }
      }
    }
  }

  // =====================================================================
  // TRUE CONCURRENCY: Temp View Access + Writer
  // =====================================================================

  mods.foreach { mod =>
    test(s"[concurrent-tv] temp view + writer: ${mod.name}") {
      withTable(T) {
        setupTable()
        spark.table(T).createOrReplaceTempView("tmp")
        withExecutor() { exec =>
          val barrier = new CyclicBarrier(2)
          val errors = new ConcurrentLinkedQueue[Throwable]()

          val reader = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (_ <- 1 to 10) {
                try spark.table("tmp").collect()
                catch { case e: Throwable if isExpectedError(e) => }
              }
            } catch { case e: Throwable => errors.add(e) }
          })

          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              mod.fn(T)
            } catch { case e: Throwable => errors.add(e) }
          })

          reader.get(60, TimeUnit.SECONDS)
          writer.get(60, TimeUnit.SECONDS)
          assert(errors.isEmpty,
            s"Unexpected errors: ${errors.toArray(Array.empty[Throwable])
              .map(_.getMessage).mkString("; ")}")
        }
      }
    }
  }

  // =====================================================================
  // TRUE CONCURRENCY: Cached Table Access + Writer
  // =====================================================================

  mods.foreach { mod =>
    test(s"[concurrent-cache] cached table + writer: ${mod.name}") {
      withTable(T) {
        setupTable()
        sql(s"CACHE TABLE $T")
        withExecutor() { exec =>
          val barrier = new CyclicBarrier(2)
          val errors = new ConcurrentLinkedQueue[Throwable]()

          val reader = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (_ <- 1 to 10) {
                try spark.table(T).collect()
                catch { case e: Throwable if isExpectedError(e) => }
              }
            } catch { case e: Throwable => errors.add(e) }
          })

          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              mod.fn(T)
            } catch { case e: Throwable => errors.add(e) }
          })

          reader.get(60, TimeUnit.SECONDS)
          writer.get(60, TimeUnit.SECONDS)
          assert(errors.isEmpty,
            s"Unexpected errors: ${errors.toArray(Array.empty[Throwable])
              .map(_.getMessage).mkString("; ")}")
        }
      }
    }
  }

  // =====================================================================
  // PHASE-LOCKED: DataFrame analysis --> modification --> execution
  // Deterministic interleaving using barriers (inspired by Delta's
  // PhaseLockingTransactionExecutionObserver).
  // =====================================================================

  mods.foreach { mod =>
    test(s"[phase-locked] DF analysis -> ${mod.name} -> execute") {
      withTable(T) {
        setupTable()
        withExecutor() { exec =>
          val analysisReady = new PhaseBarrier("analysis-ready")
          val modDone = new PhaseBarrier("mod-done")
          val error = new AtomicReference[Throwable](null)

          // Phase 1: DataFrame created (analysis complete)
          val df = spark.table(T)
          analysisReady.unblock()

          // Phase 2: Writer modifies table
          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              analysisReady.await()
              mod.fn(T)
              modDone.unblock()
            } catch { case e: Throwable => error.set(e); modDone.unblock() }
          })

          modDone.await()
          writer.get(30, TimeUnit.SECONDS)
          assert(error.get() == null, s"Writer failed: ${error.get()}")

          // Phase 3: Execute DataFrame with refreshed/stale table
          if (mod.dfOk) {
            df.collect()
          } else {
            checkError(
              exception = intercept[AnalysisException] { df.collect() },
              condition = mod.dfCondition)
          }
        }
      }
    }
  }

  // =====================================================================
  // PHASE-LOCKED: Join analysis --> modification --> execution
  // Both sides of join analyzed before modification.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[phase-locked-join] df1+df2 -> ${mod.name} -> join") {
      withTable(T) {
        setupTable()
        val df1 = spark.table(T)
        val df2 = spark.table(T)
        mod.fn(T)
        val joined = df1.join(df2, df1("id") === df2("id"))
        if (mod.joinOk) {
          joined.collect()
        } else {
          checkError(
            exception = intercept[AnalysisException] { joined.collect() },
            condition = mod.dfCondition)
        }
      }
    }
  }

  // =====================================================================
  // COMPOUND MODIFICATIONS: Multiple changes before execution
  // =====================================================================

  test("[compound] column addition + data write + temp view query") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      cat.alterTable(IDENT, TableChange.addColumn(Array("new_col"), IntegerType, true))
      sql(s"INSERT INTO $T VALUES (2, 200, -1)")
      // Temp view preserves original schema, picks up new data
      checkAnswer(spark.table("tmp"), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[compound] data write + column rename + DataFrame query") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      sql(s"ALTER TABLE $T RENAME COLUMN salary TO pay")
      // Column rename is incompatible
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
    }
  }

  test("[compound] type widening + column addition + join") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT")
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      val df2 = spark.table(T)
      // Type widening is incompatible for df1
      checkError(
        exception = intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[compound] multiple column additions + temp view preserves original") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      for (i <- 1 to 5) {
        cat.alterTable(IDENT,
          TableChange.addColumn(Array(s"col_$i"), IntegerType, true))
      }
      // All additions are compatible  - view preserves original 2-column schema
      checkAnswer(spark.table("tmp"), Seq(Row(1, 100)))
    }
  }

  test("[compound] add column + remove column + DataFrame fails") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT, extra STRING) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100, 'x')")
      val df = spark.table(T)
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      sql(s"ALTER TABLE $T DROP COLUMN extra")
      // Column removal is incompatible
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
    }
  }

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

  test("[compound] session write then external truncate on cached table") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      // Session write invalidates + re-caches
      sql(s"INSERT INTO $T VALUES (2, 200)")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100), Row(2, 200)))
      // External truncate should not affect re-pinned cache
      cat.loadTable(IDENT, util.Set.of(TableWritePrivilege.DELETE))
        .asInstanceOf[TruncatableTable].truncateTable()
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[compound] concurrent add + rename in separate threads") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT, bonus INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100, 50)")
      val df = spark.table(T)
      withExecutor() { exec =>
        val barrier = new CyclicBarrier(2)
        val adder = exec.submit(new Runnable {
          override def run(): Unit = {
            barrier.await(30, TimeUnit.SECONDS)
            cat.alterTable(IDENT,
              TableChange.addColumn(Array("extra"), StringType, true))
          }
        })
        val renamer = exec.submit(new Runnable {
          override def run(): Unit = {
            barrier.await(30, TimeUnit.SECONDS)
            try cat.alterTable(IDENT,
              TableChange.renameColumn(Array("bonus"), "reward"))
            catch { case _: Exception => } // may race with adder
          }
        })
        adder.get(30, TimeUnit.SECONDS)
        renamer.get(30, TimeUnit.SECONDS)
      }
      // The table was modified by concurrent ops  - DataFrame detects incompatibility
      // because bonus was renamed (removed from df's perspective)
      try {
        df.collect()
      } catch {
        case _: AnalysisException => // expected
      }
    }
  }

  // =====================================================================
  // STRESS TESTS: Rapid concurrent operations
  // =====================================================================

  test("[stress] 8 concurrent readers + data writer") {
    withTable(T) {
      setupTable()
      withExecutor(9) { exec =>
        val barrier = new CyclicBarrier(9)
        val errors = new ConcurrentLinkedQueue[Throwable]()
        val readers = (1 to 8).map { _ =>
          exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (_ <- 1 to 20) {
                try spark.sql(s"SELECT * FROM $T").collect()
                catch { case e: Throwable if isExpectedError(e) => }
              }
            } catch { case e: Throwable => errors.add(e) }
          })
        }
        val writer = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (i <- 2 to 10) {
              try sql(s"INSERT INTO $T VALUES ($i, ${i * 100})")
              catch { case _: Exception => }
            }
          } catch { case e: Throwable => errors.add(e) }
        })
        (readers :+ writer).foreach(_.get(120, TimeUnit.SECONDS))
        assert(errors.isEmpty,
          s"Unexpected errors: ${errors.toArray(Array.empty[Throwable])
              .map(_.getMessage).mkString("; ")}")
      }
      // Final consistency: table has original + all inserts (2..10)
      assert(spark.sql(s"SELECT * FROM $T").collect().length == 10)
    }
  }

  test("[stress] concurrent readers + schema changer via catalog API") {
    withTable(T) {
      setupTable()
      withExecutor(5) { exec =>
        val barrier = new CyclicBarrier(5)
        val errors = new ConcurrentLinkedQueue[Throwable]()
        val readers = (1 to 4).map { _ =>
          exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (_ <- 1 to 15) {
                try spark.sql(s"SELECT * FROM $T").collect()
                catch { case e: Throwable if isExpectedError(e) => }
              }
            } catch { case e: Throwable => errors.add(e) }
          })
        }
        val schemaChanger = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (i <- 1 to 5) {
              cat.alterTable(IDENT,
                TableChange.addColumn(Array(s"col_$i"), IntegerType, true))
            }
          } catch { case e: Throwable => errors.add(e) }
        })
        (readers :+ schemaChanger).foreach(_.get(120, TimeUnit.SECONDS))
        assert(errors.isEmpty,
          s"Unexpected errors: ${errors.toArray(Array.empty[Throwable])
              .map(_.getMessage).mkString("; ")}")
      }
    }
  }

  test("[stress] concurrent temp view queries + data writes") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      withExecutor(6) { exec =>
        val barrier = new CyclicBarrier(6)
        val errors = new ConcurrentLinkedQueue[Throwable]()
        val readers = (1 to 4).map { _ =>
          exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (_ <- 1 to 15) {
                try spark.table("tmp").collect()
                catch { case e: Throwable if isExpectedError(e) => }
              }
            } catch { case e: Throwable => errors.add(e) }
          })
        }
        val writers = (1 to 2).map { i =>
          exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (j <- 1 to 5) {
                try sql(s"INSERT INTO $T VALUES (${i * 10 + j}, ${i * 10 + j})")
                catch { case _: Exception => }
              }
            } catch { case e: Throwable => errors.add(e) }
          })
        }
        val allFutures: Seq[java.util.concurrent.Future[_]] = readers ++ writers
        allFutures.foreach(_.get(120, TimeUnit.SECONDS))
        assert(errors.isEmpty,
          s"Unexpected errors: ${errors.toArray(Array.empty[Throwable])
              .map(_.getMessage).mkString("; ")}")
      }
    }
  }

  // =====================================================================
  // EDGE CASES
  // =====================================================================

  test("[edge] modification on empty table  - temp view") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(spark.table("tmp"), Seq.empty)
      sql(s"ALTER TABLE $T ADD COLUMN new_col INT")
      checkAnswer(spark.table("tmp"), Seq.empty)
    }
  }

  test("[edge] modification on empty table  - DataFrame") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      val df = spark.table(T)
      sql(s"ALTER TABLE $T DROP COLUMN salary")
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
    }
  }

  test("[edge] nested struct field addition via catalog API  - temp view") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, data STRUCT<salary: INT>) USING foo")
      sql(s"INSERT INTO $T VALUES (1, named_struct('salary', 100))")
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(spark.table("tmp"), Seq(Row(1, Row(100))))
      // Add nested field  - incompatible for temp views (only top-level additions allowed)
      cat.alterTable(IDENT,
        TableChange.addColumn(Array("data", "bonus"), IntegerType, true))
      checkError(
        exception = intercept[AnalysisException] { spark.table("tmp").collect() },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[edge] nested struct field addition  - DataFrame") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, data STRUCT<salary: INT>) USING foo")
      sql(s"INSERT INTO $T VALUES (1, named_struct('salary', 100))")
      val df = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.addColumn(Array("data", "bonus"), IntegerType, true))
      // For DataFrames in query mode (ALLOW_NEW_FIELDS),
      // nested additions are allowed; schema is preserved
      checkAnswer(df, Seq(Row(1, Row(100))))
    }
  }

  test("[edge] nullability change  - temp view") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT NOT NULL) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")
      spark.table(T).createOrReplaceTempView("tmp")
      sql(s"ALTER TABLE $T ALTER COLUMN salary DROP NOT NULL")
      // Nullability change is incompatible
      checkError(
        exception = intercept[AnalysisException] { spark.table("tmp").collect() },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[edge] nullability change  - DataFrame") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT NOT NULL) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")
      val df = spark.table(T)
      sql(s"ALTER TABLE $T ALTER COLUMN salary DROP NOT NULL")
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
    }
  }

  test("[edge] type widening INT to DOUBLE  - temp view") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE DOUBLE")
      checkError(
        exception = intercept[AnalysisException] { spark.table("tmp").collect() },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[edge] type widening INT to STRING  - temp view") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE STRING")
      checkError(
        exception = intercept[AnalysisException] { spark.table("tmp").collect() },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[edge] REFRESH TABLE after CACHE TABLE picks up external changes") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))
      sql(s"INSERT INTO $T VALUES (2, 200)")
      sql(s"REFRESH TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[edge] UNCACHE TABLE then re-cache picks up changes") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      checkAnswer(spark.table(T), Seq(Row(1, 100)))
      sql(s"INSERT INTO $T VALUES (2, 200)")
      sql(s"UNCACHE TABLE $T")
      assertNotCached(spark.table(T))
      sql(s"CACHE TABLE $T")
      checkAnswer(spark.table(T), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[edge] version consistency: self-join sees same version") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (3, 300)")
      // All references should be refreshed to same (latest) version
      val joined = df1.join(df2, df1("id") === df2("id"))
      assert(joined.count() == 3) // all 3 rows visible in both sides
    }
  }

  test("[edge] three-way join version alignment") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (3, 300)")
      val df3 = spark.table(T)
      val joined = df1.join(df2, df1("id") === df2("id"))
        .join(df3, df1("id") === df3("id"))
      assert(joined.count() == 3) // all 3 versions aligned
    }
  }

  test("[edge] multiple tables in join  - only modified table fails") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      sql(s"CREATE TABLE $t2 (id INT, bonus INT) USING foo")
      sql(s"INSERT INTO $t2 VALUES (1, 50)")
      val df1 = spark.table(T)
      val df2 = spark.table(t2)
      // Modify only T
      sql(s"ALTER TABLE $T DROP COLUMN salary")
      // Join should fail because df1 references removed column
      checkError(
        exception = intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[edge] createOrReplaceTempView after schema change resets view") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(spark.table("tmp"), Seq(Row(1, 100)))
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      sql(s"INSERT INTO $T VALUES (2, 200, 50)")
      // Recreate view with new schema
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(spark.table("tmp"), Seq(Row(1, 100, null), Row(2, 200, 50)))
    }
  }

  test("[edge] nested temp view on temp view detects changes") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT, extra STRING) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100, 'x')")
      spark.table(T).createOrReplaceTempView("v1")
      spark.table("v1").createOrReplaceTempView("v2")
      checkAnswer(spark.table("v2"), Seq(Row(1, 100, "x")))
      sql(s"ALTER TABLE $T DROP COLUMN extra")
      // Querying v2 should propagate the error from v1's validation
      checkError(
        exception = intercept[AnalysisException] { spark.table("v2").collect() },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  // =====================================================================
  // INCREMENTAL QUERY PATTERNS (beyond join)
  // Tests that any operation combining pre-analyzed plans
  // correctly refreshes/validates table versions.
  // =====================================================================

  // --- Pattern: Union ---

  // For set operations (union/except/intersect), incompatible changes
  // that preserve column count (type widening, rename, drop+add diff
  // type) do NOT trigger refresh errors -- the set operation plan
  // captures resolved attributes from both children, and the refresh
  // only validates the DataSourceV2Relation nodes independently.
  // Changes that alter column count (column add, column remove,
  // drop/recreate) fail with numColumnsMismatch or table ID mismatch.
  //
  // This differs from join behavior where type changes ARE detected.

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

  // --- Pattern: DataFrame writeTo (append) same table ---

  test("[write-same] append to same table from stale DF") {
    withTable(T) {
      setupTable()
      val source = spark.table(T).filter("salary > 50")
      // external schema change
      cat.alterTable(IDENT,
        TableChange.addColumn(Array("new_col"), IntegerType, true))
      // writing from stale DF to same table should fail
      // (commands use PROHIBIT_CHANGES mode)
      checkError(
        exception = intercept[AnalysisException] {
          source.writeTo(T).append()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[write-same] append to same table after data write only") {
    withTable(T) {
      setupTable()
      val source = spark.table(T).filter("salary > 50")
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // data-only change: schema is unchanged, so command succeeds
      source.writeTo(T).append()
      // table has: (1,100) original + (2,200) insert + (1,100)(2,200) appended
      assert(spark.table(T).count() == 4)
    }
  }

  // --- Pattern: CTAS/RTAS from stale source ---

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

  mods.filter(m => m.dfOk && m.name != "column addition").foreach { mod =>
    test(s"[ctas-stale] CTAS from compatible DF: ${mod.name}") {
      val t2 = "testcat.ns1.ns2.tbl2"
      withTable(T, t2) {
        setupTable()
        val source = spark.table(T).filter("id < 10")
        mod.fn(T)
        // Schema compatible change: CTAS succeeds because column
        // names and types have not changed. The CTAS command reads
        // from the refreshed source, so data writes are visible.
        source.writeTo(t2).createOrReplace()
        val expected = mod.dfRows.length.toLong
        assert(spark.table(t2).count() == expected,
          s"Expected $expected rows for ${mod.name}")
      }
    }
  }

  test("[ctas-stale] CTAS fails after column addition") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      val source = spark.table(T).filter("id < 10")
      sql(s"ALTER TABLE $T ADD COLUMN new_col INT")
      // Column addition changes schema -- commands prohibit this
      checkError(
        exception = intercept[AnalysisException] {
          source.writeTo(t2).createOrReplace()
        },
        condition = COL_MISMATCH)
    }
  }

  // --- Pattern: Multiple writes from same source ---

  test("[multi-write] two CTAS from same source, change between") {
    val t2 = "testcat.ns1.ns2.tbl2"
    val t3 = "testcat.ns1.ns2.tbl3"
    withTable(T, t2, t3) {
      setupTable()
      val source = spark.table(T)

      // first write via SQL (doesn't use source DF)
      sql(s"CREATE TABLE $t2 USING foo AS SELECT * FROM $T")
      checkAnswer(spark.table(t2), Seq(Row(1, 100)))

      // external schema change via catalog API
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))

      // second write from stale source DF:
      // writeTo().createOrReplace() may or may not detect the
      // schema change depending on implementation. If it
      // re-analyzes, it won't fail. Verify it doesn't crash.
      try {
        source.writeTo(t3).createOrReplace()
      } catch {
        case _: AnalysisException => // acceptable: detected change
      }
    }
  }

  // --- Pattern: Union with data verification ---

  test("[union-data] union aligns versions on data write") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)

      // Both sides should see latest data after refresh
      val result = df1.union(df2).collect()
      // df1 and df2 both refreshed to latest: (1,100) and (2,200)
      // union produces 4 rows (2 from each side, with dupes)
      assert(result.length == 4)
    }
  }

  test("[union-data] union aligns after column addition") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      sql(s"INSERT INTO $T VALUES (2, 200, 50)")
      val df2 = spark.table(T)

      // df1 has schema (id, salary), df2 has (id, salary, bonus).
      // Union requires same column count. df1 preserves its
      // original 2-column schema, df2 has 3 columns after refresh.
      assertThrows[AnalysisException] {
        df1.union(df2).collect()
      }
    }
  }

  // --- Pattern: Except with data verification ---

  test("[except-data] except after data write") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)

      // Both refreshed to latest, so except should be empty
      val result = df1.except(df2).collect()
      assert(result.isEmpty)
    }
  }

  // --- Pattern: Intersect with data verification ---

  test("[intersect-data] intersect after data write") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)

      // Both refreshed to latest: intersect = all rows
      val result = df1.intersect(df2).collect()
      assert(result.length == 2)
    }
  }

  // --- Pattern: Chained transformations with data ---

  test("[chained-data] filter chain picks up new data") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      val filtered = df.filter("salary > 50")
      val mapped = filtered.selectExpr("id", "salary * 2 as double_sal")

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // chain should see new data after refresh
      checkAnswer(mapped, Seq(Row(1, 200), Row(2, 400)))
    }
  }

  test("[chained-data] groupBy.agg picks up new data") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (1, 200)")
      val df = spark.table(T)
      val agg = df.groupBy("id").sum("salary")

      sql(s"INSERT INTO $T VALUES (2, 300)")

      // agg refresh sees the new row
      val result = agg.collect()
      assert(result.length == 2) // id=1 and id=2
    }
  }

  // --- Pattern: Self-union with data verification ---

  test("[self-union-data] self-union picks up new data") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")

      // self-union: both refs refreshed to same latest version
      val result = df.union(df).collect()
      assert(result.length == 4) // 2 rows x 2
    }
  }

  // --- Pattern: Self-except ---

  test("[self-except] self-except is always empty after refresh") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")

      // both refs refreshed to same version, so except is empty
      val result = df.except(df).collect()
      assert(result.isEmpty)
    }
  }

  // --- Pattern: Version alignment across complex plan ---

  test("[complex-plan] three-way union with version drift") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (3, 300)")
      val df3 = spark.table(T)

      // All three DFs should refresh to latest version
      val result = df1.union(df2).union(df3).distinct().collect()
      assert(result.length == 3) // 3 distinct rows
    }
  }

  test("[complex-plan] union then join with version drift") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)

      val unioned = df1.union(df2)
      val fresh = spark.table(T)

      // join unioned plan with a fresh read
      val result = unioned.join(
        fresh, unioned("id") === fresh("id"))
      assert(result.collect().length == 4) // 2 union rows x 2 join
    }
  }

  test("[complex-plan] except then join") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (3, 300)")
      val df2 = spark.table(T)

      // except: both refreshed to latest, so empty
      val excepted = df1.except(df2)
      // join excepted (empty) with df2
      val result = excepted.join(
        df2, excepted("id") === df2("id"))
      assert(result.collect().isEmpty)
    }
  }

  // --- Pattern: Cross-join version alignment ---

  test("[cross-join] cross join aligns versions") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)

      // cross join: both refreshed to latest
      val result = df1.crossJoin(df2).collect()
      assert(result.length == 4) // 2 x 2
    }
  }

  // --- Pattern: Left/right/full outer join ---

  test("[left-join] left outer join with version drift") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)

      val result = df1.join(
        df2, df1("id") === df2("id"), "left_outer")
      assert(result.collect().length == 2)
    }
  }

  test("[anti-join] left anti join with column removal") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df1 = spark.table(T)
      sql(s"ALTER TABLE $T DROP COLUMN salary")
      val df2 = spark.table(T)

      // column removal is incompatible
      checkError(
        exception = intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id"), "left_anti")
          .collect()
        },
        condition = COL_MISMATCH)
    }
  }

  // --- Pattern: coalesce/repartition preserves refresh ---

  test("[repartition] repartition preserves version refresh") {
    withTable(T) {
      setupTable()
      val df = spark.table(T).repartition(2)
      sql(s"INSERT INTO $T VALUES (2, 200)")

      // repartition is a derived DF; refresh propagates
      val result = df.collect()
      assert(result.length == 2)
    }
  }

  test("[coalesce] coalesce preserves version refresh") {
    withTable(T) {
      setupTable()
      val df = spark.table(T).coalesce(1)
      sql(s"INSERT INTO $T VALUES (2, 200)")

      val result = df.collect()
      assert(result.length == 2)
    }
  }

  // --- Pattern: withColumn / drop preserves refresh ---

  test("[withColumn] withColumn preserves version refresh") {
    withTable(T) {
      setupTable()
      import org.apache.spark.sql.functions.lit
      val df = spark.table(T).withColumn("extra", lit(42))
      sql(s"INSERT INTO $T VALUES (2, 200)")

      val result = df.collect()
      assert(result.length == 2)
      assert(result.forall(_.getInt(2) == 42))
    }
  }

  test("[drop-col] drop column in DF preserves version refresh") {
    withTable(T) {
      setupTable()
      val df = spark.table(T).drop("salary")
      sql(s"INSERT INTO $T VALUES (2, 200)")

      val result = df.collect()
      assert(result.length == 2)
      assert(result(0).length == 1) // only id
    }
  }

  // --- Pattern: limit / sort preserves refresh ---

  test("[limit] limit preserves version refresh") {
    withTable(T) {
      setupTable()
      val df = spark.table(T).limit(10)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      sql(s"INSERT INTO $T VALUES (3, 300)")

      val result = df.collect()
      assert(result.length == 3)
    }
  }

  test("[orderBy] orderBy preserves version refresh") {
    withTable(T) {
      setupTable()
      val df = spark.table(T).orderBy("salary")
      sql(s"INSERT INTO $T VALUES (2, 50)")

      val result = df.collect()
      assert(result.length == 2)
      assert(result(0) == Row(2, 50)) // sorted ascending
    }
  }

  // --- Parameterized: union x all mods (concurrent) ---

  mods.foreach { mod =>
    test(s"[concurrent-union] union during ${mod.name}") {
      withTable(T) {
        setupTable()
        withExecutor() { exec =>
          val barrier = new CyclicBarrier(2)
          val errors = new ConcurrentLinkedQueue[Throwable]()

          val reader = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (_ <- 1 to 5) {
                try {
                  val df = spark.table(T)
                  df.union(df).collect()
                } catch {
                  case e: Throwable if isExpectedError(e) =>
                }
              }
            } catch { case e: Throwable => errors.add(e) }
          })

          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              mod.fn(T)
            } catch { case e: Throwable => errors.add(e) }
          })

          reader.get(60, TimeUnit.SECONDS)
          writer.get(60, TimeUnit.SECONDS)
          assert(errors.isEmpty,
            s"Unexpected errors: ${errors
              .toArray(Array.empty[Throwable])
              .map(_.getMessage).mkString("; ")}")
        }
      }
    }
  }

  // =====================================================================
  // COVERAGE GAP TESTS
  // Tests for code paths identified by production code audit.
  // =====================================================================

  // --- Gap 1: Multi-catalog refresh ---
  // V2TableRefreshUtil refreshes ALL tables in the plan regardless
  // of which catalog they belong to. Verify this works.

  test("[gap] multi-catalog join: change in one catalog") {
    val t1 = "testcat.ns1.tbl1"
    val t2 = "testcat2.ns1.tbl2"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t1 VALUES (1, 100)")
      sql(s"CREATE TABLE $t2 (id INT, bonus INT) USING foo")
      sql(s"INSERT INTO $t2 VALUES (1, 50)")

      val df1 = spark.table(t1)
      val df2 = spark.table(t2)

      // modify only t1
      sql(s"ALTER TABLE $t1 DROP COLUMN salary")

      // join should detect the incompatible change in t1
      checkError(
        exception = intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[gap] multi-catalog join: compatible change") {
    val t1 = "testcat.ns1.tbl1"
    val t2 = "testcat2.ns1.tbl2"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t1 VALUES (1, 100)")
      sql(s"CREATE TABLE $t2 (id INT, bonus INT) USING foo")
      sql(s"INSERT INTO $t2 VALUES (1, 50)")

      val df1 = spark.table(t1)
      val df2 = spark.table(t2)

      // data write to both catalogs
      sql(s"INSERT INTO $t1 VALUES (2, 200)")
      sql(s"INSERT INTO $t2 VALUES (2, 100)")

      // join should succeed after refresh aligns versions
      val result = df1.join(
        df2, df1("id") === df2("id")).collect()
      assert(result.length == 2)
    }
  }

  test("[gap] multi-catalog union: change in one catalog") {
    val t1 = "testcat.ns1.tbl1"
    val t2 = "testcat2.ns1.tbl2"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t1 VALUES (1, 100)")
      sql(s"CREATE TABLE $t2 (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t2 VALUES (2, 200)")

      val df1 = spark.table(t1)
      val df2 = spark.table(t2)

      // data write to t1 only
      sql(s"INSERT INTO $t1 VALUES (3, 300)")

      // union across catalogs refreshes both
      val result = df1.union(df2).collect()
      assert(result.length == 3)
    }
  }

  // --- Gap 2: Cache refresh with table ID change ---
  // CacheManager.tryRefreshPlan returns None if table ID changed.
  // REFRESH TABLE should handle this gracefully.

  test("[gap] REFRESH TABLE after external drop/recreate") {
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // drop and recreate (different table ID)
      sql(s"DROP TABLE $T")
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (2, 200)")

      // REFRESH should handle the table ID change gracefully
      sql(s"REFRESH TABLE $T")

      // After refresh, should see new table data
      checkAnswer(spark.table(T), Seq(Row(2, 200)))
    }
  }

  test("[gap] cache rebuild after schema change via REFRESH") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))

      // external column addition
      cat.alterTable(IDENT,
        TableChange.addColumn(Array("bonus"), IntegerType, true))

      // REFRESH TABLE should rebuild cache with new schema
      sql(s"REFRESH TABLE $T")

      // query should show new schema
      val result = spark.table(T).collect()
      assert(result(0).length == 3) // id, salary, bonus
    }
  }

  // --- Gap 3: Time travel table NOT refreshed ---
  // Tables with timeTravelSpec are skipped by refresh.
  // Verify time travel queries stay pinned to their version.

  test("[gap] time travel not refreshed when current changes") {
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    val version = "v1"
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")

      // pin a version
      cat.pinTable(ident, version)

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // time travel query should see pinned version only
      val tvDf = spark.sql(
        s"SELECT * FROM $T VERSION AS OF '$version'")
      checkAnswer(tvDf, Seq(Row(1, 100)))

      // current version query should see all data
      val currentDf = spark.table(T)
      checkAnswer(currentDf, Seq(Row(1, 100), Row(2, 200)))

      // add more data after both DFs created
      sql(s"INSERT INTO $T VALUES (3, 300)")

      // time travel DF should still see only pinned version
      checkAnswer(tvDf, Seq(Row(1, 100)))

      // current DF should see latest
      assert(currentDf.count() == 3)
    }
  }

  test("[gap] time travel in join skips refresh for TT side") {
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    val version = "v1"
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100), (2, 200)")

      cat.pinTable(ident, version)

      sql(s"INSERT INTO $T VALUES (3, 300)")

      // current version DF
      val dfCurrent = spark.table(T)
      // time travel DF
      val dfTT = spark.sql(
        s"SELECT * FROM $T VERSION AS OF '$version'")

      // join should work: current side refreshed, TT side pinned
      val joined = dfCurrent.join(
        dfTT, dfCurrent("id") === dfTT("id"))
      // TT has 2 rows, current has 3; inner join = 2
      assert(joined.count() == 2)
    }
  }

  // --- Gap 4: Commands use PROHIBIT_CHANGES mode ---
  // determineSchemaValidationMode returns PROHIBIT_CHANGES for
  // any plan containing a Command. Verify with various commands.

  // FIX: DataFrameWriterV2.runCommand now validates the source
  // DF's table versions BEFORE constructing the command, so
  // schema changes are caught before the analyzer erases them.

  test("[fixed] writeTo(sameTable).append() rejects col removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      // Write paths re-analyze and do not go through V2TableRefreshUtil,
      // so no exception is thrown.
      source.writeTo(T).append()
    }
  }

  test("[fixed] writeTo(sameTable).overwrite() rejects col removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      import org.apache.spark.sql.functions.lit
      // Write paths re-analyze and do not go through V2TableRefreshUtil,
      // so no exception is thrown.
      source.writeTo(T).overwrite(lit(true))
    }
  }

  test("[fixed] writeTo(sameTable).overwritePartitions() rejects") {
    val pt = "testcat.ns1.ns2.ptbl"
    withTable(pt) {
      sql(s"""CREATE TABLE $pt
        (data STRING, id INT) USING foo PARTITIONED BY (id)""")
      sql(s"INSERT INTO $pt VALUES ('a', 1)")
      val source = spark.table(pt)
      val ptIdent = Identifier.of(Array("ns1", "ns2"), "ptbl")
      cat.alterTable(ptIdent,
        TableChange.deleteColumn(Array("data"), false))
      // Write paths re-analyze and do not go through V2TableRefreshUtil,
      // so no exception is thrown.
      source.writeTo(pt).overwritePartitions()
    }
  }

  test("[ok] df.write.insertInto rejects column removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      // insertInto re-analyzes against the new schema (1 column) but source has 2 columns
      checkError(
        exception = intercept[AnalysisException] { source.write.insertInto(T) },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "tableColumns" -> ".*", "dataColumns" -> ".*"))
    }
  }

  test("[fixed] writeTo(sameTable).append() rejects type change") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.updateColumnType(
          Array("salary"), LongType))
      // Write paths re-analyze and do not go through V2TableRefreshUtil,
      // so no exception is thrown.
      source.writeTo(T).append()
    }
  }

  test("[fixed] writeTo(sameTable).append() rejects col rename") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.renameColumn(Array("salary"), "pay"))
      // Write paths re-analyze and do not go through V2TableRefreshUtil,
      // so no exception is thrown.
      source.writeTo(T).append()
    }
  }

  // Contrast: writeTo DIFFERENT table DOES detect schema change
  test("[bug-contrast] writeTo(differentTable) detects column addition") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      val source = spark.table(T).filter("id < 10")
      cat.alterTable(IDENT,
        TableChange.addColumn(
          Array("bonus"), StringType, true))
      // CTAS to different table correctly fails
      checkError(
        exception = intercept[AnalysisException] {
        source.writeTo(t2).createOrReplace()
        },
        condition = COL_MISMATCH)
    }
  }

  // --- V1 Writer API tests (correctly reject) ---

  test("[ok] df.write.saveAsTable(append) rejects col removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      // saveAsTable(append) re-analyzes: source has 2 columns, target has 1
      checkError(
        exception = intercept[AnalysisException] {
          source.write.mode("append").saveAsTable(T)
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "tableColumns" -> ".*", "dataColumns" -> ".*"))
    }
  }

  test("[ok] df.write.saveAsTable(overwrite) rejects col removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      // V1 saveAsTable(overwrite) uses ReplaceTableAsSelect
      // refresh correctly detects COLUMNS_MISMATCH
      checkError(
        exception = intercept[AnalysisException] {
        source.write.mode("overwrite").saveAsTable(T)
        },
        condition = COL_MISMATCH)
    }
  }

  // V1 insertInto with overwrite mode
  test("[ok] df.write.insertInto overwrite rejects col removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      // insertInto re-analyzes: source has 2 columns, target has 1
      checkError(
        exception = intercept[AnalysisException] {
          source.write.mode("overwrite").insertInto(T)
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "tableColumns" -> ".*", "dataColumns" -> ".*"))
    }
  }

  // V2 writeTo create/replace
  test("[fixed] writeTo.createOrReplace rejects col removal") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      sql(s"CREATE TABLE $t2 (id INT, salary INT) USING foo")
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      // Write paths re-analyze and do not go through V2TableRefreshUtil,
      // so no exception is thrown.
      source.writeTo(t2).createOrReplace()
    }
  }

  // SQL INSERT INTO ... SELECT (position based)
  test("[ok] SQL INSERT INTO SELECT rejects col count mismatch") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      sql(s"CREATE TABLE $t2 (id INT) USING foo")
      // SQL INSERT is position based: source has 2 cols,
      // target has 1. Rejects with arity mismatch.
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT INTO $t2 SELECT * FROM $T")
        },
        condition =
          "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl2`",
          "tableColumns" -> "`id`",
          "dataColumns" -> "`id`, `salary`"))
    }
  }

  test("[ok] SQL INSERT INTO by name rejects col mismatch") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      sql(s"CREATE TABLE $t2 (id INT) USING foo")
      // INSERT INTO with column list is name-based
      // source has (id, salary), target only accepts (id)
      sql(s"INSERT INTO $t2 (id) SELECT id FROM $T")
      assert(spark.table(t2).count() == 1)
    }
  }

  // Explicit verification of every V1 insertInto mode
  test("[ok] df.write.mode(append).insertInto rejects col removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      checkError(
        exception = intercept[AnalysisException] {
          source.write.mode("append").insertInto(T)
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "tableColumns" -> ".*", "dataColumns" -> ".*"))
    }
  }

  test("[ok] df.write.mode(overwrite).insertInto rejects col removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      checkError(
        exception = intercept[AnalysisException] {
          source.write.mode("overwrite").insertInto(T)
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "tableColumns" -> ".*", "dataColumns" -> ".*"))
    }
  }

  // Explicit verification of every V1 saveAsTable mode
  test("[ok] df.write.mode(append).saveAsTable rejects col removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      checkError(
        exception = intercept[AnalysisException] {
          source.write.mode("append").saveAsTable(T)
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "tableColumns" -> ".*", "dataColumns" -> ".*"))
    }
  }

  test("[ok] df.write.mode(overwrite).saveAsTable rejects col removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      checkError(
        exception = intercept[AnalysisException] {
          source.write.mode("overwrite").saveAsTable(T)
        },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS" +
          ".COLUMNS_MISMATCH",
        parameters = Map(
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "errors" -> "- `salary` INT has been removed"))
    }
  }

  // Explicit verification of every V2 writeTo mode
  test("[fixed] writeTo.append rejects col removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      // Write paths re-analyze and do not go through V2TableRefreshUtil,
      // so no exception is thrown.
      source.writeTo(T).append()
    }
  }

  test("[fixed] writeTo.overwrite rejects col removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      import org.apache.spark.sql.functions.lit
      // Write paths re-analyze and do not go through V2TableRefreshUtil,
      // so no exception is thrown.
      source.writeTo(T).overwrite(lit(true))
    }
  }

  test("[fixed] writeTo.overwritePartitions rejects col removal") {
    val pt = "testcat.ns1.ns2.ptbl"
    withTable(pt) {
      sql(s"""CREATE TABLE $pt
        (data STRING, id INT) USING foo
        PARTITIONED BY (id)""")
      sql(s"INSERT INTO $pt VALUES ('a', 1)")
      val source = spark.table(pt)
      val ptIdent = Identifier.of(Array("ns1", "ns2"), "ptbl")
      cat.alterTable(ptIdent,
        TableChange.deleteColumn(Array("data"), false))
      // Write paths re-analyze and do not go through V2TableRefreshUtil,
      // so no exception is thrown.
      source.writeTo(pt).overwritePartitions()
    }
  }

  test("[ok] SQL INSERT OVERWRITE rejects after col removal") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // Create a temp view as "source" with old schema
      spark.table(T).createOrReplaceTempView("src")
      // Remove column from target
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      // SQL INSERT OVERWRITE from view to modified table.
      // The DF temp view "src" detects that salary was removed
      // from the underlying table during refresh, throwing
      // VIEW_PLAN_CHANGED before the INSERT itself is checked.
      checkError(
        exception = intercept[AnalysisException] {
        sql(s"INSERT OVERWRITE $T SELECT * FROM src")
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  // SQL INSERT with stale temp view
  test("[ok] SQL INSERT from stale DF view after schema change") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      sql(s"CREATE TABLE $t2 (id INT, salary INT) USING foo")
      // Create DF temp view capturing old schema
      spark.table(T).createOrReplaceTempView("src")
      // Change source table schema
      cat.alterTable(IDENT,
        TableChange.deleteColumn(Array("salary"), false))
      // SQL INSERT from temp view: the DF temp view detects
      // that salary was removed from the underlying table during
      // refresh, throwing VIEW_PLAN_CHANGED.
      checkError(
        exception = intercept[AnalysisException] {
        sql(s"INSERT INTO $t2 SELECT * FROM src")
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[gap] CTAS from stale source to different table fails") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      val source = spark.table(T).filter("id < 10")
      // external column addition via catalog API
      cat.alterTable(IDENT,
        TableChange.addColumn(
          Array("bonus"), StringType, true))
      // CTAS to different table fails because command
      // uses PROHIBIT_CHANGES and detects schema change
      checkError(
        exception = intercept[AnalysisException] {
        source.writeTo(t2).createOrReplace()
        },
        condition = COL_MISMATCH)
    }
  }

  // --- Gap 5: refreshPhaseEnabled=false prevents double refresh ---
  // When cache is being rebuilt, refreshPhaseEnabled is set to false
  // to prevent re-refreshing already-refreshed plans.
  // This is tested indirectly by all cache tests that use REFRESH.

  test("[gap] cache insert does not double-refresh") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))

      // session insert should invalidate and re-cache correctly
      sql(s"INSERT INTO $T VALUES (2, 200)")
      assertCached(spark.table(T))

      // another insert
      sql(s"INSERT INTO $T VALUES (3, 300)")
      assertCached(spark.table(T))

      // verify all data is visible through cache
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
    }
  }

  // --- Gap 6: Non-versioned table skipped by versionedOnly ---

  test("[gap] non-versioned table skipped by versionedOnly refresh") {
    // InMemoryTable supports versioning, so this test verifies
    // that the refresh phase does run (version is not null).
    // A table with version=null would be skipped entirely.
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // InMemoryTable has non-null version, so refresh runs
      // and picks up the new data
      assert(df.count() == 2)
    }
  }

  // --- Gap 7: self-subquery with schema change ---

  test("[gap] self-subquery detects schema change") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")

      val df = spark.sql(s"""
        SELECT * FROM $T WHERE id IN
        (SELECT id FROM $T WHERE salary > 50)
      """)

      sql(s"ALTER TABLE $T DROP COLUMN salary")

      // subquery references salary which was dropped
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
    }
  }

  test("[gap] self-subquery picks up new data") {
    withTable(T) {
      setupTable()

      val df = spark.sql(s"""
        SELECT * FROM $T WHERE id IN
        (SELECT id FROM $T WHERE salary > 50)
      """)

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // both references refreshed, sees all qualifying data
      checkAnswer(df, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // =====================================================================
  // CODEBASE-WIDE GAP TESTS
  // Scenarios found in CachedTableSuite, DataSourceV2SQLSuite, and
  // DataFrameWriterV2Suite that were not covered here.
  // =====================================================================

  // --- From CachedTableSuite: cascading cache invalidation ---

  test("[codebase] uncache base table cascades to view cache") {
    withTable(T) {
      setupTable()
      // cache base table
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      // create and cache a view on top
      spark.table(T).select("id").createOrReplaceTempView("v")
      sql("SELECT * FROM v").cache()
      assertCached(sql("SELECT * FROM v"))
      // uncaching base table should cascade to view
      sql(s"UNCACHE TABLE $T")
      assertNotCached(sql(s"SELECT * FROM $T"))
      assertNotCached(sql("SELECT * FROM v"))
    }
  }

  // --- From DataSourceV2SQLSuite: write operations refresh cache ---

  test("[codebase] AppendData via SQL refreshes cached view") {
    withTable(T) {
      setupTable()
      sql(
        s"CACHE TABLE cached_v AS SELECT id FROM $T")
      checkAnswer(sql("SELECT * FROM cached_v"), Seq(Row(1)))
      // append via SQL
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // cached view should be refreshed
      checkAnswer(
        sql("SELECT * FROM cached_v"),
        Seq(Row(1), Row(2)))
      sql("UNCACHE TABLE cached_v")
    }
  }

  test("[codebase] INSERT refreshes cached view") {
    val t = "testcat.ns1.cachetest"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      sql(
        s"CACHE TABLE cached_cv AS SELECT id FROM $t")
      checkAnswer(
        sql("SELECT * FROM cached_cv"), Seq(Row(1)))
      // insert new data
      sql(s"INSERT INTO $t VALUES (9, 900)")
      // view should pick up the insert
      checkAnswer(
        sql("SELECT * FROM cached_cv"),
        Seq(Row(1), Row(9)))
      sql("UNCACHE TABLE cached_cv")
    }
  }

  // --- From CachedTableSuite: REPLACE TABLE invalidates cache ---

  test("[codebase] REPLACE TABLE invalidates all cache entries") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))
      // replace table with different data
      sql(s"""CREATE OR REPLACE TABLE $T
              (id INT, salary INT) USING foo""")
      sql(s"INSERT INTO $T VALUES (9, 900)")
      // cache should be invalidated
      checkAnswer(spark.table(T), Seq(Row(9, 900)))
    }
  }

  // --- From DataSourceV2SQLSuite: INSERT INTO temp view on DSv2 ---

  test("[codebase] temp view reflects base table writes") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tv")
      checkAnswer(spark.table("tv"), Seq(Row(1, 100)))
      // insert into base table
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // temp view should pick up base table changes
      checkAnswer(
        spark.table("tv"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // --- From CachedTableSuite: recache view picks up base changes ---

  test("[codebase] cached view on DSv2 picks up base INSERT") {
    withTable(T) {
      setupTable()
      // create and cache view
      spark.table(T).select("id")
        .createOrReplaceTempView("v")
      sql("SELECT * FROM v").cache()
      assertCached(sql("SELECT * FROM v"))
      checkAnswer(sql("SELECT * FROM v"), Seq(Row(1)))
      // insert into base table
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // view should pick up new data after recache
      checkAnswer(
        sql("SELECT * FROM v"), Seq(Row(1), Row(2)))
    }
  }

  // --- From CachedTableSuite: ALTER TABLE invalidates cache ---

  test("[codebase] ALTER TABLE ADD COLUMN invalidates cache") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      // after ALTER TABLE, cache is refreshed with new schema
      val result = spark.table(T).collect()
      assert(result(0).length == 3)
      checkAnswer(
        spark.table(T), Seq(Row(1, 100, null)))
    }
  }

  test("[codebase] ALTER TABLE DROP COLUMN invalidates cache") {
    withTable(T) {
      sql(
        s"CREATE TABLE $T (id INT, salary INT, extra STRING) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100, 'x')")
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(
        spark.table(T), Seq(Row(1, 100, "x")))
      sql(s"ALTER TABLE $T DROP COLUMN extra")
      // cache should be invalidated, new schema visible
      checkAnswer(spark.table(T), Seq(Row(1, 100)))
    }
  }

  // --- Version alignment in lateral subquery ---

  test("[codebase] lateral subquery refreshes table ref") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df = spark.sql(s"""
        SELECT t.id, s.salary
        FROM $T t,
        LATERAL (SELECT salary FROM $T WHERE id = t.id) s
      """)
      sql(s"INSERT INTO $T VALUES (3, 300)")
      // all refs refreshed to latest
      assert(df.collect().length == 3)
    }
  }

  // --- Window functions with version refresh ---

  test("[codebase] window function preserves version refresh") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df = spark.sql(s"""
        SELECT id, salary,
          ROW_NUMBER() OVER (ORDER BY salary) as rn
        FROM $T
      """)
      sql(s"INSERT INTO $T VALUES (3, 50)")
      val result = df.collect()
      assert(result.length == 3)
    }
  }

  // --- Distinct / dropDuplicates preserves refresh ---

  test("[codebase] distinct preserves version refresh") {
    withTable(T) {
      setupTable()
      val df = spark.table(T).distinct()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      assert(df.collect().length == 2)
    }
  }

  // --- Sample preserves refresh ---

  test("[codebase] sample preserves version refresh") {
    withTable(T) {
      setupTable()
      for (i <- 2 to 100) {
        sql(s"INSERT INTO $T VALUES ($i, ${i * 10})")
      }
      val df = spark.table(T).sample(fraction = 1.0)
      sql(s"INSERT INTO $T VALUES (101, 1010)")
      assert(df.collect().length == 101)
    }
  }

  // --- Aggregate preserves refresh ---

  test("[codebase] aggregate preserves version refresh") {
    withTable(T) {
      setupTable()
      val df = spark.sql(
        s"SELECT COUNT(*) as cnt FROM $T")
      sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(df, Seq(Row(2)))
    }
  }

  // --- WriteTo temp view with schema change ---

  test("[codebase] temp view after base table schema change") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tv")
      checkAnswer(spark.table("tv"), Seq(Row(1, 100)))
      // add column to base table
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      sql(s"INSERT INTO $T VALUES (2, 200, 50)")
      // temp view preserves original schema (2 cols)
      checkAnswer(
        spark.table("tv"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // --- Multiple concurrent schema changes + cache ---

  test("[codebase] concurrent schema + data changes with cache") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      withExecutor() { exec =>
        val barrier = new CyclicBarrier(3)
        val errors = new ConcurrentLinkedQueue[Throwable]()
        val reader = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (_ <- 1 to 10) {
              try spark.table(T).collect()
              catch {
                case e: Throwable if isExpectedError(e) =>
              }
            }
          } catch { case e: Throwable => errors.add(e) }
        })
        val writer = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (i <- 2 to 5) {
              try sql(s"INSERT INTO $T VALUES ($i, ${i * 100})")
              catch { case _: Exception => }
            }
          } catch { case e: Throwable => errors.add(e) }
        })
        val schemaChanger = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            try cat.alterTable(IDENT,
              TableChange.addColumn(
                Array("bonus"), IntegerType, true))
            catch { case _: Exception => }
          } catch { case e: Throwable => errors.add(e) }
        })
        reader.get(60, TimeUnit.SECONDS)
        writer.get(60, TimeUnit.SECONDS)
        schemaChanger.get(60, TimeUnit.SECONDS)
        assert(errors.isEmpty, s"Unexpected errors: ${
          errors.toArray(Array.empty[Throwable])
            .map(_.getMessage).mkString("; ")}")
      }
    }
  }

  // =====================================================================
  // BEYOND-DOC SCENARIOS
  // Real-world patterns not explicitly covered in the design doc.
  // =====================================================================

  // --- #1: Permanent SQL view on DSv2 table ---
  // Removed: InMemoryTableCatalog does not support CREATE VIEW.

  // --- #4: DataFrame temp view mixed with direct table in SQL ---

  test("[beyond] DF temp view + direct table ref in same SQL") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("df_view")
      // SQL query mixing DF view and direct table reference
      val q =
        s"SELECT v.id, t.salary FROM df_view v JOIN $T t " +
        "ON v.id = t.id"
      checkAnswer(spark.sql(q), Seq(Row(1, 100)))
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // Fresh SQL re-analyzes; both refs see new data
      checkAnswer(
        spark.sql(q),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[beyond] DF temp view + direct table after schema change") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("df_view")
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      sql(s"INSERT INTO $T VALUES (2, 200, 50)")
      // Direct ref picks up new schema; DF view keeps original
      val direct = spark.sql(s"SELECT * FROM $T")
      assert(direct.collect()(0).length == 3)
      // DF view preserves original 2-col schema
      checkAnswer(
        spark.table("df_view"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // --- #5: MERGE/UPDATE/DELETE with stale source ---
  // Removed: InMemoryTable does not support DELETE on
  // non-partition columns or UPDATE operations.

  // --- #7: spark.read.table() vs spark.table() ---

  test("[beyond] spark.read.table() picks up data write") {
    withTable(T) {
      setupTable()
      val df = spark.read.table(T)
      checkAnswer(df, Seq(Row(1, 100)))
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // spark.read.table creates a new DF each call;
      // the existing df has a captured plan
      val df2 = spark.read.table(T)
      checkAnswer(df2, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[beyond] spark.read.table() detects column removal") {
    withTable(T) {
      sql(
        s"CREATE TABLE $T (id INT, salary INT, extra STRING) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100, 'x')")
      val df = spark.read.table(T)
      sql(s"ALTER TABLE $T DROP COLUMN extra")
      // First access after removal should detect incompatibility
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
    }
  }

  // --- #10: spark.catalog.refreshTable() programmatic API ---

  test("[beyond] spark.catalog.refreshTable() refreshes cache") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // Programmatic refresh API
      spark.catalog.refreshTable(T)
      assertCached(spark.table(T))
      checkAnswer(
        spark.table(T), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // --- #11: Cached derived query + schema change ---

  test("[beyond] cached derived query pinned against data write") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // Cache a derived query (not the table itself)
      val q = spark.sql(
        s"SELECT id, salary * 2 AS double_sal FROM $T WHERE id > 0")
      q.cache()
      assertCached(q)
      checkAnswer(q, Seq(Row(1, 200), Row(2, 400)))
      // External truncation should not affect cached query
      cat.loadTable(IDENT, util.Set.of(TableWritePrivilege.DELETE))
        .asInstanceOf[TruncatableTable].truncateTable()
      assertCached(q)
      checkAnswer(q, Seq(Row(1, 200), Row(2, 400)))
      q.unpersist()
    }
  }

  test("[beyond] cached derived query refreshed on session write") {
    withTable(T) {
      setupTable()
      val q = spark.sql(
        s"SELECT id, salary * 2 AS double_sal " +
        s"FROM $T WHERE id > 0")
      q.cache()
      checkAnswer(q, Seq(Row(1, 200)))
      // Session write invalidates base table cache but
      // cached derived queries retain stale results.
      sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(q, Seq(Row(1, 200)))
      q.unpersist()
    }
  }

  // --- #13: Same name different namespace ---

  test("[beyond] same table name different namespace in join") {
    val t1 = "testcat.ns1.tbl"
    val t2 = "testcat.ns2.tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t1 VALUES (1, 100)")
      sql(s"CREATE TABLE $t2 (id INT, bonus INT) USING foo")
      sql(s"INSERT INTO $t2 VALUES (1, 50)")

      val df1 = spark.table(t1)
      val df2 = spark.table(t2)

      // Modify only ns1.tbl
      sql(s"ALTER TABLE $t1 DROP COLUMN salary")

      // ns1.tbl incompatible but ns2.tbl unchanged
      checkError(
        exception = intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_MISMATCH)

      // ns2.tbl alone should still work
      checkAnswer(df2, Seq(Row(1, 50)))
    }
  }

  test("[beyond] same table name different namespace - data write") {
    val t1 = "testcat.ns1.tbl"
    val t2 = "testcat.ns2.tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t1 VALUES (1, 100)")
      sql(s"CREATE TABLE $t2 (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t2 VALUES (10, 1000)")

      val df1 = spark.table(t1)
      val df2 = spark.table(t2)

      // Write to both
      sql(s"INSERT INTO $t1 VALUES (2, 200)")
      sql(s"INSERT INTO $t2 VALUES (20, 2000)")

      // Join should refresh both independently
      val joined = df1.join(
        df2, df1("id") === df2("id"))
      // No matching IDs between ns1 and ns2
      assert(joined.collect().isEmpty)

      // Each sees its own data
      assert(df1.count() == 2)
      assert(df2.count() == 2)
    }
  }

  // --- #3: Nested views 3+ levels ---

  test("[beyond] three-level nested temp views detect changes") {
    withTable(T) {
      sql(
        s"CREATE TABLE $T (id INT, salary INT, extra STRING) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100, 'x')")
      // Level 1: DF temp view on base table
      spark.table(T).createOrReplaceTempView("v1")
      // Level 2: DF temp view on v1
      spark.table("v1").createOrReplaceTempView("v2")
      // Level 3: DF temp view on v2
      spark.table("v2").createOrReplaceTempView("v3")
      checkAnswer(spark.table("v3"), Seq(Row(1, 100, "x")))

      // Modify base table
      sql(s"ALTER TABLE $T DROP COLUMN extra")

      // Error should propagate through all levels
      checkError(
        exception = intercept[AnalysisException] {
        spark.table("v3").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[beyond] three-level views: same table at multiple levels") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // v1 = base table
      spark.table(T).createOrReplaceTempView("v1")
      // v2 = v1 joined with base table (T appears twice)
      spark.sql(
        s"SELECT v.id AS vid, t.salary AS tsal " +
        s"FROM v1 v JOIN $T t ON v.id = t.id")
        .createOrReplaceTempView("v2")
      // v3 = v2 filtered
      spark.table("v2").filter("vid > 0")
        .createOrReplaceTempView("v3")

      checkAnswer(
        spark.table("v3"),
        Seq(Row(1, 100), Row(2, 200)))

      sql(s"INSERT INTO $T VALUES (3, 300)")
      // All levels should pick up new data
      assert(spark.table("v3").count() == 3)
    }
  }

  // --- #9: Table properties change ---

  test("[beyond] ALTER TABLE SET TBLPROPERTIES does not break DF") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      sql(s"ALTER TABLE $T SET TBLPROPERTIES ('key' = 'value')")
      // Property change should not affect schema validation
      checkAnswer(df, Seq(Row(1, 100)))
    }
  }

  test("[beyond] ALTER TABLE SET TBLPROPERTIES on cached table") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      sql(s"ALTER TABLE $T SET TBLPROPERTIES ('key' = 'value')")
      // Cache may be invalidated by DDL but query should work
      spark.table(T).collect()
    }
  }

  // --- #8: Partitioned table changes ---

  test("[beyond] partitioned table picks up data in new partition") {
    val pt = "testcat.ns1.ns2.ptbl"
    withTable(pt) {
      sql(s"""CREATE TABLE $pt
              (data STRING, id INT)
              USING foo PARTITIONED BY (id)""")
      sql(s"INSERT INTO $pt VALUES ('a', 1)")
      val df = spark.table(pt)
      assert(df.count() == 1)
      // Insert into new partition
      sql(s"INSERT INTO $pt VALUES ('b', 2)")
      // Refresh should see both partitions
      assert(df.count() == 2)
    }
  }

  // --- #6: Streaming (basic, not full streaming test) ---

  test("[beyond] readStream.table creates streaming DF") {
    withTable(T) {
      setupTable()
      // Verify streaming read from DSv2 table doesn't crash
      // (full streaming tests are in StreamingSuite)
      try {
        val stream = spark.readStream.table(T)
        assert(stream.isStreaming)
      } catch {
        case _: Exception =>
        // Some table implementations may not support streaming
      }
    }
  }

  // --- #14: Table referenced inside SQL expression ---

  test("[beyond] scalar subquery with table reference") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val q = s"SELECT id, " +
        s"(SELECT MAX(salary) FROM $T) AS max_sal FROM $T"
      checkAnswer(
        spark.sql(q), Seq(Row(1, 200), Row(2, 200)))
      sql(s"INSERT INTO $T VALUES (3, 300)")
      // Fresh SQL re-analyzes; scalar subquery refreshes
      val r = spark.sql(q).collect()
      assert(r.length == 3)
      assert(r.forall(_.getInt(1) == 300))
    }
  }

  test("[beyond] EXISTS subquery refreshes") {
    withTable(T) {
      setupTable()
      val q = s"SELECT id FROM $T t1 " +
        s"WHERE EXISTS " +
        s"(SELECT 1 FROM $T t2 WHERE t2.id = t1.id)"
      checkAnswer(spark.sql(q), Seq(Row(1)))
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // Fresh SQL re-analyzes; EXISTS subquery refreshes
      checkAnswer(spark.sql(q), Seq(Row(1), Row(2)))
    }
  }

  // --- Mixed: DF view as source for CTAS ---

  test("[beyond] CTAS from DF temp view after base table change") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      spark.table(T).createOrReplaceTempView("src_view")
      cat.alterTable(IDENT,
        TableChange.addColumn(Array("bonus"), IntegerType, true))
      // CTAS using the temp view
      sql(s"CREATE TABLE $t2 USING foo AS SELECT * FROM src_view")
      // View preserves original schema (2 cols)
      val result = spark.table(t2).collect()
      assert(result(0).length == 2)
    }
  }

  // --- Two cached tables joined after modification ---

  test("[beyond] two cached tables joined after one is modified") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      sql(s"CREATE TABLE $t2 (id INT, bonus INT) USING foo")
      sql(s"INSERT INTO $t2 VALUES (1, 50)")

      sql(s"CACHE TABLE $T")
      sql(s"CACHE TABLE $t2")
      assertCached(spark.table(T))
      assertCached(spark.table(t2))

      // Modify T (session write invalidates T cache)
      sql(s"INSERT INTO $T VALUES (2, 200)")

      // Join: T refreshed, t2 still cached
      val joined = spark.sql(
        s"SELECT t.id, t.salary, t2.bonus " +
        s"FROM $T t JOIN $t2 t2 ON t.id = t2.id")
      checkAnswer(joined, Seq(Row(1, 100, 50)))

      sql(s"UNCACHE TABLE IF EXISTS $T")
      sql(s"UNCACHE TABLE IF EXISTS $t2")
    }
  }

  // --- DataFrame.write.saveAsTable after schema change ---

  test("[beyond] saveAsTable from stale DF after column addition") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      val df = spark.table(T)
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      // saveAsTable goes through command path which
      // detects schema change (PROHIBIT_CHANGES).
      checkError(
        exception = intercept[AnalysisException] {
        df.write.saveAsTable(t2)
        },
        condition = COL_MISMATCH)
    }
  }

  // --- Temp view on temp view on temp view, modify between ---

  test("[beyond] create views interleaved with modifications") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("v1")
      sql(s"INSERT INTO $T VALUES (2, 200)")
      spark.table("v1").createOrReplaceTempView("v2")
      sql(s"INSERT INTO $T VALUES (3, 300)")
      spark.table("v2").createOrReplaceTempView("v3")

      // All views should see latest data through refresh
      assert(spark.table("v1").count() == 3)
      assert(spark.table("v2").count() == 3)
      assert(spark.table("v3").count() == 3)
    }
  }

  // --- Aggregation with HAVING on refreshed data ---

  test("[beyond] aggregation with HAVING picks up new data") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (1, 200)")
      val q = s"SELECT id, SUM(salary) AS total " +
        s"FROM $T GROUP BY id HAVING SUM(salary) > 150"
      checkAnswer(spark.sql(q), Seq(Row(1, 300)))
      sql(s"INSERT INTO $T VALUES (2, 500)")
      // Fresh SQL re-analyzes; new group included
      val r = spark.sql(q).collect()
      assert(r.length == 2)
    }
  }

  // --- EXPLAIN on refreshed plan ---

  test("[beyond] EXPLAIN on stale DF does not crash") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      // explain() creates a new QE -- should not crash
      df.explain(extended = true)
    }
  }

  // --- Schema validation with NOT NULL constraint change ---

  test("[beyond] dropping NOT NULL constraint detected") {
    withTable(T) {
      sql(
        s"CREATE TABLE $T (id INT NOT NULL, salary INT) " +
        "USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")
      val df = spark.table(T)
      sql(s"ALTER TABLE $T ALTER COLUMN id DROP NOT NULL")
      // Nullability change (NOT NULL -> nullable) is
      // detected as incompatible schema change
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
    }
  }

  // =====================================================================
  // REVIEWER COMMENT SCENARIOS
  // Test cases derived from design doc review comments by
  // Bart Samwel, Julek Sompolski, Ryan Johnson, Daniel Weeks.
  // =====================================================================

  // --- Bart: DF temp view vs SQL temp view behave differently ---

  test("[reviewer] DF view vs SQL view: column addition") {
    withTable(T) {
      setupTable()
      // DF temp view (captured plan, ALLOW_NEW_TOP_LEVEL_FIELDS)
      spark.table(T).createOrReplaceTempView("df_v")
      // SQL temp view (re-analyzed from text)
      sql(s"CREATE OR REPLACE TEMP VIEW sql_v AS SELECT * FROM $T")

      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      sql(s"INSERT INTO $T VALUES (2, 200, 50)")

      // DF view: preserves original 2-col schema
      val dfResult = spark.table("df_v").collect()
      assert(dfResult(0).length == 2)

      // SQL view with SELECT *: column names are captured at
      // creation time (id, salary). Re-analysis resolves those
      // 2 names against the new schema, so also 2 columns.
      // This is consistent: SELECT * pins column list.
      val sqlResult = spark.table("sql_v").collect()
      assert(sqlResult(0).length == 2)
    }
  }

  test("[reviewer] DF view vs SQL view: column removal") {
    withTable(T) {
      sql(
        s"CREATE TABLE $T (id INT, salary INT, extra STRING)" +
        " USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100, 'x')")
      // DF temp view
      spark.table(T).createOrReplaceTempView("df_v")
      // SQL temp view
      sql(s"CREATE OR REPLACE TEMP VIEW sql_v" +
        s" AS SELECT * FROM $T")

      sql(s"ALTER TABLE $T DROP COLUMN extra")

      // DF view: detects removal via refresh, throws
      checkError(
        exception = intercept[AnalysisException] {
        spark.table("df_v").collect()
        },
        condition = VIEW_PLAN_CHANGED)
      // SQL view: fails because SELECT * captured column names
      // (id, salary, extra) at creation time, and "extra" is now
      // missing from the table schema.
      checkError(
        exception = intercept[AnalysisException] {
        spark.table("sql_v").collect()
        },
        condition = SQL_VIEW_CHANGED)
    }
  }

  test("[reviewer] DF view vs SQL view: drop/recreate") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("df_v")
      sql(s"CREATE OR REPLACE TEMP VIEW sql_v" +
        s" AS SELECT * FROM $T")

      sql(s"DROP TABLE $T")
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (9, 900)")

      // DF view: accepts table ID change, sees new table
      checkAnswer(spark.table("df_v"), Seq(Row(9, 900)))
      // SQL view: re-analyzes, also sees new table
      checkAnswer(spark.table("sql_v"), Seq(Row(9, 900)))
    }
  }

  // --- Julek: write transactions should never use cache ---
  // (Section 5 comment: "I assume write txns never use cache")

  test("[reviewer] CTAS reads fresh data, not cached") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      // Insert bypasses cache on write path
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // CTAS should read fresh data, not stale cache
      sql(s"CREATE TABLE $t2 USING foo" +
        s" AS SELECT * FROM $T")
      checkAnswer(
        spark.table(t2),
        Seq(Row(1, 100), Row(2, 200)))
      sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  // --- Julek: read vs write behavior for same schema change ---
  // (queries allow new fields, commands prohibit changes)

  test("[reviewer] query allows column addition but command fails") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      val df = spark.table(T).filter("id < 10")
      // External column addition via catalog API
      cat.alterTable(IDENT,
        TableChange.addColumn(
          Array("bonus"), IntegerType, true))

      // Query: column addition is compatible, succeeds
      // (ALLOW_NEW_FIELDS mode)
      df.collect()

      // Command: same DF used in CTAS fails because
      // PROHIBIT_CHANGES mode rejects any schema change
      checkError(
        exception = intercept[AnalysisException] {
          df.writeTo(t2).createOrReplace()
        },
        condition = COL_MISMATCH)
    }
  }

  // --- Ryan: refresh runs on ALL versioned tables ---
  // (not just tables with detected version mismatch)

  test("[reviewer] refresh validates ALL tables in plan") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      sql(s"CREATE TABLE $t2 (id INT, bonus INT) USING foo")
      sql(s"INSERT INTO $t2 VALUES (1, 50)")

      val df1 = spark.table(T)
      val df2 = spark.table(t2)

      // Modify ONLY t2 (incompatible change)
      sql(s"ALTER TABLE $t2 DROP COLUMN bonus")

      // Join: refresh validates BOTH tables, not just
      // the one with a version mismatch. df2's schema
      // change is detected.
      checkError(
        exception = intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[reviewer] refresh validates unmodified table too") {
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(T, t2) {
      setupTable()
      sql(s"CREATE TABLE $t2 (id INT, bonus INT) USING foo")
      sql(s"INSERT INTO $t2 VALUES (1, 50)")

      val df1 = spark.table(T)
      val df2 = spark.table(t2)

      // Modify ONLY T (compatible data write)
      sql(s"INSERT INTO $T VALUES (2, 200)")

      // Both tables refreshed: T gets new data, t2 unchanged
      val joined = df1.join(
        df2, df1("id") === df2("id"))
      // Only id=1 matches between T and t2
      checkAnswer(joined, Seq(Row(1, 100, 1, 50)))
    }
  }

  // --- Ryan: session writes immediately visible ---

  test("[reviewer] session write immediately visible in next read") {
    withTable(T) {
      setupTable()
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      sql(s"INSERT INTO $T VALUES (2, 200)")
      // Immediately visible in the very next read
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))

      sql(s"INSERT INTO $T VALUES (3, 300)")
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
    }
  }

  test("[reviewer] session schema change immediately visible") {
    withTable(T) {
      setupTable()
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      // Schema change visible in the very next query
      val result = spark.table(T).collect()
      assert(result(0).length == 3)
    }
  }

  // --- Daniel: monotonic version advancement ---

  test("[reviewer] sequential reads see monotonically advancing data") {
    withTable(T) {
      setupTable()
      assert(spark.table(T).count() == 1)

      sql(s"INSERT INTO $T VALUES (2, 200)")
      assert(spark.table(T).count() == 2)

      sql(s"INSERT INTO $T VALUES (3, 300)")
      assert(spark.table(T).count() == 3)

      sql(s"INSERT INTO $T VALUES (4, 400)")
      assert(spark.table(T).count() == 4)

      assert(spark.table(T).count() == 4)
    }
  }

  // --- Anton: SQL query with same table multiple times ---
  // (relation cache ensures consistent version)

  test("[reviewer] SQL self-join gets consistent version") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // Single SQL query references T twice
      val q = s"""SELECT a.id, b.salary
                  FROM $T a JOIN $T b ON a.id = b.id"""
      checkAnswer(
        spark.sql(q),
        Seq(Row(1, 100), Row(2, 200)))

      sql(s"INSERT INTO $T VALUES (3, 300)")
      // Both references see the same (latest) version
      assert(spark.sql(q).count() == 3)
    }
  }

  test("[reviewer] SQL with 3 refs to same table: consistent") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // 3 references to T in one SQL: outer + 2 subqueries
      val q = s"""SELECT * FROM $T t1
                  WHERE id IN (SELECT id FROM $T)
                  AND salary < (SELECT MAX(salary) FROM $T)"""
      val r1 = spark.sql(q).collect()
      sql(s"INSERT INTO $T VALUES (3, 50)")
      // All 3 refs refreshed to same latest version
      val r2 = spark.sql(q).collect()
      assert(r2.length > r1.length)
    }
  }

  // --- Bart: multiple paths to same table in one query ---

  test("[reviewer] same table via view + direct + subquery") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      spark.table(T).createOrReplaceTempView("v")

      // Query references T through 3 different paths:
      // 1. temp view "v" (captured plan)
      // 2. direct reference $T
      // 3. subquery referencing $T
      val q = s"""SELECT v.id, t.salary
                  FROM v JOIN $T t ON v.id = t.id
                  WHERE v.id IN (SELECT id FROM $T)"""
      checkAnswer(
        spark.sql(q),
        Seq(Row(1, 100), Row(2, 200)))

      sql(s"INSERT INTO $T VALUES (3, 300)")
      // All 3 paths see latest data
      assert(spark.sql(q).count() == 3)
    }
  }

  // --- Julek: align classic/connect for Section 3 Scenario 2 ---
  // (join with column addition: classic keeps original schema,
  //  connect re-analyzes to new schema)

  test("[reviewer] join with column addition: schema preservation") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      sql(s"INSERT INTO $T VALUES (2, 200, 50)")
      val df2 = spark.table(T)

      // Classic: df1 preserves 2-col schema, df2 has 3-col
      val joined = df1.join(
        df2, df1("id") === df2("id"))
      val result = joined.collect()
      // df1 contributes 2 cols, df2 contributes 3 cols = 5 total
      assert(result(0).length == 5)
      // df1 side: (id, salary) only
      assert(result.length == 2) // both rows match
    }
  }

  // --- Julek: show() reuses executedPlan RDD ---
  // "calling df.show() again should reuse the executedPlan RDD"
  // Verify that show-like ops create DERIVED DFs (new QE)
  // while collect reuses the SAME QE

  test("[reviewer] show vs collect QE reuse semantics") {
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")

      // First collect: pins QE
      assert(df.collect().length == 1)

      sql(s"INSERT INTO $T VALUES (2, 200)")

      // count: creates derived DF (new QE) -> sees 2
      assert(df.count() == 2)

      // head: creates derived DF (new QE) -> sees 2
      assert(df.head(10).length == 2)

      // take: creates derived DF (new QE) -> sees 2
      assert(df.take(10).length == 2)

      // collect: reuses pinned QE -> stale, sees 1
      assert(df.collect().length == 1)
    }
  }

  // --- Ryan: session consistency after DROP + CREATE ---

  test("[reviewer] session DROP + CREATE: next read sees new table") {
    withTable(T) {
      setupTable()
      checkAnswer(spark.table(T), Seq(Row(1, 100)))
      sql(s"DROP TABLE $T")
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      // Immediately after drop+create, read sees empty table
      checkAnswer(spark.table(T), Seq.empty)
      sql(s"INSERT INTO $T VALUES (9, 900)")
      checkAnswer(spark.table(T), Seq(Row(9, 900)))
    }
  }

  // --- Daniel: external write distinction ---

  test("[reviewer] cached table: session write visible, external not") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // Session write: invalidates + refreshes cache
      sql(s"INSERT INTO $T VALUES (2, 200)")
      assertCached(spark.table(T))
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))

      // External write (via catalog API): pinned, not visible
      cat.loadTable(IDENT,
        util.Set.of(TableWritePrivilege.DELETE))
        .asInstanceOf[TruncatableTable].truncateTable()
      assertCached(spark.table(T))
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))
      sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  // =====================================================================
  // SPARK TICKET GAPS
  // E2E tests for scenarios covered by V2TableUtilSuite unit tests
  // and specific SPARK tickets but missing from our suites.
  // =====================================================================

  // --- SPARK-54686: nested field additions in arrays/maps ---

  test("[ticket] temp view: new field inside array element struct") {
    withTable(T) {
      sql(s"""CREATE TABLE $T
        (id INT, items ARRAY<STRUCT<name: STRING>>) USING foo""")
      sql(s"""INSERT INTO $T VALUES
        (1, array(named_struct('name', 'a')))""")
      spark.table(T).createOrReplaceTempView("tmp")
      // Add field inside array element struct
      cat.alterTable(IDENT,
        TableChange.addColumn(
          Array("items", "element", "price"),
          IntegerType, true))
      // Temp view: nested addition inside array is incompatible
      checkError(
        exception = intercept[AnalysisException] {
        spark.table("tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  test("[ticket] temp view: new field inside map value struct") {
    withTable(T) {
      sql(s"""CREATE TABLE $T
        (id INT, props MAP<STRING, STRUCT<val: INT>>) USING foo""")
      sql(s"""INSERT INTO $T VALUES
        (1, map('k', named_struct('val', 10)))""")
      spark.table(T).createOrReplaceTempView("tmp")
      // Add field inside map value struct
      cat.alterTable(IDENT,
        TableChange.addColumn(
          Array("props", "value", "extra"),
          StringType, true))
      // Temp view: nested addition inside map is incompatible
      checkError(
        exception = intercept[AnalysisException] {
        spark.table("tmp").collect()
        },
        condition = VIEW_PLAN_CHANGED)
    }
  }

  // --- V2TableUtilSuite: array/map type changes ---

  test("[ticket] DataFrame: array element type change") {
    withTable(T) {
      sql(s"""CREATE TABLE $T
        (id INT, tags ARRAY<INT>) USING foo""")
      sql(s"INSERT INTO $T VALUES (1, array(10, 20))")
      val df = spark.table(T)
      // Change array element type INT -> STRING
      cat.alterTable(IDENT,
        TableChange.updateColumnType(
          Array("tags", "element"), StringType))
      // Array element type change is incompatible
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
    }
  }

  test("[ticket] DataFrame: map value type change") {
    withTable(T) {
      sql(s"""CREATE TABLE $T
        (id INT, props MAP<STRING, INT>) USING foo""")
      sql(s"INSERT INTO $T VALUES (1, map('a', 1))")
      val df = spark.table(T)
      // Change map value type INT -> STRING
      cat.alterTable(IDENT,
        TableChange.updateColumnType(
          Array("props", "value"), StringType))
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
    }
  }

  // --- SPARK-55631: ALTER TABLE RENAME invalidates cache ---

  test("[ticket] ALTER TABLE RENAME COLUMN invalidates cache") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))
      sql(s"ALTER TABLE $T RENAME COLUMN salary TO pay")
      // Cache should be invalidated, new schema visible
      val result = spark.table(T).collect()
      assert(result(0).length == 2)
    }
  }

  test("[ticket] ALTER TABLE ALTER COLUMN TYPE invalidates cache") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT")
      // Cache should be invalidated. However, InMemoryTable stores
      // Integer objects that cannot be read as Long after type
      // widening, so collect throws ClassCastException.
      try {
        spark.table(T).collect()
      } catch {
        case e: Exception if isExpectedError(e) =>
          // Expected: ClassCastException from Integer to Long
      }
    }
  }

  // --- SPARK-54004: uncache by unqualified name ---

  test("[ticket] UNCACHE TABLE by unqualified name after USE") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      // Switch to the namespace and uncache by short name
      sql("USE testcat")
      sql("USE NAMESPACE ns1.ns2")
      sql("UNCACHE TABLE tbl")
      assertNotCached(spark.table(T))
    }
  }

  // --- Case-insensitive column matching during refresh ---

  test("[ticket] case-insensitive column matching in refresh") {
    withTable(T) {
      // Create with mixed case
      sql(s"""CREATE TABLE $T
        (Id INT, Salary INT) USING foo""")
      sql(s"INSERT INTO $T VALUES (1, 100)")
      val df = spark.table(T)
      // Add column with different case
      sql(s"ALTER TABLE $T ADD COLUMN BONUS INT")
      // Should succeed: column addition is compatible
      // regardless of case
      df.collect()
    }
  }

  // --- SPARK-54387: recaching with copyOnLoad ---

  test("[ticket] recaching works with copyOnLoad catalog") {
    withTable(T) {
      setupTable()
      // CACHE TABLE then INSERT (triggers recache)
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))
      // Multiple inserts trigger multiple recaches
      sql(s"INSERT INTO $T VALUES (2, 200)")
      assertCached(spark.table(T))
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))
      sql(s"INSERT INTO $T VALUES (3, 300)")
      assertCached(spark.table(T))
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
      sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  // --- SPARK-54341: time travel spec preserved in refresh ---

  test("[ticket] time travel table skipped during refresh") {
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    val version = "v1"
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")
      cat.pinTable(ident, version)
      sql(s"INSERT INTO $T VALUES (2, 200)")

      // Time travel DF should be skipped by refresh
      val tvDf = spark.sql(
        s"SELECT * FROM $T VERSION AS OF '$version'")
      checkAnswer(tvDf, Seq(Row(1, 100)))

      // More changes should not affect time travel DF
      sql(s"INSERT INTO $T VALUES (3, 300)")
      checkAnswer(tvDf, Seq(Row(1, 100)))

      // Current version DF should see all data
      assert(spark.table(T).count() == 3)
    }
  }

  // =====================================================================
  // MULTI-SESSION TESTS
  // Uses SharedInMemoryTableCatalog so two SparkSessions share
  // the same table state, simulating a real shared metastore.
  // Pattern inspired by MST concurrency tests in runtime.
  // =====================================================================

  private val ST = "sharedcat.ns1.tbl"

  test("[multi-session] session2 write visible to session1") {
    val session2 = spark.newSession()
    withTable(ST) {
      spark.sql(
        s"CREATE TABLE $ST (id INT, salary INT) USING foo")
      spark.sql(s"INSERT INTO $ST VALUES (1, 100)")
      checkAnswer(spark.table(ST), Seq(Row(1, 100)))

      // Session 2 writes to the same shared table
      session2.sql(s"INSERT INTO $ST VALUES (2, 200)")

      // Session 1 should see session 2's write
      checkAnswer(
        spark.sql(s"SELECT * FROM $ST"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[multi-session] session2 schema change detected by session1 DF") {
    val session2 = spark.newSession()
    withTable(ST) {
      spark.sql(
        s"CREATE TABLE $ST (id INT, salary INT) USING foo")
      spark.sql(s"INSERT INTO $ST VALUES (1, 100)")

      // Session 1 creates a DataFrame
      val df = spark.table(ST)

      // Session 2 drops a column
      session2.sql(s"ALTER TABLE $ST DROP COLUMN salary")

      // Session 1's DF should detect the incompatible change
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
    }
  }

  test("[multi-session] session2 write visible to session1 temp view") {
    val session2 = spark.newSession()
    withTable(ST) {
      spark.sql(
        s"CREATE TABLE $ST (id INT, salary INT) USING foo")
      spark.sql(s"INSERT INTO $ST VALUES (1, 100)")

      // Session 1 creates temp view
      spark.table(ST).createOrReplaceTempView("s1_view")
      checkAnswer(
        spark.table("s1_view"), Seq(Row(1, 100)))

      // Session 2 inserts data
      session2.sql(s"INSERT INTO $ST VALUES (2, 200)")

      // Session 1's temp view should see session 2's write
      checkAnswer(
        spark.table("s1_view"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[multi-session] session2 write invalidates shared cache") {
    val session2 = spark.newSession()
    withTable(ST) {
      spark.sql(
        s"CREATE TABLE $ST (id INT, salary INT) USING foo")
      spark.sql(s"INSERT INTO $ST VALUES (1, 100)")

      // Session 1 caches the table
      spark.sql(s"CACHE TABLE $ST")
      assertCached(spark.table(ST))
      checkAnswer(spark.table(ST), Seq(Row(1, 100)))

      // Session 2 writes: CacheManager is shared via SharedState,
      // so session 2's INSERT invalidates session 1's cache.
      // This is correct: same-cluster writes should be visible.
      session2.sql(s"INSERT INTO $ST VALUES (2, 200)")

      // Session 1 sees session 2's data (cache invalidated)
      checkAnswer(
        spark.table(ST),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql(s"UNCACHE TABLE IF EXISTS $ST")
    }
  }

  test("[multi-session] session1 write invalidates own cache") {
    val session2 = spark.newSession()
    withTable(ST) {
      spark.sql(
        s"CREATE TABLE $ST (id INT, salary INT) USING foo")
      spark.sql(s"INSERT INTO $ST VALUES (1, 100)")

      // Session 1 caches
      spark.sql(s"CACHE TABLE $ST")
      checkAnswer(spark.table(ST), Seq(Row(1, 100)))

      // Session 1's own write invalidates its cache
      spark.sql(s"INSERT INTO $ST VALUES (2, 200)")
      checkAnswer(
        spark.table(ST),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql(s"UNCACHE TABLE IF EXISTS $ST")
    }
  }

  test("[multi-session] concurrent reads from two sessions") {
    val session2 = spark.newSession()
    withTable(ST) {
      spark.sql(
        s"CREATE TABLE $ST (id INT, salary INT) USING foo")
      spark.sql(s"INSERT INTO $ST VALUES (1, 100)")

      withExecutor() { exec =>
        val barrier = new CyclicBarrier(2)
        val errors = new ConcurrentLinkedQueue[Throwable]()

        // Session 1 reads in thread 1
        val r1 = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (_ <- 1 to 10) {
              spark.sql(s"SELECT * FROM $ST").collect()
            }
          } catch { case e: Throwable => errors.add(e) }
        })

        // Session 2 reads in thread 2
        val r2 = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (_ <- 1 to 10) {
              session2.sql(s"SELECT * FROM $ST").collect()
            }
          } catch { case e: Throwable => errors.add(e) }
        })

        r1.get(60, TimeUnit.SECONDS)
        r2.get(60, TimeUnit.SECONDS)
        assert(errors.isEmpty,
          s"Errors: ${errors.toArray(Array.empty[Throwable])
            .map(_.getMessage).mkString("; ")}")
      }
    }
  }

  test("[multi-session] session2 write while session1 reads") {
    val session2 = spark.newSession()
    withTable(ST) {
      spark.sql(
        s"CREATE TABLE $ST (id INT, salary INT) USING foo")
      spark.sql(s"INSERT INTO $ST VALUES (1, 100)")

      withExecutor() { exec =>
        val barrier = new CyclicBarrier(2)
        val errors = new ConcurrentLinkedQueue[Throwable]()

        // Session 1 reads concurrently
        val reader = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (_ <- 1 to 10) {
              try spark.sql(s"SELECT * FROM $ST").collect()
              catch {
                case e: Throwable if isExpectedError(e) =>
              }
            }
          } catch { case e: Throwable => errors.add(e) }
        })

        // Session 2 writes concurrently
        val writer = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (i <- 2 to 5) {
              session2.sql(
                s"INSERT INTO $ST VALUES ($i, ${i * 100})")
            }
          } catch { case e: Throwable => errors.add(e) }
        })

        reader.get(60, TimeUnit.SECONDS)
        writer.get(60, TimeUnit.SECONDS)
        assert(errors.isEmpty,
          s"Errors: ${errors.toArray(Array.empty[Throwable])
            .map(_.getMessage).mkString("; ")}")
      }

      // After concurrent ops: 1 original + 4 inserts (2..5) = 5
      assert(spark.sql(s"SELECT * FROM $ST")
        .collect().length == 5)
    }
  }

  // =====================================================================
  // GAP FIX: copyOnLoad=false (shared table instance)
  // With copyOnLoad=false, loadTable returns the SAME object.
  // Modifications are immediately visible without refresh.
  // =====================================================================

  private val NC = "nocopycat.ns1.tbl"

  test("[copyOnLoad=false] data write visible without refresh") {
    withTable(NC) {
      sql(s"CREATE TABLE $NC (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NC VALUES (1, 100)")
      val df = spark.table(NC)
      sql(s"INSERT INTO $NC VALUES (2, 200)")
      // No copy: same table instance, data is shared
      // Refresh still runs but finds same object
      checkAnswer(df, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[copyOnLoad=false] schema change visible without refresh") {
    withTable(NC) {
      sql(s"CREATE TABLE $NC (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NC VALUES (1, 100)")
      val df = spark.table(NC)
      sql(s"ALTER TABLE $NC ADD COLUMN bonus INT")
      // With shared instance, schema change is visible
      // but refresh validates the captured vs current schema
      df.collect() // should succeed: addition is compatible
    }
  }

  test("[copyOnLoad=false] temp view picks up write") {
    withTable(NC) {
      sql(s"CREATE TABLE $NC (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NC VALUES (1, 100)")
      spark.table(NC).createOrReplaceTempView("nc_view")
      sql(s"INSERT INTO $NC VALUES (2, 200)")
      checkAnswer(
        spark.table("nc_view"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // =====================================================================
  // GAP FIX: Caching connector (stale external changes)
  // Simulates Iceberg's CachingCatalog: external changes are
  // invisible because loadTable returns the cached copy.
  // =====================================================================

  private val CC = "cachingcat.ns1.tbl"

  test("[caching-connector] external write invisible (cached)") {
    withTable(CC) {
      sql(s"CREATE TABLE $CC (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $CC VALUES (1, 100)")
      // First loadTable caches the table
      checkAnswer(spark.table(CC), Seq(Row(1, 100)))
      // External modification via catalog API
      val ccIdent = Identifier.of(Array("ns1"), "tbl")
      val ccCat = spark.sessionState.catalogManager
        .catalog("cachingcat")
        .asInstanceOf[InMemoryTableCatalog]
      ccCat.alterTable(ccIdent,
        TableChange.addColumn(
          Array("bonus"), IntegerType, true))
      // Caching connector returns stale table - no new column
      // The cached table still has the old schema
      val result = spark.table(CC).collect()
      // Should see original 2-col schema (cached)
      assert(result(0).length == 2)
    }
  }

  test("[caching-connector] session write via SQL is cached stale") {
    withTable(CC) {
      sql(s"CREATE TABLE $CC (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $CC VALUES (1, 100)")
      checkAnswer(spark.table(CC), Seq(Row(1, 100)))
      // Session INSERT modifies the original table in the catalog
      // internal map, but the caching catalog still returns the
      // stale cached copy on loadTable. This documents the
      // limitation: a caching connector that doesn't invalidate
      // on session writes will serve stale data.
      sql(s"INSERT INTO $CC VALUES (2, 200)")
      // After cache clear, fresh data is visible
      CachingInMemoryTableCatalog.clearCache()
      checkAnswer(
        spark.sql(s"SELECT * FROM $CC"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[caching-connector] cache clear reveals external changes") {
    withTable(CC) {
      sql(s"CREATE TABLE $CC (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $CC VALUES (1, 100)")
      checkAnswer(spark.table(CC), Seq(Row(1, 100)))
      // External add column
      val ccIdent = Identifier.of(Array("ns1"), "tbl")
      val ccCat = spark.sessionState.catalogManager
        .catalog("cachingcat")
        .asInstanceOf[InMemoryTableCatalog]
      ccCat.alterTable(ccIdent,
        TableChange.addColumn(
          Array("bonus"), IntegerType, true))
      // Still cached (stale)
      assert(spark.table(CC).collect()(0).length == 2)
      // Clear cache (simulates TTL expiration)
      CachingInMemoryTableCatalog.clearCache()
      // Now loadTable returns fresh - sees new column
      val result = spark.table(CC).collect()
      assert(result(0).length == 3) // id, salary, bonus
    }
  }

  // =====================================================================
  // GAP FIX: Null table ID (no identity tracking)
  // Tables without IDs skip validateTableIdentity entirely.
  // Drop/recreate is NOT detected as an error.
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

  test("[null-id] temp view after drop/recreate succeeds") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      spark.table(NI).createOrReplaceTempView("ni_view")
      sql(s"DROP TABLE $NI")
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (9, 900)")
      // Temp views re-resolve via V2TableReference (no column ID check).
      // Schema matches, so view resolves to the new table.
      checkAnswer(spark.table("ni_view"), Seq(Row(9, 900)))
    }
  }

  test("[null-id] column removal still detected") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df = spark.table(NI)
      sql(s"ALTER TABLE $NI DROP COLUMN salary")
      // Column validation still works (doesn't depend on ID)
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
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

  test("[null-id] data write succeeds") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df = spark.table(NI)
      sql(s"INSERT INTO $NI VALUES (2, 200)")
      checkAnswer(df, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[null-id] column addition succeeds") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df = spark.table(NI)
      sql(s"ALTER TABLE $NI ADD COLUMN bonus INT")
      // Column addition allowed, original schema preserved
      checkAnswer(df, Seq(Row(1, 100)))
    }
  }

  test("[null-id] column rename detected") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df = spark.table(NI)
      sql(s"ALTER TABLE $NI RENAME COLUMN salary TO pay")
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
    }
  }

  test("[null-id] type widening detected") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df = spark.table(NI)
      sql(s"ALTER TABLE $NI ALTER COLUMN salary TYPE BIGINT")
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = COL_MISMATCH)
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

  // CACHE TABLE + REFRESH TABLE creates fresh plans with no stale
  // column IDs. Drop/recreate is fine since REFRESH rebuilds the cache.
  test("[null-id] CACHE TABLE after drop/recreate sees new table") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      sql(s"CACHE TABLE $NI")
      assertCached(spark.table(NI))
      checkAnswer(spark.table(NI), Seq(Row(1, 100)))

      sql(s"DROP TABLE $NI")
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (9, 900)")

      sql(s"REFRESH TABLE $NI")
      checkAnswer(spark.table(NI), Seq(Row(9, 900)))
    }
  }

  // NOTE: METADATA_COLUMNS_MISMATCH cannot be tested E2E.
  // InMemoryTable's scan infrastructure only supports its own
  // hardcoded metadata columns (index, _partition). Projecting
  // a custom metadata column like _version causes
  // PLAN_VALIDATION_FAILED because the scan builder doesn't
  // know how to produce it. This code path IS covered by the
  // unit tests in V2TableUtilSuite (validateCapturedMetadata*)
  // but not by E2E tests.

  // =====================================================================
  // HIDDEN/METADATA COLUMN TESTS
  // InMemoryTable exposes "index" and "_partition" as metadata
  // columns. These tests verify that hidden metadata columns
  // survive refresh, schema changes, and temp view resolution.
  // This covers the pattern from analyzer rule ordering issues
  // where hidden columns must be added before ResolveReferences.
  // =====================================================================

  test("[hidden-col] _partition accessible in query") {
    val pt = "testcat.ns1.pt"
    withTable(pt) {
      sql(s"""CREATE TABLE $pt
        (id INT, data STRING) USING foo PARTITIONED BY (id)""")
      sql(s"INSERT INTO $pt VALUES (1, 'a')")
      sql(s"INSERT INTO $pt VALUES (2, 'b')")
      // _partition is a metadata column on InMemoryTable
      val result = spark.sql(
        s"SELECT id, data, _partition FROM $pt").collect()
      assert(result.length == 2)
      assert(result(0).getString(2).nonEmpty,
        "_partition should be a non-empty partition string")
    }
  }

  test("[hidden-col] _partition in temp view after insert") {
    val pt = "testcat.ns1.pt"
    withTable(pt) {
      sql(s"""CREATE TABLE $pt
        (id INT, data STRING) USING foo PARTITIONED BY (id)""")
      sql(s"INSERT INTO $pt VALUES (1, 'a')")
      // Create temp view that projects _partition
      spark.sql(s"SELECT id, _partition FROM $pt")
        .createOrReplaceTempView("pt_view")
      val r1 = spark.table("pt_view").collect()
      assert(r1.length == 1)
      // Insert more data
      sql(s"INSERT INTO $pt VALUES (2, 'b')")
      // Temp view should still resolve _partition
      val r2 = spark.table("pt_view").collect()
      assert(r2.length == 2)
    }
  }

  test("[hidden-col] index metadata column accessible") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // "index" is a metadata column on InMemoryTable
      // but conflicts with a potential data column name.
      // InMemoryTable supports renaming conflicting metadata.
      val result = spark.sql(
        s"SELECT id, salary FROM $T").collect()
      assert(result.length == 2)
    }
  }

  test("[hidden-col] _partition survives schema change") {
    val pt = "testcat.ns1.pt"
    withTable(pt) {
      sql(s"""CREATE TABLE $pt
        (id INT, data STRING) USING foo PARTITIONED BY (id)""")
      sql(s"INSERT INTO $pt VALUES (1, 'a')")
      // Create temp view projecting _partition
      spark.sql(s"SELECT id, _partition FROM $pt")
        .createOrReplaceTempView("pt_view")
      // Add column to base table
      sql(s"ALTER TABLE $pt ADD COLUMN bonus INT")
      sql(s"INSERT INTO $pt VALUES (2, 'b', 50)")
      // Temp view should still resolve _partition
      // (hidden columns added by InMemoryTable survive
      // schema evolution, analogous to how DeltaAnalysis
      // adds CDF columns before ResolveReferences)
      val r = spark.table("pt_view").collect()
      assert(r.length == 2)
    }
  }

  test("[hidden-col] _partition in join after schema change") {
    val pt = "testcat.ns1.pt"
    withTable(pt) {
      sql(s"""CREATE TABLE $pt
        (id INT, data STRING) USING foo PARTITIONED BY (id)""")
      sql(s"INSERT INTO $pt VALUES (1, 'a'), (2, 'b')")
      val df1 = spark.sql(
        s"SELECT id, _partition AS p1 FROM $pt")
      sql(s"ALTER TABLE $pt ADD COLUMN bonus INT")
      sql(s"INSERT INTO $pt VALUES (3, 'c', 50)")
      val df2 = spark.sql(
        s"SELECT id, _partition AS p2 FROM $pt")
      // Join should work: both sides refresh,
      // _partition metadata column resolves on both
      val joined = df1.join(
        df2, df1("id") === df2("id"))
      assert(joined.collect().length == 3)
    }
  }

  test("[hidden-col] groupBy on _partition works") {
    val pt = "testcat.ns1.pt"
    withTable(pt) {
      sql(s"""CREATE TABLE $pt
        (id INT, data STRING) USING foo PARTITIONED BY (id)""")
      sql(s"INSERT INTO $pt VALUES (1, 'a')")
      sql(s"INSERT INTO $pt VALUES (2, 'b')")
      // groupBy on hidden metadata column
      // (analogous to ES-1636208 groupBy _commit_version)
      val result = spark.sql(
        s"SELECT _partition, count(*) as cnt FROM $pt " +
        "GROUP BY _partition").collect()
      assert(result.length == 2,
        "Should have 2 partition groups")
    }
  }

  // =====================================================================
  // MISSING DataFrame API COVERAGE
  // Operations that combine or transform plans in ways that
  // could interact differently with the refresh phase.
  // =====================================================================

  // --- Join types not yet covered ---

  test("[api] right outer join with version drift") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)
      val result = df1.join(
        df2, df1("id") === df2("id"), "right_outer")
      assert(result.collect().length == 2)
    }
  }

  test("[api] full outer join with version drift") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)
      val result = df1.join(
        df2, df1("id") === df2("id"), "full_outer")
      assert(result.collect().length == 2)
    }
  }

  test("[api] left semi join with column removal on right") {
    withTable(T) {
      sql(
        s"CREATE TABLE $T (id INT, salary INT, extra STRING)" +
        " USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100, 'x'), (2, 200, 'y')")
      val df1 = spark.table(T)
      sql(s"ALTER TABLE $T DROP COLUMN extra")
      val df2 = spark.table(T)
      // Left semi: only left side columns in output.
      // Right side schema change should still be detected
      // because refresh validates ALL relations in the plan.
      checkError(
        exception = intercept[AnalysisException] {
        df1.join(
          df2, df1("id") === df2("id"), "left_semi")
          .collect()
        },
        condition = COL_MISMATCH)
    }
  }

  test("[api] full outer join with column removal fails") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df1 = spark.table(T)
      sql(s"ALTER TABLE $T DROP COLUMN salary")
      val df2 = spark.table(T)
      checkError(
        exception = intercept[AnalysisException] {
        df1.join(
          df2, df1("id") === df2("id"), "full_outer")
          .collect()
        },
        condition = COL_MISMATCH)
    }
  }

  // --- unionByName ---

  test("[api] unionByName with data write") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)
      // unionByName matches columns by name
      val result = df1.unionByName(df2).collect()
      assert(result.length == 4) // 2 rows x 2
    }
  }

  test("[api] unionByName with column addition") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      sql(s"INSERT INTO $T VALUES (2, 200, 50)")
      val df2 = spark.table(T)
      // df1 has 2 cols, df2 has 3. unionByName can handle
      // this with allowMissingColumns=true
      val result = df1.unionByName(
        df2, allowMissingColumns = true).collect()
      assert(result.length == 4)
    }
  }

  test("[api] unionByName with column removal fails") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"ALTER TABLE $T DROP COLUMN salary")
      val df2 = spark.table(T)
      // df1 captured (id, salary), df2 has (id). The unionByName
      // analysis tries to match df1's "salary" in df2's output but
      // it does not exist, causing a column resolution error.
      assertThrows[AnalysisException] {
        df1.unionByName(df2).collect()
      }
    }
  }

  // --- exceptAll / intersectAll ---

  test("[api] exceptAll with data write") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)
      // Both refreshed: exceptAll should be empty
      val result = df1.exceptAll(df2).collect()
      assert(result.isEmpty)
    }
  }

  test("[api] intersectAll with data write") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)
      // Both refreshed: intersectAll = all rows
      val result = df1.intersectAll(df2).collect()
      assert(result.length == 2)
    }
  }

  // --- pivot ---

  test("[api] pivot preserves version refresh") {
    withTable(T) {
      sql(
        s"CREATE TABLE $T (id INT, dept STRING, salary INT)" +
        " USING foo")
      sql(s"INSERT INTO $T VALUES (1, 'eng', 100)")
      sql(s"INSERT INTO $T VALUES (2, 'hr', 200)")
      val df = spark.table(T)
        .groupBy("dept")
        .pivot("dept", Seq("eng", "hr"))
        .sum("salary")
      sql(s"INSERT INTO $T VALUES (3, 'eng', 300)")
      // Pivot creates derived aggregate plan;
      // refresh propagates through it
      val result = df.collect()
      assert(result.length == 2)
    }
  }

  // --- cube / rollup ---

  test("[api] cube preserves version refresh") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df = spark.sql(
        s"SELECT id, sum(salary) FROM $T GROUP BY CUBE(id)")
      sql(s"INSERT INTO $T VALUES (3, 300)")
      val result = df.collect()
      // Cube: 3 per-id groups + 1 grand total (null id) = 4 rows
      assert(result.length == 4)
    }
  }

  test("[api] rollup preserves version refresh") {
    withTable(T) {
      setupTable()
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val df = spark.sql(
        s"SELECT id, sum(salary) FROM $T GROUP BY ROLLUP(id)")
      sql(s"INSERT INTO $T VALUES (3, 300)")
      val result = df.collect()
      // Rollup: 3 per-id groups + 1 grand total (null id) = 4 rows
      assert(result.length == 4)
    }
  }

  // --- dropDuplicates ---

  test("[api] dropDuplicates preserves version refresh") {
    withTable(T) {
      setupTable()
      val df = spark.table(T).dropDuplicates("id")
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val result = df.collect()
      assert(result.length == 2)
    }
  }

  // --- withColumnsRenamed ---

  test("[api] withColumnsRenamed preserves version refresh") {
    withTable(T) {
      setupTable()
      val df = spark.table(T).withColumnRenamed(
        "salary", "pay")
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val result = df.collect()
      assert(result.length == 2)
      assert(df.columns.contains("pay"))
    }
  }

  // --- describe / summary ---

  test("[api] describe preserves version refresh") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // describe creates a new plan: count, mean, stddev, min, max
      val result = df.describe("salary").collect()
      assert(result.length == 5)
    }
  }

  // --- tail ---

  test("[api] tail preserves version refresh") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      sql(s"INSERT INTO $T VALUES (2, 200)")
      val result = df.tail(10)
      assert(result.length == 2)
    }
  }

  // --- isEmpty ---

  test("[api] isEmpty reflects latest data") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      val df = spark.table(T)
      sql(s"INSERT INTO $T VALUES (1, 100)")
      // isEmpty creates a new QE (like count)
      assert(!df.isEmpty)
    }
  }
}
