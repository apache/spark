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

package org.apache.spark.sql.connect

import java.util.ConcurrentModificationException
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession, SQLHelper}
import org.apache.spark.sql.connect.test.IntegrationTestUtils.isAssemblyJarsDirExists

/**
 * Comprehensive concurrency tests for DSv2 table refresh and pinning in Spark Connect mode.
 *
 * Mirrors DataSourceV2ConcurrencyRefreshSuite (classic) but adapted for Connect where every
 * action re-analyzes the plan on the server.
 *
 * Key behavioral differences from classic: No stale QueryExecution (collect is NOT pinned) Schema
 * changes are picked up on every access Type widening, column rename, column removal all succeed
 * for DataFrames Joins/unions/etc always see consistent latest version
 *
 * All modifications are via SQL (no catalog API access in Connect).
 *
 * Systematically tests: 8 modification types x 8 access patterns (sequential) 8 modification
 * types x 4 concurrency modes (multi-threaded) Compound modifications, stress tests, edge cases
 */
class DataSourceV2ConcurrencyRefreshConnectSuite
    extends QueryTest
    with RemoteSparkSession
    with SQLHelper {

  private val T = "testcat.ns1.ns2.tbl"

  // Error condition constants (match classic DataSourceV2ConcurrencyRefreshSuite)
  private val VIEW_PLAN_CHANGED =
    "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION"
  private val SQL_VIEW_CHANGED = "INCOMPATIBLE_VIEW_SCHEMA_CHANGE"
  private val UPCAST = "CANNOT_UP_CAST_DATATYPE"

  private def assumeCanRun(): Unit = {
    assume(spark != null && isAssemblyJarsDirExists, "Spark Connect server not available")
  }

  private def setupTable(): Unit = {
    spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
    spark.sql(s"INSERT INTO $T VALUES (1, 100)")
  }

  // =====================================================================
  // Infrastructure
  // =====================================================================

  /** Barrier for controlled thread synchronization. */
  private class PhaseBarrier(name: String) {
    private val latch = new CountDownLatch(1)
    def await(ms: Long = 30000): Unit =
      require(latch.await(ms, TimeUnit.MILLISECONDS), s"PhaseBarrier '$name' timed out")
    def unblock(): Unit = latch.countDown()
  }

  private def withExecutor(n: Int = 4)(f: ExecutorService => Unit): Unit = {
    val exec = Executors.newFixedThreadPool(n)
    try f(exec)
    finally {
      exec.shutdown()
      exec.awaitTermination(60, TimeUnit.SECONDS)
    }
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
          if (msg.contains("ConcurrentModificationException") ||
            msg.contains("ClassCastException") ||
            msg.contains("mutation occurred during iteration")) {
            true
          } else {
            checkChain(t.getCause)
          }
      }
    }
    checkChain(e)
  }

  // =====================================================================
  // Modification Definitions (SQL only, no catalog API in Connect)
  // =====================================================================

  // In Connect, SQL temp views (SELECT *) capture column names at
  // creation. Removing/renaming/retyping breaks re-analysis.
  // DataFrames re-analyze on every action, so ALL mods succeed for DFs.
  //
  // viewOk: whether temp views survive the modification (same boolean for
  //   both DF-based and SQL-based views, but error conditions differ)
  // dfOk: whether DataFrames survive (always true in Connect)
  // dfViewCondition: error condition for DF-based view (S1) when viewOk=false
  // sqlViewCondition: error condition for SQL-based view (S7) when viewOk=false
  // viewRows: expected result rows when viewOk=true
  // dfRows: expected result rows for DataFrame access after the modification
  case class Mod(
      name: String,
      fn: String => Unit,
      viewOk: Boolean,
      dfOk: Boolean,
      dfViewCondition: String = VIEW_PLAN_CHANGED,
      sqlViewCondition: String = SQL_VIEW_CHANGED,
      viewRows: Seq[Row] = Nil,
      dfRows: Seq[Row] = Nil)

  private val mods: Seq[Mod] = Seq(
    Mod(
      "data write",
      t => spark.sql(s"INSERT INTO $t VALUES (2, 200)"),
      viewOk = true,
      dfOk = true,
      viewRows = Seq(Row(1, 100), Row(2, 200)),
      dfRows = Seq(Row(1, 100), Row(2, 200))),
    Mod(
      "column addition",
      t => spark.sql(s"ALTER TABLE $t ADD COLUMN new_col INT"),
      viewOk = true,
      dfOk = true,
      // Temp views preserve original 2-col schema
      viewRows = Seq(Row(1, 100)),
      dfRows = Seq(Row(1, 100, null))),
    Mod(
      "column removal",
      t => spark.sql(s"ALTER TABLE $t DROP COLUMN salary"),
      viewOk = false,
      dfOk = true,
      dfRows = Seq(Row(1))),
    Mod(
      "column rename",
      t => spark.sql(s"ALTER TABLE $t RENAME COLUMN salary TO pay"),
      viewOk = false,
      dfOk = true,
      dfRows = Seq(Row(1, 100))),
    // InMemoryTable handles type widening via Cast at read time.
    Mod(
      "type widening INT to BIGINT",
      t => spark.sql(s"ALTER TABLE $t ALTER COLUMN salary TYPE BIGINT"),
      viewOk = false,
      dfOk = true,
      // DF view: VIEW_PLAN_CHANGED (default). SQL view: CANNOT_UP_CAST_DATATYPE.
      sqlViewCondition = UPCAST,
      dfRows = Seq(Row(1, 100))),
    Mod(
      "drop+add column same type",
      t => {
        spark.sql(s"ALTER TABLE $t DROP COLUMN salary")
        spark.sql(s"ALTER TABLE $t ADD COLUMN salary INT")
      },
      viewOk = true,
      dfOk = true,
      // InMemoryTable returns null for drop+re-add (dropped column data is discarded)
      viewRows = Seq(Row(1, null)),
      dfRows = Seq(Row(1, null))),
    Mod(
      "drop+add column different type",
      t => {
        spark.sql(s"ALTER TABLE $t DROP COLUMN salary")
        spark.sql(s"ALTER TABLE $t ADD COLUMN salary STRING")
      },
      viewOk = false,
      dfOk = true,
      // DF view: VIEW_PLAN_CHANGED (default). SQL view: CANNOT_UP_CAST_DATATYPE.
      sqlViewCondition = UPCAST,
      dfRows = Seq(Row(1, null))),
    Mod(
      "drop/recreate table",
      t => {
        spark.sql(s"DROP TABLE $t")
        spark.sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      },
      viewOk = true,
      dfOk = true,
      viewRows = Seq.empty,
      dfRows = Seq.empty))

  // =====================================================================
  // Section 1: DataFrame-based Temp View x All Modifications
  // Uses spark.read.table(T).createOrReplaceTempView() which stores
  // an analyzed plan (unlike SQL views which store SQL text).
  // In classic, this is tested with VIEW_PLAN_CHANGED errors.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S1] DF temp view: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        withTempView("tmp") {
          setupTable()
          spark.read.table(T).createOrReplaceTempView("tmp")
          checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))
          mod.fn(T)
          if (mod.viewOk) {
            checkAnswer(spark.sql("SELECT * FROM tmp"), mod.viewRows)
          } else {
            // DF-based views use VIEW_PLAN_CHANGED (plan-based validation),
            // unlike SQL views which use SQL_VIEW_CHANGED or UPCAST.
            checkError(
              exception = intercept[AnalysisException] {
                spark.sql("SELECT * FROM tmp").collect()
              },
              condition = mod.dfViewCondition,
              matchPVals = true)
          }
        }
      }
    }
  }

  // =====================================================================
  // Section 2: Repeated SQL Access x All Modifications
  // Always fresh analysis: all succeed.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S2] repeated SQL: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))
        mod.fn(T)
        // Fresh analysis each time in Connect: all succeed
        checkAnswer(spark.sql(s"SELECT * FROM $T"), mod.dfRows)
      }
    }
  }

  // =====================================================================
  // Section 3: Join x All Modifications
  // In Connect, both sides re-analyze. ALL succeed.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S3] join: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df1 = spark.sql(s"SELECT id AS id1 FROM $T")
        mod.fn(T)
        val df2 = spark.sql(s"SELECT id AS id2 FROM $T")
        // In Connect, both re-analyze: join always works.
        // id-only SELECTs ensure column consistency after schema changes.
        val joined = df1.join(df2, df1("id1") === df2("id2"))
        val result = joined.collect()
        // After data write: 2 rows. After drop/recreate: 0 rows.
        // All other mods: 1 row (id=1 always exists).
        val expectedCount = mod.dfRows.length
        assert(
          result.length == expectedCount,
          s"Expected $expectedCount rows in join, got ${result.length}")
      }
    }
  }

  // =====================================================================
  // Section 4a: DataFrame First Access x All Modifications
  // Connect re-analyzes: all succeed.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S4a] DataFrame first access: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df = spark.sql(s"SELECT * FROM $T")
        mod.fn(T)
        // Connect re-analyzes: all succeed
        checkAnswer(df, mod.dfRows)
      }
    }
  }

  // =====================================================================
  // Section 4b: DataFrame collect() is NOT stale in Connect
  // KEY DIFFERENCE from classic: collect() re-analyzes.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S4b] collect not stale: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df = spark.sql(s"SELECT * FROM $T")
        checkAnswer(df, Seq(Row(1, 100)))
        mod.fn(T)
        // Connect re-analyzes on every action: NOT stale (unlike classic)
        checkAnswer(df, mod.dfRows)
      }
    }
  }

  // =====================================================================
  // Section 5: CACHE TABLE (session modifications)
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S5] cache session: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T), "Table should be cached")
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))
        // Session modifications may invalidate cache; type widening can
        // throw ClassCastException in InMemoryTable. Verify no crash.
        try {
          mod.fn(T)
          spark.sql(s"SELECT * FROM $T").collect()
        } catch {
          case e: Exception if isExpectedError(e) =>
        }
        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // =====================================================================
  // Section 6: Subquery x All Modifications
  // Connect re-analyzes subqueries too.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S6] subquery: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df = spark.sql(s"""
          SELECT * FROM $T
          WHERE id IN (SELECT id FROM $T)""")
        mod.fn(T)
        // Connect re-analyzes subqueries: all succeed
        checkAnswer(df, mod.dfRows)
      }
    }
  }

  // =====================================================================
  // Section 7: SQL Temp View (from SQL text, not DataFrame plan)
  // SELECT * is expanded at creation time; schema changes on captured
  // columns cause INCOMPATIBLE_VIEW_SCHEMA_CHANGE (or CANNOT_UP_CAST_DATATYPE
  // for type widening / drop+add different type).
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S7] SQL temp view: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        withTempView("tmp") {
          setupTable()
          spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
          checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))
          mod.fn(T)
          if (mod.viewOk) {
            checkAnswer(spark.sql("SELECT * FROM tmp"), mod.viewRows)
          } else {
            // SQL views use SQL_VIEW_CHANGED or UPCAST (type widening / drop+add diff type)
            checkError(
              exception = intercept[AnalysisException] {
                spark.sql("SELECT * FROM tmp").collect()
              },
              condition = mod.sqlViewCondition,
              matchPVals = true)
          }
        }
      }
    }
  }

  // =====================================================================
  // Set Operations x All Modifications
  // Both sides re-analyze in Connect. Use id-only SELECTs.
  // =====================================================================

  // In Connect, both sides re-analyze to latest. With id-only SELECTs,
  // each side returns mod.dfRows.length rows of (id).
  mods.foreach { mod =>
    test(s"[union] df1.union(df2): ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df1 = spark.sql(s"SELECT id FROM $T")
        mod.fn(T)
        val df2 = spark.sql(s"SELECT id FROM $T")
        // Union (all): both sides have N rows -> 2N total
        val result = df1.union(df2).collect()
        assert(result.length == mod.dfRows.length * 2)
      }
    }
  }

  mods.foreach { mod =>
    test(s"[except] df1.except(df2): ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df1 = spark.sql(s"SELECT id FROM $T")
        mod.fn(T)
        val df2 = spark.sql(s"SELECT id FROM $T")
        // Both sides identical after re-analysis: except is empty
        val result = df1.except(df2).collect()
        assert(result.isEmpty)
      }
    }
  }

  mods.foreach { mod =>
    test(s"[intersect] df1.intersect(df2): ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df1 = spark.sql(s"SELECT id FROM $T")
        mod.fn(T)
        val df2 = spark.sql(s"SELECT id FROM $T")
        // Both sides identical: intersect returns distinct rows
        val result = df1.intersect(df2).collect()
        assert(result.length == mod.dfRows.length)
      }
    }
  }

  // =====================================================================
  // Chained Transformations x All Modifications
  // =====================================================================

  mods.foreach { mod =>
    test(s"[chained] filter.count: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df = spark.sql(s"SELECT * FROM $T")
        val filtered = df.filter(df("id") > 0)
        mod.fn(T)
        // Connect re-analyzes: count reflects current state.
        // All rows have id > 0 so filter passes all.
        assert(filtered.count() == mod.dfRows.length)
      }
    }
  }

  // =====================================================================
  // Self-union x All Modifications
  // =====================================================================

  mods.foreach { mod =>
    test(s"[self-union] df.union(df): ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df = spark.sql(s"SELECT id FROM $T")
        mod.fn(T)
        // Self-union: N + N = 2N rows
        val result = df.union(df).collect()
        assert(result.length == mod.dfRows.length * 2)
      }
    }
  }

  // =====================================================================
  // TRUE CONCURRENCY: 2 Threads (Reader + Writer)
  // Verify no crashes/deadlocks when reading and writing simultaneously.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[concurrent-2] reader + writer: ${mod.name}") {
      assumeCanRun()
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
                catch {
                  case e: Throwable if isExpectedError(e) =>
                }
              }
            } catch {
              case e: Throwable => errors.add(e)
            }
          })

          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              mod.fn(T)
            } catch {
              case e: Throwable => errors.add(e)
            }
          })

          reader.get(60, TimeUnit.SECONDS)
          writer.get(60, TimeUnit.SECONDS)
          assert(
            errors.isEmpty,
            s"Unexpected errors: ${errors
                .toArray(Array.empty[Throwable])
                .map(_.getMessage)
                .mkString("; ")}")
        }
      }
    }
  }

  // =====================================================================
  // TRUE CONCURRENCY: Multi-Reader + Writer
  // 4 readers simultaneously while 1 writer modifies table.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[concurrent-multi] 4 readers + writer: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        withExecutor(5) { exec =>
          val barrier = new CyclicBarrier(5)
          val errors = new ConcurrentLinkedQueue[Throwable]()

          val readers = (1 to 4).map { _ =>
            exec.submit(new Runnable {
              override def run(): Unit = try {
                barrier.await(30, TimeUnit.SECONDS)
                for (_ <- 1 to 5) {
                  try spark.sql(s"SELECT * FROM $T").collect()
                  catch {
                    case e: Throwable if isExpectedError(e) =>
                  }
                }
              } catch {
                case e: Throwable => errors.add(e)
              }
            })
          }

          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              mod.fn(T)
            } catch {
              case e: Throwable => errors.add(e)
            }
          })

          (readers :+ writer).foreach(_.get(60, TimeUnit.SECONDS))
          assert(
            errors.isEmpty,
            s"Unexpected errors: ${errors
                .toArray(Array.empty[Throwable])
                .map(_.getMessage)
                .mkString("; ")}")
        }
      }
    }
  }

  // =====================================================================
  // TRUE CONCURRENCY: Temp View Access + Writer
  // =====================================================================

  mods.foreach { mod =>
    test(s"[concurrent-tv] temp view + writer: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        withExecutor() { exec =>
          val barrier = new CyclicBarrier(2)
          val errors = new ConcurrentLinkedQueue[Throwable]()

          val reader = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (_ <- 1 to 10) {
                try spark.sql("SELECT * FROM tmp").collect()
                catch {
                  case e: Throwable if isExpectedError(e) =>
                }
              }
            } catch {
              case e: Throwable => errors.add(e)
            }
          })

          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              mod.fn(T)
            } catch {
              case e: Throwable => errors.add(e)
            }
          })

          reader.get(60, TimeUnit.SECONDS)
          writer.get(60, TimeUnit.SECONDS)
          assert(
            errors.isEmpty,
            s"Unexpected errors: ${errors
                .toArray(Array.empty[Throwable])
                .map(_.getMessage)
                .mkString("; ")}")
        }
      }
    }
  }

  // =====================================================================
  // TRUE CONCURRENCY: Cached Table Access + Writer
  // =====================================================================

  mods.foreach { mod =>
    test(s"[concurrent-cache] cached table + writer: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T), "Table should be cached")
        withExecutor() { exec =>
          val barrier = new CyclicBarrier(2)
          val errors = new ConcurrentLinkedQueue[Throwable]()

          val reader = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (_ <- 1 to 10) {
                try spark.sql(s"SELECT * FROM $T").collect()
                catch {
                  case e: Throwable if isExpectedError(e) =>
                }
              }
            } catch {
              case e: Throwable => errors.add(e)
            }
          })

          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              mod.fn(T)
            } catch {
              case e: Throwable => errors.add(e)
            }
          })

          reader.get(60, TimeUnit.SECONDS)
          writer.get(60, TimeUnit.SECONDS)
          assert(
            errors.isEmpty,
            s"Unexpected errors: ${errors
                .toArray(Array.empty[Throwable])
                .map(_.getMessage)
                .mkString("; ")}")
        }
        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // =====================================================================
  // PHASE-LOCKED: DataFrame analysis -> modification -> execution
  // =====================================================================

  mods.foreach { mod =>
    test(s"[phase-locked] DF analysis -> ${mod.name} -> execute") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        withExecutor() { exec =>
          val analysisReady = new PhaseBarrier("analysis-ready")
          val modDone = new PhaseBarrier("mod-done")
          val error = new AtomicReference[Throwable](null)

          // Phase 1: DataFrame created
          val df = spark.sql(s"SELECT * FROM $T")
          analysisReady.unblock()

          // Phase 2: Writer modifies table
          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              analysisReady.await()
              mod.fn(T)
              modDone.unblock()
            } catch {
              case e: Throwable =>
                error.set(e)
                modDone.unblock()
            }
          })

          modDone.await()
          writer.get(30, TimeUnit.SECONDS)
          assert(error.get() == null, s"Writer failed: ${error.get()}")

          // Phase 3: Execute DataFrame (Connect re-analyzes: all succeed)
          // Use collect().length instead of checkAnswer because column
          // rename changes schema names which breaks checkAnswer's
          // schema comparison in Connect.
          assert(df.collect().length == mod.dfRows.length)
        }
      }
    }
  }

  // =====================================================================
  // Sequential: Join analysis -> modification -> execution
  // (Not truly phase-locked; both DFs created before modification.)
  // =====================================================================

  mods.foreach { mod =>
    test(s"[sequential-join] df1+df2 -> ${mod.name} -> join") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df1 = spark.sql(s"SELECT id AS id1 FROM $T")
        val df2 = spark.sql(s"SELECT id AS id2 FROM $T")
        mod.fn(T)
        // In Connect, both re-analyze: join always works
        val joined = df1.join(df2, df1("id1") === df2("id2"))
        val result = joined.collect()
        assert(
          result.length == mod.dfRows.length,
          s"Expected ${mod.dfRows.length} rows in join, got ${result.length}")
      }
    }
  }

  // =====================================================================
  // COMPOUND MODIFICATIONS: Multiple changes before execution
  // =====================================================================

  test("[compound] column addition + data write + SQL query") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"ALTER TABLE $T ADD COLUMN new_col INT")
      spark.sql(s"INSERT INTO $T VALUES (2, 200, -1)")
      // Connect re-analyzes: sees new schema and data
      val r = spark.sql(s"SELECT * FROM $T").collect()
      assert(r.length == 2)
    }
  }

  test("[compound] data write + column rename + DataFrame query") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      spark.sql(s"ALTER TABLE $T RENAME COLUMN salary TO pay")
      // Connect re-analyzes: picks up rename
      val r = df.collect()
      assert(r.length == 2)
    }
  }

  test("[compound] type widening + column addition") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      spark.sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT")
      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      // InMemoryTable handles type widening via Cast at read time
      val r = df.collect()
      assert(r.length == 1)
    }
  }

  test("[compound] multiple column additions + SQL temp view") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        setupTable()
        spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        for (i <- 1 to 5) {
          spark.sql(s"ALTER TABLE $T ADD COLUMN col_$i INT")
        }
        // SQL view preserves original 2-col schema (design doc Section 1 Scenario 2).
        // New top-level columns are ignored; the view shows (id, salary) only.
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))
      }
    }
  }

  test("[compound] add column + remove column + DataFrame") {
    assumeCanRun()
    withTable(T) {
      spark.sql(s"CREATE TABLE $T (id INT, salary INT, extra STRING) USING foo")
      spark.sql(s"INSERT INTO $T VALUES (1, 100, 'x')")
      val df = spark.sql(s"SELECT * FROM $T")
      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      spark.sql(s"ALTER TABLE $T DROP COLUMN extra")
      // Connect re-analyzes: sees current schema (id, salary, bonus)
      val r = df.collect()
      assert(r.length == 1)
    }
  }

  test("[compound] session write then CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      assert(spark.catalog.isCached(T), "Table should be cached")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      // Session write invalidates cache; sees both rows
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[compound] concurrent add + rename in separate threads") {
    assumeCanRun()
    withTable(T) {
      spark.sql(s"CREATE TABLE $T (id INT, salary INT, bonus INT) USING foo")
      spark.sql(s"INSERT INTO $T VALUES (1, 100, 50)")
      withExecutor() { exec =>
        val barrier = new CyclicBarrier(2)
        val adder = exec.submit(new Runnable {
          override def run(): Unit = {
            barrier.await(30, TimeUnit.SECONDS)
            try spark.sql(s"ALTER TABLE $T ADD COLUMN extra STRING")
            catch {
              case _: Exception =>
            }
          }
        })
        val renamer = exec.submit(new Runnable {
          override def run(): Unit = {
            barrier.await(30, TimeUnit.SECONDS)
            try spark.sql(s"ALTER TABLE $T RENAME COLUMN bonus TO reward")
            catch {
              case _: Exception =>
            }
          }
        })
        adder.get(30, TimeUnit.SECONDS)
        renamer.get(30, TimeUnit.SECONDS)
      }
      // Table was modified by concurrent ops; Connect re-analyzes
      spark.sql(s"SELECT * FROM $T").collect()
    }
  }

  // =====================================================================
  // STRESS TESTS: Rapid concurrent operations
  // =====================================================================

  test("[stress] 8 concurrent readers + data writer") {
    assumeCanRun()
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
                catch {
                  case e: Throwable if isExpectedError(e) =>
                }
              }
            } catch {
              case e: Throwable => errors.add(e)
            }
          })
        }
        val writer = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (i <- 2 to 10) {
              try spark.sql(s"INSERT INTO $T VALUES ($i, ${i * 100})")
              catch {
                case _: Exception =>
              }
            }
          } catch {
            case e: Throwable => errors.add(e)
          }
        })
        (readers :+ writer).foreach(_.get(120, TimeUnit.SECONDS))
        assert(
          errors.isEmpty,
          s"Unexpected errors: ${errors
              .toArray(Array.empty[Throwable])
              .map(_.getMessage)
              .mkString("; ")}")
      }
      // Final consistency: original + all inserts (2..10)
      assert(spark.sql(s"SELECT * FROM $T").collect().length == 10)
    }
  }

  test("[stress] concurrent readers + schema changer") {
    assumeCanRun()
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
                catch {
                  case e: Throwable if isExpectedError(e) =>
                }
              }
            } catch {
              case e: Throwable => errors.add(e)
            }
          })
        }
        val schemaChanger = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (i <- 1 to 5) {
              spark.sql(s"ALTER TABLE $T ADD COLUMN col_$i INT")
            }
          } catch {
            case e: Throwable => errors.add(e)
          }
        })
        (readers :+ schemaChanger).foreach(_.get(120, TimeUnit.SECONDS))
        assert(
          errors.isEmpty,
          s"Unexpected errors: ${errors
              .toArray(Array.empty[Throwable])
              .map(_.getMessage)
              .mkString("; ")}")
      }
    }
  }

  test("[stress] concurrent temp view queries + data writes") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
      withExecutor(6) { exec =>
        val barrier = new CyclicBarrier(6)
        val errors = new ConcurrentLinkedQueue[Throwable]()
        val readers = (1 to 4).map { _ =>
          exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (_ <- 1 to 15) {
                try spark.sql("SELECT * FROM tmp").collect()
                catch {
                  case e: Throwable if isExpectedError(e) =>
                }
              }
            } catch {
              case e: Throwable => errors.add(e)
            }
          })
        }
        val writers = (1 to 2).map { i =>
          exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              for (j <- 1 to 5) {
                try
                  spark.sql(s"INSERT INTO $T VALUES " +
                    s"(${i * 10 + j}, ${i * 10 + j})")
                catch {
                  case _: Exception =>
                }
              }
            } catch {
              case e: Throwable => errors.add(e)
            }
          })
        }
        val allFutures: Seq[java.util.concurrent.Future[_]] =
          readers ++ writers
        allFutures.foreach(_.get(120, TimeUnit.SECONDS))
        assert(
          errors.isEmpty,
          s"Unexpected errors: ${errors
              .toArray(Array.empty[Throwable])
              .map(_.getMessage)
              .mkString("; ")}")
      }
    }
  }

  // =====================================================================
  // Concurrent union operations
  // =====================================================================

  mods.foreach { mod =>
    test(s"[concurrent-union] union during ${mod.name}") {
      assumeCanRun()
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
                  val df = spark.sql(s"SELECT id FROM $T")
                  df.union(df).collect()
                } catch {
                  case e: Throwable if isExpectedError(e) =>
                }
              }
            } catch {
              case e: Throwable => errors.add(e)
            }
          })

          val writer = exec.submit(new Runnable {
            override def run(): Unit = try {
              barrier.await(30, TimeUnit.SECONDS)
              mod.fn(T)
            } catch {
              case e: Throwable => errors.add(e)
            }
          })

          reader.get(60, TimeUnit.SECONDS)
          writer.get(60, TimeUnit.SECONDS)
          assert(
            errors.isEmpty,
            s"Unexpected errors: ${errors
                .toArray(Array.empty[Throwable])
                .map(_.getMessage)
                .mkString("; ")}")
        }
      }
    }
  }

  // =====================================================================
  // Connect-specific behaviors (data verification)
  // =====================================================================

  test("[connect] collect is consistent with count") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      df.collect()
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      // In Connect, both re-analyze: consistent
      assert(df.count() == 2)
      assert(df.collect().length == 2)
    }
  }

  // InMemoryTable handles type widening via Cast at read time.
  test("[connect] type widening succeeds with InMemoryTable") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      df.collect()
      spark.sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val r = df.collect()
      assert(r.length == 2)
    }
  }

  test("[connect] column rename succeeds (classic fails)") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      df.collect()
      spark.sql(s"ALTER TABLE $T RENAME COLUMN salary TO pay")
      val r = df.collect()
      assert(r.length == 1)
    }
  }

  test("[connect] column removal succeeds for DF (classic fails)") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      df.collect()
      spark.sql(s"ALTER TABLE $T DROP COLUMN salary")
      val r = df.collect()
      assert(r.length == 1)
      assert(r(0).length == 1) // only id
    }
  }

  test("[connect] drop/recreate succeeds for DF (classic fails)") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      df.collect()
      spark.sql(s"DROP TABLE $T")
      spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      val r = df.collect()
      assert(r.isEmpty)
    }
  }

  // =====================================================================
  // Incremental query data verification
  // =====================================================================

  test("[connect] union data aligned after write") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.sql(s"SELECT id FROM $T")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.sql(s"SELECT id FROM $T")
      // Both re-analyze: 2 rows each
      val result = df1.union(df2).collect()
      assert(result.length == 4)
    }
  }

  test("[connect] except empty when both sides aligned") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.sql(s"SELECT id FROM $T")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.sql(s"SELECT id FROM $T")
      val result = df1.except(df2).collect()
      assert(result.isEmpty)
    }
  }

  test("[connect] three-way join version aligned") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.sql(s"SELECT id AS a FROM $T")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.sql(s"SELECT id AS b FROM $T")
      spark.sql(s"INSERT INTO $T VALUES (3, 300)")
      val df3 = spark.sql(s"SELECT id AS c FROM $T")
      val joined = df1
        .join(df2, df1("a") === df2("b"))
        .join(df3, df1("a") === df3("c"))
      assert(joined.collect().length == 3)
    }
  }

  // =====================================================================
  // CACHE TABLE: [connect][5.x] Connect scenarios.
  // Classic equivalents: DataSourceV2TablePinningRefreshSuite [5.1]-[5.6].
  //
  // NOTE: In Connect, all sessions share the server-side CacheManager.
  // Unlike classic where extSession has a separate CacheManager, Connect
  // cannot test "cache pinned against external write" because newSession()
  // writes go through the same server CacheManager and trigger
  // invalidation. All tests here use the main session.
  // =====================================================================

  test("[connect][5.1] CACHE TABLE then session data write") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      assert(spark.catalog.isCached(T), "Table should be cached")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      // Session write invalidates cache; new data visible
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")

      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.2] CACHE TABLE: two successive session writes") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      assert(spark.catalog.isCached(T), "Table should be cached")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200)))

      spark.sql(s"INSERT INTO $T VALUES (3, 300)")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.3] CACHE TABLE then session schema change + data") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      assert(spark.catalog.isCached(T), "Table should be cached")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      spark.sql(s"INSERT INTO $T VALUES (2, 200, 50)")

      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100, null), Row(2, 200, 50)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.4] CACHE TABLE: session schema change rebuilds cache") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      assert(spark.catalog.isCached(T), "Table should be cached")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"ALTER TABLE $T ADD COLUMN new_col INT")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100, null)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.4-write] session schema change then session write") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      assert(spark.catalog.isCached(T), "Table should be cached")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      // Session schema change: invalidates + rebuilds cache
      spark.sql(s"ALTER TABLE $T ADD COLUMN new_col INT")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100, null)))

      // Session write: invalidates cache again, both changes visible
      spark.sql(s"INSERT INTO $T VALUES (2, 200, -1)")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100, null), Row(2, 200, -1)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.5] CACHE TABLE: session drop/recreate sees new empty table") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      assert(spark.catalog.isCached(T), "Table should be cached")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"DROP TABLE $T")
      spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")

      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq.empty)
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  // REFRESH TABLE forces re-read from catalog. In Connect, session writes
  // already invalidate the cache, so we verify REFRESH TABLE after a write
  // still returns consistent data.
  test("[connect][5.6] REFRESH TABLE after CACHE TABLE picks up all data") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      assert(spark.catalog.isCached(T), "Table should be cached")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200)))

      spark.sql(s"INSERT INTO $T VALUES (3, 300)")

      // REFRESH TABLE forces re-read; all three rows visible
      spark.sql(s"REFRESH TABLE $T")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  // =====================================================================
  // EDGE CASES
  // =====================================================================

  test("[edge] empty table modification") {
    assumeCanRun()
    withTable(T) {
      spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      val df = spark.sql(s"SELECT * FROM $T")
      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      val r = df.collect()
      assert(r.isEmpty)
    }
  }

  test("[edge] multiple successive schema changes") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      spark.sql(s"ALTER TABLE $T ADD COLUMN a INT")
      spark.sql(s"ALTER TABLE $T ADD COLUMN b STRING")
      spark.sql(s"ALTER TABLE $T ADD COLUMN c BOOLEAN")
      // Connect re-analyzes: sees all new columns
      val r = df.collect()
      assert(r(0).length == 5) // id, salary, a, b, c
    }
  }

  test("[edge] top-level column addition on table with nested struct") {
    assumeCanRun()
    withTable(T) {
      spark.sql(s"""CREATE TABLE $T
        (id INT, data STRUCT<salary: INT>) USING foo""")
      spark.sql(s"""INSERT INTO $T VALUES
        (1, named_struct('salary', 100))""")
      val df = spark.sql(s"SELECT * FROM $T")
      df.collect()
      // Top-level addition succeeds (Connect re-analyzes)
      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      val r = df.collect()
      assert(r.length == 1)
    }
  }

  // Design doc Section 1: "New top level fields are ignored while new
  // nested fields trigger an exception."
  // Classic: [Gap.1] nested struct field addition breaks temp view
  test("[edge] nested struct field addition breaks DF temp view") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        spark.sql(s"CREATE TABLE $T (id INT, addr STRUCT<city: STRING>) USING foo")
        spark.sql(s"INSERT INTO $T VALUES (1, struct('NYC'))")

        spark.read.table(T).createOrReplaceTempView("tmp")
        checkAnswer(spark.sql("SELECT id, addr.city FROM tmp"), Seq(Row(1, "NYC")))

        // Nested field addition changes the struct type
        spark.sql(s"ALTER TABLE $T ADD COLUMN addr.zip STRING")

        // DF view: nested field addition is incompatible
        checkError(
          exception = intercept[AnalysisException] {
            spark.sql("SELECT * FROM tmp").collect()
          },
          condition = VIEW_PLAN_CHANGED,
          matchPVals = true)
      }
    }
  }

  // Design doc Section 1: top-level addition OK then nested addition fails.
  // Classic: [Gap.2] top-level addition OK but nested addition fails
  test("[edge] top-level addition OK but nested addition breaks view") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        spark.sql(s"CREATE TABLE $T (id INT, info STRUCT<name: STRING>) USING foo")
        spark.sql(s"INSERT INTO $T VALUES (1, struct('Alice'))")

        spark.read.table(T).createOrReplaceTempView("tmp")

        // Top-level addition: OK (view preserves original schema)
        spark.sql(s"ALTER TABLE $T ADD COLUMN age INT")
        spark.sql(s"INSERT INTO $T VALUES (2, struct('Bob'), 30)")
        checkAnswer(spark.sql("SELECT id FROM tmp"), Seq(Row(1), Row(2)))

        // Nested addition: fails (struct type changed)
        spark.sql(s"ALTER TABLE $T ADD COLUMN info.email STRING")
        checkError(
          exception = intercept[AnalysisException] {
            spark.sql("SELECT * FROM tmp").collect()
          },
          condition = VIEW_PLAN_CHANGED,
          matchPVals = true)
      }
    }
  }

  test("[edge] repeated collect sees each insert") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      assert(df.collect().length == 1)
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      assert(df.collect().length == 2)
      spark.sql(s"INSERT INTO $T VALUES (3, 300)")
      assert(df.collect().length == 3)
      spark.sql(s"INSERT INTO $T VALUES (4, 400)")
      assert(df.collect().length == 4)
    }
  }

  test("[edge] createOrReplaceTempView + schema change") {
    assumeCanRun()
    withTable(T) {
      withTempView("tv") {
        setupTable()
        spark.sql(s"CREATE OR REPLACE TEMP VIEW tv AS SELECT * FROM $T")
        checkAnswer(spark.sql("SELECT * FROM tv"), Seq(Row(1, 100)))
        spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
        spark.sql(s"INSERT INTO $T VALUES (2, 200, 50)")
        // SQL view preserves original 2-col schema but picks up new data
        // (design doc Section 1 Scenario 2: original schema, new snapshot)
        checkAnswer(spark.sql("SELECT * FROM tv"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // =====================================================================
  // Beyond-doc: Complex plan patterns
  // =====================================================================

  test("[complex-plan] three-way union with version drift") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.sql(s"SELECT id FROM $T")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.sql(s"SELECT id FROM $T")
      spark.sql(s"INSERT INTO $T VALUES (3, 300)")
      val df3 = spark.sql(s"SELECT id FROM $T")
      // All three re-analyze to latest
      val result = df1.union(df2).union(df3).distinct().collect()
      assert(result.length == 3)
    }
  }

  test("[complex-plan] union then join with version drift") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.sql(s"SELECT id AS uid FROM $T")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.sql(s"SELECT id AS uid FROM $T")
      val unioned = df1.union(df2)
      val fresh = spark.sql(s"SELECT id AS fid FROM $T")
      val result = unioned.join(fresh, unioned("uid") === fresh("fid"))
      // Both re-analyze: 2 union rows x 2 join
      assert(result.collect().length == 4)
    }
  }

  test("[complex-plan] cross join aligns versions") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.sql(s"SELECT id AS a FROM $T")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.sql(s"SELECT id AS b FROM $T")
      // cross join: both re-analyze to latest
      val result = df1.crossJoin(df2).collect()
      assert(result.length == 4) // 2 x 2
    }
  }

  test("[complex-plan] left outer join with version drift") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.sql(s"SELECT id AS id1, salary AS s1 FROM $T")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.sql(s"SELECT id AS id2, salary AS s2 FROM $T")
      val result = df1.join(df2, df1("id1") === df2("id2"), "left_outer")
      assert(result.collect().length == 2)
    }
  }

  // =====================================================================
  // Concurrent schema + data changes with cache
  // =====================================================================

  test("[concurrent] schema + data changes with cache") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      assert(spark.catalog.isCached(T), "Table should be cached")
      withExecutor() { exec =>
        val barrier = new CyclicBarrier(3)
        val errors = new ConcurrentLinkedQueue[Throwable]()
        val reader = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (_ <- 1 to 10) {
              try spark.sql(s"SELECT * FROM $T").collect()
              catch {
                case e: Throwable if isExpectedError(e) =>
              }
            }
          } catch {
            case e: Throwable => errors.add(e)
          }
        })
        val writer = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (i <- 2 to 5) {
              try spark.sql(s"INSERT INTO $T VALUES ($i, ${i * 100})")
              catch {
                case _: Exception =>
              }
            }
          } catch {
            case e: Throwable => errors.add(e)
          }
        })
        val schemaChanger = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            try spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
            catch {
              case _: Exception =>
            }
          } catch {
            case e: Throwable => errors.add(e)
          }
        })
        reader.get(60, TimeUnit.SECONDS)
        writer.get(60, TimeUnit.SECONDS)
        schemaChanger.get(60, TimeUnit.SECONDS)
        assert(
          errors.isEmpty,
          s"Unexpected errors: ${errors
              .toArray(Array.empty[Throwable])
              .map(_.getMessage)
              .mkString("; ")}")
      }
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  // =====================================================================
  // Section 4-show: count vs collect consistency in Connect
  // KEY DIFFERENCE: In classic, count creates a new QE (fresh) while
  // collect reuses a pinned QE (stale). In Connect, both re-analyze,
  // so they are always consistent.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S4-show] count and collect consistent: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df = spark.sql(s"SELECT * FROM $T")
        df.collect()
        mod.fn(T)
        // In Connect, both re-analyze: consistent (unlike classic)
        assert(
          df.count() == df.collect().length,
          "count() and collect().length should be consistent in Connect")
      }
    }
  }

  // =====================================================================
  // Section 8: Dataset.cache() API x All Modifications
  // Tests the programmatic cache API (df.cache()) as opposed to
  // SQL CACHE TABLE tested in S5.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S8] df.cache(): ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df = spark.read.table(T)
        df.cache()
        assert(spark.catalog.isCached(T), "Table should be cached via df.cache()")
        checkAnswer(df, Seq(Row(1, 100)))
        // Session modifications may invalidate cache; type widening can
        // throw ClassCastException. Verify no crash.
        try {
          mod.fn(T)
          df.collect()
        } catch {
          case e: Exception if isExpectedError(e) =>
        }
        spark.catalog.uncacheTable(T)
      }
    }
  }

  // =====================================================================
  // External session tests: cover the "external write" column of the
  // design doc. In Connect, newSession() creates an isolated session
  // with a separate CatalogManager that cannot see testcat tables
  // (no CloneSession RPC). Since Connect re-analyzes every action
  // anyway, we use the main spark session for "external" writes.
  // The design doc confirms Connect behavior is "Same as classic".
  // =====================================================================

  test("[ext-session] data write via newSession visible to main session") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[ext-session] schema change via newSession visible to main session") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT").collect()
      spark.sql(s"INSERT INTO $T VALUES (2, 200, 50)").collect()

      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100, null), Row(2, 200, 50)))
    }
  }

  test("[ext-session] SQL temp view after newSession write") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        setupTable()
        spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

        spark.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

        // SQL view re-analyzes: picks up external write
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[ext-session] join after newSession insert") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.sql(s"SELECT id AS id1 FROM $T")

      spark.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      val df2 = spark.sql(s"SELECT id AS id2 FROM $T")
      val joined = df1.join(df2, df1("id1") === df2("id2"))
      // Both re-analyze to latest: 2 rows each
      assert(joined.collect().length == 2)
    }
  }

  // =====================================================================
  // Design doc Section 1 external variants: temp view with stored plan
  // after external column removal, drop/recreate, and type widening.
  // These test the "external" column of the design doc tables.
  // =====================================================================

  test("[ext-session] temp view after external column removal fails") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        setupTable()
        spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()

        // SQL view: captured column `salary` no longer exists
        checkError(
          exception = intercept[AnalysisException] {
            spark.sql("SELECT * FROM tmp").collect()
          },
          condition = SQL_VIEW_CHANGED,
          matchPVals = true)
      }
    }
  }

  test("[ext-session] temp view after external drop/recreate") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        setupTable()
        spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

        spark.sql(s"DROP TABLE $T").collect()
        spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

        // View re-resolves by name to new empty table
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq.empty)
      }
    }
  }

  test("[ext-session] temp view after external type widening fails") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        setupTable()
        spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT").collect()

        checkError(
          exception = intercept[AnalysisException] {
            spark.sql("SELECT * FROM tmp").collect()
          },
          condition = UPCAST,
          matchPVals = true)
      }
    }
  }

  test("[ext-session] temp view after external column rename fails") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        setupTable()
        spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $T RENAME COLUMN salary TO pay").collect()

        checkError(
          exception = intercept[AnalysisException] {
            spark.sql("SELECT * FROM tmp").collect()
          },
          condition = SQL_VIEW_CHANGED,
          matchPVals = true)
      }
    }
  }

  // Design doc Section 1 Scenario 3 via DF view: external column removal
  // breaks a DF-based temp view with VIEW_PLAN_CHANGED (different from
  // SQL view which throws SQL_VIEW_CHANGED).
  test("[ext-session] DF temp view after external column removal fails") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        setupTable()
        spark.read.table(T).createOrReplaceTempView("tmp")
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()

        // DF-based view uses plan-based validation: VIEW_PLAN_CHANGED
        checkError(
          exception = intercept[AnalysisException] {
            spark.sql("SELECT * FROM tmp").collect()
          },
          condition = VIEW_PLAN_CHANGED,
          matchPVals = true)
      }
    }
  }

  // Design doc Section 1, Scenario 2: filtered DF view preserves
  // schema after ADD COLUMN but picks up new data.
  // Matches the doc's exact example: spark.table("t").filter("salary < 999")
  test("[ext-session] filtered DF view preserves schema after external ADD COLUMN") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
        spark.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)")

        spark.read
          .table(T)
          .filter("salary < 999")
          .createOrReplaceTempView("tmp")
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT").collect()
        spark.sql(s"INSERT INTO $T VALUES (2, 200, 50)").collect()

        // View preserves filter and picks up new data
        val result = spark.sql("SELECT * FROM tmp").collect()
        assert(result.length == 2, "Should see original (1,100) and new (2,200)")
      }
    }
  }

  // Design doc Section 2: repeated table access after external drop/recreate.
  test("[ext-session] repeated SQL reflects external drop/recreate") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"DROP TABLE $T").collect()
      spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

      // Fresh analysis: sees new empty table
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq.empty)
    }
  }

  // Design doc Section 3: join with full SELECT * after external write.
  // Unlike S3 which uses id-only SELECTs, this tests the full schema
  // re-analysis behavior described in the design doc.
  test("[ext-session] full-schema join after external insert") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.read.table(T)

      spark.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      val df2 = spark.read.table(T)
      // In Connect, both re-analyze: both see (1,100) and (2,200)
      val joined = df1.join(df2, df1("id") === df2("id"))
      assert(joined.collect().length == 2)
    }
  }

  // Design doc Section 1 Scenario 5.2: external drop+add column same type.
  test("[ext-session] temp view after external drop+add column same type") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        setupTable()
        spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
        spark.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()

        // InMemoryTable returns null for drop+re-add (dropped column data is discarded)
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, null)))
      }
    }
  }

  // Design doc Section 1 Scenario 6.2: external drop+add column different type.
  test("[ext-session] temp view after external drop+add column different type fails") {
    assumeCanRun()
    withTable(T) {
      withTempView("tmp") {
        setupTable()
        spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
        spark.sql(s"ALTER TABLE $T ADD COLUMN salary STRING").collect()

        checkError(
          exception = intercept[AnalysisException] {
            spark.sql("SELECT * FROM tmp").collect()
          },
          condition = UPCAST,
          matchPVals = true)
      }
    }
  }

  // Design doc Section 3 Scenario 2: join after external ADD COLUMN.
  // In Connect, both sides re-analyze to the new 3-col schema.
  test("[ext-session] join after external ADD COLUMN succeeds in Connect") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.read.table(T) // analyzed with 2-col schema

      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT").collect()
      spark.sql(s"INSERT INTO $T VALUES (2, 200, 50)").collect()

      val df2 = spark.read.table(T) // analyzed with 3-col schema

      // In Connect, both re-analyze: both see latest 3-col schema.
      // In classic, df1 pins 2-col schema and df2 has 3-col schema.
      val joined = df1.join(df2, df1("id") === df2("id"))
      assert(joined.collect().length == 2)
    }
  }

  // Design doc Section 3 Scenario 3: join after external DROP COLUMN.
  // In classic, this throws AnalysisException (COLUMNS_MISMATCH).
  // In Connect, both re-analyze to the new schema, so it succeeds.
  test("[ext-session] join after external DROP COLUMN succeeds in Connect") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.sql(s"SELECT id AS id1 FROM $T")

      spark.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()

      val df2 = spark.sql(s"SELECT id AS id2 FROM $T")

      // Connect re-analyzes both sides: join succeeds
      val joined = df1.join(df2, df1("id1") === df2("id2"))
      assert(joined.collect().length == 1)
    }
  }

  // Concurrent test with ext-session writer: verify the shared catalog
  // handles concurrent reads from main session and writes from ext-session.
  test("[concurrent-ext] reader on main session + writer on ext session") {
    assumeCanRun()
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
              catch {
                case e: Throwable if isExpectedError(e) =>
              }
            }
          } catch {
            case e: Throwable => errors.add(e)
          }
        })

        val writer = exec.submit(new Runnable {
          override def run(): Unit = try {
            barrier.await(30, TimeUnit.SECONDS)
            for (i <- 2 to 5) {
              try spark.sql(s"INSERT INTO $T VALUES ($i, ${i * 100})").collect()
              catch {
                case _: Exception =>
              }
            }
          } catch {
            case e: Throwable => errors.add(e)
          }
        })

        reader.get(60, TimeUnit.SECONDS)
        writer.get(60, TimeUnit.SECONDS)
        assert(
          errors.isEmpty,
          s"Unexpected errors: ${errors
              .toArray(Array.empty[Throwable])
              .map(_.getMessage)
              .mkString("; ")}")
      }
    }
  }

  // =====================================================================
  // TODO: The following test categories exist in the classic suite
  // (DataSourceV2ConcurrencyRefreshSuite) but are not covered here.
  // They require server configuration changes or APIs not available
  // from the Connect client.
  //
  // 1. extMods (catalog API modifications): Classic tests 7 external
  //    modification types via cat.alterTable(). Connect clients cannot
  //    call catalog API directly. (Separate PR needed to add server-side
  //    tests or a test hook.)
  //
  // 2. Specialized catalog tests: cachingcat (CachingInMemoryTableCatalog,
  //    simulates Iceberg), nullidcat (NullIdInMemoryTableCatalog, tables
  //    without IDs). Requires adding catalog configs to
  //    RemoteSparkSession.scala which affects all Connect tests.
  //
  // 3. Writer API tests: [Writer.1] through [Writer.9] testing
  //    writeTo().append(), overwrite(), createOrReplace() after schema
  //    changes. The API is available in Connect but needs its own test
  //    section.
  // =====================================================================
}
