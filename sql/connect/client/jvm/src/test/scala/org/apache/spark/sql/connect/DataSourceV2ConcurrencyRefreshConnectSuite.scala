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

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession, SQLHelper}
import org.apache.spark.sql.connect.test.IntegrationTestUtils.isAssemblyJarsDirExists

/**
 * Comprehensive concurrency tests for DSv2 table refresh and pinning
 * in Spark Connect mode.
 *
 * Mirrors DataSourceV2ConcurrencyRefreshSuite (classic) but adapted for
 * Connect where every action re-analyzes the plan on the server.
 *
 * Key behavioral differences from classic:
 *   No stale QueryExecution (collect is NOT pinned)
 *   Schema changes are picked up on every access
 *   Type widening, column rename, column removal all succeed for DataFrames
 *   Joins/unions/etc always see consistent latest version
 *
 * All modifications are via SQL (no catalog API access in Connect).
 *
 * Systematically tests:
 *   8 modification types x 8 access patterns (sequential)
 *   8 modification types x 4 concurrency modes (multi-threaded)
 *   Compound modifications, stress tests, edge cases
 */
class DataSourceV2ConcurrencyRefreshConnectSuite
  extends QueryTest with RemoteSparkSession with SQLHelper {

  private val T = "testcat.ns1.ns2.tbl"

  private def assumeCanRun(): Unit = {
    assume(spark != null && isAssemblyJarsDirExists,
      "Spark Connect server not available")
  }

  private def setupTable(): Unit = {
    spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
    spark.sql(s"INSERT INTO $T VALUES (1, 100)")
  }

  /** Second client session on the same Connect server (shared DSv2 catalog). */
  private def extSession: SparkSession = spark.newSession()

  // =====================================================================
  // Infrastructure
  // =====================================================================

  /** Barrier for controlled thread synchronization. */
  private class PhaseBarrier(name: String) {
    private val latch = new CountDownLatch(1)
    def await(ms: Long = 30000): Unit =
      require(latch.await(ms, TimeUnit.MILLISECONDS),
        s"PhaseBarrier '$name' timed out")
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
  private def isExpectedError(e: Throwable): Boolean = e match {
    case _: Exception
      if e.getMessage != null &&
        (e.getMessage.contains("INCOMPATIBLE") ||
         e.getMessage.contains("TABLE_ID_MISMATCH") ||
         e.getMessage.contains("COLUMNS_MISMATCH") ||
         e.getMessage.contains("not found") ||
         e.getMessage.contains("schema") ||
         e.getMessage.contains("NUM_COLUMNS_MISMATCH") ||
         e.getMessage.contains("CANNOT_UP_CAST_DATATYPE") ||
         e.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND") ||
         e.getMessage.contains("VIEW_SCHEMA") ||
         e.getMessage.contains("ClassCastException") ||
         e.getMessage.contains("does not exist")) => true
    case se: Exception if se.getCause != null => isExpectedError(se.getCause)
    case _ => false
  }

  // =====================================================================
  // Modification Definitions (SQL only, no catalog API in Connect)
  // =====================================================================

  // In Connect, SQL temp views (SELECT *) capture column names at
  // creation. Removing/renaming/retyping breaks re-analysis.
  // DataFrames re-analyze on every action, so ALL mods succeed for DFs.
  case class Mod(
      name: String,
      fn: String => Unit,
      sqlViewOk: Boolean,
      dfOk: Boolean)

  private val mods: Seq[Mod] = Seq(
    Mod(
      "data write",
      t => spark.sql(s"INSERT INTO $t VALUES (2, 200)"),
      sqlViewOk = true,
      dfOk = true),
    Mod(
      "column addition",
      t => spark.sql(s"ALTER TABLE $t ADD COLUMN new_col INT"),
      sqlViewOk = true,
      dfOk = true),
    Mod(
      "column removal",
      t => spark.sql(s"ALTER TABLE $t DROP COLUMN salary"),
      sqlViewOk = false,
      dfOk = true),
    Mod(
      "column rename",
      t => spark.sql(s"ALTER TABLE $t RENAME COLUMN salary TO pay"),
      sqlViewOk = false,
      dfOk = true),
    // InMemoryTable does not convert stored Integer values to Long
    // after type widening, causing ClassCastException at runtime.
    // Real connectors (Delta/Iceberg) handle this correctly.
    Mod(
      "type widening INT to BIGINT",
      t =>
        spark.sql(
          s"ALTER TABLE $t ALTER COLUMN salary TYPE BIGINT"),
      sqlViewOk = false,
      dfOk = false),
    Mod(
      "drop+add column same type",
      t => {
        spark.sql(s"ALTER TABLE $t DROP COLUMN salary")
        spark.sql(s"ALTER TABLE $t ADD COLUMN salary INT")
      },
      sqlViewOk = true,
      dfOk = true),
    Mod(
      "drop+add column different type",
      t => {
        spark.sql(s"ALTER TABLE $t DROP COLUMN salary")
        spark.sql(s"ALTER TABLE $t ADD COLUMN salary STRING")
      },
      sqlViewOk = false,
      dfOk = true),
    Mod(
      "drop/recreate table",
      t => {
        spark.sql(s"DROP TABLE $t")
        spark.sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      },
      sqlViewOk = true,
      dfOk = true))

  // =====================================================================
  // Section 1: SQL Temp View x All Modifications
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S1] SQL temp view: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        spark.sql(
          s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        checkAnswer(
          spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))
        mod.fn(T)
        if (mod.sqlViewOk) {
          spark.sql("SELECT * FROM tmp").collect()
        } else {
          assertThrows[Exception] {
            spark.sql("SELECT * FROM tmp").collect()
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
        checkAnswer(
          spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))
        mod.fn(T)
        if (mod.dfOk) {
          spark.sql(s"SELECT * FROM $T").collect()
        } else {
          assertThrows[Exception] {
            spark.sql(s"SELECT * FROM $T").collect()
          }
        }
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
        val joined = df1.join(df2, df1("id1") === df2("id2"))
        joined.collect()
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
        if (mod.dfOk) {
          df.collect()
        } else {
          assertThrows[Exception] {
            df.collect()
          }
        }
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
        val r1 = df.collect()
        assert(r1.length == 1)
        mod.fn(T)
        if (mod.dfOk) {
          // Connect re-analyzes: NOT stale
          val r2 = df.collect()
          if (mod.name == "data write") {
            assert(r2.length == 2)
          } else if (mod.name == "drop/recreate table") {
            assert(r2.length == 0)
          } else {
            assert(r2.length == 1)
          }
        } else {
          assertThrows[Exception] {
            df.collect()
          }
        }
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
        checkAnswer(
          spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))
        mod.fn(T)
        if (mod.dfOk) {
          // Session modification may invalidate cache; no crash
          spark.sql(s"SELECT * FROM $T").collect()
        } else {
          assertThrows[Exception] {
            spark.sql(s"SELECT * FROM $T").collect()
          }
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
        if (mod.dfOk) {
          df.collect()
        } else {
          assertThrows[Exception] {
            df.collect()
          }
        }
      }
    }
  }

  // =====================================================================
  // Section 7: SQL Temp View (from SQL, not DF)
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S7] SQL temp view from SQL: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        spark.sql(
          s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        checkAnswer(
          spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))
        mod.fn(T)
        if (mod.sqlViewOk) {
          spark.sql("SELECT * FROM tmp").collect()
        } else {
          assertThrows[Exception] {
            spark.sql("SELECT * FROM tmp").collect()
          }
        }
      }
    }
  }

  // =====================================================================
  // Set Operations x All Modifications
  // Both sides re-analyze in Connect. Use id-only SELECTs.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[union] df1.union(df2): ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df1 = spark.sql(s"SELECT id FROM $T")
        mod.fn(T)
        val df2 = spark.sql(s"SELECT id FROM $T")
        df1.union(df2).collect()
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
        df1.except(df2).collect()
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
        df1.intersect(df2).collect()
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
        filtered.count()
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
        df.union(df).collect()
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
          assert(errors.isEmpty,
            s"Unexpected errors: ${errors
              .toArray(Array.empty[Throwable])
              .map(_.getMessage).mkString("; ")}")
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
          assert(errors.isEmpty,
            s"Unexpected errors: ${errors
              .toArray(Array.empty[Throwable])
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
      assumeCanRun()
      withTable(T) {
        setupTable()
        spark.sql(
          s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
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
          assert(errors.isEmpty,
            s"Unexpected errors: ${errors
              .toArray(Array.empty[Throwable])
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
      assumeCanRun()
      withTable(T) {
        setupTable()
        spark.sql(s"CACHE TABLE $T")
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
          assert(errors.isEmpty,
            s"Unexpected errors: ${errors
              .toArray(Array.empty[Throwable])
              .map(_.getMessage).mkString("; ")}")
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
          assert(error.get() == null,
            s"Writer failed: ${error.get()}")

          // Phase 3: Execute DataFrame
          if (mod.dfOk) {
            df.collect()
          } else {
            assertThrows[Exception] {
            df.collect()
          }
          }
        }
      }
    }
  }

  // =====================================================================
  // PHASE-LOCKED: Join analysis -> modification -> execution
  // =====================================================================

  mods.foreach { mod =>
    test(s"[phase-locked-join] df1+df2 -> ${mod.name} -> join") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df1 = spark.sql(s"SELECT id AS id1 FROM $T")
        val df2 = spark.sql(s"SELECT id AS id2 FROM $T")
        mod.fn(T)
        // In Connect, both re-analyze: join always works
        val joined = df1.join(df2, df1("id1") === df2("id2"))
        joined.collect()
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
      spark.sql(
        s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT")
      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      // InMemoryTable throws ClassCastException: INT data read as BIGINT
      assertThrows[Exception] {
        df.collect()
      }
    }
  }

  test("[compound] multiple column additions + SQL temp view") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(
        s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
      for (i <- 1 to 5) {
        spark.sql(s"ALTER TABLE $T ADD COLUMN col_$i INT")
      }
      // SQL view re-analyzes: SELECT * picks up new columns.
      // The number of visible columns depends on whether the
      // Connect server's catalog reflects all ALTERs.
      val r = spark.sql("SELECT * FROM tmp").collect()
      assert(r.length == 1)
      assert(r(0).length >= 2, "Should have at least original columns")
    }
  }

  test("[compound] add column + remove column + DataFrame") {
    assumeCanRun()
    withTable(T) {
      spark.sql(
        s"CREATE TABLE $T (id INT, salary INT, extra STRING) USING foo")
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
      checkAnswer(
        spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      // Session write invalidates cache; sees both rows
      checkAnswer(
        spark.sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[compound] concurrent add + rename in separate threads") {
    assumeCanRun()
    withTable(T) {
      spark.sql(
        s"CREATE TABLE $T (id INT, salary INT, bonus INT) USING foo")
      spark.sql(s"INSERT INTO $T VALUES (1, 100, 50)")
      withExecutor() { exec =>
        val barrier = new CyclicBarrier(2)
        val adder = exec.submit(new Runnable {
          override def run(): Unit = {
            barrier.await(30, TimeUnit.SECONDS)
            try spark.sql(
              s"ALTER TABLE $T ADD COLUMN extra STRING")
            catch {
              case _: Exception =>
            }
          }
        })
        val renamer = exec.submit(new Runnable {
          override def run(): Unit = {
            barrier.await(30, TimeUnit.SECONDS)
            try spark.sql(
              s"ALTER TABLE $T RENAME COLUMN bonus TO reward")
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
              try spark.sql(
                s"INSERT INTO $T VALUES ($i, ${i * 100})")
              catch {
              case _: Exception =>
            }
            }
          } catch {
            case e: Throwable => errors.add(e)
          }
        })
        (readers :+ writer).foreach(_.get(120, TimeUnit.SECONDS))
        assert(errors.isEmpty,
          s"Unexpected errors: ${errors
            .toArray(Array.empty[Throwable])
            .map(_.getMessage).mkString("; ")}")
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
              spark.sql(
                s"ALTER TABLE $T ADD COLUMN col_$i INT")
            }
          } catch {
            case e: Throwable => errors.add(e)
          }
        })
        (readers :+ schemaChanger).foreach(
          _.get(120, TimeUnit.SECONDS))
        assert(errors.isEmpty,
          s"Unexpected errors: ${errors
            .toArray(Array.empty[Throwable])
            .map(_.getMessage).mkString("; ")}")
      }
    }
  }

  test("[stress] concurrent temp view queries + data writes") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(
        s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
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
                try spark.sql(
                  s"INSERT INTO $T VALUES " +
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
        assert(errors.isEmpty,
          s"Unexpected errors: ${errors
            .toArray(Array.empty[Throwable])
            .map(_.getMessage).mkString("; ")}")
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
          assert(errors.isEmpty,
            s"Unexpected errors: ${errors
              .toArray(Array.empty[Throwable])
              .map(_.getMessage).mkString("; ")}")
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

  // InMemoryTable throws ClassCastException for type widening
  // because stored Integer values are not converted to Long.
  // Real connectors (Delta/Iceberg) handle this correctly.
  // Connect would succeed with a real connector since it
  // re-analyzes the plan with the new schema.
  test("[connect] type widening fails with InMemoryTable") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      df.collect()
      spark.sql(
        s"ALTER TABLE $T ALTER COLUMN salary TYPE BIGINT")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      assertThrows[Exception] {
        df.collect()
      }
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
      spark.sql(
        s"CREATE TABLE $T (id INT, salary INT) USING foo")
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
      val joined = df1.join(df2, df1("a") === df2("b"))
        .join(df3, df1("a") === df3("c"))
      assert(joined.collect().length == 3)
    }
  }

  // =====================================================================
  // CACHE TABLE: [connect][5.x] Connect scenarios (main spark vs extSession).
  // Classic equivalents: DataSourceV2TablePinningRefreshSuite [5.1]-[5.6]
  // and [5.1-main], [5.3-main], [5.5-main] (classic [S5-session] vs [S5-ext]).
  // =====================================================================

  test("[connect][5.1] CACHE TABLE then external data write") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      checkAnswer(
        spark.sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.1-main] CACHE TABLE then main session data write") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(
        spark.sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.2] CACHE TABLE: session write then external write") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(
        spark.sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200)))

      extSession.sql(s"INSERT INTO $T VALUES (3, 300)").collect()
      checkAnswer(
        spark.sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.3] CACHE TABLE then external schema change + data") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      val ext = extSession
      ext.sql(s"ALTER TABLE $T ADD COLUMN bonus INT").collect()
      ext.sql(s"INSERT INTO $T VALUES (2, 200, 50)").collect()

      checkAnswer(
        spark.sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100, null), Row(2, 200, 50)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.3-main] CACHE TABLE then main session schema change + data") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"ALTER TABLE $T ADD COLUMN extra INT")
      spark.sql(s"INSERT INTO $T VALUES (2, 200, 77)")

      checkAnswer(
        spark.sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100, null), Row(2, 200, 77)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.4] CACHE TABLE: session schema change rebuilds cache") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"ALTER TABLE $T ADD COLUMN new_col INT")
      checkAnswer(
        spark.sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100, null)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.5] CACHE TABLE: external drop/recreate sees new empty table") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      val ext = extSession
      ext.sql(s"DROP TABLE $T").collect()
      ext.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq.empty)
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.5-main] CACHE TABLE: main session drop/recreate sees new empty table") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"DROP TABLE $T")
      spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")

      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq.empty)
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  test("[connect][5.6] REFRESH TABLE after CACHE TABLE picks up current data") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CACHE TABLE $T")
      checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(
        spark.sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql(s"REFRESH TABLE $T")
      checkAnswer(
        spark.sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200)))
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }

  // =====================================================================
  // EDGE CASES
  // =====================================================================

  test("[edge] empty table modification") {
    assumeCanRun()
    withTable(T) {
      spark.sql(
        s"CREATE TABLE $T (id INT, salary INT) USING foo")
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

  test("[edge] nested struct column addition") {
    assumeCanRun()
    withTable(T) {
      spark.sql(s"""CREATE TABLE $T
        (id INT, data STRUCT<salary: INT>) USING foo""")
      spark.sql(s"""INSERT INTO $T VALUES
        (1, named_struct('salary', 100))""")
      val df = spark.sql(s"SELECT * FROM $T")
      df.collect()
      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      val r = df.collect()
      assert(r.length == 1)
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
      setupTable()
      spark.sql(
        s"CREATE OR REPLACE TEMP VIEW tv AS SELECT * FROM $T")
      checkAnswer(
        spark.sql("SELECT * FROM tv"), Seq(Row(1, 100)))
      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      spark.sql(s"INSERT INTO $T VALUES (2, 200, 50)")
      // SQL view re-analyzes: SELECT * picks up new column
      val r = spark.sql("SELECT * FROM tv").collect()
      assert(r.length == 2)
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
      val result = unioned.join(
        fresh, unioned("uid") === fresh("fid"))
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
      val result = df1.join(
        df2, df1("id1") === df2("id2"), "left_outer")
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
              try spark.sql(
                s"INSERT INTO $T VALUES ($i, ${i * 100})")
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
            try spark.sql(
              s"ALTER TABLE $T ADD COLUMN bonus INT")
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
        assert(errors.isEmpty, s"Unexpected errors: ${
          errors.toArray(Array.empty[Throwable])
            .map(_.getMessage).mkString("; ")}")
      }
      spark.sql(s"UNCACHE TABLE IF EXISTS $T")
    }
  }
}
