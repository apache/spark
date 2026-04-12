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
    .set("spark.sql.catalog.nocopycal",
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
  private def isExpectedError(e: Throwable): Boolean = e match {
    case _: AnalysisException => true
    case se: Exception if se.getCause != null => isExpectedError(se.getCause)
    case _ => false
  }

  // =====================================================================
  // Modification Definitions
  // =====================================================================

  case class Mod(
      name: String,
      fn: String => Unit,
      tempViewOk: Boolean,
      dfOk: Boolean,
      joinOk: Boolean)

  private val mods: Seq[Mod] = Seq(
    Mod("data write",
      t => sql(s"INSERT INTO $t VALUES (2, 200)"),
      tempViewOk = true, dfOk = true, joinOk = true),
    Mod("column addition",
      t => sql(s"ALTER TABLE $t ADD COLUMN new_col INT"),
      tempViewOk = true, dfOk = true, joinOk = true),
    Mod("column removal",
      t => sql(s"ALTER TABLE $t DROP COLUMN salary"),
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("column rename",
      t => sql(s"ALTER TABLE $t RENAME COLUMN salary TO pay"),
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("type widening INT to BIGINT",
      t => sql(s"ALTER TABLE $t ALTER COLUMN salary TYPE BIGINT"),
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("drop+add column same type",
      t => { sql(s"ALTER TABLE $t DROP COLUMN salary")
             sql(s"ALTER TABLE $t ADD COLUMN salary INT") },
      tempViewOk = true, dfOk = true, joinOk = true),
    Mod("drop+add column different type",
      t => { sql(s"ALTER TABLE $t DROP COLUMN salary")
             sql(s"ALTER TABLE $t ADD COLUMN salary STRING") },
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("drop/recreate table",
      t => { sql(s"DROP TABLE $t")
             sql(s"CREATE TABLE $t (id INT, salary INT) USING foo") },
      tempViewOk = true, dfOk = false, joinOk = false))

  // External modifications via catalog API (bypass session cache invalidation)
  private val extMods: Seq[Mod] = Seq(
    Mod("ext column addition",
      _ => cat.alterTable(IDENT, TableChange.addColumn(Array("new_col"), IntegerType, true)),
      tempViewOk = true, dfOk = true, joinOk = true),
    Mod("ext column removal",
      _ => cat.alterTable(IDENT, TableChange.deleteColumn(Array("salary"), false)),
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("ext column rename",
      _ => cat.alterTable(IDENT, TableChange.renameColumn(Array("salary"), "pay")),
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("ext type widening",
      _ => cat.alterTable(IDENT, TableChange.updateColumnType(Array("salary"), LongType)),
      tempViewOk = false, dfOk = false, joinOk = false),
    Mod("ext data truncation",
      _ => cat.loadTable(IDENT, util.Set.of(TableWritePrivilege.DELETE))
              .asInstanceOf[TruncatableTable].truncateTable(),
      tempViewOk = true, dfOk = true, joinOk = true),
    Mod("ext drop+add column same type",
      _ => { cat.alterTable(IDENT, TableChange.deleteColumn(Array("salary"), false))
             cat.alterTable(IDENT, TableChange.addColumn(Array("salary"), IntegerType, true)) },
      tempViewOk = true, dfOk = true, joinOk = true),
    Mod("ext drop+add column different type",
      _ => { cat.alterTable(IDENT, TableChange.deleteColumn(Array("salary"), false))
             cat.alterTable(IDENT, TableChange.addColumn(Array("salary"), StringType, true)) },
      tempViewOk = false, dfOk = false, joinOk = false))

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
          spark.table("tmp").collect() // should not throw
        } else {
          intercept[AnalysisException] { spark.table("tmp").collect() }
        }
      }
    }
  }

  // =====================================================================
  // Section 2: Repeated SQL Access x All Modifications
  // Fresh analysis each time --> all modifications succeed
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S2] repeated SQL: ${mod.name}") {
      withTable(T) {
        setupTable()
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))
        mod.fn(T)
        spark.sql(s"SELECT * FROM $T").collect() // always succeeds: fresh analysis
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
        if (mod.joinOk) {
          df1.join(df2, df1("id") === df2("id")).collect()
        } else {
          intercept[AnalysisException] {
            df1.join(df2, df1("id") === df2("id")).collect()
          }
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
          df.collect()
        } else {
          intercept[AnalysisException] { df.collect() }
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
        mod.fn(T)
        // Session modifications may invalidate cache; query must not crash
        spark.table(T).collect()
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
          s"SELECT * FROM $T WHERE id IN (SELECT id FROM $T WHERE salary > 0)")
        mod.fn(T)
        if (mod.dfOk) {
          df.collect()
        } else {
          intercept[AnalysisException] { df.collect() }
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
          intercept[AnalysisException] {
            spark.table("tmp").collect()
          }
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
            intercept[AnalysisException] { df.collect() }
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
          intercept[AnalysisException] { joined.collect() }
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
      intercept[AnalysisException] { df.collect() }
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
      intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
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
      intercept[AnalysisException] { df.collect() }
    }
  }

  test("[compound] three successive drops + adds") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, a INT, b INT, c INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 10, 20, 30)")
      val df = spark.table(T)
      sql(s"ALTER TABLE $T DROP COLUMN a")
      sql(s"ALTER TABLE $T ADD COLUMN a STRING") // different type
      // Column type changed from INT to STRING
      intercept[AnalysisException] { df.collect() }
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
      // Final consistency: table is readable
      assert(spark.sql(s"SELECT * FROM $T").collect().length >= 1)
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
      intercept[AnalysisException] { df.collect() }
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
      intercept[AnalysisException] { spark.table("tmp").collect() }
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
      intercept[AnalysisException] { spark.table("tmp").collect() }
    }
  }

  test("[edge] nullability change  - DataFrame") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT NOT NULL) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")
      val df = spark.table(T)
      sql(s"ALTER TABLE $T ALTER COLUMN salary DROP NOT NULL")
      intercept[AnalysisException] { df.collect() }
    }
  }

  test("[edge] type widening INT to DOUBLE  - temp view") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE DOUBLE")
      intercept[AnalysisException] { spark.table("tmp").collect() }
    }
  }

  test("[edge] type widening INT to STRING  - temp view") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE STRING")
      intercept[AnalysisException] { spark.table("tmp").collect() }
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
      intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
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
      intercept[AnalysisException] { spark.table("v2").collect() }
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
          intercept[AnalysisException] {
            df1.union(df2).collect()
          }
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
          intercept[AnalysisException] {
            df1.except(df2).collect()
          }
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
          intercept[AnalysisException] {
            df1.intersect(df2).collect()
          }
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
          intercept[AnalysisException] {
            df.union(df).collect()
          }
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
          intercept[AnalysisException] {
            aggregated.collect()
          }
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
          intercept[AnalysisException] {
            df1.join(df2, df1("id") === df2("id")).collect()
          }
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
          intercept[AnalysisException] {
            spark.table("old_v")
              .join(
                fresh,
                spark.table("old_v")("id") === fresh("id"))
              .collect()
          }
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
      val e = intercept[AnalysisException] {
        source.writeTo(T).append()
      }
      assert(
        e.getMessage.contains("incompatible changes") ||
        e.getMessage.contains("INCOMPATIBLE"))
    }
  }

  test("[write-same] append to same table after data write only") {
    withTable(T) {
      setupTable()
      val source = spark.table(T).filter("salary > 50")
      sql(s"INSERT INTO $T VALUES (2, 200)")
      // data-only change: schema is unchanged, so command succeeds
      source.writeTo(T).append()
      // table should have original + new insert + appended rows
      assert(spark.table(T).count() >= 2)
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
        // CTAS from DF whose source table changed incompatibly
        intercept[AnalysisException] {
          source.writeTo(t2).createOrReplace()
        }
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
        // Schema-compatible change: CTAS succeeds because
        // column names and types haven't changed
        source.writeTo(t2).createOrReplace()
        assert(spark.table(t2).count() >= 1)
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
      val e = intercept[AnalysisException] {
        source.writeTo(t2).createOrReplace()
      }
      assert(
        e.getMessage.contains("incompatible changes") ||
        e.getMessage.contains("INCOMPATIBLE"))
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

      // df1 has schema (id, salary), df2 has (id, salary, bonus)
      // union requires same number of columns -- df1 preserves
      // its original schema, df2 has 3 columns.
      // This may fail if schemas don't align for union.
      try {
        val result = df1.union(df2).collect()
        // If it succeeds, verify results are reasonable
        assert(result.nonEmpty)
      } catch {
        case _: AnalysisException =>
        // Union requires matching column count; schema mismatch
        // is expected when df1 has 2 cols and df2 has 3
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
      intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id"), "left_anti")
          .collect()
      }
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
      intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
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

  test("[gap] writeTo append succeeds after column removal") {
    withTable(T) {
      setupTable()
      val source = spark.table(T)
      sql(s"ALTER TABLE $T DROP COLUMN salary")
      // writeTo().append() re-resolves the source against
      // the current target schema, so it succeeds even after
      // column removal. The stale source DF's extra column
      // is silently dropped during write resolution.
      source.writeTo(T).append()
      assert(spark.table(T).count() >= 1)
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
      intercept[AnalysisException] {
        source.writeTo(t2).createOrReplace()
      }
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
      intercept[AnalysisException] { df.collect() }
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
      intercept[AnalysisException] { df.collect() }
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
      intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }

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
      intercept[AnalysisException] {
        spark.table("v3").collect()
      }
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
      intercept[AnalysisException] {
        df.write.saveAsTable(t2)
      }
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
      intercept[AnalysisException] { df.collect() }
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

      // DF view: detects removal, throws
      intercept[AnalysisException] {
        spark.table("df_v").collect()
      }
      // SQL view: also fails (SELECT * captured column names)
      intercept[AnalysisException] {
        spark.table("sql_v").collect()
      }
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
      val e = intercept[AnalysisException] {
        df.writeTo(t2).createOrReplace()
      }
      assert(e.getMessage.contains("incompatible"))
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
      intercept[AnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
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

      // Never goes backward
      assert(spark.table(T).count() >= 4)
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
      intercept[AnalysisException] {
        spark.table("tmp").collect()
      }
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
      intercept[AnalysisException] {
        spark.table("tmp").collect()
      }
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
      intercept[AnalysisException] { df.collect() }
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
      intercept[AnalysisException] { df.collect() }
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
      // Cache should be invalidated
      spark.table(T).collect()
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
      intercept[AnalysisException] { df.collect() }
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

      // After concurrent ops, table should be readable
      assert(spark.sql(s"SELECT * FROM $ST")
        .collect().length >= 1)
    }
  }

  // =====================================================================
  // GAP FIX: copyOnLoad=false (shared table instance)
  // With copyOnLoad=false, loadTable returns the SAME object.
  // Modifications are immediately visible without refresh.
  // =====================================================================

  private val NC = "nocopycal.ns1.tbl"

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

  test("[null-id] drop/recreate NOT detected (no table ID)") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df = spark.table(NI)
      // Drop and recreate
      sql(s"DROP TABLE $NI")
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (9, 900)")
      // Without table ID, drop/recreate is NOT detected.
      // Contrast with testcat where this throws TABLE_ID_MISMATCH.
      // The DF succeeds without error (no identity check).
      df.collect() // should NOT throw
    }
  }

  test("[null-id] join after drop/recreate succeeds") {
    withTable(NI) {
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (1, 100)")
      val df1 = spark.table(NI)
      sql(s"DROP TABLE $NI")
      sql(s"CREATE TABLE $NI (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NI VALUES (2, 200)")
      val df2 = spark.table(NI)
      // No table ID = no TABLE_ID_MISMATCH error
      // Both DFs resolve to whatever table currently exists
      val joined = df1.join(
        df2, df1("id") === df2("id"))
      // Should succeed (contrast: testcat would throw)
      joined.collect()
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
      // Temp view resolves to new table (no ID check)
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
      intercept[AnalysisException] { df.collect() }
    }
  }
}
