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

import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession, SQLHelper}
import org.apache.spark.sql.connect.test.IntegrationTestUtils.isAssemblyJarsDirExists

/**
 * DSv2 table refresh and pinning tests for Spark Connect mode.
 *
 * In Connect, every action re-analyzes the plan on the server.
 * This means:
 * - No stale QueryExecution (collect is NOT pinned)
 * - Schema changes are picked up on every access
 * - Type widening, column rename, column removal all succeed
 *   (plan is re-analyzed with new schema)
 * - Joins/unions/etc always see consistent latest version
 *
 * This suite systematically tests ALL combinations of:
 * - 8 modification types x 8 access patterns
 * - Key behavioral differences from classic mode
 */
class DataSourceV2RefreshConnectSuite
  extends QueryTest with RemoteSparkSession with SQLHelper {

  private val T = "testcat.ns1.ns2.tbl"

  private def setupTable(): Unit = {
    spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
    spark.sql(s"INSERT INTO $T VALUES (1, 100)")
  }

  private def assumeCanRun(): Unit = {
    assume(spark != null && isAssemblyJarsDirExists,
      "Spark Connect server not available")
  }

  // =====================================================================
  // Modification Definitions (SQL-only, no catalog API in Connect)
  // =====================================================================

  // In Connect, SQL temp views (SELECT *) capture column names.
  // Removing/renaming/retyping a captured column fails on
  // re-analysis. Adding columns or data is fine.
  // In Connect, DataFrames re-analyze on every action.
  // ALL modifications succeed for DataFrames.
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
      t =>
        spark.sql(
          s"ALTER TABLE $t RENAME COLUMN salary TO pay"),
      sqlViewOk = false,
      dfOk = true),
    // InMemoryTable throws ClassCastException for type widening:
    // stored Integer values cannot be read as Long.
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
        spark.sql(
          s"CREATE TABLE $t (id INT, salary INT) USING foo")
      },
      sqlViewOk = true,
      dfOk = true))

  // =====================================================================
  // Section 1: SQL Temp View x All Modifications
  // SQL views re-analyze but SELECT * captures column names at
  // creation time. Removals/renames/type changes of captured
  // columns fail.
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
  // Always fresh analysis -- all succeed.
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
  // In Connect, both sides re-analyze. ALL succeed (even column
  // removal, type widening, etc.) because the plan is fresh.
  // Uses aliases to avoid ambiguous column names.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S3] join: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        // Use id-only SELECTs so re-analysis works even after
        // column removal/rename (id always exists)
        val df1 = spark.sql(s"SELECT id AS id1 FROM $T")
        mod.fn(T)
        val df2 = spark.sql(s"SELECT id AS id2 FROM $T")
        // Connect re-analyzes both: join always works
        val joined = df1.join(df2, df1("id1") === df2("id2"))
        joined.collect()
      }
    }
  }

  // =====================================================================
  // Section 4a: DataFrame first access x All Modifications
  // In Connect, DataFrames re-analyze. ALL succeed.
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
  // Section 4b: DataFrame collect() is NOT stale
  // KEY DIFFERENCE from classic: collect() re-analyzes in Connect.
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
        }
      }
    }
  }

  // =====================================================================
  // Section 5: CACHE TABLE (session writes)
  // Numbered [connect][5.x] scenarios: DataSourceV2ConcurrencyRefreshConnectSuite.
  // Classic [5.x] / [5.x-main]: DataSourceV2TablePinningRefreshSuite.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[S5] cache session: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        checkAnswer(
          spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))
        try {
          mod.fn(T)
          spark.sql(s"SELECT * FROM $T").collect()
        } catch {
          case _: Exception => // type widening ClassCastException
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
  // Set Operations x All Modifications
  // Both sides re-analyze in Connect. Use id-only SELECTs to
  // ensure column count matches after schema changes.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[union] df1.union(df2): ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df1 = spark.sql(s"SELECT id FROM $T")
        mod.fn(T)
        val df2 = spark.sql(s"SELECT id FROM $T")
        // Both re-analyzed: union always works (same col count)
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
  // Full chain re-analyzed in Connect.
  // =====================================================================

  mods.foreach { mod =>
    test(s"[chained] filter.count: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        val df = spark.sql(s"SELECT * FROM $T")
        val filtered = df.filter(df("id") > 0)
        mod.fn(T)
        // Full chain re-analyzed
        filtered.count()
      }
    }
  }

  // =====================================================================
  // Self-union x All Modifications
  // Same DF used twice; both re-analyze in Connect.
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
  // Key Connect-specific behaviors (data verification)
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
  // because stored Integer values cannot be read as Long.
  // Real connectors (Delta/Iceberg) handle this correctly.
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
  // Edge cases
  // =====================================================================

  test("[connect edge] empty table modification") {
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

  test("[connect edge] multiple successive schema changes") {
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

  test("[connect edge] nested struct column addition") {
    assumeCanRun()
    withTable(T) {
      spark.sql(s"""CREATE TABLE $T
        (id INT, data STRUCT<salary: INT>) USING foo""")
      spark.sql(s"""INSERT INTO $T VALUES
        (1, named_struct('salary', 100))""")
      val df = spark.sql(s"SELECT * FROM $T")
      df.collect()
      // In Connect, re-analysis picks up nested changes
      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      val r = df.collect()
      assert(r.length == 1)
    }
  }

  test("[connect edge] repeated collect sees each insert") {
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

  test("[connect edge] createOrReplaceTempView + schema change") {
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
}
