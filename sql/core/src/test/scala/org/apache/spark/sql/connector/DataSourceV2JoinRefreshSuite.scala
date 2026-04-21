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

import org.apache.spark.sql._

/**
 * Design doc Section [3]: Incrementally constructed queries.
 * Tests that joins, unions, and set operations between pre-analyzed DataFrames
 * properly align table versions during the refresh phase in QueryExecution.
 *
 * Each scenario tests both session writes and external writes.
 * External writes use [[DataSourceV2TableRefreshTestBase.extSession]].
 */
class DataSourceV2JoinRefreshSuite
    extends DataSourceV2TableRefreshTestBase {


  // [3] Incrementally constructed queries (joins)
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
      // InMemoryTable returns null for drop+re-add (dropped column data is discarded).
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


  test("[3.5] join after drop/re-add column same type succeeds") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary INT")

      val df2 = spark.table(T)

      // InMemoryTable returns null for drop+re-add (dropped column data is discarded).
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


  // 3.9+: Non-join incremental patterns

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


  // [Conc] Concurrent writes from threads

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


  // [3+] Additional incremental query patterns

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
}
