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
 * Design doc Section [1]: Temp views with stored plans.
 * Tests that temp views (created via createOrReplaceTempView with a DataFrame plan)
 * properly refresh table state on each access and detect incompatible schema changes.
 *
 * Each scenario tests both session writes and external writes.
 * External writes use [[DataSourceV2TableRefreshTestBase.extSession]].
 */
class DataSourceV2TempViewRefreshSuite
    extends DataSourceV2TableRefreshTestBase {


  // [1] Temp views with stored plans

  // 1.1: Session and external data writes

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


  // 1.2: Adding new columns + data

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


  // 1.3: Removing existing columns

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


  // 1.4: Drop and recreate table

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


  // 1.5: Drop and add column with same name and type

  test("[1.5] temp view after session drop/re-add column same type") {
    withTable(T) {
      setupTable()
      spark.table(T).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary INT")

      // InMemoryTable returns null for drop+re-add (dropped column data is discarded).
      // Real connectors with column IDs (Delta/Iceberg) would detect this as
      // a new column and return null instead.
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


  // 1.6: Drop and add column with same name but different type

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


  // 1.7: Type widening

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


  // 1.8: Column rename

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
      // InMemoryTable returns null for drop+re-add (dropped column data is discarded).
      checkAnswer(sql("SELECT * FROM nullid_view"), Seq(Row(1, null)))
    }
  }


  // [Edge] Edge cases

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


  // [Gap] Tests for missing coverage from design doc

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
