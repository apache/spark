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
 * Design doc Section [4]: Version pinning in Dataset.
 * Tests the behavioral difference between show() (creates fresh QE each time)
 * and collect() (reuses the stale QueryExecution).
 *
 * Each scenario tests both session writes and external writes.
 * External writes use [[DataSourceV2TableRefreshTestBase.extSession]].
 */
class DataSourceV2DataFramePinningSuite
    extends DataSourceV2TableRefreshTestBase {


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


  // [4] Version pinning in Dataset (show vs collect)

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
  // InMemoryTable returns null for drop+re-add (dropped column data is discarded).
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
      // InMemoryTable returns null for drop+re-add (dropped column data is discarded).
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
      // InMemoryTable returns null for drop+re-add (dropped column data is discarded).
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


  // [DocComment] Tests from design doc review comments

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


  // show vs collect inconsistency (doc Section 4.1.2)

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
}
