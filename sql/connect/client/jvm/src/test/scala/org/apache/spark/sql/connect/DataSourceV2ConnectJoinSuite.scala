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

/**
 * Design doc Section [3] in Connect: Incrementally constructed queries.
 *
 * In Connect, both sides of a join re-analyze. ALL modifications succeed because each side gets a
 * fresh plan with the latest schema and data.
 */
class DataSourceV2ConnectJoinSuite extends DataSourceV2RefreshConnectTestBase {

  // Section 3: Join x All Modifications
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

  // Section 6: Subquery x All Modifications
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

  // Set Operations x All Modifications
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

  // Self-union x All Modifications
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

  test("[connect] union data aligned after write") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.sql(s"SELECT id FROM $T")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.sql(s"SELECT id FROM $T")
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

  // Incrementally constructed queries: join scenarios mirroring the classic
  // DataSourceV2DataFrameSuite tests. In Connect, both sides re-analyze on
  // every action, so operations that fail in classic mode succeed here.

  test("SPARK-54157: [connect] join refreshes both sides after insert") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)
      // Both sides re-analyze to latest version
      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
    }
  }

  test("SPARK-54157: [connect] join after ADD COLUMN sees new schema on both sides") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      spark.sql(s"ALTER TABLE $T ADD COLUMN new_column INT")
      spark.sql(s"INSERT INTO $T VALUES (2, 200, -1)")
      val df2 = spark.table(T)
      // In Connect, df1 also re-analyzes to the 3-column schema
      // (unlike classic where df1 keeps original 2-column schema)
      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
    }
  }

  test("SPARK-54157: [connect] join after DROP COLUMN succeeds") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      spark.sql(s"ALTER TABLE $T DROP COLUMN salary")
      spark.sql(s"INSERT INTO $T VALUES (2)")
      val df2 = spark.table(T)
      // In Connect, both sides re-analyze: both see only 'id'
      // (classic fails with COLUMNS_MISMATCH)
      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(1, 1), Row(2, 2)))
    }
  }

  test("SPARK-54157: [connect] join after drop and recreate table succeeds") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      spark.sql(s"DROP TABLE $T")
      spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)
      // In Connect, both sides re-analyze against the new table
      // (classic fails with TABLE_ID_MISMATCH)
      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(2, 200, 2, 200)))
    }
  }

  test("SPARK-54157: [connect] join after drop and re-add column with same type") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      spark.sql(s"ALTER TABLE $T DROP COLUMN salary")
      spark.sql(s"ALTER TABLE $T ADD COLUMN salary INT")
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      val df2 = spark.table(T)
      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(1, null, 1, null), Row(2, 200, 2, 200)))
    }
  }

  test("SPARK-54157: [connect] join after drop and re-add column with different type succeeds") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)
      spark.sql(s"ALTER TABLE $T DROP COLUMN salary")
      spark.sql(s"ALTER TABLE $T ADD COLUMN salary STRING")
      spark.sql(s"INSERT INTO $T VALUES (2, 'high')")
      val df2 = spark.table(T)
      // In Connect, both sides re-analyze: both see salary as STRING
      // (classic fails with COLUMNS_MISMATCH)
      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(1, null, 1, null), Row(2, "high", 2, "high")))
    }
  }
}
