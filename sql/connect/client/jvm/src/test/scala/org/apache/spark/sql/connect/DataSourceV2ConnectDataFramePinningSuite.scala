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


/**
 * Design doc Section [4] in Connect: Version pinning in Dataset.
 *
 * KEY DIFFERENCE from classic: In Connect, collect() re-analyzes the plan
 * on the server, so there is NO stale QueryExecution. Both show() and
 * collect() always see the latest data and schema.
 */
class DataSourceV2ConnectDataFramePinningSuite extends DataSourceV2RefreshConnectTestBase {

  // Section 4a: DataFrame first access x All Modifications
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

  // Section 4b: DataFrame collect() is NOT stale (Connect re-analyzes)
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

  test("[connect] collect is consistent with count") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      val df = spark.sql(s"SELECT * FROM $T")
      df.collect()
      spark.sql(s"INSERT INTO $T VALUES (2, 200)")
      assert(df.count() == 2)
      assert(df.collect().length == 2)
    }
  }

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

  test("[connect edge] empty table modification") {
    assumeCanRun()
    withTable(T) {
      spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
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
      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      val r = df.collect()
      assert(r.length == 1)
    }
  }
}
