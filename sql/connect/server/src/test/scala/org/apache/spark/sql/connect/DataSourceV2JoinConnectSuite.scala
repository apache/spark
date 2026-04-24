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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog

/**
 * DSv2 join tests for Spark Connect mirroring the classic
 * DataSourceV2DataFrameSuite join scenarios.
 *
 * In Connect, both sides of a join re-analyze on every action,
 * so operations that fail in classic mode (DROP COLUMN, drop/recreate
 * table, type change) succeed here because each side gets a fresh
 * plan with the latest schema and data.
 */
class DataSourceV2JoinConnectSuite extends SparkConnectServerTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")

  private val T = "testcat.ns1.ns2.tbl"

  // Scenario 1: join after insert refreshes both sides to latest version.
  test("[connect] join refreshes both sides after insert") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = s.table(T)
      s.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      val df2 = s.table(T)

      // Both sides re-analyze to latest version
      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 2: join after ADD COLUMN.
  // In Connect, df1 also re-analyzes to the 3-column schema
  // (unlike classic where df1 keeps original 2-column schema).
  test("[connect] join after ADD COLUMN sees new schema on both sides") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = s.table(T)
      s.sql(s"ALTER TABLE $T ADD COLUMN new_column INT").collect()
      s.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()
      val df2 = s.table(T)

      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 3: join after DROP COLUMN.
  // Classic fails with COLUMNS_MISMATCH; Connect succeeds because
  // both sides re-analyze and see only 'id'.
  test("[connect] join after DROP COLUMN succeeds") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = s.table(T)
      s.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      s.sql(s"INSERT INTO $T VALUES (2)").collect()
      val df2 = s.table(T)

      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(1, 1), Row(2, 2)))

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 4: join after drop and recreate table.
  // Classic fails with TABLE_ID_MISMATCH; Connect succeeds because
  // both sides re-analyze against the new table.
  test("[connect] join after drop and recreate table succeeds") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = s.table(T)
      s.sql(s"DROP TABLE $T").collect()
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      val df2 = s.table(T)

      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(2, 200, 2, 200)))

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 5: join after drop and re-add column with same type.
  // Without column IDs, Spark cannot detect the column was replaced.
  test("[connect] join after drop and re-add column with same type") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = s.table(T)
      s.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      s.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()
      s.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      val df2 = s.table(T)

      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(1, null, 1, null), Row(2, 200, 2, 200)))

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 6: join after drop and re-add column with different type.
  // Classic fails with COLUMNS_MISMATCH; Connect succeeds because
  // both sides re-analyze and see salary as STRING.
  test("[connect] join after drop and re-add column with different type succeeds") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = s.table(T)
      s.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      s.sql(s"ALTER TABLE $T ADD COLUMN salary STRING").collect()
      s.sql(s"INSERT INTO $T VALUES (2, 'high')").collect()
      val df2 = s.table(T)

      checkAnswer(
        df1.join(df2, df1("id") === df2("id")),
        Seq(Row(1, null, 1, null), Row(2, "high", 2, "high")))

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }
}
