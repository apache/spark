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
 * DSv2 join tests for Spark Connect mirroring the classic DataSourceV2DataFrameSuite join
 * scenarios.
 *
 * In Connect, both sides of a join re-analyze on every action, so operations that fail in classic
 * mode (DROP COLUMN, drop/recreate table, type change) succeed here because each side gets a
 * fresh plan with the latest schema and data.
 */
class DataSourceV2JoinConnectSuite extends SparkConnectServerTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")

  private val T = "testcat.ns1.ns2.tbl"

  private def assertRows(actual: Array[Row], expected: Seq[Row]): Unit = {
    val actualStrs = actual.map(_.toString()).toSet
    val expectedStrs = expected.map(_.toString()).toSet
    assert(
      actualStrs == expectedStrs,
      s"Expected ${expected.mkString(", ")} but got ${actual.mkString(", ")}")
  }

  // Scenario 1: join after insert refreshes both sides to latest version.
  test("[connect] join refreshes both sides after insert") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = session.table(T)
      session.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      val df2 = session.table(T)

      // Both sides re-analyze to latest version
      assertRows(
        df1.join(df2, df1("id") === df2("id")).collect(),
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))

      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 2: join after ADD COLUMN.
  // In Connect, df1 also re-analyzes to the 3-column schema
  // (unlike classic where df1 keeps original 2-column schema).
  test("[connect] join after ADD COLUMN sees new schema on both sides") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = session.table(T)
      session.sql(s"ALTER TABLE $T ADD COLUMN new_column INT").collect()
      session.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()
      val df2 = session.table(T)

      assertRows(
        df1.join(df2, df1("id") === df2("id")).collect(),
        Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))

      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 3: join after DROP COLUMN.
  // Classic fails with COLUMNS_MISMATCH; Connect succeeds because
  // both sides re-analyze and see only 'id'.
  test("[connect] join after DROP COLUMN succeeds") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = session.table(T)
      session.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      session.sql(s"INSERT INTO $T VALUES (2)").collect()
      val df2 = session.table(T)

      assertRows(df1.join(df2, df1("id") === df2("id")).collect(), Seq(Row(1, 1), Row(2, 2)))

      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 4: join after drop and recreate table.
  // Classic fails with TABLE_ID_MISMATCH; Connect succeeds because
  // both sides re-analyze against the new table.
  test("[connect] join after drop and recreate table succeeds") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = session.table(T)
      session.sql(s"DROP TABLE $T").collect()
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      val df2 = session.table(T)

      assertRows(df1.join(df2, df1("id") === df2("id")).collect(), Seq(Row(2, 200, 2, 200)))

      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 5: join after drop and re-add column with same type.
  // InMemoryTableCatalog does not migrate old data on schema change,
  // so the original salary value (100) is retained after drop+re-add.
  test("[connect] join after drop and re-add column with same type") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = session.table(T)
      session.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      session.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()
      session.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      val df2 = session.table(T)

      assertRows(
        df1.join(df2, df1("id") === df2("id")).collect(),
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))

      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 6: join after drop and re-add column with different type.
  // Classic fails with COLUMNS_MISMATCH; Connect succeeds because
  // both sides re-analyze and see salary as STRING.
  // InMemoryTableCatalog does not migrate old data on schema change,
  // so the original salary value (100) is retained after drop+re-add.
  test("[connect] join after drop and re-add column with different type succeeds") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

      val df1 = session.table(T)
      session.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      session.sql(s"ALTER TABLE $T ADD COLUMN salary STRING").collect()
      session.sql(s"INSERT INTO $T VALUES (2, 'high')").collect()
      val df2 = session.table(T)

      assertRows(
        df1.join(df2, df1("id") === df2("id")).collect(),
        Seq(Row(1, 100, 1, 100), Row(2, "high", 2, "high")))

      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }
}
