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

package org.apache.spark.sql.hive

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{IntegerType, TimeType}

class HiveTimeTypeSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {

  test("CREATE Hive table with TIME column and implicit string conversion") {
    val tableName = "hive_time_implicit_test"
    withTable(tableName) {
      // CREATE TABLE without USING clause - defaults to Hive format
      sql(s"CREATE TABLE $tableName (id INT, logintime TIME)")

      // INSERT with implicit conversion (no CAST)
      sql(s"""
        INSERT INTO $tableName VALUES
          (1, '08:00:00'),
          (2, '12:30:45'),
          (3, '23:59:59')
      """)

      // SELECT and verify
      val result = sql(s"SELECT * FROM $tableName ORDER BY id")
      val rows = result.collect()

      assert(rows.length === 3)
      assert(result.schema.fields(0).name === "id")
      assert(result.schema.fields(0).dataType === IntegerType)
      assert(result.schema.fields(1).name === "logintime")
      assert(result.schema.fields(1).dataType === TimeType)

      assert(rows(0).getInt(0) === 1)
      assert(rows(0).get(1) === java.time.LocalTime.of(8, 0, 0))

      assert(rows(1).getInt(0) === 2)
      assert(rows(1).get(1) === java.time.LocalTime.of(12, 30, 45))

      assert(rows(2).getInt(0) === 3)
      assert(rows(2).get(1) === java.time.LocalTime.of(23, 59, 59))
    }
  }

  test("CREATE Hive table with TIME column and explicit CAST") {
    val tableName = "hive_time_explicit_test"
    withTable(tableName) {
      // CREATE TABLE without USING clause - defaults to Hive format
      sql(s"CREATE TABLE $tableName (id INT, logintime TIME)")

      // INSERT with explicit CAST
      sql(s"""
        INSERT INTO $tableName VALUES
          (1, CAST('14:30:00' AS TIME)),
          (2, CAST('09:15:30.123456' AS TIME))
      """)

      // SELECT and verify
      val result = sql(s"SELECT * FROM $tableName ORDER BY id")
      checkAnswer(result, Seq(
        Row(1, java.time.LocalTime.of(14, 30, 0)),
        Row(2, java.time.LocalTime.of(9, 15, 30, 123456000))
      ))
    }
  }

  test("JOIN on TIME column") {
    val t1 = "hive_time_join_1"
    val t2 = "hive_time_join_2"

    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id INT, t TIME)")
      sql(s"CREATE TABLE $t2 (id INT, t TIME)")

      sql(s"""
        INSERT INTO $t1 VALUES
          (1, '10:00:00'),
          (2, '12:00:00')
      """)

      sql(s"""
        INSERT INTO $t2 VALUES
          (10, '10:00:00'),
          (20, '15:00:00')
      """)

      val result = sql(s"""
        SELECT a.id, b.id
        FROM $t1 a JOIN $t2 b
        ON a.t = b.t
      """)

      checkAnswer(result, Seq(
        Row(1, 10)
      ))
    }
  }

  test("ORDER BY TIME column") {
    val table = "hive_time_order"

    withTable(table) {
      sql(s"CREATE TABLE $table (id INT, t TIME)")

      sql(s"""
        INSERT INTO $table VALUES
          (1, '12:00:00'),
          (2, '08:00:00'),
          (3, '23:59:59')
      """)

      val result = sql(s"SELECT id FROM $table ORDER BY t")

      checkAnswer(result, Seq(
        Row(2), // 08:00
        Row(1), // 12:00
        Row(3)  // 23:59
      ))
    }
  }

  test("GROUP BY TIME column") {
    val table = "hive_time_group"

    withTable(table) {
      sql(s"CREATE TABLE $table (t TIME)")

      sql(s"""
        INSERT INTO $table VALUES
          ('10:00:00'),
          ('10:00:00'),
          ('12:00:00')
      """)

      val result = sql(s"""
        SELECT t, COUNT(*) FROM $table GROUP BY t
      """)

      checkAnswer(result, Seq(
        Row(java.time.LocalTime.of(10, 0, 0), 2),
        Row(java.time.LocalTime.of(12, 0, 0), 1)
      ))
    }
  }

  test("WHERE filter on TIME column") {
    val table = "hive_time_filter"

    withTable(table) {
      sql(s"CREATE TABLE $table (t TIME)")

      sql(s"""
        INSERT INTO $table VALUES
          ('08:00:00'),
          ('12:00:00'),
          ('18:00:00')
      """)

      val result = sql(s"""
        SELECT t FROM $table WHERE t > TIME '10:00:00'
      """)

      checkAnswer(result, Seq(
        Row(java.time.LocalTime.of(12, 0, 0)),
        Row(java.time.LocalTime.of(18, 0, 0))
      ))
    }
  }

  test("MIN/MAX on TIME column") {
    val table = "hive_time_minmax"

    withTable(table) {
      sql(s"CREATE TABLE $table (t TIME)")

      sql(s"""
        INSERT INTO $table VALUES
          ('08:00:00'),
          ('12:00:00'),
          ('23:59:59')
      """)

      val result = sql(s"""
        SELECT MIN(t), MAX(t) FROM $table
      """)

      checkAnswer(result, Seq(
        Row(
          java.time.LocalTime.of(8, 0, 0),
          java.time.LocalTime.of(23, 59, 59)
        )
      ))
    }
  }

  test("DISTINCT TIME values") {
    val table = "hive_time_distinct"

    withTable(table) {
      sql(s"CREATE TABLE $table (t TIME)")

      sql(s"""
        INSERT INTO $table VALUES
          ('10:00:00'),
          ('10:00:00'),
          ('12:00:00')
      """)

      val result = sql(s"SELECT DISTINCT t FROM $table")

      checkAnswer(result, Seq(
        Row(java.time.LocalTime.of(10, 0, 0)),
        Row(java.time.LocalTime.of(12, 0, 0))
      ))
    }
  }

  test("CAST TIME inside query") {
    val table = "hive_time_cast"

    withTable(table) {
      sql(s"CREATE TABLE $table (t TIME)")

      sql(s"INSERT INTO $table VALUES ('10:30:45')")

      val result = sql(s"""
        SELECT CAST(t AS STRING) FROM $table
      """)

      checkAnswer(result, Seq(
        Row("10:30:45")
      ))
    }
  }

  test("JOIN TIME with other columns") {
    val t1 = "hive_time_join_mix1"
    val t2 = "hive_time_join_mix2"

    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id INT, t TIME)")
      sql(s"CREATE TABLE $t2 (id INT, name STRING)")

      sql(s"INSERT INTO $t1 VALUES (1, '10:00:00'), (2, '12:00:00')")
      sql(s"INSERT INTO $t2 VALUES (1, 'A'), (2, 'B')")

      val result = sql(s"""
        SELECT t1.t, t2.name
        FROM $t1 t1 JOIN $t2 t2
        ON t1.id = t2.id
      """)

      checkAnswer(result, Seq(
        Row(java.time.LocalTime.of(10, 0, 0), "A"),
        Row(java.time.LocalTime.of(12, 0, 0), "B")
      ))
    }
  }

  test("NULL handling in TIME column") {
    val table = "hive_time_null"

    withTable(table) {
      sql(s"CREATE TABLE $table (t TIME)")

      sql(s"""
        INSERT INTO $table VALUES
          ('10:00:00'),
          (NULL)
      """)

      val result = sql(s"SELECT * FROM $table WHERE t IS NULL")

      assert(result.collect().length === 1)
    }
  }

  test("SELF JOIN on TIME column") {
    val table = "hive_time_self_join"

    withTable(table) {
      sql(s"CREATE TABLE $table (id INT, t TIME)")

      sql(s"""
        INSERT INTO $table VALUES
          (1, '10:00:00'),
          (2, '10:00:00')
      """)

      val result = sql(s"""
        SELECT a.id, b.id
        FROM $table a JOIN $table b
        ON a.t = b.t
      """)

      assert(result.count() === 4) // cartesian match
    }
  }
}

