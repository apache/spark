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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class TimeTypeSupportSuite extends QueryTest with SharedSparkSession {

  test("CREATE TABLE, INSERT and SELECT with TIME type") {
    val tableName = "test_time_support"
    withTable(tableName) {
      // CREATE TABLE
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING parquet")

      // INSERT INTO
      sql(s"INSERT INTO $tableName VALUES (1, TIME '08:00:00'), (2, TIME '12:30:45.123456')")

      // SELECT
      val df = sql(s"SELECT * FROM $tableName ORDER BY id")

      // Verify schema
      assert(df.schema === StructType(Seq(
        StructField("id", IntegerType),
        StructField("t", TimeType)
      )))

      // Verify data
      val result = df.collect()
      assert(result.length === 2)

      assert(result(0).getInt(0) === 1)
      assert(result(0).get(1) === java.time.LocalTime.of(8, 0))
      assert(result(1).getInt(0) === 2)
      assert(result(1).get(1) === java.time.LocalTime.of(12, 30, 45, 123456000))
    }
  }

  test("SELECT TIME literal") {
    val df = sql("SELECT TIME '10:30:00' as t")
    checkAnswer(df, Row(java.time.LocalTime.of(10, 30)))
  }

  test("CAST STRING to TIME") {
    val df = sql("SELECT CAST('14:20:00' AS TIME) as t")
    checkAnswer(df, Row(java.time.LocalTime.of(14, 20)))
  }

  test("cast string to time - DataFrame") {
    import testImplicits._
    val df = Seq("12:30:00").toDF("t")
    val result = df.selectExpr("CAST(t AS TIME)")
    assert(result.collect()(0).get(0) != null)
  }

  test("cast invalid string to time") {
    import testImplicits._
    val df = Seq("invalid").toDF("t")
    val result = df.selectExpr("CAST(t AS TIME)")
    assert(result.collect()(0).isNullAt(0))
  }

  test("cast invalid string to time in ANSI mode") {
    import testImplicits._
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      val df = Seq("invalid").toDF("t")
      intercept[Exception] {
        df.selectExpr("CAST(t AS TIME)").collect()
      }
    }
  }

  test("cast time to string") {
    import testImplicits._
    val df = Seq("12:30:00").toDF("t")
      .selectExpr("CAST(t AS TIME) as time")

    val result = df.selectExpr("CAST(time AS STRING)")
    assert(result.collect()(0).getString(0).contains("12:30:00"))
  }

  test("cast time to timestamp") {
    import testImplicits._
    val df = Seq("12:30:00").toDF("t")
      .selectExpr("CAST(t AS TIME) as time")

    val result = df.selectExpr("CAST(time AS TIMESTAMP)")
    assert(result.collect()(0).get(0) != null)
  }

  test("cast timestamp to time") {
    import testImplicits._
    val df = Seq("2023-01-01 12:30:00").toDF("t")
      .selectExpr("CAST(t AS TIMESTAMP) as ts")
    val result = df.selectExpr("CAST(ts AS TIME)")
    assert(result.collect()(0).get(0) != null)
  }

  test("cast time to date") {
    import testImplicits._
    val df = Seq("12:30:00").toDF("t")
      .selectExpr("CAST(t AS TIME) as time")

    val result = df.selectExpr("CAST(time AS DATE)")
    assert(result.collect()(0).get(0) != null)
  }

  test("cast time to long") {
    import testImplicits._
    val df = Seq("12:30:00").toDF("t")
      .selectExpr("CAST(t AS TIME) as time")

    val result = df.selectExpr("CAST(time AS LONG)")
    assert(result.collect()(0).getLong(0) > 0)
  }

  test("cast long to time") {
    import testImplicits._
    val df = Seq(45000L).toDF("t") // 12:30:00 in seconds
    val result = df.selectExpr("CAST(t AS TIME)")
    assert(result.collect()(0).get(0) != null)
  }

  test("cast null to time") {
    import testImplicits._
    val df = Seq(null.asInstanceOf[String]).toDF("t")
    val result = df.selectExpr("CAST(t AS TIME)")
    assert(result.collect()(0).isNullAt(0))
  }

  test("time cast roundtrip") {
    import testImplicits._
    val df = Seq("12:30:45").toDF("t")
    val result = df
      .selectExpr("CAST(t AS TIME) as time")
      .selectExpr("CAST(time AS STRING)")

    assert(result.collect()(0).getString(0).contains("12:30:45"))
  }

  test("cast time with codegen enabled") {
    import testImplicits._
    withSQLConf("spark.sql.codegen.wholeStage" -> "true") {
      val df = Seq("12:30:00").toDF("t")
      val result = df.selectExpr("CAST(t AS TIME)")
      assert(result.collect()(0).get(0) != null)
    }
  }

  test("cast time to boolean and vice-versa") {
    import testImplicits._
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      val df = Seq("00:00:00", "12:30:00").toDF("t")
        .selectExpr("CAST(t AS TIME) as time")

      val result = df.selectExpr("CAST(time AS BOOLEAN) as b")
      val rows = result.collect()
      assert(rows(0).getBoolean(0) === false) // 00:00:00 is 0 micros -> false
      assert(rows(1).getBoolean(0) === true)

      val backToTime = result.selectExpr("CAST(b AS TIME)")
      val timeRows = backToTime.collect()
      assert(timeRows(0).get(0) === java.time.LocalTime.of(0, 0))
      // true -> 1 micro -> 00:00:00.000001
      val expected = java.time.LocalTime.of(0, 0, 0, 1000)
      assert(timeRows(1).get(0) === expected)
    }
  }
}
