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

class DateTimeExpressionsTimeSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  // ----------------------------------------------------------------------
  // 1. Basic extraction tests
  // ----------------------------------------------------------------------

  test("hour() should work with TimeType") {
    val df = Seq("12:30:45").toDF("t")
      .selectExpr("CAST(t AS TIME) as t")

    val result = df.selectExpr("hour(t)").collect().head.getInt(0)
    assert(result == 12)
  }

  test("minute() should work with TimeType") {
    val df = Seq("12:30:45").toDF("t")
      .selectExpr("CAST(t AS TIME) as t")

    val result = df.selectExpr("minute(t)").collect().head.getInt(0)
    assert(result == 30)
  }

  test("second() should work with TimeType") {
    val df = Seq("12:30:45").toDF("t")
      .selectExpr("CAST(t AS TIME) as t")

    val result = df.selectExpr("second(t)").collect().head.getInt(0)
    assert(result == 45)
  }

  // ----------------------------------------------------------------------
  // 2. Fractional seconds
  // ----------------------------------------------------------------------

  test("second with fraction should work with TimeType") {
    val df = Seq("12:30:45.123456").toDF("t")
      .selectExpr("CAST(t AS TIME) as t")

    val result = df.selectExpr("extract(SECOND FROM t)").collect().head.get(0)

    // Spark returns Decimal(8,6)
    assert(result.toString.startsWith("45.123456"))
  }

  // ----------------------------------------------------------------------
  // 3. Edge cases
  // ----------------------------------------------------------------------

  test("midnight time extraction") {
    val df = Seq("00:00:00").toDF("t")
      .selectExpr("CAST(t AS TIME) as t")

    val row = df.selectExpr("hour(t)", "minute(t)", "second(t)").collect().head

    assert(row.getInt(0) == 0)
    assert(row.getInt(1) == 0)
    assert(row.getInt(2) == 0)
  }

  test("max time extraction") {
    val df = Seq("23:59:59.999999").toDF("t")
      .selectExpr("CAST(t AS TIME) as t")

    val row = df.selectExpr("hour(t)", "minute(t)", "second(t)").collect().head

    assert(row.getInt(0) == 23)
    assert(row.getInt(1) == 59)
    assert(row.getInt(2) == 59)
  }

  // ----------------------------------------------------------------------
  // 4. Null handling
  // ----------------------------------------------------------------------

  test("null input should return null") {
    val df = Seq[java.lang.String](null).toDF("t")
      .selectExpr("CAST(t AS TIME) as t")

    val row = df.selectExpr("hour(t)").collect().head
    assert(row.isNullAt(0))
  }

  // ----------------------------------------------------------------------
  // 5. Multiple rows
  // ----------------------------------------------------------------------

  test("multiple rows extraction") {
    val df = Seq("01:00:00", "02:30:00", "03:45:00").toDF("t")
      .selectExpr("CAST(t AS TIME) as t")

    val result = df.selectExpr("hour(t)").collect().map(_.getInt(0))

    assert(result.sameElements(Array(1, 2, 3)))
  }

  // ----------------------------------------------------------------------
  // 6. Consistency with TimestampType
  // ----------------------------------------------------------------------

  test("TimeType and TimestampType should give same hour/min/sec") {
    val df = Seq("12:30:45").toDF("t")

    val timeDF = df.selectExpr("CAST(t AS TIME) as t")
    val tsDF = df.selectExpr("CAST(t AS TIMESTAMP) as ts")

    val timeRow = timeDF.selectExpr("hour(t)", "minute(t)", "second(t)").collect().head
    val tsRow = tsDF.selectExpr("hour(ts)", "minute(ts)", "second(ts)").collect().head

    assert(timeRow == tsRow)
  }

  // ----------------------------------------------------------------------
  // 7. Invalid function usage
  // ----------------------------------------------------------------------

  test("unsupported time function should fail") {
    val df = Seq("12:30:45").toDF("t")
      .selectExpr("CAST(t AS TIME) as t")

    // day() is supported via implicit cast to DATE
    // Let's use a function that doesn't exist or doesn't support TIME
    intercept[AnalysisException] {
      df.selectExpr("t + t").collect()
    }
  }

  // ----------------------------------------------------------------------
  // 8. Codegen vs non-codegen consistency
  // ----------------------------------------------------------------------

  test("codegen consistency for TimeType extraction") {
    val df = Seq("12:30:45").toDF("t")
      .selectExpr("CAST(t AS TIME) as t")

    val result1 = df.selectExpr("hour(t)").collect()
    spark.conf.set("spark.sql.codegen.wholeStage", "false")
    val result2 = df.selectExpr("hour(t)").collect()
    spark.conf.set("spark.sql.codegen.wholeStage", "true")

    assert(result1.sameElements(result2))
  }
}
