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

package org.apache.spark.sql.catalyst.expressions

import java.util.Calendar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

class CsvExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper with PlanTestBase {
  val gmtId = Option(DateTimeUtils.TimeZoneGMT.getID)

  test("to_csv - struct") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(create_row(1), schema)
    checkEvaluation(StructsToCsv(Map.empty, struct, gmtId), "1")
  }

  test("to_csv null input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(null, schema)
    checkEvaluation(
      StructsToCsv(Map.empty, struct, gmtId),
      null
    )
  }

  test("to_csv with timestamp") {
    val schema = StructType(StructField("t", TimestampType) :: Nil)
    val c = Calendar.getInstance(DateTimeUtils.TimeZoneGMT)
    c.set(2016, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    val struct = Literal.create(create_row(c.getTimeInMillis * 1000L), schema)

    checkEvaluation(StructsToCsv(Map.empty, struct, gmtId), "2016-01-01T00:00:00.000Z")
    checkEvaluation(
      StructsToCsv(Map.empty, struct, Option("PST")), "2015-12-31T16:00:00.000-08:00")

    checkEvaluation(
      StructsToCsv(
        Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
          DateTimeUtils.TIMEZONE_OPTION -> gmtId.get),
        struct,
        gmtId),
      "2016-01-01T00:00:00"
    )
    checkEvaluation(
      StructsToCsv(
        Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
          DateTimeUtils.TIMEZONE_OPTION -> "PST"),
        struct,
        gmtId),
      "2015-12-31T16:00:00"
    )
  }
}
