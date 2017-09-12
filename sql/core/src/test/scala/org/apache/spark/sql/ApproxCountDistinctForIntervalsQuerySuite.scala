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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.Decimal


class ApproxCountDistinctForIntervalsQuerySuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private val table = "IntervalDistinctApprox_test"

  private def checkNdv(ndv: Long, expectedNdv: Long, rsd: Double): Unit = {
    // ndv is an approximate value, so we make sure we have the value, and it should be
    // within 3*SD's of the given rsd.
    if (expectedNdv == 0) {
      assert(ndv == 0)
    } else if (expectedNdv > 0) {
      assert(ndv > 0)
      val error = math.abs((ndv / expectedNdv.toDouble) - 1.0d)
      assert(error <= rsd * 3.0d, "Error should be within 3 std. errors.")
    }
  }

  test("null handling") {
    withTempView(table) {
      // empty input row
      val emptyValues: Seq[Option[Double]] = Seq(None, None)
      emptyValues.toDF("col").createOrReplaceTempView(table)
      val query = s"SELECT approx_count_distinct_for_intervals(col, array(0.1, 0.9)) FROM $table"
      checkAnswer(spark.sql(query), Row(Array(0L)))

      // add some non-empty row
      val values: Seq[Option[Double]] = emptyValues :+ Some(0.5d)
      values.toDF("col").createOrReplaceTempView(table)
      checkAnswer(spark.sql(query), Row(Array(1L)))
    }
  }

  test("multiple columns of different types") {
    val intSeq = Seq(Some(5), Some(3), None)
    val doubleSeq = Seq(None, Some(3.0d), Some(5.0d))
    val dateSeq = Seq(Some("1970-01-01"), None, Some("1970-02-02"))
    val timestampSeq = Seq(Some("1970-01-01 00:00:00"), None, Some("1970-01-01 00:00:05"))

    val data = intSeq.indices.map { i =>
      (intSeq(i).map(_.toByte),
        intSeq(i).map(_.toShort),
        intSeq(i).map(_.toInt),
        intSeq(i).map(_.toLong),
        doubleSeq(i).map(_.toFloat),
        doubleSeq(i).map(_.toDouble),
        doubleSeq(i).map(Decimal(_)),
        dateSeq(i).map(Date.valueOf),
        timestampSeq(i).map(Timestamp.valueOf))
    }

    withTempView(table) {
      data.toDF("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9").createOrReplaceTempView(table)
      val dateEndpoints = Seq("1970-01-01", "1970-02-02", "1970-03-03")
        .map(date => DateTimeUtils.fromJavaDate(Date.valueOf(date)))
      val tsEndpoints = Seq("1970-01-01 00:00:00", "1970-01-01 00:00:05", "1970-01-01 00:00:06")
        .map(ts => DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(ts)))
      val expectedAnswer: Array[Array[Long]] = new Array[Array[Long]](9)
      for (i <- expectedAnswer.indices) {
        expectedAnswer(i) = Array(2L, 0L)
      }
      val sparkAnswer =
        spark.sql(
          s"""
             |SELECT
             |  approx_count_distinct_for_intervals(c1, array(3, 5, 7)),
             |  approx_count_distinct_for_intervals(c2, array(3, 5, 7)),
             |  approx_count_distinct_for_intervals(c3, array(3, 5, 7)),
             |  approx_count_distinct_for_intervals(c4, array(3, 5, 7)),
             |  approx_count_distinct_for_intervals(c5, array(3, 5, 7)),
             |  approx_count_distinct_for_intervals(c6, array(3, 5, 7)),
             |  approx_count_distinct_for_intervals(c7, array(3, 5, 7)),
             |  approx_count_distinct_for_intervals(c8, array(${dateEndpoints.mkString(", ")})),
             |  approx_count_distinct_for_intervals(c9, array(${tsEndpoints.mkString(", ")}))
             |FROM $table
           """.stripMargin).collect().head

      for (i <- expectedAnswer.indices) {
        val array = sparkAnswer.getSeq[Long](i)
        val expectedArray = expectedAnswer(i)
        assert(array.length == expectedArray.length)
        for (j <- array.indices) {
          checkNdv(
            ndv = array(j),
            expectedNdv = expectedArray(j),
            rsd = spark.sessionState.conf.ndvMaxError)
        }
      }
    }
  }
}
