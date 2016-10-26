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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.Decimal


class HistogramEndpointsQuerySuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private val table = "histogram_endpoints_test"
  private val col = "col"
  private def query(numBins: Int) = sql(s"SELECT histogram_endpoints($col, $numBins) FROM $table")

  test("null handling") {
    withTempView(table) {
      val nullString: String = null
      // Empty input row
      Seq(nullString).toDF(col).createOrReplaceTempView(table)
      checkAnswer(query(numBins = 2), Row(Map.empty))

      // Add some non-empty row
      Seq(nullString, "a", nullString).toDF(col).createOrReplaceTempView(table)
      checkAnswer(query(numBins = 2), Row(Map(("a", 1))))
    }
  }

  test("string type - returns empty result when ndv exceeds numBins") {
    withTempView(table) {
      spark.sparkContext.makeRDD(Seq("a", "dddd", "ccc", "bb", "a", "a"), 2).toDF(col)
        .createOrReplaceTempView(table)
      checkAnswer(query(numBins = 4), Row(Map(("a", 3), ("bb", 1), ("ccc", 1), ("dddd", 1))))
      // One partial exceeds numBins during update()
      checkAnswer(query(numBins = 2), Row(Map.empty))
      // Exceeding numBins during merge()
      checkAnswer(query(numBins = 3), Row(Map.empty))
    }
  }

  test("numeric type - returns percentiles when ndv exceeds numBins") {
    withTempView(table) {
      spark.sparkContext.makeRDD(Seq(1, 3, 2, 4, 2, 4), 2).toDF(col)
        .createOrReplaceTempView(table)
      checkAnswer(query(numBins = 4), Row(Map((1.0D, 1), (2.0D, 2), (3.0D, 1), (4.0D, 2))))
      // One partial exceeds numBins during update()
      // Returns percentiles 0.0, 0.5, 1.0
      checkAnswer(query(numBins = 2), Row(Map((1.0D, 0), (2.0D, 0), (4.0D, 0))))
      // Exceeding numBins during merge()
      // Returns percentiles 0.0, 1/3, 2/3, 1.0
      checkAnswer(query(numBins = 3), Row(Map((1.0D, 0), (2.0D, 0), (3.0D, 0), (4.0D, 0))))
    }
  }

  test("multiple columns of different types") {
    def queryMultiColumns(numBins: Int): DataFrame = {
      sql(
        s"""
           |SELECT
           |  histogram_endpoints(c1, $numBins),
           |  histogram_endpoints(c2, $numBins),
           |  histogram_endpoints(c3, $numBins),
           |  histogram_endpoints(c4, $numBins),
           |  histogram_endpoints(c5, $numBins),
           |  histogram_endpoints(c6, $numBins),
           |  histogram_endpoints(c7, $numBins),
           |  histogram_endpoints(c8, $numBins),
           |  histogram_endpoints(c9, $numBins),
           |  histogram_endpoints(c10, $numBins)
           |FROM $table
	        """.stripMargin)
    }

    val ints = ArrayBuffer(7, 5, 3, 1)
    val doubles = ArrayBuffer(1.0D, 3.0D, 5.0D, 7.0D)
    val dates = ArrayBuffer("1970-01-01", "1970-02-02", "1970-03-03", "1970-04-04")
    val timestamps = ArrayBuffer("1970-01-01 00:00:00", "1970-01-01 00:00:05",
      "1970-01-01 00:00:10", "1970-01-01 00:00:15")
    val strings = ArrayBuffer("a", "bb", "ccc", "dddd")

    val data = ints.indices.map { i =>
      (ints(i).toByte,
        ints(i).toShort,
        ints(i),
        ints(i).toLong,
        doubles(i).toFloat,
        doubles(i),
        Decimal(doubles(i)),
        Date.valueOf(dates(i)),
        Timestamp.valueOf(timestamps(i)),
        strings(i))
    }
    withTempView(table) {
      data.toDF("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10")
        .createOrReplaceTempView(table)
      // check answers of hashmaps
      val expected1 = ArrayBuffer.empty[Map[Any, Long]]
      val frequency = Seq(1L, 1L, 1L, 1L)
      for (i <- 1 to 7) {
        expected1 += doubles.zip(frequency).toMap
      }
      expected1 += dates.map { d =>
        DateTimeUtils.fromJavaDate(Date.valueOf(d)).toDouble
      }.zip(frequency).toMap
      expected1 += timestamps.map { t =>
        DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(t)).toDouble
      }.zip(frequency).toMap
      expected1 += strings.zip(frequency).toMap
      checkAnswer(queryMultiColumns(ints.length), Row(expected1: _*))

      // check answers of percentiles: 0.0, 0.5, 1.0
      val expected2 = ArrayBuffer.empty[Map[Any, Long]]
      val padding = Seq(0L, 0L, 0L)
      for (i <- 1 to 7) {
        expected2 += (doubles - doubles(2)).zip(padding).toMap
      }
      expected2 += (dates - dates(2)).map { d =>
        DateTimeUtils.fromJavaDate(Date.valueOf(d)).toDouble
      }.zip(padding).toMap
      expected2 += (timestamps - timestamps(2)).map { t =>
        DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(t)).toDouble
      }.zip(padding).toMap
      // column of string type doesn't have percentiles
      expected2 += Map.empty
      checkAnswer(queryMultiColumns(ints.length / 2), Row(expected2: _*))
    }
  }
}
