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
import java.time.LocalDateTime

import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.{DEFAULT_PERCENTILE_ACCURACY, PercentileDigest}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for approximate percentile aggregate function.
 */
class HistogramNumericQuerySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val table = "percentile_test"

  test("histogram_numeric, single percentile value") {
    withTempView(table) {
      (1 to 50).toDF("col").createOrReplaceTempView(table)
      sql("SELECT histogram_numeric(col, 2) FROM VALUES (0), (1), (2), (10) AS tab(col)")
        .show(2, false)
      checkAnswer(
        spark.sql(
          s"""
             |SELECT
             |  histogram_numeric(col, 2)
             |FROM $table
           """.stripMargin),
        // [{"x":17.58333333333333,"y":36.0},{"x":43.5,"y":14.0}]
        // [{"x":5.75,"y":12.0},{"x":20.0,"y":17.0},
        //    {"x":37.06250000000001,"y":16.0},{"x":48.0,"y":5.0}]
        // [{"x":3.875,"y":8.0},{"x":12.5,"y":10.0},{"x":20.5,"y":6.0},
        //    {"x":26.999999999999996,"y":7.0},{"x":34.875,"y":8.0},
        //    {"x":44.99999999999999,"y":11.0}]
        Row(Seq(Row(12.5, 24.0), Row(37.499999999999986, 26.0)))
      )
    }
  }

  test("histogram_numeric, different column types") {
    withTempView(table) {
      val intSeq = 1 to 50
      val data: Seq[(java.math.BigDecimal, Date, Timestamp, LocalDateTime)] = intSeq.map { i =>
        (new java.math.BigDecimal(i), DateTimeUtils.toJavaDate(i),
          DateTimeUtils.toJavaTimestamp(i), DateTimeUtils.microsToLocalDateTime(i))
      }
      data.toDF("cdecimal", "cdate", "ctimestamp", "ctimestampntz").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  histogram_numeric(cdecimal, 5),
             |  histogram_numeric(cdate, 5),
             |  histogram_numeric(ctimestamp, 5),
             |  histogram_numeric(ctimestampntz, 5)
             |FROM $table
           """.stripMargin),
        Row(
          Seq("250.000000000000000000", "500.000000000000000000", "750.000000000000000000")
              .map(i => new java.math.BigDecimal(i)),
          Seq(250, 500, 750).map(DateTimeUtils.toJavaDate),
          Seq(250, 500, 750).map(i => DateTimeUtils.toJavaTimestamp(i.toLong)),
          Seq(250, 500, 750).map(i => DateTimeUtils.microsToLocalDateTime(i.toLong)))
      )
    }
  }

  test("histogram_numeric, multiple records with the minimum value in a partition") {
    withTempView(table) {
      spark.sparkContext.makeRDD(Seq(1, 1, 2, 1, 1, 3, 1, 1, 4, 1, 1, 5), 4).toDF("col")
        .createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT histogram_numeric(col, 3) FROM $table"),
        Row(Seq(1.0D))
      )
    }
  }

  test("histogram_numeric, supports constant folding for parameter nBins") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT histogram_numeric(col, 2 + 3) FROM $table"),
        Row(Seq(500))
      )
    }
  }

  test("histogram_numeric, aggregation on empty input table, no group by") {
    withTempView(table) {
      Seq.empty[Int].toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT sum(col), histogram_numeric(col, 3) FROM $table"),
        Row(null, null)
      )
    }
  }

  test("percentile_approx(), aggregation on empty input table, with group by") {
    withTempView(table) {
      Seq.empty[Int].toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT sum(col), percentile_approx(col, 0.5) FROM $table GROUP BY col"),
        Seq.empty[Row]
      )
    }
  }

  test("histogram_numeric(null), aggregation with group by") {
    withTempView(table) {
      (1 to 1000).map(x => (x % 3, x)).toDF("key", "value").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  key,
             |  histogram_numeric(null, 2)
             |FROM $table
             |GROUP BY key
           """.stripMargin),
        Seq(
          Row(0, null),
          Row(1, null),
          Row(2, null))
      )
    }
  }

  test("percentile_approx(null), aggregation without group by") {
    withTempView(table) {
      (1 to 1000).map(x => (x % 3, x)).toDF("key", "value").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
              |  histogram_numeric(null, 3),
              |  sum(null),
              |  histogram_numeric(null, 3)
              |FROM $table
           """.stripMargin),
         Row(null, null, null)
      )
    }
  }

  test("histogram_numeric(col, ...), input rows contains null, with out group by") {
    withTempView(table) {
      (1 to 1000).map(Integer.valueOf(_)).flatMap(Seq(null: Integer, _)).toDF("col")
        .createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
              |  histogram_numeric(col, 3),
              |  sum(null),
              |  histogram_numeric(col, 3)
              |FROM $table
           """.stripMargin),
        Row(500D, null, 500D))
    }
  }

  test("histogram_numeric(col, ...), input rows contains null, with group by") {
    withTempView(table) {
      val rand = new java.util.Random()
      (1 to 1000)
        .map(Integer.valueOf(_))
        .map(v => (Integer.valueOf(v % 2), v))
        // Add some nulls
        .flatMap(Seq(_, (null: Integer, null: Integer)))
        .toDF("key", "value").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
              |  histogram_numeric(value, 3),
              |  sum(value),
              |  histogram_numeric(value, 3)
              |FROM $table
              |GROUP BY key
           """.stripMargin),
        Seq(
          Row(499.0D, 250000, 499.0D),
          Row(500.0D, 250500, 500.0D),
          Row(null, null, null))
      )
    }
  }

  test("histogram_numeric(col, ...) works in window function") {
    withTempView(table) {
      val data = (1 to 10).map(v => (v % 2, v))
      data.toDF("key", "value").createOrReplaceTempView(table)

      val query = spark.sql(
        s"""
           |SElECT histogram_numeric(value, 3)
           |OVER
           |  (PARTITION BY key ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
           |    AS percentile
           |FROM $table
           """.stripMargin)

      val expected = data.groupBy(_._1).toSeq.flatMap { group =>
        val (key, values) = group
        val sortedValues = values.map(_._2).sorted

        var outputRows = Seq.empty[Row]
        var i = 0

        val percentile = new PercentileDigest(1.0 / DEFAULT_PERCENTILE_ACCURACY)
        sortedValues.foreach { value =>
          percentile.add(value)
          outputRows :+= Row(percentile.getPercentiles(Array(0.5D)).head)
        }
        outputRows
      }

      checkAnswer(query, expected)
    }
  }
}
