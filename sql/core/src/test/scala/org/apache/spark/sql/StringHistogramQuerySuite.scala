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

import org.apache.spark.sql.test.SharedSQLContext


class StringHistogramQuerySuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private val table = "string_histogram_test"
  private val col = "col"
  private def query(numBins: Int) = s"SELECT string_histogram($col, $numBins) FROM $table"

  test("null handling") {
    withTempView(table) {
      val nullString: String = null
      // Empty input row
      Seq(nullString).toDF(col).createOrReplaceTempView(table)
      checkAnswer(sql(query(numBins = 2)), Row(Map.empty))

      // Add some non-empty row
      Seq(nullString, "a", nullString).toDF(col).createOrReplaceTempView(table)
      checkAnswer(sql(query(numBins = 2)), Row(Map(("a", 1))))
    }
  }

  test("returns empty result when ndv exceeds numBins") {
    withTempView(table) {
      spark.sparkContext.makeRDD(Seq("a", "bb", "ccc", "dddd", "a"), 2).toDF(col)
        .createOrReplaceTempView(table)
      // One partial exceeds numBins during update()
      checkAnswer(sql(query(numBins = 2)), Row(Map.empty))
      // Exceeding numBins during merge()
      checkAnswer(sql(query(numBins = 3)), Row(Map.empty))
    }
  }

  test("multiple columns") {
    withTempView(table) {
      Seq(("a", null, "c"),
        (null, "bb", "cc"),
        ("a", "bbb", "ccc"),
        ("aaaa", "bb", "c")
      ).toDF("c1", "c2", "c3").createOrReplaceTempView(table)
      checkAnswer(
        sql(
          s"""
             |SELECT
             |  string_histogram(c1, 2),
             |  string_histogram(c2, 1),
             |  string_histogram(c3, 3)
             |FROM $table
           """.stripMargin),
        Row(Map(("a", 2), ("aaaa", 1)), Map.empty, Map(("c", 2), ("cc", 1), ("ccc", 1)))
      )
    }
  }
}
