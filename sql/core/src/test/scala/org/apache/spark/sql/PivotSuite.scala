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

class PivotSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  setupTestData()

  test("pivot courses") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM (
          |  SELECT year, course, earnings FROM courseSales
          |)
          |PIVOT (
          |  sum(earnings)
          |  FOR course IN ('dotNET', 'Java')
          |)
        """.stripMargin),
      Row(2012, 15000.0, 20000.0) :: Row(2013, 48000.0, 30000.0) :: Nil)
  }

  test("pivot years with no subquery") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM courseSales
          |PIVOT (
          |  sum(earnings)
          |  FOR year IN (2012, 2013)
          |)
        """.stripMargin),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil)
  }

  test("pivot courses with multiple aggregations") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM (
          |  SELECT year, course, earnings FROM courseSales
          |)
          |PIVOT (
          |  sum(earnings), avg(earnings)
          |  FOR course IN ('dotNET', 'Java')
          |)
        """.stripMargin),
      Row(2012, 15000.0, 7500.0, 20000.0, 20000.0) ::
        Row(2013, 48000.0, 48000.0, 30000.0, 30000.0) :: Nil)
  }

  test("pivot with no group by column") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM (
          |  SELECT course, earnings FROM courseSales
          |)
          |PIVOT (
          |  sum(earnings)
          |  FOR course IN ('dotNET', 'Java')
          |)
        """.stripMargin),
      Row(63000.0, 50000.0) :: Nil)
  }

  test("pivot with no group by column and with multiple aggregations on different columns") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM (
          |  SELECT year, course, earnings FROM courseSales
          |)
          |PIVOT (
          |  sum(earnings), min(year)
          |  FOR course IN ('dotNET', 'Java')
          |)
        """.stripMargin),
      Row(63000.0, 2012, 50000.0, 2012) :: Nil)
  }

  test("pivot on join query with multiple group by columns") {
    withTempView("years") {
      Seq((2012, 1), (2013, 2)).toDF("y", "s").createOrReplaceTempView("years")

      checkAnswer(
        sql(
          """
            |SELECT * FROM (
            |  SELECT course, year, earnings, s
            |  FROM courseSales
            |  JOIN years ON year = y
            |)
            |PIVOT (
            |  sum(earnings)
            |  FOR s IN (1, 2)
            |)
          """.stripMargin),
        Row("Java", 2012, 20000.0, null) ::
          Row("Java", 2013, null, 30000.0) ::
          Row("dotNET", 2012, 15000.0, null) ::
          Row("dotNET", 2013, null, 48000.0) :: Nil)
    }
  }

  test("pivot on join query with multiple aggregations on different columns") {
    withTempView("years") {
      Seq((2012, 1), (2013, 2)).toDF("y", "s").createOrReplaceTempView("years")

      checkAnswer(
        sql(
          """
            |SELECT * FROM (
            |  SELECT course, year, earnings, s
            |  FROM courseSales
            |  JOIN years ON year = y
            |)
            |PIVOT (
            |  sum(earnings), min(s)
            |  FOR course IN ('dotNET', 'Java')
            |)
          """.stripMargin),
        Row(2012, 15000.0, 1, 20000.0, 1) ::
          Row(2013, 48000.0, 2, 30000.0, 2) :: Nil)
    }
  }

  test("pivot on join query with multiple columns in one aggregation") {
    withTempView("years") {
      Seq((2012, 1), (2013, 2)).toDF("y", "s").createOrReplaceTempView("years")

      checkAnswer(
        sql(
          """
            |SELECT * FROM (
            |  SELECT course, year, earnings, s
            |  FROM courseSales
            |  JOIN years ON year = y
            |)
            |PIVOT (
            |  sum(earnings * s)
            |  FOR course IN ('dotNET', 'Java')
            |)
          """.stripMargin),
        Row(2012, 15000.0, 20000.0) :: Row(2013, 96000.0, 60000.0) :: Nil)
    }
  }

  test("pivot with aliases and projection") {
    checkAnswer(
      sql(
        """
          |SELECT 2012_s, 2013_s, 2012_a, 2013_a, c FROM (
          |  SELECT year y, course c, earnings e FROM courseSales
          |)
          |PIVOT (
          |  sum(e) s, avg(e) a
          |  FOR y IN (2012, 2013)
          |)
        """.stripMargin),
      Row(15000.0, 48000.0, 7500.0, 48000.0, "dotNET") ::
        Row(20000.0, 30000.0, 20000.0, 30000.0, "Java") :: Nil)
  }

  test("pivot years with non-aggregate function") {
    val error = intercept[AnalysisException] {
      sql(
        """
          |SELECT * FROM courseSales
          |PIVOT (
          |  abs(earnings)
          |  FOR year IN (2012, 2013)
          |)
        """.stripMargin)
    }
    assert(error.message contains "Aggregate expression required for pivot")
  }
}
