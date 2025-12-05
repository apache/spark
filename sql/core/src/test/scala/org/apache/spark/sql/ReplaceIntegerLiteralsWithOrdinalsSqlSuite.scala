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

import org.apache.spark.sql.catalyst.analysis.UnresolvedOrdinal
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ReplaceIntegerLiteralsWithOrdinalsSqlSuite extends QueryTest with SharedSparkSession {

  test("Group by ordinal - SQL") {
    val correctSqlText = "SELECT col1, max(col2) FROM VALUES(1,2),(1,3),(2,4) GROUP BY 1"
    val groupByPosOutOfRangeSqlText =
      "SELECT col1, max(col2) FROM VALUES(1,2),(1,3),(2,4) GROUP BY -1"

    withSQLConf(SQLConf.GROUP_BY_ORDINAL.key -> "true") {
      val query = sql(correctSqlText)
      val parsedPlan = query.queryExecution.logical
      val analyzedPlan = query.queryExecution.analyzed

      assert(parsedPlan.expressions.collect {
        case ordinal @ UnresolvedOrdinal(1) => ordinal
      }.nonEmpty)

      assert(analyzedPlan.expressions.collect {
        case ordinal @ UnresolvedOrdinal(1) => ordinal
      }.isEmpty)

      checkAnswer(query, Row(1, 3) :: Row(2, 4) :: Nil)

      checkError(
        exception = intercept[AnalysisException](sql(groupByPosOutOfRangeSqlText)),
        condition = "GROUP_BY_POS_OUT_OF_RANGE",
        parameters = Map("index" -> "-1", "size" -> "2"),
        queryContext = Array(ExpectedContext(fragment = "-1", start = 61, stop = 62))
      )
    }

    withSQLConf(SQLConf.GROUP_BY_ORDINAL.key -> "false") {
      val parsedPlan = spark.sessionState.sqlParser.parsePlan(correctSqlText)

      assert(parsedPlan.expressions.collect {
        case ordinal @ UnresolvedOrdinal(1) => ordinal
      }.isEmpty)

      checkError(
        exception = intercept[AnalysisException](sql(groupByPosOutOfRangeSqlText)),
        condition = "MISSING_AGGREGATION",
        parameters = Map("expression" -> "\"col1\"", "expressionAnyValue" -> "\"any_value(col1)\"")
      )
    }
  }

  test("Order by ordinal - SQL") {
    val correctSqlText = "SELECT col1 FROM VALUES(2,1),(1,2) ORDER BY 1"
    val orderByPosOutOfRangeSqlText = "SELECT col1 FROM VALUES(2,1),(1,2) ORDER BY -1"

    withSQLConf(SQLConf.ORDER_BY_ORDINAL.key -> "true") {
      val query = sql(correctSqlText)
      val parsedPlan = query.queryExecution.logical
      val analyzedPlan = query.queryExecution.analyzed

      assert(parsedPlan.expressions.collect {
        case ordinal @ SortOrder(UnresolvedOrdinal(1), _, _, _) => ordinal
      }.nonEmpty)

      assert(analyzedPlan.expressions.collect {
        case ordinal @ SortOrder(UnresolvedOrdinal(1), _, _, _) => ordinal
      }.isEmpty)

      checkAnswer(query, Row(1) :: Row(2) :: Nil)

      checkError(
        exception = intercept[AnalysisException](sql(orderByPosOutOfRangeSqlText)),
        condition = "ORDER_BY_POS_OUT_OF_RANGE",
        parameters = Map("index" -> "-1", "size" -> "1"),
        queryContext = Array(ExpectedContext(fragment = "-1", start = 44, stop = 45))
      )
    }

    withSQLConf(SQLConf.ORDER_BY_ORDINAL.key -> "false") {
      val query = sql(correctSqlText)
      val parsedPlan = query.queryExecution.logical
      val analyzedPlan = query.queryExecution.analyzed

      assert(parsedPlan.expressions.collect {
        case ordinal @ SortOrder(UnresolvedOrdinal(1), _, _, _) => ordinal
      }.isEmpty)

      assert(analyzedPlan.expressions.collect {
        case ordinal @ SortOrder(UnresolvedOrdinal(1), _, _, _) => ordinal
      }.isEmpty)

      checkAnswer(query, Row(2) :: Row(1) :: Nil)

      checkAnswer(sql(orderByPosOutOfRangeSqlText), Row(2) :: Row(1) :: Nil)
    }
  }
}
