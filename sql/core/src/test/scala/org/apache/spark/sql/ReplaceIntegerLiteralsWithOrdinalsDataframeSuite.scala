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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ReplaceIntegerLiteralsWithOrdinalsDataframeSuite extends QueryTest with SharedSparkSession {

  test("Group by ordinal - Dataframe") {
    val query = "SELECT * FROM VALUES(1,2),(1,3),(2,4)"

    withSQLConf(SQLConf.GROUP_BY_ORDINAL.key -> "true") {
      val groupedDataset = sql(query).groupBy(lit(1))

      assert(groupedDataset.groupingExprs.collect {
        case ordinal @ UnresolvedOrdinal(1) => ordinal
      }.nonEmpty)

      checkError(
        exception = intercept[AnalysisException](sql(query).groupBy(lit(-1)).count()),
        condition = "GROUP_BY_POS_OUT_OF_RANGE",
        parameters = Map("index" -> "-1", "size" -> "2"),
        context = ExpectedContext(fragment = "lit", getCurrentClassCallSitePattern)
      )
    }

    withSQLConf(SQLConf.GROUP_BY_ORDINAL.key -> "false") {
      val groupedDataset = sql(query).groupBy(lit(1))

      assert(groupedDataset.groupingExprs.collect {
        case ordinal @ UnresolvedOrdinal(1) => ordinal
      }.isEmpty)
    }
  }

  test("Order/Sort by ordinal - Dataframe") {
    val sqlText = "SELECT * FROM VALUES(2,1),(1,2)"

    withSQLConf(SQLConf.ORDER_BY_ORDINAL.key -> "true") {
      val queries = Seq(
        sql(sqlText).orderBy(lit(1)),
        sql(sqlText).sort(lit(1))
      )

      for (query <- queries) {
        val unresolvedPlan = query.queryExecution.logical
        val resolvedPlan = query.queryExecution.analyzed

        assert(unresolvedPlan.expressions.collect {
          case ordinal @ SortOrder(UnresolvedOrdinal(1), _, _, _) => ordinal
        }.nonEmpty)

        assert(resolvedPlan.expressions.collect {
          case ordinal @ SortOrder(UnresolvedOrdinal(1), _, _, _) => ordinal
        }.isEmpty)

        checkAnswer(query, Row(1, 2) :: Row(2, 1) :: Nil)
      }

      checkError(
        exception = intercept[AnalysisException](sql(sqlText).orderBy(lit(-1))),
        condition = "ORDER_BY_POS_OUT_OF_RANGE",
        parameters = Map("index" -> "-1", "size" -> "2")
      )

      checkError(
        exception = intercept[AnalysisException](sql(sqlText).sort(lit(-1))),
        condition = "ORDER_BY_POS_OUT_OF_RANGE",
        parameters = Map("index" -> "-1", "size" -> "2")
      )
    }

    withSQLConf(SQLConf.ORDER_BY_ORDINAL.key -> "false") {
      val queries = Seq(
        sql(sqlText).orderBy(lit(1)),
        sql(sqlText).sort(lit(1))
      )

      for (query <- queries) {
        val unresolvedPlan = query.queryExecution.logical
        val resolvedPlan = query.queryExecution.analyzed

        assert(unresolvedPlan.expressions.collect {
          case ordinal @ SortOrder(UnresolvedOrdinal(1), _, _, _) => ordinal
        }.isEmpty)

        assert(resolvedPlan.expressions.collect {
          case ordinal @ SortOrder(UnresolvedOrdinal(1), _, _, _) => ordinal
        }.isEmpty)

        checkAnswer(query, Row(2, 1) :: Row(1, 2) :: Nil)
      }

      checkAnswer(sql(sqlText).orderBy(lit(-1)), Row(2, 1) :: Row(1, 2) :: Nil)

      checkAnswer(sql(sqlText).sort(lit(-1)), Row(2, 1) :: Row(1, 2) :: Nil)
    }
  }
}
