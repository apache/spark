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

package org.apache.spark.sql.collation

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.test.SharedSparkSession

class CollationAggregationSuite
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  test("group by collated column doesn't work with obj hash aggregate") {
    val tblName = "grp_by_tbl"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, c2 INT) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('hello', 1), ('HELLO', 2), ('HeLlO', 3)")

      // Result is correct without forcing object hash aggregate.
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $tblName GROUP BY c1"),
        Seq(Row(3)))

      withSQLConf("spark.sql.test.forceApplyObjectHashAggregate" -> true.toString) {
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $tblName GROUP BY c1"),
          Seq(Row(1), Row(1), Row(1)))

        checkAnswer(
          sql(s"SELECT COLLECT_LIST(c2) AS c3 FROM $tblName GROUP BY c1 ORDER BY c3"),
          Seq(Row(Seq(1)), Row(Seq(2)), Row(Seq(3))))
      }
    }
  }

  test("imperative aggregate fn does not use objectHashAggregate when group by collated column") {
    val tblName = "imp_agg"
    Seq(true, false).foreach { useObjHashAgg =>
      withTable(tblName) {
        withSQLConf("spark.sql.execution.useObjectHashAggregateExec" -> useObjHashAgg.toString) {
          sql(
            s"""
               |CREATE TABLE $tblName (
               |  c1 STRING COLLATE UTF8_LCASE,
               |  c2 INT
               |) USING PARQUET
               |""".stripMargin)
          sql(s"INSERT INTO $tblName VALUES ('HELLO', 1), ('hello', 2), ('HeLlO', 3)")

          val df = sql(s"SELECT COLLECT_LIST(c2) as list FROM $tblName GROUP BY c1")
          val executedPlan = df.queryExecution.executedPlan

          // Plan should not have any hash aggregate nodes.
          collectFirst(executedPlan) {
            case _: ObjectHashAggregateExec => fail("ObjectHashAggregateExec should not be used.")
            case _: HashAggregateExec => fail("HashAggregateExec should not be used.")
          }

          // Plan should have a [[SortAggregateExec]] node.
          assert(collectFirst(executedPlan) {
            case _: SortAggregateExec => true
          }.nonEmpty)

          checkAnswer(
            // Sort the values to get deterministic output.
            df.selectExpr("array_sort(list)"),
            Seq(Row(Seq(1, 2, 3)))
          )
        }
      }
    }
  }
}
