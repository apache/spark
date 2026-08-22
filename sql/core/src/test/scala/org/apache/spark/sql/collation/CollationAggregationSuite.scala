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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CollationAggregationSuite
  extends SharedSparkSession
  with AdaptiveSparkPlanHelper {

  test("hash aggregate on collated grouping key with RewriteCollationAggregate") {
    val tblName = "grp_by_tbl"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, c2 INT) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('hello', 1), ('HELLO', 2), ('HeLlO', 3)")

      val df = sql(s"SELECT c1, COUNT(*), SUM(c2) FROM $tblName GROUP BY c1")
      val executedPlan = df.queryExecution.executedPlan

      assert(collectFirst(executedPlan) {
        case _: ObjectHashAggregateExec => true
      }.nonEmpty)
      assert(collectFirst(executedPlan) {
        case _: SortAggregateExec => true
      }.isEmpty)

      val res = df.collect()
      assert(res.length == 1)
      assert(res(0).getString(0).toLowerCase() == "hello")
      assert(res(0).getLong(1) == 3L)
      assert(res(0).getLong(2) == 6L)
    }
  }

  test("disable hash aggregate on collated column via SQLConf") {
    val tblName = "grp_by_disabled_tbl"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, c2 INT) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('hello', 1), ('HELLO', 2), ('HeLlO', 3)")

      withSQLConf(SQLConf.COLLATION_HASH_AGGREGATION_ENABLED.key -> "false") {
        val df = sql(s"SELECT c1, COUNT(*) FROM $tblName GROUP BY c1")
        val executedPlan = df.queryExecution.executedPlan

        assert(collectFirst(executedPlan) {
          case _: SortAggregateExec => true
        }.nonEmpty)
        assert(collectFirst(executedPlan) {
          case _: ObjectHashAggregateExec => true
          case _: HashAggregateExec => true
        }.isEmpty)

        val res = df.collect()
        assert(res.length == 1)
        assert(res(0).getString(0).toLowerCase() == "hello")
        assert(res(0).getLong(1) == 3L)
      }
    }
  }

  test("imperative aggregate fn uses objectHashAggregate when group by collated column") {
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

          if (useObjHashAgg) {
            assert(collectFirst(executedPlan) {
              case _: ObjectHashAggregateExec => true
            }.nonEmpty)
          } else {
            assert(collectFirst(executedPlan) {
              case _: SortAggregateExec => true
            }.nonEmpty)
          }

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
