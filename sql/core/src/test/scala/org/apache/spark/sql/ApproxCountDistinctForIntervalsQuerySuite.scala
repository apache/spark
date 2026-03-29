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

import java.time.{Duration, Period}

import org.apache.spark.sql.catalyst.expressions.{Alias, CreateArray, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproxCountDistinctForIntervals
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.test.SharedSparkSession

class ApproxCountDistinctForIntervalsQuerySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  // ApproxCountDistinctForIntervals is used in equi-height histogram generation. An equi-height
  // histogram usually contains hundreds of buckets. So we need to test
  // ApproxCountDistinctForIntervals with large number of endpoints
  // (the number of endpoints == the number of buckets + 1).
  test("test ApproxCountDistinctForIntervals with large number of endpoints") {
    val table = "approx_count_distinct_for_intervals_tbl"
    withTable(table) {
      (1 to 100000).toDF("col").createOrReplaceTempView(table)
      // percentiles of 0, 0.001, 0.002 ... 0.999, 1
      val endpoints = (0 to 1000).map(_ * 100000 / 1000)

      // Since approx_count_distinct_for_intervals is not a public function, here we do
      // the computation by constructing logical plan.
      val relation = spark.table(table).logicalPlan
      val attr = relation.output.find(_.name == "col").get
      val aggFunc = ApproxCountDistinctForIntervals(attr, CreateArray(endpoints.map(Literal(_))))
      val aggExpr = aggFunc.toAggregateExpression()
      val namedExpr = Alias(aggExpr, aggExpr.toString)()
      val ndvsRow = new QueryExecution(spark, Aggregate(Nil, Seq(namedExpr), relation))
        .executedPlan.executeTake(1).head
      val ndvArray = ndvsRow.getArray(0).toLongArray()
      assert(endpoints.length == ndvArray.length + 1)

      // Each bucket has 100 distinct values.
      val expectedNdv = 100
      for (i <- ndvArray.indices) {
        val ndv = ndvArray(i)
        val error = math.abs((ndv / expectedNdv.toDouble) - 1.0d)
        assert(error <= aggFunc.relativeSD * 3.0d, "Error should be within 3 std. errors.")
      }
    }
  }

  test("SPARK-37138: Support Ansi Interval type in ApproxCountDistinctForIntervals") {
    val table = "approx_count_distinct_for_ansi_intervals_tbl"
    withTable(table) {
      Seq((Period.ofMonths(100), Duration.ofSeconds(100L)),
        (Period.ofMonths(200), Duration.ofSeconds(200L)),
        (Period.ofMonths(300), Duration.ofSeconds(300L)))
        .toDF("col1", "col2").createOrReplaceTempView(table)
      val endpoints = (0 to 5).map(_ / 10)

      val relation = spark.table(table).logicalPlan
      val ymAttr = relation.output.find(_.name == "col1").get
      val ymAggFunc =
        ApproxCountDistinctForIntervals(ymAttr, CreateArray(endpoints.map(Literal(_))))
      val ymAggExpr = ymAggFunc.toAggregateExpression()
      val ymNamedExpr = Alias(ymAggExpr, ymAggExpr.toString)()

      val dtAttr = relation.output.find(_.name == "col2").get
      val dtAggFunc =
        ApproxCountDistinctForIntervals(dtAttr, CreateArray(endpoints.map(Literal(_))))
      val dtAggExpr = dtAggFunc.toAggregateExpression()
      val dtNamedExpr = Alias(dtAggExpr, dtAggExpr.toString)()
      val result = classic.Dataset.ofRows(
        spark,
        Aggregate(Nil, Seq(ymNamedExpr, dtNamedExpr), relation))
      checkAnswer(result, Row(Array(1, 1, 1, 1, 1), Array(1, 1, 1, 1, 1)))
    }
  }
}
