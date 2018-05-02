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

import org.apache.spark.sql.catalyst.expressions.{Alias, CreateArray, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproxCountDistinctForIntervals
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.test.SharedSQLContext

class ApproxCountDistinctForIntervalsQuerySuite extends QueryTest with SharedSQLContext {
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
}
