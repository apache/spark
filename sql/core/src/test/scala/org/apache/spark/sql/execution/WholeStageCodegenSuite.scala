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

package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.execution.aggregate.TungstenAggregate
import org.apache.spark.sql.execution.joins.BroadcastHashJoin
import org.apache.spark.sql.functions.{avg, broadcast, col, max}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class WholeStageCodegenSuite extends SparkPlanTest with SharedSQLContext {

  test("range/filter should be combined") {
    val df = sqlContext.range(10).filter("id = 1").selectExpr("id + 1")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[WholeStageCodegen]).isDefined)

    checkThatPlansAgree(
      sqlContext.range(100),
      (p: SparkPlan) =>
        WholeStageCodegen(Filter('a == 1, InputAdapter(p)), Seq()),
      (p: SparkPlan) => Filter('a == 1, p),
      sortAnswers = false
    )
  }

  test("Aggregate should be included in WholeStageCodegen") {
    val df = sqlContext.range(10).groupBy().agg(max(col("id")), avg(col("id")))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegen] &&
        p.asInstanceOf[WholeStageCodegen].plan.isInstanceOf[TungstenAggregate]).isDefined)
    assert(df.collect() === Array(Row(9, 4.5)))
  }

  test("Aggregate with grouping keys should be included in WholeStageCodegen") {
    val df = sqlContext.range(3).groupBy("id").count().orderBy("id")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegen] &&
        p.asInstanceOf[WholeStageCodegen].plan.isInstanceOf[TungstenAggregate]).isDefined)
    assert(df.collect() === Array(Row(0, 1), Row(1, 1), Row(2, 1)))
  }

  test("BroadcastHashJoin should be included in WholeStageCodegen") {
    val rdd = sqlContext.sparkContext.makeRDD(Seq(Row(1, "1"), Row(1, "1"), Row(2, "2")))
    val schema = new StructType().add("k", IntegerType).add("v", StringType)
    val smallDF = sqlContext.createDataFrame(rdd, schema)
    val df = sqlContext.range(10).join(broadcast(smallDF), col("k") === col("id"))
    assert(df.queryExecution.executedPlan.find(p =>
      p.isInstanceOf[WholeStageCodegen] &&
        p.asInstanceOf[WholeStageCodegen].plan.isInstanceOf[BroadcastHashJoin]).isDefined)
    assert(df.collect() === Array(Row(1, 1, "1"), Row(1, 1, "1"), Row(2, 2, "2")))
  }
}
