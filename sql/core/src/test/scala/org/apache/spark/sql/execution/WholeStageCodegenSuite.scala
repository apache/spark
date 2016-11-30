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

import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Add, Literal, Stack}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions.{avg, broadcast, col, max}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class WholeStageCodegenSuite extends SparkPlanTest with SharedSQLContext {

  test("range/filter should be combined") {
    val df = spark.range(10).filter("id = 1").selectExpr("id + 1")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined)
    assert(df.collect() === Array(Row(2)))
  }

  test("Aggregate should be included in WholeStageCodegen") {
    val df = spark.range(10).groupBy().agg(max(col("id")), avg(col("id")))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[HashAggregateExec]).isDefined)
    assert(df.collect() === Array(Row(9, 4.5)))
  }

  test("Aggregate with grouping keys should be included in WholeStageCodegen") {
    val df = spark.range(3).groupBy("id").count().orderBy("id")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[HashAggregateExec]).isDefined)
    assert(df.collect() === Array(Row(0, 1), Row(1, 1), Row(2, 1)))
  }

  test("BroadcastHashJoin should be included in WholeStageCodegen") {
    val rdd = spark.sparkContext.makeRDD(Seq(Row(1, "1"), Row(1, "1"), Row(2, "2")))
    val schema = new StructType().add("k", IntegerType).add("v", StringType)
    val smallDF = spark.createDataFrame(rdd, schema)
    val df = spark.range(10).join(broadcast(smallDF), col("k") === col("id"))
    assert(df.queryExecution.executedPlan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[BroadcastHashJoinExec]).isDefined)
    assert(df.collect() === Array(Row(1, 1, "1"), Row(1, 1, "1"), Row(2, 2, "2")))
  }

  test("Sort should be included in WholeStageCodegen") {
    val df = spark.range(3, 0, -1).toDF().sort(col("id"))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[SortExec]).isDefined)
    assert(df.collect() === Array(Row(1), Row(2), Row(3)))
  }

  test("MapElements should be included in WholeStageCodegen") {
    import testImplicits._

    val ds = spark.range(10).map(_.toString)
    val plan = ds.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
      p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[SerializeFromObjectExec]).isDefined)
    assert(ds.collect() === 0.until(10).map(_.toString).toArray)
  }

  test("typed filter should be included in WholeStageCodegen") {
    val ds = spark.range(10).filter(_ % 2 == 0)
    val plan = ds.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[FilterExec]).isDefined)
    assert(ds.collect() === Array(0, 2, 4, 6, 8))
  }

  test("back-to-back typed filter should be included in WholeStageCodegen") {
    val ds = spark.range(10).filter(_ % 2 == 0).filter(_ % 3 == 0)
    val plan = ds.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
      p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[FilterExec]).isDefined)
    assert(ds.collect() === Array(0, 6))
  }

  test("simple typed UDAF should be included in WholeStageCodegen") {
    import testImplicits._

    val ds = Seq(("a", 10), ("b", 1), ("b", 2), ("c", 1)).toDS()
      .groupByKey(_._1).agg(typed.sum(_._2))

    val plan = ds.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[HashAggregateExec]).isDefined)
    assert(ds.collect() === Array(("a", 10.0), ("b", 3.0), ("c", 1.0)))
  }

  test("generate should be included in WholeStageCodegen") {
    import org.apache.spark.sql.functions._
    val ds = spark.range(2).select(
      col("id"),
      explode(array(col("id") + 1, col("id") + 2)).as("value"))
    val plan = ds.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[GenerateExec]).isDefined)
    assert(ds.collect() === Array(Row(0, 1), Row(0, 2), Row(1, 2), Row(1, 3)))
  }

  test("large stack generator should not use WholeStageCodegen") {
    def createStackGenerator(rows: Int): SparkPlan = {
      val id = UnresolvedAttribute("id")
      val stack = Stack(Literal(rows) +: Seq.tabulate(rows)(i => Add(id, Literal(i))))
      spark.range(500).select(Column(stack)).queryExecution.executedPlan
    }
    val isCodeGenerated: SparkPlan => Boolean = {
      case WholeStageCodegenExec(_: GenerateExec) => true
      case _ => false
    }

    // Only 'stack' generators that produce 50 rows or less are code generated.
    assert(createStackGenerator(50).find(isCodeGenerated).isDefined)
    assert(createStackGenerator(100).find(isCodeGenerated).isEmpty)
  }
}
