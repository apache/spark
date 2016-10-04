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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameCacheSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("range/filter should be combined with column codegen") {
    val df = sparkContext.parallelize(0 to 9, 1).map(i => i.toFloat).toDF().cache()
      .filter("value = 1").selectExpr("value + 1")
    assert(df.collect() === Array(Row(2.0)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].enableColumnCodeGen).isDefined)
  }

  test("filters should be combined with column codegen") {
    val df = sparkContext.parallelize(0 to 9, 1).map(i => i.toFloat).toDF().cache()
      .filter("value % 2.0 == 0").filter("value % 3.0 == 0")
    assert(df.collect() === Array(Row(0), Row(6.0)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].enableColumnCodeGen).isDefined)
  }

  test("filter with null should be included in WholeStageCodegen with column codegen") {
    val toFloat = udf[java.lang.Float, String] { s => if (s == "2") null else s.toFloat }
    val df0 = sparkContext.parallelize(0 to 4, 1).map(i => i.toString).toDF()
    val df = df0.withColumn("i", toFloat(df0("value"))).select("i").toDF().cache()
      .filter("i % 2.0 == 0")
    assert(df.collect() === Array(Row(0), Row(4.0)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].enableColumnCodeGen).isDefined)
  }

  test("Aggregate should be included in WholeStageCodegen with column codegen") {
    val df = sparkContext.parallelize(0 to 9, 1).map(i => i.toFloat).toDF().cache()
      .groupBy().agg(max(col("value")), avg(col("value")))
    assert(df.collect() === Array(Row(9, 4.5)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].enableColumnCodeGen &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[HashAggregateExec]).isDefined)
  }

  test("Aggregate with grouping keys should be included in WholeStageCodegen with column codegen") {
    val df = sparkContext.parallelize(0 to 2, 1).map(i => i.toFloat).toDF().cache()
      .groupBy("value").count().orderBy("value")
    assert(df.collect() === Array(Row(0.0, 1), Row(1.0, 1), Row(2.0, 1)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].enableColumnCodeGen &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[HashAggregateExec]).isDefined)
  }

  test("Aggregate with columns should be included in WholeStageCodegen with column codegen") {
    val df = sparkContext.parallelize(0 to 10, 1).map(i => (i, (i * 2).toDouble)).toDF("i", "d")
      .cache().agg(sum("d"))
    assert(df.collect() === Array(Row(110.0)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].enableColumnCodeGen &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[HashAggregateExec]).isDefined)
  }

  test("Sort should be included in WholeStageCodegen without column codegen") {
    val df = sparkContext.parallelize(Seq(3.toFloat, 2.toFloat, 1.toFloat), 1).toDF()
      .sort(col("value"))
    val plan = df.queryExecution.executedPlan
    assert(df.collect() === Array(Row(1.0), Row(2.0), Row(3.0)))
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        !p.asInstanceOf[WholeStageCodegenExec].enableColumnCodeGen &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[SortExec]).isDefined)
  }
}
