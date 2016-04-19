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
import org.apache.spark.sql.execution.aggregate.TungstenAggregate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameCacheSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("range/filter should be combined with column codegen") {
    val df = sparkContext.parallelize(0 to 9, 1).toDF().cache()
      .filter("value = 1").selectExpr("value + 1")
    assert(df.collect() === Array(Row(2)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegen] &&
        p.asInstanceOf[WholeStageCodegen].enableColumnCodeGen).isDefined)
  }

  test("filters should be combined with column codegen") {
    val df = sparkContext.parallelize(0 to 9, 1).toDF().cache()
      .filter("value % 2 == 0").filter("value % 3 == 0")
    assert(df.collect() === Array(Row(0), Row(6)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegen] &&
        p.asInstanceOf[WholeStageCodegen].enableColumnCodeGen).isDefined)
  }

  test("filter with null should be included in WholeStageCodegen with column codegen") {
    val toInt = udf[java.lang.Integer, String] { s => if (s == "2") null else s.toInt }
    val df0 = sparkContext.parallelize(0 to 4, 1).map(i => i.toString).toDF()
    val df = df0.withColumn("i", toInt(df0("value"))).select("i").toDF().cache()
      .filter("i % 2 == 0")
    assert(df.collect() === Array(Row(0), Row(4)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegen] &&
        p.asInstanceOf[WholeStageCodegen].enableColumnCodeGen).isDefined)
  }

  test("Aggregate should be included in WholeStageCodegen with column codegen") {
    val df = sparkContext.parallelize(0 to 9, 1).toDF().cache()
      .groupBy().agg(max(col("value")), avg(col("value")))
    assert(df.collect() === Array(Row(9, 4.5)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegen] &&
        p.asInstanceOf[WholeStageCodegen].enableColumnCodeGen &&
        p.asInstanceOf[WholeStageCodegen].child.isInstanceOf[TungstenAggregate]).isDefined)
  }

  test("Aggregate with grouping keys should be included in WholeStageCodegen with column codegen") {
    val df = sparkContext.parallelize(0 to 2, 1).toDF().cache()
      .groupBy("value").count().orderBy("value")
    assert(df.collect() === Array(Row(0, 1), Row(1, 1), Row(2, 1)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegen] &&
        p.asInstanceOf[WholeStageCodegen].enableColumnCodeGen &&
        p.asInstanceOf[WholeStageCodegen].child.isInstanceOf[TungstenAggregate]).isDefined)
  }

  test("Aggregate with columns should be included in WholeStageCodegen with column codegen") {
    val df = sparkContext.parallelize(0 to 10, 1).map(i => (i, (i * 2).toDouble)).toDF("i", "d")
      .cache().agg(sum("d"))
    assert(df.collect() === Array(Row(110.0)))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegen] &&
        p.asInstanceOf[WholeStageCodegen].enableColumnCodeGen &&
        p.asInstanceOf[WholeStageCodegen].child.isInstanceOf[TungstenAggregate]).isDefined)
  }

  test("Sort should be included in WholeStageCodegen without column codegen") {
    val df = sqlContext.range(3, 0, -1).toDF().sort(col("id"))
    val plan = df.queryExecution.executedPlan
    assert(df.collect() === Array(Row(1), Row(2), Row(3)))
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegen] &&
        !p.asInstanceOf[WholeStageCodegen].enableColumnCodeGen &&
        p.asInstanceOf[WholeStageCodegen].child.isInstanceOf[Sort]).isDefined)
  }
}
