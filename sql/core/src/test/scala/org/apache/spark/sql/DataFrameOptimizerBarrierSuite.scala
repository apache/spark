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

import org.apache.spark.sql.execution.{FilterExec, SortExec, UnionExec}
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameOptimizerBarrierSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("filter") {
    val df1 = Seq((1, "1"), (2, "2"), (3, "3")).toDF("key", "value")
    val dfBarrier = df1.where('key > 1).withOptimizerBarrier().where('value === "3")
    val dfNoBarrier = df1.where('key > 1).where('value === "3")
    // Filters not combined due to the barrier.
    assert(dfBarrier.queryExecution.sparkPlan.collect {
      case f: FilterExec => f }.size === 2)
    assert(dfNoBarrier.queryExecution.sparkPlan.collect {
      case f: FilterExec => f }.size === 1)
    checkAnswer(dfBarrier, dfNoBarrier)
  }

  test("sort over project") {
    val df1 = Seq((1, "1"), (2, "2"), (3, "3")).toDF("key", "value")
    val dfBarrier = df1.select('value).withOptimizerBarrier().orderBy('key.desc)
    val dfNoBarrier = df1.select('value).orderBy('key.desc)
    checkAnswer(dfBarrier, dfNoBarrier)
  }

  test("sort over aggregate") {
    val df1 = Seq((1, "1"), (2, "1"), (3, "3")).toDF("key", "value")
    val dfBarrier = df1.groupBy('value).min("key")
      .withOptimizerBarrier().orderBy($"min(key)".desc)
    val dfNoBarrier = df1.groupBy('value).min("key").orderBy($"min(key)".desc)
    checkAnswer(dfBarrier, dfNoBarrier)
  }

  test("filter over aggregate") {
    val df1 = Seq((1, "1"), (2, "1"), (3, "3"), (4, "3")).toDF("key", "value")
    val dfBarrier = df1.groupBy('value).min("key")
      .withOptimizerBarrier().where("max(key) < 4")
    val dfNoBarrier = df1.groupBy('value).min("key").where("max(key) < 4")
    checkAnswer(dfBarrier, dfNoBarrier)
  }

  test("self-join") {
    val df1 = Seq((1, "1"), (2, "2"), (3, "3")).toDF("key", "value")
    val dfBarrier = df1.join(df1.withOptimizerBarrier(), "key")
    val dfNoBarrier = df1.join(df1, "key")
    checkAnswer(dfBarrier, dfNoBarrier)
  }

  test("union") {
    val df1 = Seq((1, "1"), (2, "2"), (3, "3")).toDF("key1", "value1")
    val df2 = Seq((4, "4"), (5, "5"), (6, "6")).toDF("key2", "value2")
    val dfBarrier1 = df1.union(df2).union(df1.union(df2).withOptimizerBarrier())
    val dfBarrier2 = df1.union(df2).withOptimizerBarrier().union(df1.union(df2))
    val dfNoBarrier = df1.union(df2).union(df1.union(df2))
    // Union nested within another Union will be flattened, but that under a barrier will not.
    assert(dfBarrier1.queryExecution.sparkPlan.collect {
      case u: UnionExec => u }.size === 2)
    assert(dfBarrier2.queryExecution.sparkPlan.collect {
      case u: UnionExec => u }.size === 2)
    checkAnswer(dfBarrier1, dfNoBarrier)
    checkAnswer(dfBarrier2, dfNoBarrier)
  }

  test("unionByName") {
    val df1 = Seq((1, "1"), (2, "2"), (3, "3")).toDF("key", "value")
    val df2 = Seq(("4", 4), ("5", 5), ("6", 6)).toDF("value", "key")
    val dfBarrier1 = df1.unionByName(df2).unionByName(df1.unionByName(df2).withOptimizerBarrier())
    val dfBarrier2 = df1.unionByName(df2).withOptimizerBarrier().unionByName(df1.unionByName(df2))
    val dfNoBarrier = df1.unionByName(df2).unionByName(df1.unionByName(df2))
    // Union nested within another Union will be flattened, but that under a barrier will not.
    assert(dfBarrier1.queryExecution.sparkPlan.collect {
      case u: UnionExec => u }.size === 2)
    assert(dfBarrier2.queryExecution.sparkPlan.collect {
      case u: UnionExec => u }.size === 2)
    checkAnswer(dfBarrier1, dfNoBarrier)
    checkAnswer(dfBarrier2, dfNoBarrier)
  }

  test("order preserving") {
    val df1 = Seq((1, "1"), (2, "2"), (3, "3")).toDF("key", "value")
    val dfBarrier = df1.orderBy('key.desc).withOptimizerBarrier().orderBy('key.desc)
    val dfNoBarrier = df1.orderBy('key.desc).orderBy('key.desc)
    // Barrier preserves the ordering, so the parent sort can be optimized out.
    assert(dfBarrier.queryExecution.sparkPlan.collect {
      case s: SortExec => s }.size === 1)
    assert(dfNoBarrier.queryExecution.sparkPlan.collect {
      case s: SortExec => s }.size === 1)
    checkAnswer(dfBarrier, dfNoBarrier)
  }

  test("constraint preserving") {
    val df1 = Seq((1, "1"), (2, "2"), (3, "3")).toDF("key", "value")
    val df2 = Seq((1, 1), (2, 2), (3, 3)).toDF("key", "value")
    val dfBarrier1 = df1.where('key > 1).withOptimizerBarrier().join(df2, "key")
    val dfBarrier2 = df1.join(df2.where('key > 1).withOptimizerBarrier(), "key")
    // Barrier preserves the child's constraints which can be propagated.
    assert(dfBarrier1.queryExecution.sparkPlan.collect {
      case f: FilterExec => f }.size === 2)
    assert(dfBarrier2.queryExecution.sparkPlan.collect {
      case f: FilterExec => f }.size === 2)
    val dfNoBarrier = df1.where('key > 1).join(df2, "key")
    checkAnswer(dfBarrier1, dfNoBarrier)
    checkAnswer(dfBarrier2, dfNoBarrier)
  }
}
