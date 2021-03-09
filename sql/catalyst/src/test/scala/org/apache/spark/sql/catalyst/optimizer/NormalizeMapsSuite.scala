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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types.{MapType, StringType}

class NormalizeMapsSuite extends PlanTest with ExpressionEvalHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("NormalizeMaps", Once, NormalizeMaps) :: Nil
  }

  val testRelation = LocalRelation(
    'a.int,
    'm1.map(MapType(StringType, StringType, false)),
    'm2.map(MapType(StringType, StringType, false)))
  val a = testRelation.output(0)
  val m1 = testRelation.output(1)
  val m2 = testRelation.output(2)

  test("SPARK-34819: normalize map types in window function expressions") {
    val query = testRelation.window(Seq(sum(a).as("sum")), Seq(m1), Seq(m1.asc))
    val optimized = Optimize.execute(query)
    // For idempotence checks
    val doubleOptimized = Optimize.execute(optimized)
    val correctAnswer = testRelation.window(Seq(sum(a).as("sum")),
      Seq(SortMapKeys(m1)), Seq(SortMapKeys(m1).asc))
    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-34819: normalize map types in join keys") {
    val testRelation2 = LocalRelation('a.int, 'm.map(MapType(StringType, StringType, false)))
    val a2 = testRelation2.output(0)
    val m21 = testRelation2.output(1)
    val query = testRelation.join(testRelation2, condition = Some(m1 === m21))
    val optimized = Optimize.execute(query)
    // For idempotence checks
    val doubleOptimized = Optimize.execute(optimized)
    val joinCond = Some(SortMapKeys(m1) === SortMapKeys(m21))
    val correctAnswer = testRelation.join(testRelation2, condition = joinCond)
    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-34819: normalize map types in predicates") {
    val query = testRelation.where(m1 === m2)
    val optimized = Optimize.execute(query)
    // For idempotence checks
    val doubleOptimized = Optimize.execute(optimized)
    val correctAnswer = testRelation.where(SortMapKeys(m1) === SortMapKeys(m2))
    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-34819: normalize map types in sort orders") {
    val query = testRelation.orderBy(m1.asc, m2.desc)
    val optimized = Optimize.execute(query)
    // For idempotence checks
    val doubleOptimized = Optimize.execute(optimized)
    val correctAnswer = testRelation.orderBy(SortMapKeys(m1).asc, SortMapKeys(m2).desc)
    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-34819: sort map keys") {
    def createMap(keys: Seq[_], values: Seq[_]): ArrayBasedMapData = {
      val keyArray = CatalystTypeConverters
        .convertToCatalyst(keys)
        .asInstanceOf[ArrayData]
      val valueArray = CatalystTypeConverters
        .convertToCatalyst(values)
        .asInstanceOf[ArrayData]
      new ArrayBasedMapData(keyArray, valueArray)
    }

    checkEvaluation(SortMapKeys(CreateMap(Seq(2, "b", 1, "a"))),
      createMap(Seq(1, 2), Seq("a", "b")))
    checkEvaluation(SortMapKeys(CreateArray(Seq(CreateMap(Seq(2, "b", 1, "a"))))),
      Seq(createMap(Seq(1, 2), Seq("a", "b"))))
    checkEvaluation(SortMapKeys(CreateNamedStruct(Seq("c", CreateMap(Seq(2, "b", 1, "a"))))),
      InternalRow(createMap(Seq(1, 2), Seq("a", "b"))))
    checkEvaluation(SortMapKeys(
        CreateMap(Seq(2, CreateMap(Seq(4, "d", 3, "c")), 1, CreateMap(Seq(2, "b", 1, "a"))))),
      createMap(Seq(1, 2), Seq(
        createMap(Seq(1, 2), Seq("a", "b")),
        createMap(Seq(3, 4), Seq("c", "d")))))
  }
}
