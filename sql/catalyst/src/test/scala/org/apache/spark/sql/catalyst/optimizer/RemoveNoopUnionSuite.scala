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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class RemoveNoopUnionSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CollapseProject", Once,
        CollapseProject) ::
        Batch("RemoveNoopUnion", Once,
          RemoveNoopUnion) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int)
  val testRelation2 = LocalRelation(output = Seq($"a".int, $"b".int), data = Seq(InternalRow(1, 2)))

  test("SPARK-34474: Remove redundant Union under Distinct") {
    val union = Union(testRelation :: testRelation :: Nil)
    val distinct = Distinct(union)
    val optimized = Optimize.execute(distinct)
    comparePlans(optimized, Distinct(testRelation))
  }

  test("SPARK-34474: Remove redundant Union under Deduplicate") {
    val union = Union(testRelation :: testRelation :: Nil)
    val deduplicate = Deduplicate(testRelation.output, union)
    val optimized = Optimize.execute(deduplicate)
    comparePlans(optimized, Deduplicate(testRelation.output, testRelation))
  }

  test("SPARK-34474: Do not remove necessary Project 1") {
    val child1 = Project(Seq(testRelation.output(0), testRelation.output(1),
      (testRelation.output(0) + 1).as("expr")), testRelation)
    val child2 = Project(Seq(testRelation.output(0), testRelation.output(1),
      (testRelation.output(0) + 2).as("expr")), testRelation)
    val union = Union(child1 :: child2 :: Nil)
    val distinct = Distinct(union)
    val optimized = Optimize.execute(distinct)
    comparePlans(optimized, distinct)
  }

  test("SPARK-34474: Do not remove necessary Project 2") {
    val child1 = Project(Seq(testRelation.output(0), testRelation.output(1)), testRelation)
    val child2 = Project(Seq(testRelation.output(1), testRelation.output(0)), testRelation)
    val union = Union(child1 :: child2 :: Nil)
    val distinct = Distinct(union)
    val optimized = Optimize.execute(distinct)
    comparePlans(optimized, distinct)
  }

  test("SPARK-34548: Remove unnecessary children from Union") {
    val union = Union(testRelation :: testRelation :: testRelation2 :: Nil)
    val distinct = Distinct(union)
    val optimized1 = Optimize.execute(distinct)
    comparePlans(optimized1, Distinct(Union(testRelation :: testRelation2 :: Nil)))

    val deduplicate = Deduplicate(testRelation.output, union)
    val optimized2 = Optimize.execute(deduplicate)
    comparePlans(optimized2,
      Deduplicate(testRelation.output, Union(testRelation :: testRelation2 :: Nil)))
  }
}
