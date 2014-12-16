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

import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.EliminateAnalysisOperators
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{PlanTest, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._

class UnionPushdownSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateAnalysisOperators) ::
      Batch("Union Pushdown", Once,
        UnionPushdown) :: Nil
  }

  val testRelation =  LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation2 =  LocalRelation('d.int, 'e.int, 'f.int)
  val testUnion = Union(testRelation, testRelation2)

  test("union: filter to each side") {
    val query = testUnion.where('a === 1)

    val optimized = Optimize(query.analyze)

    val correctAnswer =
      Union(testRelation.where('a === 1), testRelation2.where('d === 1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("union: project to each side") {
    val query = testUnion.select('b)

    val optimized = Optimize(query.analyze)

    val correctAnswer =
      Union(testRelation.select('b), testRelation2.select('e)).analyze

    comparePlans(optimized, correctAnswer)
  }
}
