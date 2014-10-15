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

import scala.collection.immutable.HashSet
import org.apache.spark.sql.catalyst.analysis.{EliminateAnalysisOperators, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.types._

// For implicit conversions
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._

class OptimizeInSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateAnalysisOperators) ::
      Batch("ConstantFolding", Once,
        ConstantFolding,
        BooleanSimplification,
        OptimizeIn) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("OptimizedIn test: In clause optimized to InSet") {
    val originalQuery =
      testRelation
        .where(In(UnresolvedAttribute("a"), Seq(Literal(1),Literal(2))))
        .analyze

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(InSet(UnresolvedAttribute("a"), HashSet[Any]()+1+2, 
            UnresolvedAttribute("a") +: Seq(Literal(1),Literal(2))))
        .analyze

    comparePlans(optimized, correctAnswer)
  }
  
  test("OptimizedIn test: In clause not optimized in case filter has attributes") {
    val originalQuery =
      testRelation
        .where(In(UnresolvedAttribute("a"), Seq(Literal(1),Literal(2), UnresolvedAttribute("b"))))
        .analyze

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(In(UnresolvedAttribute("a"), Seq(Literal(1),Literal(2), UnresolvedAttribute("b"))))
        .analyze

    comparePlans(optimized, correctAnswer)
  }
}
