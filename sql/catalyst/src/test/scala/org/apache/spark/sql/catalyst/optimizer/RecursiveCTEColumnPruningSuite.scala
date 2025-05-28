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

import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.optimizer.SimpleTestOptimizer.RecursiveCTEColumnPruning
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class RecursiveCTEColumnPruningSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Column pruning", FixedPoint(100),
      PushPredicateThroughNonJoin,
      ColumnPruning,
      RecursiveCTEColumnPruning,
      RemoveNoopOperators,
      CollapseProject) :: Nil
  }

  test("Column Pruning for rCTEs") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()

    val anchor1 = Project(Seq(a, b, c), LocalRelation(a, b, c))
    val recursion1 = Filter(a < Literal(5), UnionLoopRef(0, Seq(a, b, c), accumulated = false))
    val plan1 = Project(Seq(a), UnionLoop(0, anchor1, recursion1))

    val expectedAnchor1 = Project(Seq(a), LocalRelation(a, b, c))
    val expectedRecursion1 = Project(Seq(a), Filter(a < Literal(5),
      UnionLoopRef(0, Seq(a, b, c), accumulated = false)))
    val expectedPlan1 = UnionLoop(0, expectedAnchor1, expectedRecursion1)

    comparePlans(Optimize.execute(plan1), expectedPlan1)

    val anchor2 = Project(Seq(a, b, c), LocalRelation(a, b, c))
    val recursion2 = Filter(b < Literal(5), UnionLoopRef(0, Seq(a, b, c), accumulated = false))
    val plan2 = Project(Seq(a), UnionLoop(0, anchor2, recursion2))

    val expectedAnchor2 = Project(Seq(a, b), LocalRelation(a, b, c))
    val expectedRecursion2 = Project(Seq(a, b), Filter(b < Literal(5),
      UnionLoopRef(0, Seq(a, b, c), accumulated = false)))
    val expectedPlan2 = Project(Seq(a), UnionLoop(0, expectedAnchor2, expectedRecursion2))

    comparePlans(Optimize.execute(plan2), expectedPlan2)

    val anchor3 = Project(Seq(a, b, c), LocalRelation(a, b, c))
    val recursion3 = Project(Seq(a, b, c), Filter(a < Literal(5),
      UnionLoopRef(0, Seq(a, b, c), accumulated = false)))
    val plan3 = Project(Seq(a), UnionLoop(0, anchor3, recursion3))

    val expectedAnchor3 = Project(Seq(a), LocalRelation(a, b, c))
    val expectedRecursion3 = Project(Seq(a), Filter(a < Literal(5),
      UnionLoopRef(0, Seq(a, b, c), accumulated = false)))
    val expectedPlan3 = UnionLoop(0, expectedAnchor3, expectedRecursion3)

    comparePlans(Optimize.execute(plan3), expectedPlan3)

    val anchor4 = Project(Seq(a, b, c), LocalRelation(a, b, c))
    val recursion4 = Project(Seq(a, b, c), Filter(a < Literal(5),
      UnionLoopRef(0, Seq(a, b, c), accumulated = false)))
    val plan4 = Project(Seq(b, c), UnionLoop(0, anchor4, recursion4))

    val expectedAnchor4 = LocalRelation(a, b, c)
    val expectedRecursion4 = Filter(a < Literal(5),
      UnionLoopRef(0, Seq(a, b, c), accumulated = false))
    val expectedPlan4 = Project(Seq(b, c), UnionLoop(0, expectedAnchor4, expectedRecursion4))

    comparePlans(Optimize.execute(plan4), expectedPlan4)

    val anchor5 = Project(Seq(a, b, c), LocalRelation(a, b, c))
    val recursion5 = Project(Seq(a, b, c), Filter(a < Literal(5),
      UnionLoopRef(0, Seq(a, b, c), accumulated = false)))
    val plan5 = Project(Seq(a, b), UnionLoop(0, anchor5, recursion5))

    val expectedAnchor5 = Project(Seq(a, b), LocalRelation(a, b, c))
    val expectedRecursion5 = Project(Seq(a, b), Filter(a < Literal(5),
      UnionLoopRef(0, Seq(a, b, c), accumulated = false)))
    val expectedPlan5 = UnionLoop(0, expectedAnchor5, expectedRecursion5)

    comparePlans(Optimize.execute(plan5), expectedPlan5)

    val anchor6 = Project(Seq(a, b, c), LocalRelation(a, b, c))
    val recursion6 = Project(Seq(a, b, c), UnionLoopRef(0, Seq(a, b, c), accumulated = false))
    val plan6 = Project(Seq(a), UnionLoop(0, anchor6, recursion6))

    val expectedAnchor6 = Project(Seq(a), LocalRelation(a, b, c))
    val expectedRecursion6 = Project(Seq(a), UnionLoopRef(0, Seq(a, b, c), accumulated = false))
    val expectedPlan6 = UnionLoop(0, expectedAnchor6, expectedRecursion6)

    comparePlans(Optimize.execute(plan6), expectedPlan6)

    val anchor7 = Project(Seq(a, b, c), LocalRelation(a, b, c))
    val recursion7 = Project(Seq(a, b, c), UnionLoopRef(0, Seq(a, b, c), accumulated = false))
    val plan7 = Project(Seq(a, b), UnionLoop(0, anchor7, recursion7))

    val expectedAnchor7 = Project(Seq(a, b), LocalRelation(a, b, c))
    val expectedRecursion7 = Project(Seq(a, b), UnionLoopRef(0, Seq(a, b, c), accumulated = false))
    val expectedPlan7 = UnionLoop(0, expectedAnchor7, expectedRecursion7)

    comparePlans(Optimize.execute(plan7), expectedPlan7)
  }

}
