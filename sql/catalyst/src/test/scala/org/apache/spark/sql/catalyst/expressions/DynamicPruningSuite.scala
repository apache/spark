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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

class DynamicPruningSuite extends PlanTest {

  private val pruningSideRelation = LocalRelation($"a".int, $"b".long, $"c".string)
  private val a = pruningSideRelation.output(0)

  private val originBuildSideRelation = LocalRelation($"x".int, $"y".long)
  private val x = originBuildSideRelation.output(0)

  private val originDynamicPruningSubquery =
    DynamicPruningSubquery(a, originBuildSideRelation, Seq(x), 0, true, ExprId(0))

  test("Test withNewPlan can successfully update buildKeys") {
    val testRelation = LocalRelation($"x1".int, $"y1".long)
    val withNewPlan = originDynamicPruningSubquery.withNewPlan(testRelation)
    assert(withNewPlan.buildKeys.head.semanticEquals(testRelation.output.head))
  }

  test("Test withNewPlan can not update plan") {
    val e1 = intercept[AnalysisException] {
      originDynamicPruningSubquery.withNewPlan(LocalRelation($"x1".long, $"y1".long))
    }
    e1.getMessage.contains("Failed to update the plan")

    val e2 = intercept[AnalysisException] {
      originDynamicPruningSubquery.withNewPlan(LocalRelation($"x1".int))
    }
    e2.getMessage.contains("Failed to update the plan")
  }
}
