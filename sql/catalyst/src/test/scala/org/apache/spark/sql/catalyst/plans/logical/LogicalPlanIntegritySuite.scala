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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

class LogicalPlanIntegritySuite extends PlanTest {
  import LogicalPlanIntegrity._

  case class OutputTestPlan(child: LogicalPlan, output: Seq[Attribute]) extends UnaryNode {
    override val analyzed = true
    override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
      copy(child = newChild)
  }

  test("Checks if the same `ExprId` refers to a semantically-equal attribute in a plan output") {
    val t = LocalRelation($"a".int, $"b".int)
    assert(hasUniqueExprIdsForOutput(OutputTestPlan(t, t.output)).isEmpty)
    assert(hasUniqueExprIdsForOutput(OutputTestPlan(t, t.output.zipWithIndex.map {
      case (a, i) => AttributeReference(s"c$i", LongType)(a.exprId)
    })).isDefined)
  }

  test("Checks if reference ExprIds are not reused when assigning a new ExprId") {
    val t = LocalRelation($"a".int, $"b".int)
    val Seq(a, b) = t.output
    assert(checkIfSameExprIdNotReused(t.select(Alias(a + 1, "a")())).isEmpty)
    assert(checkIfSameExprIdNotReused(t.select(Alias(a + 1, "a")(exprId = a.exprId))).isDefined)
    assert(checkIfSameExprIdNotReused(t.select(Alias(a + 1, "a")(exprId = b.exprId))).isEmpty)
    assert(checkIfSameExprIdNotReused(t.select(Alias(a + b, "ab")())).isEmpty)
    assert(checkIfSameExprIdNotReused(t.select(Alias(a + b, "ab")(exprId = a.exprId))).isDefined)
    assert(checkIfSameExprIdNotReused(t.select(Alias(a + b, "ab")(exprId = b.exprId))).isDefined)
  }

  test("Checks if an attribute is referenced as non-nullable over a nullable child") {
    val nullableChild = LocalRelation(AttributeReference("a", LongType, nullable = true)())
    val childAttr = nullableChild.output.head
    // A Project that references the child's column but declares it non-nullable (same ExprId) --
    // the SPARK-56395 plan-integrity defect, where a synthesized operator references an
    // outer-join-widened (nullable) column with its original non-nullable metadata.
    val narrowedRef = AttributeReference("a", LongType, nullable = false)(childAttr.exprId)
    val badPlan = Project(Seq(narrowedRef), nullableChild)

    // Disabled by default: even the inconsistent plan passes.
    assert(validateNullability(badPlan).isEmpty)

    withSQLConf(SQLConf.PLAN_CHANGE_VALIDATION_NULLABILITY.key -> "true") {
      // A faithful reference (same nullability as the child) passes.
      assert(validateNullability(Project(Seq(childAttr), nullableChild)).isEmpty)
      // The narrowed reference is flagged, naming the offending attribute.
      assert(validateNullability(badPlan).exists(_.contains(s"a#${childAttr.exprId.id}")))
      // Declaring a reference *wider* (nullable over a non-nullable child) is safe.
      val nonNullChild = LocalRelation(AttributeReference("b", LongType, nullable = false)())
      val widerRef =
        AttributeReference("b", LongType, nullable = true)(nonNullChild.output.head.exprId)
      assert(validateNullability(Project(Seq(widerRef), nonNullChild)).isEmpty)
    }
  }
}
