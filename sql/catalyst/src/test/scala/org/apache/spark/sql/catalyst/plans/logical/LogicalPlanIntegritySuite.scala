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
import org.apache.spark.sql.catalyst.plans.{LeftOuter, PlanTest}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

class LogicalPlanIntegritySuite extends PlanTest {
  import LogicalPlanIntegrity._

  case class OutputTestPlan(child: LogicalPlan, output: Seq[Attribute]) extends UnaryNode {
    override val analyzed = true
    override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
      copy(child = newChild)
  }

  // A binary node that references an attribute (via the `ref` field, picked up by `expressions`)
  // and exposes both children's outputs. Used to exercise nullability validation when the same
  // `ExprId` is produced by more than one child.
  case class RefTestPlan(left: LogicalPlan, right: LogicalPlan, ref: AttributeReference)
    extends BinaryNode {
    override val analyzed = true
    override def output: Seq[Attribute] = left.output ++ right.output
    override protected def withNewChildrenInternal(
        newLeft: LogicalPlan, newRight: LogicalPlan): LogicalPlan =
      copy(left = newLeft, right = newRight)
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

  test("Nullability check flags the motivating outer-join shape over a multi-child plan") {
    // SPARK-56395: an operator over a LEFT OUTER join references the null-widened (right) side
    // with its original non-nullable metadata. This is the realistic motivating shape -- a
    // synthesized Aggregate over a join whose output widened the right side to nullable.
    val left = LocalRelation(AttributeReference("a", LongType, nullable = false)())
    val right = LocalRelation(AttributeReference("b", LongType, nullable = false)())
    val a = left.output.head
    val b = right.output.head
    // The join widens `b` to nullable in its output, but `b` here still carries the original
    // non-nullable metadata -- the synthesized, stale reference.
    val join = left.join(right, LeftOuter)
    val agg = Aggregate(Seq(a), Seq(a, b), join)

    withSQLConf(SQLConf.PLAN_CHANGE_VALIDATION_NULLABILITY.key -> "true") {
      assert(validateNullability(agg).exists(_.contains(s"b#${b.exprId.id}")))
    }
  }

  test("Nullability check finds a narrowed reference nested in a compound expression") {
    val nullableChild = LocalRelation(AttributeReference("a", LongType, nullable = true)())
    val childAttr = nullableChild.output.head
    val narrowedRef = AttributeReference("a", LongType, nullable = false)(childAttr.exprId)
    // The offending reference is buried inside `narrowedRef + 1L`, wrapped in an Alias, to
    // confirm the inner `_.collect` descends into compound expressions.
    val plan = Project(Seq(Alias(narrowedRef + 1L, "x")()), nullableChild)

    withSQLConf(SQLConf.PLAN_CHANGE_VALIDATION_NULLABILITY.key -> "true") {
      assert(validateNullability(plan).exists(_.contains(s"a#${childAttr.exprId.id}")))
    }
  }

  test("Nullability check traverses below the root to find the offending node") {
    val nullableChild = LocalRelation(AttributeReference("a", LongType, nullable = true)())
    val childAttr = nullableChild.output.head
    val narrowedRef = AttributeReference("a", LongType, nullable = false)(childAttr.exprId)
    val badPlan = Project(Seq(narrowedRef), nullableChild)
    // The root Project is a faithful pass-through (references the already-narrowed attribute as
    // non-nullable, matching its child's output); only the inner Project is inconsistent.
    val plan = badPlan.select(narrowedRef)

    withSQLConf(SQLConf.PLAN_CHANGE_VALIDATION_NULLABILITY.key -> "true") {
      assert(validateNullability(plan).exists(_.contains(s"a#${childAttr.exprId.id}")))
    }
  }

  test("Nullability check ignores references not produced by any child") {
    val nullableChild = LocalRelation(AttributeReference("a", LongType, nullable = true)())
    // `o` is declared non-nullable but its ExprId is not produced by the child (e.g. an outer
    // reference), so it must be ignored rather than flagged.
    val outerRef = AttributeReference("o", LongType, nullable = false)()
    val plan = Project(Seq(outerRef), nullableChild)

    withSQLConf(SQLConf.PLAN_CHANGE_VALIDATION_NULLABILITY.key -> "true") {
      assert(validateNullability(plan).isEmpty)
    }
  }

  test("Nullability check treats a duplicated ExprId across children as nullable if any is") {
    // Two children produce the same ExprId with differing nullability; the inline "any nullable
    // wins" rule must treat the column as nullable, so a non-nullable reference is flagged.
    val sharedId = AttributeReference("k", LongType, nullable = false)().exprId
    val nonNullChild = LocalRelation(AttributeReference("k", LongType, nullable = false)(sharedId))
    val nullableChild = LocalRelation(AttributeReference("k", LongType, nullable = true)(sharedId))
    val ref = AttributeReference("k", LongType, nullable = false)(sharedId)
    val plan = RefTestPlan(nonNullChild, nullableChild, ref)

    withSQLConf(SQLConf.PLAN_CHANGE_VALIDATION_NULLABILITY.key -> "true") {
      assert(validateNullability(plan).exists(_.contains(s"k#${sharedId.id}")))
    }
  }
}
