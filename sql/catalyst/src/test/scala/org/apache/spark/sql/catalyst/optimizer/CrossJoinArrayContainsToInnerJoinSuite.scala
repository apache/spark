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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

/**
 * Test suite for CrossJoinArrayContainsToInnerJoin optimizer rule.
 *
 * This rule converts cross joins with array_contains filter into inner joins
 * using explode/unnest, which is much more efficient.
 *
 * Example transformation:
 * {{{
 * Filter(array_contains(arr, elem))
 *   CrossJoin(left, right)
 * }}}
 * becomes:
 * {{{
 * InnerJoin(unnested = elem)
 *   Generate(Explode(ArrayDistinct(arr)), left)
 *   right
 * }}}
 */
class CrossJoinArrayContainsToInnerJoinSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CrossJoinArrayContainsToInnerJoin", Once,
        CrossJoinArrayContainsToInnerJoin) :: Nil
  }

  // Table with array column (simulates "orders" with item_ids array)
  val ordersRelation: LocalRelation = LocalRelation(
    $"order_id".int,
    $"item_ids".array(IntegerType)
  )

  // Table with element column (simulates "items" with id)
  val itemsRelation: LocalRelation = LocalRelation(
    $"id".int,
    $"name".string
  )

  test("converts cross join with array_contains to inner join with explode") {
    // Original query: SELECT * FROM orders, items WHERE array_contains(item_ids, id)
    val originalPlan = ordersRelation
      .join(itemsRelation, Cross)
      .where(ArrayContains($"item_ids", $"id"))
      .analyze

    val optimized = Optimize.execute(originalPlan)

    // After optimization, should be an inner join with explode
    // The plan should NOT contain a Cross join anymore
    assert(!optimized.exists {
      case j: Join if j.joinType == Cross => true
      case _ => false
    }, "Optimized plan should not contain Cross join")

    // Should contain a Generate (explode) node
    assert(optimized.exists {
      case _: Generate => true
      case _ => false
    }, "Optimized plan should contain Generate (explode) node")

    // Should contain an Inner join
    assert(optimized.exists {
      case j: Join if j.joinType == Inner => true
      case _ => false
    }, "Optimized plan should contain Inner join")
  }

  test("does not transform when array_contains is not present") {
    // Query without array_contains: SELECT * FROM orders, items WHERE order_id = id
    val originalPlan = ordersRelation
      .join(itemsRelation, Cross)
      .where($"order_id" === $"id")
      .analyze

    val optimized = Optimize.execute(originalPlan)

    // Should remain unchanged (still a cross join with filter)
    assert(optimized.exists {
      case j: Join if j.joinType == Cross => true
      case _ => false
    }, "Plan without array_contains should remain unchanged")
  }

  test("does not transform inner join with existing conditions") {
    // Already an inner join with equi-condition
    val originalPlan = ordersRelation
      .join(itemsRelation, Inner, Some($"order_id" === $"id"))
      .where(ArrayContains($"item_ids", $"id"))
      .analyze

    val optimized = Optimize.execute(originalPlan)

    // Should not add another explode since this is already an equi-join
    // The array_contains becomes just a filter
    assert(optimized.isInstanceOf[Filter] || optimized.exists {
      case _: Filter => true
      case _ => false
    })
  }

  test("handles array column on right side of join") {
    // Swap the tables - array is on right side
    val rightWithArray: LocalRelation = LocalRelation(
      $"arr_id".int,
      $"values".array(IntegerType)
    )
    val leftWithElement: LocalRelation = LocalRelation(
      $"elem".int
    )

    val originalPlan = leftWithElement
      .join(rightWithArray, Cross)
      .where(ArrayContains($"values", $"elem"))
      .analyze

    val optimized = Optimize.execute(originalPlan)

    // Should still be transformed
    assert(!optimized.exists {
      case j: Join if j.joinType == Cross => true
      case _ => false
    }, "Should transform even when array is on right side")
  }

  test("preserves remaining filter predicates") {
    // Query with additional conditions beyond array_contains
    val originalPlan = ordersRelation
      .join(itemsRelation, Cross)
      .where(ArrayContains($"item_ids", $"id") && ($"order_id" > 100))
      .analyze

    val optimized = Optimize.execute(originalPlan)

    // Should still have a filter for the remaining predicate (order_id > 100)
    assert(optimized.exists {
      case Filter(cond, _) =>
        cond.find {
          case GreaterThan(_, Literal(100, IntegerType)) => true
          case _ => false
        }.isDefined
      case _ => false
    }, "Should preserve remaining filter predicates")
  }

  test("uses array_distinct to avoid duplicate matches") {
    val originalPlan = ordersRelation
      .join(itemsRelation, Cross)
      .where(ArrayContains($"item_ids", $"id"))
      .analyze

    val optimized = Optimize.execute(originalPlan)

    // The optimized plan should use ArrayDistinct before exploding
    // to avoid duplicate rows when array has duplicate elements
    assert(optimized.exists {
      case Generate(Explode(ArrayDistinct(_)), _, _, _, _, _) => true
      case Project(_, Generate(Explode(ArrayDistinct(_)), _, _, _, _, _)) => true
      case _ => false
    }, "Should use ArrayDistinct before Explode")
  }

  test("supports ByteType elements") {
    val leftRel = LocalRelation($"id".int, $"arr".array(ByteType))
    val rightRel = LocalRelation($"elem".byte)
    val plan = leftRel.join(rightRel, Cross).where(ArrayContains($"arr", $"elem")).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.exists { case _: Generate => true; case _ => false })
  }

  test("supports ShortType elements") {
    val leftRel = LocalRelation($"id".int, $"arr".array(ShortType))
    val rightRel = LocalRelation($"elem".short)
    val plan = leftRel.join(rightRel, Cross).where(ArrayContains($"arr", $"elem")).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.exists { case _: Generate => true; case _ => false })
  }

  test("supports DecimalType elements") {
    val leftRel = LocalRelation($"id".int, $"arr".array(DecimalType(10, 2)))
    val rightRel = LocalRelation($"elem".decimal(10, 2))
    val plan = leftRel.join(rightRel, Cross).where(ArrayContains($"arr", $"elem")).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.exists { case _: Generate => true; case _ => false })
  }

  test("supports TimestampType elements") {
    val leftRel = LocalRelation($"id".int, $"arr".array(TimestampType))
    val rightRel = LocalRelation($"elem".timestamp)
    val plan = leftRel.join(rightRel, Cross).where(ArrayContains($"arr", $"elem")).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.exists { case _: Generate => true; case _ => false })
  }

  test("supports BooleanType elements") {
    val leftRel = LocalRelation($"id".int, $"arr".array(BooleanType))
    val rightRel = LocalRelation($"elem".boolean)
    val plan = leftRel.join(rightRel, Cross).where(ArrayContains($"arr", $"elem")).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.exists { case _: Generate => true; case _ => false })
  }

  test("supports BinaryType elements") {
    val leftRel = LocalRelation($"id".int, $"arr".array(BinaryType))
    val rightRel = LocalRelation($"elem".binary)
    val plan = leftRel.join(rightRel, Cross).where(ArrayContains($"arr", $"elem")).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.exists { case _: Generate => true; case _ => false })
  }

  test("does not transform FloatType elements due to NaN semantics") {
    val leftRel = LocalRelation($"id".int, $"arr".array(FloatType))
    val rightRel = LocalRelation($"elem".float)
    val plan = leftRel.join(rightRel, Cross).where(ArrayContains($"arr", $"elem")).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.exists {
      case j: Join if j.joinType == Cross => true
      case _ => false
    }, "FloatType should not be transformed due to NaN semantics")
  }

  test("does not transform DoubleType elements due to NaN semantics") {
    val leftRel = LocalRelation($"id".int, $"arr".array(DoubleType))
    val rightRel = LocalRelation($"elem".double)
    val plan = leftRel.join(rightRel, Cross).where(ArrayContains($"arr", $"elem")).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.exists {
      case j: Join if j.joinType == Cross => true
      case _ => false
    }, "DoubleType should not be transformed due to NaN semantics")
  }

  test("does not transform StructType elements") {
    val structType = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    val leftRel = LocalRelation($"id".int, $"arr".array(structType))
    val rightRel = LocalRelation($"elem".struct(structType))
    val plan = leftRel.join(rightRel, Cross).where(ArrayContains($"arr", $"elem")).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.exists {
      case j: Join if j.joinType == Cross => true
      case _ => false
    }, "StructType should not be transformed due to complex equality semantics")
  }
}
