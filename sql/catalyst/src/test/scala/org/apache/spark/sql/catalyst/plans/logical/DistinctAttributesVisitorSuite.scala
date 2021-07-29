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

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExpressionSet}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, LeftAnti, LeftOuter, LeftSemi, PlanTest, RightOuter}
import org.apache.spark.sql.types.IntegerType

class DistinctAttributesVisitorSuite extends PlanTest {

  private val a = AttributeReference("a", IntegerType)()
  private val b = AttributeReference("b", IntegerType)()
  private val c = AttributeReference("c", IntegerType)()
  private val d = a.as("d")
  private val e = b.as("e")
  private val testRelation = LocalRelation(a, b, c)
  private val x = testRelation.as("x")
  private val y = testRelation.as("y")


  private def checkDistinctAttributes(plan: LogicalPlan, distinctKeys: Set[ExpressionSet]) = {
    assert(plan.analyze.distinctAttributes === distinctKeys)
  }

  implicit private def productEncoder[T <: Product : TypeTag] = ExpressionEncoder[T]()

  test("Check distinct attributes visitor") {
    // Aggregate
    checkDistinctAttributes(testRelation.groupBy('a, 'b)('a, 'b, 1), Set(ExpressionSet(Seq(a, b))))
    checkDistinctAttributes(testRelation.groupBy('a)('a), Set(ExpressionSet(Seq(a))))
    checkDistinctAttributes(testRelation.groupBy('a, 'b)('a, 'b), Set(ExpressionSet(Seq(a, b))))
    checkDistinctAttributes(testRelation.groupBy('a, 'b, 1)('a, 'b), Set(ExpressionSet(Seq(a, b))))
    checkDistinctAttributes(testRelation.groupBy('a, 'b)('a, 'b, 1), Set(ExpressionSet(Seq(a, b))))
    checkDistinctAttributes(testRelation.groupBy('a, 'b, 1)('a, 'b, 1),
      Set(ExpressionSet(Seq(a, b))))
    checkDistinctAttributes(testRelation.groupBy('a, 'b)('a), Set.empty)
    checkDistinctAttributes(testRelation.groupBy('a)('a, max('b)), Set(ExpressionSet(Seq(a))))
    checkDistinctAttributes(testRelation.groupBy('a, 'b)('a, 'b, d, e),
      Set(ExpressionSet(Seq(a, b)), ExpressionSet(Seq(a, e.toAttribute)),
        ExpressionSet(Seq(b, d.toAttribute)), ExpressionSet(Seq(d.toAttribute, e.toAttribute))))

    // Distinct
    checkDistinctAttributes(Distinct(testRelation), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(Distinct(testRelation.select('a, 'c)), Set(ExpressionSet(Seq(a, c))))

    // Except
    checkDistinctAttributes(Except(x, y, false), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(Except(x, y, true), Set.empty)

    // Intersect
    checkDistinctAttributes(Intersect(x, y, false), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(Intersect(x, y, true), Set.empty)

    // Repartition
    checkDistinctAttributes(x.repartition(8), Set.empty)
    checkDistinctAttributes(Distinct(x).repartition(8), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(RepartitionByExpression(Seq(a), Distinct(x), None),
      Set(ExpressionSet(Seq(a, b, c))))

    // Sort
    checkDistinctAttributes(x.sortBy('a.asc), Set.empty)
    checkDistinctAttributes(Distinct(x).sortBy('a.asc), Set(ExpressionSet(Seq(a, b, c))))

    // Filter
    checkDistinctAttributes(Filter('a > 1, x), Set.empty)
    checkDistinctAttributes(Filter('a > 1, Distinct(x)), Set(ExpressionSet(Seq(a, b, c))))

    // Project
    checkDistinctAttributes(x.select('a, 'b), Set.empty)
    checkDistinctAttributes(Distinct(x).select('a), Set.empty)
    checkDistinctAttributes(Distinct(x).select('a, 'b, d, e), Set.empty)
    checkDistinctAttributes(Distinct(x).select('a, 'b, 'c, 1), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(Distinct(x).select('a, 'b, c, d),
      Set(ExpressionSet(Seq(a, b, c)), ExpressionSet(Seq(b, c, d.toAttribute))))
    checkDistinctAttributes(testRelation.groupBy('a, 'b)('a, 'b, d).select('a, 'b, e),
      Set(ExpressionSet(Seq(a, b)), ExpressionSet(Seq(a, e.toAttribute))))

    // Join
    checkDistinctAttributes(x.join(y, LeftSemi, Some("x.a".attr === "y.a".attr)), Set.empty)
    checkDistinctAttributes(
      Distinct(x).join(y, LeftSemi, Some("x.a".attr === "y.a".attr)),
      Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(
      Distinct(x).join(y, LeftAnti, Some("x.a".attr === "y.a".attr)),
      Set(ExpressionSet(Seq(a, b, c))))
    Seq(LeftOuter, Cross, Inner, RightOuter).foreach { joinType =>
      checkDistinctAttributes(
        Distinct(x).join(y, joinType, Some("x.a".attr === "y.a".attr)), Set.empty)
    }
  }

}
