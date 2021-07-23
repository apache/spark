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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionSet}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, LeftAnti, LeftOuter, LeftSemi, PlanTest, RightOuter}
import org.apache.spark.sql.types.IntegerType

class DistinctAttributesVisitorSuite extends PlanTest {

  private val a = AttributeReference("a", IntegerType)()
  private val b = AttributeReference("b", IntegerType)()
  private val c = AttributeReference("c", IntegerType)()
  private val testRelation = LocalRelation(a, b, c)
  private val x = testRelation.as("x")
  private val y = testRelation.as("y")


  private def checkDistinctAttributes(plan: LogicalPlan, distinctKeys: Seq[Expression]) = {
    assert(plan.analyze.distinctAttributes === ExpressionSet(distinctKeys))
  }

  implicit private def productEncoder[T <: Product : TypeTag] = ExpressionEncoder[T]()

  test("1") {
    // Aggregate
    checkDistinctAttributes(testRelation.groupBy('a)('a), Seq(a))
    checkDistinctAttributes(testRelation.groupBy('a, 'b)('a, 'b), Seq(a, b))
    checkDistinctAttributes(testRelation.groupBy('a, 'b, 1)('a, 'b), Seq(a, b))
    checkDistinctAttributes(testRelation.groupBy('a, 'b)('a, 'b, 1), Seq.empty)
    checkDistinctAttributes(testRelation.groupBy('a, 'b, 1)('a, 'b, 1), Seq.empty)
    checkDistinctAttributes(testRelation.groupBy('a, 'b)('a), Seq(a))

    // Distinct
    checkDistinctAttributes(Distinct(testRelation), Seq(a, b, c))
    checkDistinctAttributes(Distinct(testRelation.select('a, 'c)), Seq(a, c))

    // Except
    checkDistinctAttributes(Except(x, y, false), Seq(a, b, c))
    checkDistinctAttributes(Except(x, y, true), Seq.empty)

    // Intersect
    checkDistinctAttributes(Intersect(x, y, false), Seq(a, b, c))
    checkDistinctAttributes(Intersect(x, y, true), Seq.empty)

    // Repartition
    checkDistinctAttributes(x.repartition(8), Seq.empty)
    checkDistinctAttributes(Distinct(x).repartition(8), Seq(a, b, c))
    checkDistinctAttributes(RepartitionByExpression(Seq(a), Distinct(x), None), Seq(a, b, c))

    // Sort
    checkDistinctAttributes(x.sortBy('a.asc), Seq.empty)
    checkDistinctAttributes(Distinct(x).sortBy('a.asc), Seq(a, b, c))

    // Filter
    checkDistinctAttributes(Filter('a > 1, x), Seq.empty)
    checkDistinctAttributes(Filter('a > 1, Distinct(x)), Seq(a, b, c))

    // Project
    checkDistinctAttributes(x.select('a, 'b), Seq.empty)
    checkDistinctAttributes(Distinct(x).select('a), Seq(a))

    // Join
    checkDistinctAttributes(x.join(y, LeftSemi, Some("x.a".attr === "y.a".attr)), Seq.empty)
    checkDistinctAttributes(
      Distinct(x).join(y, LeftSemi, Some("x.a".attr === "y.a".attr)), Seq(a, b, c))
    checkDistinctAttributes(
      Distinct(x).join(y, LeftAnti, Some("x.a".attr === "y.a".attr)), Seq(a, b, c))
    Seq(LeftOuter, Cross, Inner, RightOuter).foreach { joinType =>
      checkDistinctAttributes(
        Distinct(x).join(y, joinType, Some("x.a".attr === "y.a".attr)), Seq.empty)
    }
  }

}
