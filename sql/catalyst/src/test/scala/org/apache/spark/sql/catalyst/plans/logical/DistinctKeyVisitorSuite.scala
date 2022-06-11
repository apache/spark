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

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExpressionSet, UnspecifiedFrame}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types.IntegerType

class DistinctKeyVisitorSuite extends PlanTest {

  private val a = AttributeReference("a", IntegerType)()
  private val b = AttributeReference("b", IntegerType)()
  private val c = AttributeReference("c", IntegerType)()
  private val d = a.as("aliased_a")
  private val e = b.as("aliased_b")
  private val f = Alias(a + 1, (a + 1).toString)()
  private val x = AttributeReference("x", IntegerType)()
  private val y = AttributeReference("y", IntegerType)()
  private val z = AttributeReference("z", IntegerType)()


  private val t1 = LocalRelation(a, b, c).as("t1")
  private val t2 = LocalRelation(x, y, z).as("t2")

  private def checkDistinctAttributes(plan: LogicalPlan, distinctKeys: Set[ExpressionSet]) = {
    assert(plan.analyze.distinctKeys === distinctKeys)
  }

  implicit private def productEncoder[T <: Product : TypeTag] = ExpressionEncoder[T]()

  test("Aggregate's distinct attributes") {
    checkDistinctAttributes(t1.groupBy($"a", $"b")($"a", $"b", 1), Set(ExpressionSet(Seq(a, b))))
    checkDistinctAttributes(t1.groupBy($"a")($"a"), Set(ExpressionSet(Seq(a))))
    checkDistinctAttributes(t1.groupBy($"a", $"b")($"a", $"b"), Set(ExpressionSet(Seq(a, b))))
    checkDistinctAttributes(t1.groupBy($"a", $"b", 1)($"a", $"b"), Set(ExpressionSet(Seq(a, b))))
    checkDistinctAttributes(t1.groupBy($"a", $"b")($"a", $"b", 1), Set(ExpressionSet(Seq(a, b))))
    checkDistinctAttributes(t1.groupBy($"a", $"b", 1)($"a", $"b", 1), Set(ExpressionSet(Seq(a, b))))
    checkDistinctAttributes(t1.groupBy($"a", $"b")($"a", $"a"), Set.empty)
    checkDistinctAttributes(t1.groupBy($"a", $"b")($"a"), Set.empty)
    checkDistinctAttributes(t1.groupBy($"a")($"a", max($"b")), Set(ExpressionSet(Seq(a))))
    checkDistinctAttributes(t1.groupBy($"a", $"b")($"a", $"b", d, e),
      Set(ExpressionSet(Seq(a, b)), ExpressionSet(Seq(d.toAttribute, e.toAttribute))))
    checkDistinctAttributes(t1.groupBy()(sum($"c")), Set(ExpressionSet()))
    // ExpressionSet() is a subset of anything, so we do not need ExpressionSet(c2)
    checkDistinctAttributes(t1.groupBy()(sum($"c") as "c2").groupBy($"c2")("c2"),
      Set(ExpressionSet()))
    checkDistinctAttributes(t1.groupBy()(), Set(ExpressionSet()))
    checkDistinctAttributes(t1.groupBy($"a")($"a", $"a" % 10, d, sum($"b")),
      Set(ExpressionSet(Seq(a)), ExpressionSet(Seq(d.toAttribute))))
    checkDistinctAttributes(t1.groupBy(f.child, $"b")(f, $"b", sum($"c")),
      Set(ExpressionSet(Seq(f.toAttribute, b))))

    // Aggregate should also propagate distinct keys from child
    checkDistinctAttributes(t1.limit(1).groupBy($"a", $"b")($"a", $"b"),
      Set(ExpressionSet(Seq(a)), ExpressionSet(Seq(b))))
  }

  test("Distinct's distinct attributes") {
    checkDistinctAttributes(Distinct(t1), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(Distinct(t1.select($"a", $"c")), Set(ExpressionSet(Seq(a, c))))
  }

  test("Except's distinct attributes") {
    checkDistinctAttributes(Except(t1, t2, false), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(Except(t1, t2, true), Set.empty)
  }

  test("Filter's distinct attributes") {
    checkDistinctAttributes(Filter($"a" > 1, t1), Set.empty)
    checkDistinctAttributes(Filter($"a" > 1, Distinct(t1)), Set(ExpressionSet(Seq(a, b, c))))
  }

  test("Limit's distinct attributes") {
    checkDistinctAttributes(Distinct(t1).limit(10), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(LocalLimit(10, Distinct(t1)), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(t1.limit(1),
      Set(ExpressionSet(Seq(a)), ExpressionSet(Seq(b)), ExpressionSet(Seq(c))))
  }

  test("Offset's distinct attributes") {
    checkDistinctAttributes(Distinct(t1).limit(12).offset(10).limit(10),
      Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(LocalLimit(10, Offset(10, LocalLimit(12, Distinct(t1)))),
      Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(t1.offset(1).limit(1),
      Set(ExpressionSet(Seq(a)), ExpressionSet(Seq(b)), ExpressionSet(Seq(c))))
  }

  test("Intersect's distinct attributes") {
    checkDistinctAttributes(Intersect(t1, t2, false), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(Intersect(t1, t2, true), Set.empty)
  }

  test("Join's distinct attributes") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      checkDistinctAttributes(
        Distinct(t1).join(t2, joinType, Some($"a" === $"x")), Set(ExpressionSet(Seq(a, b, c))))
    }

    checkDistinctAttributes(
      Distinct(t1).join(Distinct(t2), Inner, Some($"a" === $"x" && $"b" === $"y" && $"c" === $"z")),
      Set(ExpressionSet(Seq(a, b, c)), ExpressionSet(Seq(x, y, z))))

    checkDistinctAttributes(
      Distinct(t1)
        .join(Distinct(t2), LeftOuter, Some($"a" === $"x" && $"b" === $"y" && $"c" === $"z")),
      Set(ExpressionSet(Seq(a, b, c))))

    checkDistinctAttributes(
      Distinct(t1)
        .join(Distinct(t2), RightOuter, Some($"a" === $"x" && $"b" === $"y" && $"c" === $"z")),
      Set(ExpressionSet(Seq(x, y, z))))

    Seq(Inner, Cross, LeftOuter, RightOuter).foreach { joinType =>
      checkDistinctAttributes(t1.join(t2, joinType, Some($"a" === $"x")),
        Set.empty)
      checkDistinctAttributes(
        Distinct(t1).join(Distinct(t2), joinType, Some($"a" === $"x" && $"b" === $"y")),
        Set.empty)
      checkDistinctAttributes(
        Distinct(t1).join(Distinct(t2), joinType,
          Some($"a" === $"x" && $"b" === $"y" && $"c" % 5 === $"z" % 5)),
        Set.empty)
    }

    checkDistinctAttributes(
      Distinct(t1).join(Distinct(t2), Cross, Some($"a" === $"x" && $"b" === $"y" && $"c" === $"z")),
      Set.empty)
  }

  test("Project's distinct attributes") {
    checkDistinctAttributes(t1.select($"a", $"b"), Set.empty)
    checkDistinctAttributes(Distinct(t1).select($"a"), Set.empty)
    checkDistinctAttributes(Distinct(t1).select($"a", $"b", d, e), Set.empty)
    checkDistinctAttributes(Distinct(t1)
      .select($"a", $"b", $"c", 1), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(Distinct(t1).select($"a", $"b", c, d),
      Set(ExpressionSet(Seq(a, b, c)), ExpressionSet(Seq(b, c, d.toAttribute))))
    checkDistinctAttributes(t1.groupBy($"a", $"b")($"a", $"b", d).select($"a", $"b", e),
      Set(ExpressionSet(Seq(a, b)), ExpressionSet(Seq(a, e.toAttribute))))
  }

  test("Repartition's distinct attributes") {
    checkDistinctAttributes(t1.repartition(8), Set.empty)
    checkDistinctAttributes(Distinct(t1).repartition(8), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(RepartitionByExpression(Seq(a), Distinct(t1), None),
      Set(ExpressionSet(Seq(a, b, c))))
  }

  test("Sample's distinct attributes") {
    checkDistinctAttributes(t1.sample(0, 0.2, false, 1), Set.empty)
    checkDistinctAttributes(Distinct(t1).sample(0, 0.2, false, 1), Set(ExpressionSet(Seq(a, b, c))))
  }

  test("Window's distinct attributes") {
    val winExpr = windowExpr(count($"b"),
      windowSpec($"a" :: Nil, $"b".asc :: Nil, UnspecifiedFrame))

    checkDistinctAttributes(
      Distinct(t1)
        .select($"a", $"b", $"c", winExpr.as("window")), Set(ExpressionSet(Seq(a, b, c))))
    checkDistinctAttributes(
      Distinct(t1).select($"a", $"b", winExpr.as("window")), Set())
  }

  test("Tail's distinct attributes") {
    checkDistinctAttributes(Tail(10, Distinct(t1)), Set(ExpressionSet(Seq(a, b, c))))
  }

  test("Sort's distinct attributes") {
    checkDistinctAttributes(t1.sortBy($"a".asc), Set.empty)
    checkDistinctAttributes(Distinct(t1).sortBy($"a".asc), Set(ExpressionSet(Seq(a, b, c))))
  }

  test("RebalancePartitions's distinct attributes") {
    checkDistinctAttributes(RebalancePartitions(Seq(a), Distinct(t1)),
      Set(ExpressionSet(Seq(a, b, c))))
  }

  test("WithCTE's distinct attributes") {
    checkDistinctAttributes(WithCTE(Distinct(t1), mutable.ArrayBuffer.empty[CTERelationDef].toSeq),
      Set(ExpressionSet(Seq(a, b, c))))
  }
}
