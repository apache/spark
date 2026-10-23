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
import org.apache.spark.sql.catalyst.expressions.{Alias, DropField, GetStructField, Literal, StructFieldsOperation, UpdateFields, UpdateNestedField, WithField}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

class OptimizeWithFieldsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("OptimizeUpdateFields", FixedPoint(10),
      OptimizeUpdateFields, SimplifyExtractValueOps) :: Nil
  }

  private val testRelation = LocalRelation($"a".struct($"a1".int))
  private val testRelation2 = LocalRelation($"a".struct($"a1".int).notNull)

  test("nested merges preserve overwrites, drop barriers, ordering and casing") {
    def nested(name: String, operations: StructFieldsOperation*): UpdateNestedField =
      UpdateNestedField(name, operations.toSeq)
    val first = WithField("x", Literal(1))
    val second = WithField("x", Literal(2))
    val third = WithField("y", Literal(3))
    val drop = DropField("y")
    val replacement = WithField("A", $"replacement")
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val cases = Seq(
        Seq(nested("a", first), nested("A", third), nested("a", second)) ->
          Seq(nested("a", second, third)),
        Seq(nested("a", first), nested("a", second), nested("A", drop),
          nested("a", first), nested("A", second)) ->
          Seq(nested("A", second, drop, first, second)),
        Seq(replacement, nested("a", first), nested("A", third)) ->
          Seq(WithField("A", UpdateFields(
            UpdateFields($"replacement", Seq(first)), Seq(third)))),
        Seq(nested("a", first), replacement) -> Seq(replacement),
        Seq(nested("a", first), replacement, nested("a", third)) ->
          Seq(WithField("a", UpdateFields($"replacement", Seq(third)))),
        Seq(nested("a", first), DropField("b"), nested("a", second)) ->
          Seq(nested("a", first), DropField("b"), nested("a", second)))
      cases.foreach { case (operations, expected) =>
        val optimized = OptimizeUpdateFields.optimizeUpdateFields(
          UpdateFields($"s", operations)).asInstanceOf[UpdateFields]
        assert(optimized.fieldOps == expected)
      }
    }
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val operations = Seq(nested("a", first), nested("A", third), nested("a", second))
      val optimized = OptimizeUpdateFields.optimizeUpdateFields(
        UpdateFields($"s", operations)).asInstanceOf[UpdateFields]
      assert(optimized.fieldOps == Seq(nested("a", second), nested("A", third)))
    }
  }

  test("nested merges should visit barrier operations linearly") {
    var visits = 0
    class CountedSeq[Value](values: Seq[Value])
        extends scala.collection.immutable.AbstractSeq[Value] {
      override def length: Int = values.length
      override def apply(index: Int): Value = {
        visits += 1
        values(index)
      }
      override def iterator: Iterator[Value] = values.iterator.map { value =>
        visits += 1
        value
      }
      override def map[Result](function: Value => Result): Seq[Result] =
        new CountedSeq(iterator.map(function).toVector)
      override def appendedAll[Result >: Value](suffix: IterableOnce[Result]): Seq[Result] =
        new CountedSeq(iterator.concat(suffix.iterator).toVector)
    }
    val width = 2000
    val operations = (0 until width).map { index =>
      UpdateNestedField("a", new CountedSeq[StructFieldsOperation](
        Seq(WithField(s"f$index", Literal(index)), DropField("unused"))))
    }
    val optimized = OptimizeUpdateFields.optimizeUpdateFields(
      UpdateFields($"s", operations)).asInstanceOf[UpdateFields]
    assert(optimized.fieldOps.head.asInstanceOf[UpdateNestedField].fieldOps.length == width * 2)
    assert(visits <= width * 20, s"$visits operation visits for $width nested updates")
  }

  test("combines two adjacent UpdateFields Expressions") {
    val originalQuery = testRelation
      .select(Alias(
        UpdateFields(
          UpdateFields(
            $"a",
            WithField("b1", Literal(4)) :: Nil),
          WithField("c1", Literal(5)) :: Nil), "out")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Alias(UpdateFields($"a", WithField("b1", Literal(4)) :: WithField("c1", Literal(5)) ::
        Nil), "out")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("combines three adjacent UpdateFields Expressions") {
    val originalQuery = testRelation
      .select(Alias(
        UpdateFields(
          UpdateFields(
            UpdateFields(
              $"a",
              WithField("b1", Literal(4)) :: Nil),
            WithField("c1", Literal(5)) :: Nil),
          WithField("d1", Literal(6)) :: Nil), "out")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Alias(UpdateFields($"a", WithField("b1", Literal(4)) :: WithField("c1", Literal(5)) ::
        WithField("d1", Literal(6)) :: Nil), "out")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32941: optimize WithFields followed by GetStructField") {
    val originalQuery = testRelation2
      .select(Alias(
        GetStructField(UpdateFields($"a",
          WithField("b1", Literal(4)) :: Nil), 1), "out")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation2
      .select(Alias(Literal(4), "out")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32941: optimize WithFields chain - case insensitive") {
    val originalQuery = testRelation
      .select(
        Alias(UpdateFields($"a",
          WithField("b1", Literal(4)) :: WithField("b1", Literal(5)) :: Nil), "out1")(),
        Alias(UpdateFields($"a",
          WithField("b1", Literal(4)) :: WithField("B1", Literal(5)) :: Nil), "out2")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(
        Alias(UpdateFields($"a", WithField("b1", Literal(5)) :: Nil), "out1")(),
        Alias(UpdateFields($"a", WithField("B1", Literal(5)) :: Nil), "out2")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32941: optimize WithFields chain - case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val originalQuery = testRelation
        .select(
          Alias(UpdateFields($"a",
            WithField("b1", Literal(4)) :: WithField("b1", Literal(5)) :: Nil), "out1")(),
          Alias(UpdateFields($"a",
              WithField("b1", Literal(4)) :: WithField("B1", Literal(5)) :: Nil), "out2")())

      val optimized = Optimize.execute(originalQuery.analyze)
      val correctAnswer = testRelation
        .select(
          Alias(UpdateFields($"a", WithField("b1", Literal(5)) :: Nil), "out1")(),
          Alias(
            UpdateFields($"a",
              WithField("b1", Literal(4)) :: WithField("B1", Literal(5)) :: Nil), "out2")())
        .analyze

      comparePlans(optimized, correctAnswer)
    }
  }

  test("SPARK-35213: ensure optimize WithFields maintains correct WithField ordering") {
    val originalQuery = testRelation
      .select(
        Alias(UpdateFields($"a",
          WithField("a1", Literal(3)) ::
          WithField("b1", Literal(4)) ::
          WithField("a1", Literal(5)) ::
          Nil), "out")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(
        Alias(UpdateFields($"a",
          WithField("a1", Literal(5)) ::
          WithField("b1", Literal(4)) ::
          Nil), "out")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }
}
