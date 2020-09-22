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
import org.apache.spark.sql.catalyst.expressions.{Alias, GetStructField, Literal, WithFields}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

class OptimizeWithFieldsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("OptimizeWithFields", FixedPoint(10),
      OptimizeWithFields, SimplifyExtractValueOps) :: Nil
  }

  private val testRelation = LocalRelation('a.struct('a1.int))
  private val testRelation2 = LocalRelation('a.struct('a1.int).notNull)

  test("combines two WithFields") {
    val originalQuery = testRelation
      .select(Alias(
        WithFields(
          WithFields(
            'a,
            Seq("b1"),
            Seq(Literal(4))),
          Seq("c1"),
          Seq(Literal(5))), "out")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Alias(WithFields('a, Seq("b1", "c1"), Seq(Literal(4), Literal(5))), "out")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("combines three WithFields") {
    val originalQuery = testRelation
      .select(Alias(
        WithFields(
          WithFields(
            WithFields(
              'a,
              Seq("b1"),
              Seq(Literal(4))),
            Seq("c1"),
            Seq(Literal(5))),
          Seq("d1"),
          Seq(Literal(6))), "out")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Alias(WithFields('a, Seq("b1", "c1", "d1"), Seq(4, 5, 6).map(Literal(_))), "out")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32941: optimize WithFields followed by GetStructField") {
    val originalQuery = testRelation2
      .select(Alias(
        GetStructField(WithFields(
          'a,
          Seq("b1"),
          Seq(Literal(4))), 1), "out")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation2
      .select(Alias(Literal(4), "out")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32941: optimize WithFields chain - case insensitive") {
    val originalQuery = testRelation
      .select(Alias(
        WithFields(
          WithFields(
            'a,
            Seq("b1"),
            Seq(Literal(4))),
          Seq("b1"),
          Seq(Literal(5))), "out1")(),
        Alias(
          WithFields(
            WithFields(
              'a,
              Seq("b1"),
              Seq(Literal(4))),
            Seq("B1"),
            Seq(Literal(5))), "out2")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(
        Alias(WithFields('a, Seq("b1"), Seq(Literal(5))), "out1")(),
        Alias(WithFields('a, Seq("B1"), Seq(Literal(5))), "out2")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32941: optimize WithFields chain - case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val originalQuery = testRelation
        .select(Alias(
          WithFields(
            WithFields(
              'a,
              Seq("b1"),
              Seq(Literal(4))),
            Seq("b1"),
            Seq(Literal(5))), "out1")(),
          Alias(
            WithFields(
              WithFields(
                'a,
                Seq("b1"),
                Seq(Literal(4))),
              Seq("B1"),
              Seq(Literal(5))), "out2")())

      val optimized = Optimize.execute(originalQuery.analyze)
      val correctAnswer = testRelation
        .select(
          Alias(WithFields('a, Seq("b1"), Seq(Literal(5))), "out1")(),
          Alias(WithFields('a, Seq("b1", "B1"), Seq(Literal(4), Literal(5))), "out2")())
        .analyze

      comparePlans(optimized, correctAnswer)
    }
  }
}
