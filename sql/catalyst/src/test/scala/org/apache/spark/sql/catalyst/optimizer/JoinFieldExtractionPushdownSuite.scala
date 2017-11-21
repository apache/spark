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

import org.apache.spark.sql.catalyst.SchemaPruningTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

class JoinFieldExtractionPushdownSuite extends SchemaPruningTest {
  private val leftRelation =
    LocalRelation(
      StructField("la", StructType(
        StructField("la1", IntegerType) :: Nil)),
      StructField("lb", IntegerType),
      StructField("lc", StructType(
        StructField("lc1", IntegerType) :: Nil)))

  private val rightRelation =
    LocalRelation(
      StructField("ra", StructType(
        StructField("ra1", IntegerType) :: Nil)),
      StructField("rb", IntegerType),
      StructField("rc", StructType(
        StructField("rc1", IntegerType) :: Nil)))

  private val joinTypes = Inner :: LeftOuter :: RightOuter :: FullOuter :: LeftSemi :: Nil

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Join Field Extraction Pushdown", Once,
        JoinFieldExtractionPushdown) :: Nil
  }

  test("don't modify simple joins") {
    joinTypes.foreach { joinType =>
      val originalQuery =
        leftRelation.join(rightRelation, joinType, Some($"lb" === $"rb")).analyze
      val optimized = Optimizer.execute(originalQuery)

      comparePlans(optimized, originalQuery)
    }
  }

  test("don't modify output of bare join") {
    joinTypes.filterNot(_ == LeftSemi).foreach { joinType =>
      val originalQuery =
        leftRelation.join(rightRelation, joinType, Some($"la.la1" === $"rb")).analyze
      val optimized = Optimizer.execute(originalQuery)
      val correctAnswer =
        leftRelation.select('la, 'lb, 'lc, $"la.la1" as 'la1)
          .join(rightRelation.select('ra, 'rb, 'rc), joinType, Some('la1 === 'rb))
          .select('la, 'lb, 'lc, 'ra, 'rb, 'rc)
          .analyze

      comparePlans(optimized, correctAnswer)
    }
  }

  test("push down left join condition path of degree 1") {
    joinTypes.foreach { joinType =>
      val originalQuery =
        leftRelation.join(rightRelation, joinType, Some($"la.la1" === $"rb"))
        .select($"la.la1")
        .analyze
      val optimized = Optimizer.execute(originalQuery)
      val correctAnswer =
        leftRelation.select($"la.la1" as 'la1)
          .join(rightRelation.select('rb), joinType, Some('la1 === 'rb))
          .select('la1 as 'la1)
          .analyze

      comparePlans(optimized, correctAnswer)
    }
  }

  test("push down right join condition path of degree 1") {
    joinTypes.filterNot(_ == LeftSemi).foreach { joinType =>
      val originalQuery =
        leftRelation.join(rightRelation, joinType, Some($"lb" === $"ra.ra1"))
        .select($"ra.ra1")
        .analyze
      val optimized = Optimizer.execute(originalQuery)
      val correctAnswer =
        leftRelation.select('lb)
          .join(rightRelation.select($"ra.ra1" as 'ra1), joinType, Some('lb === 'ra1))
          .select('ra1 as 'ra1)
          .analyze

      comparePlans(optimized, correctAnswer)
    }
  }

  test("push down both join condition paths of degree 1") {
    def test1(
        joinType: JoinType,
        originalSelects: Seq[NamedExpression],
        expectedSelects: Seq[NamedExpression]) {
      val originalQuery =
        leftRelation.join(rightRelation, joinType, Some($"la.la1" === $"ra.ra1"))
        .select(originalSelects: _*)
        .analyze
      val optimized = Optimizer.execute(originalQuery)
      val correctAnswer =
        leftRelation.select($"la.la1" as 'la1)
          .join(rightRelation.select($"ra.ra1" as 'ra1), joinType, Some('la1 === 'ra1))
          .select(expectedSelects: _*)
          .analyze

      comparePlans(optimized, correctAnswer)
    }

    joinTypes.filterNot(_ == LeftSemi)
      .foreach(joinType =>
        test1(joinType, $"la.la1" :: $"ra.ra1" :: Nil, ('la1 as 'la1) :: ('ra1 as 'ra1) :: Nil))

    test1(LeftSemi, $"la.la1" :: Nil, ('la1 as 'la1) :: Nil)
  }

  test("don't prune root of leaf when the root is in the projection") {
    joinTypes.filterNot(_ == LeftSemi).foreach { joinType =>
      val originalQuery =
        leftRelation.join(rightRelation, joinType, Some($"la.la1" === $"ra.ra1"))
        .select($"la.la1", 'ra)
        .analyze
      val optimized = Optimizer.execute(originalQuery)
      val correctAnswer =
        leftRelation.select($"la.la1" as 'la1)
          .join(rightRelation.select('ra, $"ra.ra1" as 'ra1), joinType, Some('la1 === 'ra1))
          .select('la1 as 'la1, 'ra)
          .analyze

      comparePlans(optimized, correctAnswer)
    }
  }
}
