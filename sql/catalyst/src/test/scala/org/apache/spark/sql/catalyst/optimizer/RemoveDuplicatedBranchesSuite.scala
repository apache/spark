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

import java.util.TimeZone

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

class RemoveDuplicatedBranchesSuite
  extends PlanTest with ExpressionEvalHelper with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("RemoveDuplicatedBranches", FixedPoint(50),
      BooleanSimplification, SimplifyConditionals, RemoveDuplicatedBranches) :: Nil
  }

  private val relation = LocalRelation('a.int, 'b.string, 'c.boolean)
  private val a = EqualTo(UnresolvedAttribute("a"), Literal(100))
  private val b = EqualTo(UnresolvedAttribute("b"), Literal("b"))
  private val c = EqualTo(UnresolvedAttribute("c"), Literal(true))

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, relation).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, relation).analyze)
    comparePlans(actual, correctAnswer)
  }

  protected def assertEquivalent(e1: Expression, e2: Expression, relation: LocalRelation): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, relation).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, relation).analyze)
    comparePlans(actual, correctAnswer)
  }

  test("remove basic duplicate branches") {
    withSQLConf(SQLConf.DEDUP_CASE_WHEN_BRANCHES_ENABLED.key -> "true") {
      val caseWhen = CaseWhen(Seq((a, Literal(1)), (c, Literal(2))), Some(Literal(3)))
      val dupCaseWhen1 = CaseWhen(
        Seq((a, Literal(1)), (a, Literal(1)), (a, Literal(1)),
          (c, Literal(2)), (c, Literal(2)), (c, Literal(2))), Some(Literal(3))
      )
      val dupCaseWhen2 = CaseWhen(
        Seq((a, Literal(1)), (c, Literal(2)), (a, Literal(1)),
          (c, Literal(2)), (a, Literal(1)), (c, Literal(2))), Some(Literal(3))
      )
      assertEquivalent(EqualTo(dupCaseWhen1, Literal(4)), EqualTo(caseWhen, Literal(4)))
      assertEquivalent(EqualTo(dupCaseWhen2, Literal(4)), EqualTo(caseWhen, Literal(4)))
    }
  }

  test("remove duplicate branches with semanticEquals") {
    withSQLConf(SQLConf.DEDUP_CASE_WHEN_BRANCHES_ENABLED.key -> "true") {
      var caseWhen = CaseWhen(Seq((b, Literal("b"))))
      var dupCaseWhen = CaseWhen(Seq((b, Literal("b")),
        (EqualTo(UnresolvedAttribute("B").withName("b"), Literal("b")), Literal("b"))))
      assertEquivalent(EqualTo(dupCaseWhen, Literal("b")), EqualTo(caseWhen, Literal("b")))

      // with needTimeZone
      val literal = Literal(1)
      val cast = EqualTo(Cast(literal, LongType), Literal(100))
      val castWithTimeZoneId =
        EqualTo(Cast(literal, LongType, Some(TimeZone.getDefault.getID)), Literal(100))
      caseWhen = CaseWhen(Seq((cast, Literal(1))), Some(Literal(3)))
      dupCaseWhen = CaseWhen(
        Seq((cast, Literal(1)), (castWithTimeZoneId, Literal(1))), Some(Literal(3))
      )
      assertEquivalent(EqualTo(dupCaseWhen, Literal(4)), EqualTo(caseWhen, Literal(4)))
    }
  }
}
