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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

class InferAntiJoinSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Batch] =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
        Batch("InferAntiJoin", Once,
          InferAntiJoin,
          PushDownLeftSemiAntiJoin,
          PushLeftSemiLeftAntiThroughJoin
        ) :: Nil
  }

  val testRelation = LocalRelation(Symbol("a").int, $"b".int, $"c".int)
  val testRelation1 = LocalRelation($"d".int, $"e".int, $"f".int)

  test("infer antijoin on plain col") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1, LeftOuter, Option("a".attr === "d".attr))
        .where($"d".isNull).select($"a", $"b", $"c").analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = testRelation.subquery("main")
        .join(testRelation1, LeftAnti, Option("a".attr === "d".attr))
        .select($"a", $"b", $"c").analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("infer antijoin on col w/function") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1, LeftOuter, Option("a".attr + 1 === "d".attr + 2))
        .where(($"d" + 2).isNull).select($"a", $"b", $"c").analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = testRelation.subquery("main")
        .join(testRelation1, LeftAnti, Option("a".attr + 1 === "d".attr + 2))
        .select($"a", $"b", $"c").analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("infer antijoin on plain col with qualifier") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1.subquery("rhs"), LeftOuter, Option("main.a".attr === "rhs.d".attr))
        .where(Symbol("rhs.d").isNull).select($"a", $"b", $"c").analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = testRelation.subquery("main")
        .join(testRelation1.subquery("rhs"), LeftAnti, Option("a".attr === "d".attr))
        .select($"a", $"b", $"c").analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("infer antijoin on aliased col") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1.select($"d".as("d1")), LeftOuter, Option("a".attr === "d1".attr))
        .where($"d1".isNull).select($"a", $"b", $"c").analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = testRelation.subquery("main")
        .join(testRelation1.select($"d".as("d1")), LeftAnti, Option("a".attr === "d1".attr))
        .select($"a", $"b", $"c").analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("infer antijoin on aliased col filter on other aliased col") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1.select($"d".as("d1"), $"d".as("d2"), $"e"),
          LeftOuter, Option("a".attr === "d1".attr))
        .where($"d2".isNull).select($"a", $"b", $"c").analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = testRelation.subquery("main")
        .join(testRelation1.select($"d".as("d1"), $"d".as("d2"), $"e"),
          LeftAnti, Option("a".attr === "d1".attr))
        .select($"a", $"b", $"c").analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("infer antijoin on aliased col filter on other plain col") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1.select($"d".as("d1"), $"d"),
          LeftOuter, Option("a".attr === "d1".attr))
        .where($"d".isNull).select($"a", $"b", $"c").analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = testRelation.subquery("main")
        .join(testRelation1.select($"d".as("d1"), $"d"),
          LeftAnti, Option("a".attr === "d1".attr))
        .select($"a", $"b", $"c").analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("infer antijoin on aliased col filter on other plain col with group by") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1.select($"d".as("d1"), $"d"),
          LeftOuter, Option("a".attr === "d1".attr))
        .where($"d".isNull)
        .groupBy($"b", $"c")($"b", max($"a").as("m"))
        .analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = testRelation.subquery("main")
        .join(testRelation1.select($"d".as("d1"), $"d"),
          LeftAnti, Option("a".attr === "d1".attr))
        .groupBy($"b", $"c")($"b", max($"a").as("m"))
        .analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("infer antijoin on col aliased in group by, filter by other alias") {
    // Aggregate that defines Alias can happen after CollapseProject
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1.groupBy($"d")($"d".as("d1"), $"d".as("d2")),
          LeftOuter, Option("a".attr === "d1".attr))
        .where($"d2".isNull).select($"b", $"c")
        .analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = testRelation.subquery("main")
        .join(testRelation1.groupBy($"d")($"d".as("d1"), $"d".as("d2")),
          LeftAnti, Option("a".attr === "d1".attr))
        .select($"b", $"c")
        .analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("Union: convert to LeftAnti join push through Union") {
    val testRelation2 = LocalRelation($"x".int, $"y".int, $"z".int)

    val originalQuery = Union(Seq(testRelation, testRelation2))
      .join(testRelation1, joinType = LeftOuter, condition = Some($"a" === $"d"))
      .where($"d".isNull).select($"b", $"c")

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = Union(Seq(
      testRelation.join(testRelation1, joinType = LeftAnti, condition = Some($"a" === $"d")),
      testRelation2.join(testRelation1, joinType = LeftAnti, condition = Some($"x" === $"d"))))
      .select($"b", $"c")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("infer antijoin on plain col with conjunction in ON clause") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1, LeftOuter, Option("a".attr === "d".attr && $"b" === $"e"))
        .where($"e".isNull)
        .select($"b")
        .analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = testRelation.subquery("main")
        .join(testRelation1, LeftAnti, Option("a".attr === "d".attr && $"b" === $"e"))
        .select($"b")
        .analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  // negative tests
  test("don't infer antijoin on plain col due to LHS output") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1, LeftOuter, Option("a".attr === "d".attr))
        .where($"d".isNull)
        .select($"b", $"e")
        .analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }

  test("don't infer antijoin on plain col due to LHS refs") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1, LeftOuter, Option("a".attr === "d".attr))
        .where($"d".isNull)
        .select($"b", ($"c" + $"e").as("sum_fcn"))
        .analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }

  test("don't infer antijoin on plain col due to not nullIntolerant function") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1, LeftOuter, Option("a".attr === "d".attr))
        .where("d".attr.isNull || $"e".isNull)
        .select($"b")
        .analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }

  // if a == d is a match, but e was null the isNull will allow it but antiJoin will not
  test("don't infer antijoin on plain col due to isNull on other col") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1, LeftOuter, Option("a".attr === "d".attr))
        .where($"e".isNull)
        .select($"b")
        .analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }

  // todo: could also be a valid rewrite
  test("don't infer antijoin on col w/function - doesn't match filter") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1, LeftOuter, Option("a".attr + 1 === "d".attr + 2))
        .where($"d".isNull).select($"a", $"b", $"c").analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }

  // todo: could be a valid rewrite
  test("don't infer antijoin on plain col with conjunction in ON clause and complex filter") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1, LeftOuter, Option("a".attr === "d".attr && $"b" === $"e"))
        .where($"e".isNull && $"d".isNull)
        .select($"b")
        .analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }

  test("don't infer antijoin due to a non-removable filter condition") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1, LeftOuter, Option("a".attr === "d".attr))
        .where($"d".isNull && ($"a" + $"b").isNull).select($"a", $"b", $"c").analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }

  test("don't infer antijoin on plain col with disjunction in ON clause") {
    withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> "true") {
      val originalQuery = testRelation.subquery("main")
        .join(testRelation1, LeftOuter, Option("a".attr === "d".attr || $"b" === $"e"))
        .where($"e".isNull)
        .select($"b")
        .analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }
}
