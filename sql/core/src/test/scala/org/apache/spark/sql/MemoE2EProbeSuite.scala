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

package org.apache.spark.sql

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class MemoE2EProbeSuite extends QueryTest with SharedSparkSession {

  // Runs `f` three ways: interpreted, codegen without whole-stage, and whole-stage codegen.
  private def eachEvalPath(f: String => Unit): Unit = {
    withSQLConf(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN")(f("interpreted"))
    withSQLConf(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY")(f("codegen"))
    withSQLConf(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY")(f("wholestage"))
  }

  private def interpreted(f: => Unit): Unit = eachEvalPath(_ => f)

  test("probe e2e: monotonically_increasing_id BETWEEN inside a conditional branch") {
    interpreted {
      val df = spark.range(0, 10, 1, 1)
      val rows = df.selectExpr(
        "CASE WHEN id < 0 THEN false ELSE monotonically_increasing_id() BETWEEN 3 AND 5 END")
        .collect().map(_.getBoolean(0))
      // scalastyle:off println
      println(s"PROBE ids 3..5 -> ${rows.mkString(",")}")
      println(s"PROBE true count = ${rows.count(identity)} (correct is 3)")
      // scalastyle:on println
      assert(rows.count(identity) == 3)
      assert(rows.toSeq == (0 until 10).map(i => i >= 3 && i <= 5))
    }
  }

  test("probe e2e: the counter only advances on rows that reach the branch") {
    interpreted {
      val df = spark.range(0, 10, 1, 1)
      // Rows 0-4 take the first branch, so the five rows that reach the ELSE see ids 0,1,2,3,4 --
      // exactly as they would if the branch were the whole expression.
      val rows = df.selectExpr(
        "CASE WHEN id < 5 THEN NULL ELSE monotonically_increasing_id() BETWEEN 1 AND 2 END")
        .collect().map(r => if (r.isNullAt(0)) None else Some(r.getBoolean(0)))
      // scalastyle:off println
      println(s"PROBE per-branch counter -> ${rows.mkString(",")}")
      // scalastyle:on println
      assert(rows.toSeq == (0 until 10).map { i =>
        if (i < 5) None else Some(i - 5 >= 1 && i - 5 <= 2)
      })
    }
  }

  test("probe e2e: a branch condition that can raise no longer matters") {
    interpreted {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        val df = spark.range(0, 6, 1, 1).selectExpr("cast(id as int) - 2 as a")
        // The guard approach needed this condition excluded, because repeating `6 / a` in a project
        // raised DIVIDE_BY_ZERO on the row where `a` is 0. Memoization puts nothing anywhere, so
        // the condition is evaluated only where it always was.
        val query = "CASE WHEN a = 0 THEN false " +
          "WHEN 6 / a > 2 THEN rand(1) BETWEEN 0 AND 1 " +
          "WHEN 6 / a < -2 THEN rand(2) BETWEEN 0 AND 1 " +
          "ELSE false END"
        checkAnswer(df.selectExpr(query),
          Seq(Row(true), Row(true), Row(false), Row(true), Row(true), Row(false)))
        // scalastyle:off println
        println("PROBE raising condition: no DIVIDE_BY_ZERO, answers correct")
        // scalastyle:on println
      }
    }
  }

  test("probe e2e: the shapes the allowlist turned down are now correct") {
    interpreted {
      val df = spark.range(0, 6, 1, 1)
      // `randstr(3, 0)` was refused by the allowlist for being a BinaryLike, so it kept the old
      // inlining and the old wrong result: the two comparisons of BETWEEN saw two different strings.
      val betweenRandstr = df.selectExpr(
        "CASE WHEN id < 0 THEN false ELSE randstr(3, 0) BETWEEN 'a' AND 'zzzz' END")
        .collect().map(_.getBoolean(0))
      // A memoized definition gives both comparisons one string, so each row is a real BETWEEN of
      // one value rather than a comparison of two draws.
      val oneValuePerRow = df.selectExpr("randstr(3, 0) BETWEEN 'a' AND 'zzzz'")
        .collect().map(_.getBoolean(0))
      // scalastyle:off println
      println(s"PROBE randstr in branch  = ${betweenRandstr.mkString(",")}")
      println(s"PROBE randstr no branch  = ${oneValuePerRow.mkString(",")}")
      // scalastyle:on println
      assert(betweenRandstr.toSeq == oneValuePerRow.toSeq,
        "a randstr BETWEEN in a branch must agree with the same expression outside one")

      // `rand() / a` -- a nondeterministic expression wrapped in arithmetic, also refused.
      val wrapped = df.selectExpr(
        "CASE WHEN id < 0 THEN false ELSE (rand(7) / 1.0) BETWEEN 0 AND 1 END")
        .collect().map(_.getBoolean(0))
      // scalastyle:off println
      println(s"PROBE rand()/1.0 in branch = ${wrapped.mkString(",")} (all true iff memoized)")
      // scalastyle:on println
      assert(wrapped.forall(identity), "rand()/1.0 must be in [0,1) for both references")
    }
  }
}
