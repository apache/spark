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

import org.apache.logging.log4j.Level

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.test.SharedSparkSession

class CTEHintSuite extends QueryTest with SharedSparkSession {

  def verifyCoalesceOrRepartitionHint(df: DataFrame): Unit = {
    def checkContainsRepartition(plan: LogicalPlan): Unit = {
      val repartitions = plan collect {
        case r: Repartition => r
        case r: RepartitionByExpression => r
      }
      assert(repartitions.nonEmpty)
    }
    val analyzed = df.queryExecution.analyzed
    val optimized = df.queryExecution.optimizedPlan
    checkContainsRepartition(analyzed)
    checkContainsRepartition(optimized)
    optimized collect {
      case _: ResolvedHint => fail("ResolvedHint should not appear after optimize.")
    }
  }

  def verifyJoinHint(df: DataFrame, expectedHints: Seq[JoinHint]): Unit = {
    val analyzed = df.queryExecution.analyzed
    val resolvedHints = analyzed collect {
      case r: ResolvedHint => r
    }
    assert(resolvedHints.nonEmpty)
    val optimized = df.queryExecution.optimizedPlan
    val joinHints = optimized collect {
      case Join(_, _, _, _, hint) => hint
      case _: ResolvedHint => fail("ResolvedHint should not appear after optimize.")
    }
    assert(joinHints == expectedHints)
  }

  def verifyJoinHintWithWarnings(
      df: => DataFrame,
      expectedHints: Seq[JoinHint],
      warnings: Seq[String]): Unit = {
    val logAppender = new LogAppender("join hints")
    withLogAppender(logAppender) {
      verifyJoinHint(df, expectedHints)
    }
    val warningMessages = logAppender.loggingEvents
      .filter(_.getLevel == Level.WARN)
      .map(_.getMessage.getFormattedMessage)
      .filter(_.contains("hint"))
    assert(warningMessages.size == warnings.size)
    warnings.foreach { w =>
      assert(warningMessages.contains(w))
    }
  }

  def msgNoJoinForJoinHint(strategy: String): String =
    s"A join hint (strategy=$strategy) is specified but it is not part of a join relation."

  def msgJoinHintOverridden(strategy: String): String =
    s"Hint (strategy=$strategy) is overridden by another hint and will not take effect."

  test("Resolve coalesce hint in CTE") {
    // COALESCE,
    // REPARTITION,
    // REPARTITION_BY_RANGE
    withTable("t") {
      sql("CREATE TABLE t USING PARQUET AS SELECT 1 AS id")
      verifyCoalesceOrRepartitionHint(
        sql("WITH cte AS (SELECT /*+ COALESCE(1) */ * FROM t) SELECT * FROM cte"))
      verifyCoalesceOrRepartitionHint(
        sql("WITH cte AS (SELECT /*+ REPARTITION(3) */ * FROM t) SELECT * FROM cte"))
      verifyCoalesceOrRepartitionHint(
        sql("WITH cte AS (SELECT /*+ REPARTITION(id) */ * FROM t) SELECT * FROM cte"))
      verifyCoalesceOrRepartitionHint(
        sql("WITH cte AS (SELECT /*+ REPARTITION(3, id) */ * FROM t) SELECT * FROM cte"))
      verifyCoalesceOrRepartitionHint(
        sql("WITH cte AS (SELECT /*+ REPARTITION_BY_RANGE(id) */ * FROM t) SELECT * FROM cte"))
      verifyCoalesceOrRepartitionHint(
        sql("WITH cte AS (SELECT /*+ REPARTITION_BY_RANGE(3, id) */ * FROM t) SELECT * FROM cte"))
    }
  }

  test("Resolve join hint in CTE") {
    // BROADCAST,
    // SHUFFLE_MERGE,
    // SHUFFLE_HASH,
    // SHUFFLE_REPLICATE_NL
    withTable("t", "s") {
      sql("CREATE TABLE a USING PARQUET AS SELECT 1 AS a1")
      sql("CREATE TABLE b USING PARQUET AS SELECT 1 AS b1")
      sql("CREATE TABLE c USING PARQUET AS SELECT 1 AS c1")
      verifyJoinHint(
        sql(
          """
            |WITH cte AS (
            |  SELECT /*+ BROADCAST(a) */ * FROM a JOIN b ON a.a1 = b.b1
            |)
            |SELECT * FROM cte
            |""".stripMargin),
        JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil
      )
      verifyJoinHint(
        sql(
          """
            |WITH cte AS (
            |  SELECT /*+ SHUFFLE_HASH(a) */ * FROM a JOIN b ON a.a1 = b.b1
            |)
            |SELECT * FROM cte
            |""".stripMargin),
        JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_HASH))),
          None) :: Nil
      )
      verifyJoinHintWithWarnings(
        sql(
          """
            |WITH cte AS (
            |  SELECT /*+ SHUFFLE_HASH MERGE(a, c) BROADCAST(a, b)*/ * FROM a, b, c
            |  WHERE a.a1 = b.b1 AND b.b1 = c.c1
            |)
            |SELECT * FROM cte
            |""".stripMargin),
        JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE)))) ::
          JoinHint(
            Some(HintInfo(strategy = Some(SHUFFLE_MERGE))),
            Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil,
          msgNoJoinForJoinHint("shuffle_hash") ::
            msgJoinHintOverridden("broadcast") :: Nil
      )
      verifyJoinHint(
        sql(
          """
            |WITH cte AS (
            |  SELECT /*+ SHUFFLE_REPLICATE_NL(a) SHUFFLE_HASH(b) */ * FROM a JOIN b ON a.a1 = b.b1
            |)
            |SELECT * FROM cte
            |""".stripMargin),
        JoinHint(
          Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL))),
          Some(HintInfo(strategy = Some(SHUFFLE_HASH)))) :: Nil
      )
    }
  }
}
