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

package org.apache.spark.sql.catalyst.analysis

import org.apache.logging.log4j.Level

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, Literal, SortOrder}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class ResolveHintsSuite extends AnalysisTest {
  import org.apache.spark.sql.catalyst.analysis.TestRelations._

  test("invalid hints should be ignored") {
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("some_random_hint_that_does_not_exist", Seq("TaBlE"), table("TaBlE")),
      testRelation,
      caseSensitive = false)
  }

  test("case-sensitive or insensitive parameters") {
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("MAPJOIN", Seq("TaBlE"), table("TaBlE")),
      ResolvedHint(testRelation, HintInfo(strategy = Some(BROADCAST))),
      caseSensitive = false)

    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("MAPJOIN", Seq("table"), table("TaBlE")),
      ResolvedHint(testRelation, HintInfo(strategy = Some(BROADCAST))),
      caseSensitive = false)

    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("MAPJOIN", Seq("TaBlE"), table("TaBlE")),
      ResolvedHint(testRelation, HintInfo(strategy = Some(BROADCAST))),
      caseSensitive = true)

    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("MAPJOIN", Seq("table"), table("TaBlE")),
      testRelation,
      caseSensitive = true)
  }

  test("multiple broadcast hint aliases") {
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("MAPJOIN", Seq("table", "table2"), table("table").join(table("table2"))),
      Join(ResolvedHint(testRelation, HintInfo(strategy = Some(BROADCAST))),
        ResolvedHint(testRelation2, HintInfo(strategy = Some(BROADCAST))),
        Inner, None, JoinHint.NONE),
      caseSensitive = false)
  }

  test("do not traverse past existing broadcast hints") {
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("MAPJOIN", Seq("table"),
        ResolvedHint(table("table").where('a > 1), HintInfo(strategy = Some(BROADCAST)))),
      ResolvedHint(testRelation.where('a > 1), HintInfo(strategy = Some(BROADCAST))).analyze,
      caseSensitive = false)
  }

  test("should work for subqueries") {
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("MAPJOIN", Seq("tableAlias"), table("table").as("tableAlias")),
      ResolvedHint(testRelation, HintInfo(strategy = Some(BROADCAST))),
      caseSensitive = false)

    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("MAPJOIN", Seq("tableAlias"), table("table").subquery('tableAlias)),
      ResolvedHint(testRelation, HintInfo(strategy = Some(BROADCAST))),
      caseSensitive = false)

    // Negative case: if the alias doesn't match, don't match the original table name.
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("MAPJOIN", Seq("table"), table("table").as("tableAlias")),
      testRelation,
      caseSensitive = false)
  }

  test("do not traverse past subquery alias") {
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("MAPJOIN", Seq("table"), table("table").where('a > 1).subquery('tableAlias)),
      testRelation.where('a > 1).analyze,
      caseSensitive = false)
  }

  test("should work for CTE") {
    checkAnalysisWithoutViewWrapper(
      CatalystSqlParser.parsePlan(
        """
          |WITH ctetable AS (SELECT * FROM table WHERE a > 1)
          |SELECT /*+ BROADCAST(ctetable) */ * FROM ctetable
        """.stripMargin
      ),
      ResolvedHint(testRelation.where('a > 1).select('a), HintInfo(strategy = Some(BROADCAST)))
        .select('a).analyze,
      caseSensitive = false,
      inlineCTE = true)
  }

  test("should not traverse down CTE") {
    checkAnalysisWithoutViewWrapper(
      CatalystSqlParser.parsePlan(
        """
          |WITH ctetable AS (SELECT * FROM table WHERE a > 1)
          |SELECT /*+ BROADCAST(table) */ * FROM ctetable
        """.stripMargin
      ),
      testRelation.where('a > 1).select('a).select('a).analyze,
      caseSensitive = false,
      inlineCTE = true)
  }

  test("coalesce and repartition hint") {
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("COALESCE", Seq(Literal(10)), table("TaBlE")),
      Repartition(numPartitions = 10, shuffle = false, child = testRelation))
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("coalesce", Seq(Literal(20)), table("TaBlE")),
      Repartition(numPartitions = 20, shuffle = false, child = testRelation))
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("REPARTITION", Seq(Literal(100)), table("TaBlE")),
      Repartition(numPartitions = 100, shuffle = true, child = testRelation))
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("RePARTITion", Seq(Literal(200)), table("TaBlE")),
      Repartition(numPartitions = 200, shuffle = true, child = testRelation))

    val errMsg = "COALESCE Hint expects a partition number as a parameter"

    assertAnalysisError(
      UnresolvedHint("COALESCE", Seq.empty, table("TaBlE")),
      Seq(errMsg))
    assertAnalysisError(
      UnresolvedHint("COALESCE", Seq(Literal(10), Literal(false)), table("TaBlE")),
      Seq(errMsg))
    assertAnalysisError(
      UnresolvedHint("COALESCE", Seq(Literal(1.0)), table("TaBlE")),
      Seq(errMsg))

    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("RePartition", Seq(Literal(10), UnresolvedAttribute("a")), table("TaBlE")),
      RepartitionByExpression(Seq(AttributeReference("a", IntegerType)()), testRelation, 10))

    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("REPARTITION", Seq(Literal(10), UnresolvedAttribute("a")), table("TaBlE")),
      RepartitionByExpression(Seq(AttributeReference("a", IntegerType)()), testRelation, 10))

    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("REPARTITION", Seq(UnresolvedAttribute("a")), table("TaBlE")),
      RepartitionByExpression(
        Seq(AttributeReference("a", IntegerType)()), testRelation, None))

    val e = intercept[AnalysisException] {
      checkAnalysis(
        UnresolvedHint("REPARTITION",
          Seq(SortOrder(AttributeReference("a", IntegerType)(), Ascending)),
          table("TaBlE")),
        RepartitionByExpression(
          Seq(SortOrder(AttributeReference("a", IntegerType)(), Ascending)), testRelation, 10)
      )
    }
    e.getMessage.contains("For range partitioning use REPARTITION_BY_RANGE instead")

    checkAnalysisWithoutViewWrapper(
      UnresolvedHint(
        "REPARTITION_BY_RANGE", Seq(Literal(10), UnresolvedAttribute("a")), table("TaBlE")),
      RepartitionByExpression(
        Seq(SortOrder(AttributeReference("a", IntegerType)(), Ascending)), testRelation, 10))

    checkAnalysisWithoutViewWrapper(
      UnresolvedHint(
        "REPARTITION_BY_RANGE", Seq(UnresolvedAttribute("a")), table("TaBlE")),
      RepartitionByExpression(
        Seq(SortOrder(AttributeReference("a", IntegerType)(), Ascending)),
        testRelation, None))

    val errMsg2 = "REPARTITION Hint parameter should include columns, but"

    assertAnalysisError(
      UnresolvedHint("REPARTITION", Seq(Literal(true)), table("TaBlE")),
      Seq(errMsg2))

    assertAnalysisError(
      UnresolvedHint("REPARTITION",
        Seq(Literal(1.0), AttributeReference("a", IntegerType)()),
        table("TaBlE")),
      Seq(errMsg2))

    val errMsg3 = "REPARTITION_BY_RANGE Hint parameter should include columns, but"

    assertAnalysisError(
      UnresolvedHint("REPARTITION_BY_RANGE",
        Seq(Literal(1.0), AttributeReference("a", IntegerType)()),
        table("TaBlE")),
      Seq(errMsg3))

    assertAnalysisError(
      UnresolvedHint("REPARTITION_BY_RANGE",
        Seq(Literal(10), Literal(10)),
        table("TaBlE")),
      Seq(errMsg3))

    assertAnalysisError(
      UnresolvedHint("REPARTITION_BY_RANGE",
        Seq(Literal(10), Literal(10), UnresolvedAttribute("a")),
        table("TaBlE")),
      Seq(errMsg3))
  }

  test("log warnings for invalid hints") {
    val logAppender = new LogAppender("invalid hints")
    withLogAppender(logAppender) {
      checkAnalysisWithoutViewWrapper(
        UnresolvedHint("unknown_hint", Seq("TaBlE"), table("TaBlE")),
        testRelation,
        caseSensitive = false)
    }
    assert(logAppender.loggingEvents.exists(
      e => e.getLevel == Level.WARN &&
        e.getMessage.getFormattedMessage.contains("Unrecognized hint: unknown_hint")))
  }

  test("SPARK-30003: Do not throw stack overflow exception in non-root unknown hint resolution") {
    checkAnalysisWithoutViewWrapper(
      Project(testRelation.output, UnresolvedHint("unknown_hint", Seq("TaBlE"), table("TaBlE"))),
      Project(testRelation.output, testRelation),
      caseSensitive = false)
  }

  test("Supports multi-part table names for join strategy hint resolution") {
    Seq(("MAPJOIN", BROADCAST),
        ("MERGEJOIN", SHUFFLE_MERGE),
        ("SHUFFLE_HASH", SHUFFLE_HASH),
        ("SHUFFLE_REPLICATE_NL", SHUFFLE_REPLICATE_NL)).foreach { case (hintName, st) =>
      // local temp table (single-part identifier case)
      checkAnalysisWithoutViewWrapper(
        UnresolvedHint(hintName, Seq("table", "table2"),
          table("TaBlE").join(table("TaBlE2"))),
        Join(
          ResolvedHint(testRelation, HintInfo(strategy = Some(st))),
          ResolvedHint(testRelation2, HintInfo(strategy = Some(st))),
          Inner,
          None,
          JoinHint.NONE),
        caseSensitive = false)

      checkAnalysisWithoutViewWrapper(
        UnresolvedHint(hintName, Seq("TaBlE", "table2"),
          table("TaBlE").join(table("TaBlE2"))),
        Join(
          ResolvedHint(testRelation, HintInfo(strategy = Some(st))),
          testRelation2,
          Inner,
          None,
          JoinHint.NONE),
        caseSensitive = true)

      // global temp table (multi-part identifier case)
      checkAnalysisWithoutViewWrapper(
        UnresolvedHint(hintName, Seq("GlOBal_TeMP.table4", "table5"),
          table("global_temp", "table4").join(table("global_temp", "table5"))),
        Join(
          ResolvedHint(testRelation4, HintInfo(strategy = Some(st))),
          ResolvedHint(testRelation5, HintInfo(strategy = Some(st))),
          Inner,
          None,
          JoinHint.NONE),
        caseSensitive = false)

      checkAnalysisWithoutViewWrapper(
        UnresolvedHint(hintName, Seq("global_temp.TaBlE4", "table5"),
          table("global_temp", "TaBlE4").join(table("global_temp", "TaBlE5"))),
        Join(
          ResolvedHint(testRelation4, HintInfo(strategy = Some(st))),
          testRelation5,
          Inner,
          None,
          JoinHint.NONE),
        caseSensitive = true)
    }
  }

  test("SPARK-35786: Support optimize repartition by expression in AQE") {
    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("REBALANCE", Seq(UnresolvedAttribute("a")), table("TaBlE")),
      RebalancePartitions(Seq(AttributeReference("a", IntegerType)()), testRelation))

    checkAnalysisWithoutViewWrapper(
      UnresolvedHint("REBALANCE", Seq.empty, table("TaBlE")),
      RebalancePartitions(Seq.empty, testRelation))

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      checkAnalysisWithoutViewWrapper(
        UnresolvedHint("REBALANCE", Seq(UnresolvedAttribute("a")), table("TaBlE")),
        testRelation)

      checkAnalysisWithoutViewWrapper(
        UnresolvedHint("REBALANCE", Seq.empty, table("TaBlE")),
        testRelation)
    }

    assertAnalysisError(
      UnresolvedHint("REBALANCE", Seq(Literal(1)), table("TaBlE")),
      Seq("Hint parameter should include columns"))
  }
}
