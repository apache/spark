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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, LessThan, Literal, Rand}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for streamed-side join condition hoisting in whole-stage codegen. The
 * generated-code shape under test requires all of the following:
 *  - spark.sql.join.splitStreamedSideJoinCondition = true
 *  - a join type that preserves streamed rows (LeftOuter, RightOuter, LeftAnti, ExistenceJoin)
 *  - a broadcast hash join, so the scan and the join share one whole-stage. Sort-merge and
 *    shuffled hash joins are out of scope here: their streamed side crosses a Sort or an
 *    exchange, which advances its cursor before running the inlined consume code, and the
 *    sort-merge guard is a standalone pre-loop guard that emits and continues before the
 *    match loop; it is not folded into the match condition. The relevant contrast is that
 *    it uses continue in its own loop instead of returning from an inlined consumer.
 *    ExistenceJoinSuite covers those join implementations functionally, with the
 *    config on and off.
 *  - a batching streamed producer (vectorized parquet scan -> ColumnarToRowExec) whose loop
 *    cursor is only written back after the inlined consume code
 *  - a streamed column (pad) not referenced by the join keys/condition, so
 *    WholeStageCodegenExec.consume does not wrap the join's doConsume in a function
 *  - a residual filter that references pad (pad % 2 = 0 is not pushed into the scan), so pad
 *    is materialized before the join's doConsume and the generated code compiles
 *
 * Expected results are produced by running the same query with the hoisting disabled. The
 * LIMIT below ORDER BY on the hoisting-enabled run is load-bearing: an early-returning guard
 * makes the generated code reprocess the same batch forever; the limit is what makes the
 * query terminate (with wrong results).
 */
class SplitStreamedSideJoinConditionSuite extends QueryTest with SharedSparkSession {
  import testImplicits.toRichColumn

  // streamed_t: (id, a, pad) with id 0..7, a = id, pad = id * 10
  // build_t:    (bid, b) with bid 0..3, b = bid * 100
  private def withStreamedAndBuildTables(f: => Unit): Unit = {
    withTempPath { path =>
      spark.range(8).selectExpr("id", "id AS a", "id * 10 AS pad")
        .write.parquet(path.getCanonicalPath + "/streamed")
      spark.range(4).selectExpr("id AS bid", "id * 100 AS b")
        .write.parquet(path.getCanonicalPath + "/build")
      spark.read.parquet(path.getCanonicalPath + "/streamed")
        .createOrReplaceTempView("streamed_t")
      spark.read.parquet(path.getCanonicalPath + "/build")
        .createOrReplaceTempView("build_t")
      f
    }
  }

  // Compares the query with hoisting enabled against the same query with hoisting disabled,
  // sorting both sides by all columns.
  private def checkDataFrame(query: => DataFrame): Unit = {
    val expected = withSQLConf(SQLConf.SPLIT_STREAMED_SIDE_JOIN_CONDITION.key -> "false") {
      sortByAllColumns(query).collect().toSeq
    }
    val df = query
    assert(df.queryExecution.executedPlan.toString.contains("BroadcastHashJoin"),
      s"expected a broadcast hash join in ${df.queryExecution.executedPlan}")
    checkAnswer(sortByAllColumns(df.limit(100)), expected)
  }

  private def sortByAllColumns(df: DataFrame): DataFrame = {
    val cols = df.columns.toSeq
    df.orderBy(cols.head, cols.tail: _*)
  }

  private def testWithCodegenOnAndOff(testName: String)(
      queryDf: => DataFrame,
      codegenOnly: Boolean = false): Unit = {
    testWithWholeStageCodegenOnAndOff(testName) { _ =>
      val confs = Seq(
        SQLConf.SPLIT_STREAMED_SIDE_JOIN_CONDITION.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") ++
        (if (codegenOnly) Seq(SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY") else Nil)
      withSQLConf(confs: _*) {
        withStreamedAndBuildTables {
          checkDataFrame(queryDf)
        }
      }
    }
  }

  // Streamed-only conjuncts that can throw or are non-deterministic must stay in the
  // residual: hoisting evaluates the hoisted part for every streamed row, including rows
  // that have no buffered match and would never evaluate the conjunct otherwise.

  test("split does not hoist throwable conjuncts") {
    val streamed = spark.range(8).selectExpr("id", "id AS a").queryExecution.executedPlan
    val a = streamed.output.find(_.name == "a").get
    val hoistable = LessThan(a, Literal(5L))
    val throwableConjunct = TestThrowableUDF(LessThan(a, Literal(4L)))
    val (streamedOnly, rest) = StreamedSideJoinCondition.split(
      Some(And(hoistable, throwableConjunct)), LeftOuter, streamed, splitEnabled = true)
    assert(streamedOnly.contains(hoistable))
    assert(rest.contains(throwableConjunct))
  }

  test("split does not hoist non-deterministic conjuncts") {
    val streamed = spark.range(8).selectExpr("id", "id AS a").queryExecution.executedPlan
    val a = streamed.output.find(_.name == "a").get
    val hoistable = LessThan(a, Literal(5L))
    val nonDeterministic = LessThan(Rand(Literal(0L)), Literal(0.5))
    val (streamedOnly, rest) = StreamedSideJoinCondition.split(
      Some(And(hoistable, nonDeterministic)), LeftOuter, streamed, splitEnabled = true)
    assert(streamedOnly.contains(hoistable))
    assert(rest.contains(nonDeterministic))
  }

  // The streamed-only condition a < 5 is false for ids 5..7, so the hoisted guard fires on
  // those rows. pad is selected but not referenced by the join.

  testWithCodegenOnAndOff(
    "left outer join, unique build key")(
    spark.sql("""
      |SELECT /*+ BROADCAST(b) */ s.id, s.a, s.pad, b.bid
      |FROM (SELECT * FROM streamed_t WHERE pad % 2 = 0) s
      |LEFT OUTER JOIN (SELECT DISTINCT bid FROM build_t) b
      |ON s.id = b.bid AND s.a < 5
    """.stripMargin))

  testWithCodegenOnAndOff(
    "left outer join, non-unique build key")(
    spark.sql("""
      |SELECT /*+ BROADCAST(b) */ s.id, s.a, s.pad, b.bid, b.b
      |FROM (SELECT * FROM streamed_t WHERE pad % 2 = 0) s
      |LEFT OUTER JOIN (SELECT bid, b FROM build_t UNION ALL SELECT bid, b FROM build_t) b
      |ON s.id = b.bid AND s.a < 5
    """.stripMargin))

  // The "compiles without pre-materialized column" variants drop the residual filter, so pad
  // stays lazy through the join and is only materialized by the join's own consume code. They
  // exercise how the generated guard / probe code scopes that materialization: if pad's
  // declaration lands in a different (nested) scope than its uses, the generated code does
  // not compile. CODEGEN_ONLY turns the silent fallback to interpreted execution into an
  // error so these tests actually assert compilation succeeds.
  testWithCodegenOnAndOff(
    "left outer join compiles without pre-materialized column")(
    spark.sql("""
      |SELECT /*+ BROADCAST(b) */ s.id, s.a, s.pad, b.bid, b.b
      |FROM streamed_t s
      |LEFT OUTER JOIN build_t b
      |ON s.id = b.bid AND s.a < 5
    """.stripMargin),
    codegenOnly = true)

  testWithCodegenOnAndOff(
    "right outer join")(
    spark.sql("""
      |SELECT /*+ BROADCAST(b) */ b.bid, b.b, s.id, s.a, s.pad
      |FROM build_t b
      |RIGHT OUTER JOIN (SELECT * FROM streamed_t WHERE pad % 2 = 0) s
      |ON b.bid = s.id AND s.a < 5
    """.stripMargin))

  testWithCodegenOnAndOff(
    "left anti join")(
    spark.sql("""
      |SELECT /*+ BROADCAST(b) */ s.id, s.a, s.pad
      |FROM (SELECT * FROM streamed_t WHERE pad % 2 = 0) s
      |LEFT ANTI JOIN build_t b
      |ON s.id = b.bid AND s.a < 5
    """.stripMargin))

  testWithCodegenOnAndOff(
    "left anti join compiles without pre-materialized column")(
    spark.sql("""
      |SELECT /*+ BROADCAST(b) */ s.id, s.a, s.pad
      |FROM streamed_t s
      |LEFT ANTI JOIN build_t b
      |ON s.id = b.bid AND s.a < 5
    """.stripMargin),
    codegenOnly = true)

  // The EXISTS flag is projected (not filtered on) so guard-fired rows (ids 5..7) are
  // emitted via the guard path with e = false, making any replay of those rows visible as
  // duplicates. Filtering on EXISTS instead would drop them and mask a premature return in
  // the guard: the stage buffer stays empty, BufferedRowIterator.hasNext treats that as
  // end-of-stream, and since the abandoned rows produce no output anyway, the query would
  // return the right answer for the wrong reason.
  testWithCodegenOnAndOff(
    "existence join")(
    spark.sql("""
      |SELECT s.id, s.a, s.pad,
      |  EXISTS (SELECT /*+ BROADCAST(b) */ 1 FROM build_t b WHERE b.bid = s.id AND s.a < 5) AS e
      |FROM (SELECT * FROM streamed_t WHERE pad % 2 = 0) s
    """.stripMargin))

  testWithCodegenOnAndOff(
    "existence join compiles without pre-materialized column")(
    spark.sql("""
      |SELECT s.id, s.a, s.pad,
      |  EXISTS (SELECT /*+ BROADCAST(b) */ 1 FROM build_t b WHERE b.bid = s.id AND s.a < 5) AS e
      |FROM streamed_t s
    """.stripMargin),
    codegenOnly = true)

  // Left outer join whose ON clause has two streamed-side-only conjuncts: a hoistable
  // a < 5 and a throwable TestThrowableUDF(a < 4). The throwable conjunct holds for the
  // matched rows (ids 0..3) and throws for the unmatched ones (ids 4..7), so it throws
  // exactly when it is wrongly hoisted and evaluated before probing.
  private def throwableConjunctJoin: DataFrame = {
    val s = spark.table("streamed_t")
    val b = spark.table("build_t").hint("broadcast")
    val condition = And(
      EqualTo(s.col("id").expr, b.col("bid").expr),
      And(
        LessThan(s.col("a").expr, Literal(5L)),
        TestThrowableUDF(LessThan(s.col("a").expr, Literal(4L)))))
    val join = Join(s.logicalPlan, b.logicalPlan, LeftOuter, Some(condition), JoinHint.NONE)
    Dataset.ofRows(spark, join).select("id", "a", "bid", "b")
  }

  // End-to-end: the throwable conjunct must not run for streamed rows 4..7, which have no
  // buffered match. The hoistable conjunct a < 5 keeps the hoisted guard on the path so
  // the test proves the throwable conjunct is excluded from it, not that hoisting is off.
  // Before the fix, rows 4..7 evaluate the hoisted throwable conjunct and throw.
  testWithCodegenOnAndOff(
    "throwable streamed-side conjunct is not evaluated for unmatched rows")(
    throwableConjunctJoin)
}
