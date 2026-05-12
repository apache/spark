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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GreaterThan, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.MergeRows
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Discard, Insert, Keep, Split, Update}
import org.apache.spark.sql.classic.{ClassicConversions, Dataset}
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Benchmark to measure performance for the MergeRowsExec codegen operator used in MERGE INTO.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/MergeRowsExecBenchmark-results.txt".
 * }}}
 */
object MergeRowsExecBenchmark extends SqlBasedBenchmark with ClassicConversions {

  private val N = 20 << 20

  /**
   * Creates a DataFrame simulating the join output from a MERGE operation.
   *
   * Schema: tgt_id, tgt_col1, tgt_col2, tgt_dep, src_id, src_col1, src_col2, src_dep,
   *         _source_marker, _target_marker
   */
  private def createJoinOutput(
      numRows: Long,
      matchedFraction: Double = 1.0,
      notMatchedFraction: Double = 0.0): DataFrame = {
    val matchEnd = (numRows * matchedFraction).toLong
    val notMatchedEnd = matchEnd + (numRows * notMatchedFraction).toLong
    val hasTarget = s"id < $matchEnd OR id >= $notMatchedEnd"
    val hasSource = s"id < $notMatchedEnd"

    spark.range(numRows).selectExpr(
      s"IF($hasTarget, CAST(id AS INT), null) as tgt_id",
      s"IF($hasTarget, CAST(id AS INT), null) as tgt_col1",
      s"IF($hasTarget, CAST(id AS INT), null) as tgt_col2",
      s"IF($hasTarget, 'hr', null) as tgt_dep",
      s"IF($hasSource, CAST(id AS INT), null) as src_id",
      s"IF($hasSource, CAST(id + 1000 AS INT), null) as src_col1",
      s"IF($hasSource, CAST(id + 1000 AS INT), null) as src_col2",
      s"IF($hasSource, 'hr', null) as src_dep",
      s"IF($hasSource, CAST(1 AS INT), null) as _source_marker",
      s"IF($hasTarget, CAST(1 AS INT), null) as _target_marker"
    )
  }

  private def outputAttrs: Seq[AttributeReference] = Seq(
    AttributeReference("id", IntegerType, nullable = true)(),
    AttributeReference("col1", IntegerType, nullable = true)(),
    AttributeReference("col2", IntegerType, nullable = true)(),
    AttributeReference("dep", StringType, nullable = true)()
  )

  private def buildMergeRowsDF(
      inputDF: DataFrame,
      matchedInstr: Seq[MergeRows.Instruction],
      notMatchedInstr: Seq[MergeRows.Instruction] = Seq.empty,
      notMatchedBySourceInstr: Seq[MergeRows.Instruction] = Seq.empty): DataFrame = {
    val inputPlan = inputDF.queryExecution.analyzed
    val attrs = inputPlan.output

    val mergeRows = MergeRows(
      isSourceRowPresent = IsNotNull(attrs.find(_.name == "_source_marker").get),
      isTargetRowPresent = IsNotNull(attrs.find(_.name == "_target_marker").get),
      matchedInstructions = matchedInstr,
      notMatchedInstructions = notMatchedInstr,
      notMatchedBySourceInstructions = notMatchedBySourceInstr,
      checkCardinality = false,
      output = outputAttrs,
      child = inputPlan
    )

    Dataset.ofRows(spark, mergeRows)
  }

  private def mergeMatchedUpdateOnly(): Unit = {
    val inputDF = createJoinOutput(N, matchedFraction = 1.0)
    val a = inputDF.queryExecution.analyzed.output

    // Update: keep target id and dep, take col1/col2 from source
    val matchedInstr = Seq(Keep(Update, TrueLiteral, Seq(
      a(0), a(5), a(6), a(3)
    )))

    codegenBenchmark("merge - matched update only", N) {
      val df = buildMergeRowsDF(inputDF, matchedInstr)
      assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[MergeRowsExec]))
      df.noop()
    }
  }

  private def mergeNotMatchedInsertOnly(): Unit = {
    val inputDF = createJoinOutput(N, matchedFraction = 0.0, notMatchedFraction = 1.0)
    val a = inputDF.queryExecution.analyzed.output

    // Insert: take all columns from source
    val notMatchedInstr = Seq(Keep(Insert, TrueLiteral, Seq(
      a(4), a(5), a(6), a(7)
    )))

    codegenBenchmark("merge - not matched insert only", N) {
      val df = buildMergeRowsDF(inputDF, Seq.empty, notMatchedInstr)
      assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[MergeRowsExec]))
      df.noop()
    }
  }

  private def mergeMatchedAndNotMatched(): Unit = {
    val inputDF = createJoinOutput(N, matchedFraction = 0.5, notMatchedFraction = 0.5)
    val a = inputDF.queryExecution.analyzed.output

    val matchedInstr = Seq(Keep(Update, TrueLiteral, Seq(
      a(0), a(5), a(6), a(3)
    )))
    val notMatchedInstr = Seq(Keep(Insert, TrueLiteral, Seq(
      a(4), a(5), a(6), a(7)
    )))

    codegenBenchmark("merge - matched update + not matched insert", N) {
      val df = buildMergeRowsDF(inputDF, matchedInstr, notMatchedInstr)
      assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[MergeRowsExec]))
      df.noop()
    }
  }

  private def mergeMatchedDelete(): Unit = {
    val inputDF = createJoinOutput(N, matchedFraction = 1.0)

    val matchedInstr = Seq(Discard(TrueLiteral))

    codegenBenchmark("merge - matched delete", N) {
      val df = buildMergeRowsDF(inputDF, matchedInstr)
      assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[MergeRowsExec]))
      df.noop()
    }
  }

  private def mergeConditionalClauses(): Unit = {
    val inputDF = createJoinOutput(N, matchedFraction = 0.5, notMatchedFraction = 0.5)
    val a = inputDF.queryExecution.analyzed.output

    // Matched: update if src_col1 > 500, otherwise delete
    val matchedInstr = Seq(
      Keep(Update, GreaterThan(a(5), Literal(500)), Seq(a(0), a(5), a(6), a(3))),
      Discard(TrueLiteral)
    )
    // Not matched: insert if src_col1 > 500
    val notMatchedInstr = Seq(
      Keep(Insert, GreaterThan(a(5), Literal(500)), Seq(a(4), a(5), a(6), a(7)))
    )

    codegenBenchmark("merge - conditional clauses", N) {
      val df = buildMergeRowsDF(inputDF, matchedInstr, notMatchedInstr)
      assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[MergeRowsExec]))
      df.noop()
    }
  }

  private def mergeAllThreeClauses(): Unit = {
    // 1/3 matched, 1/3 not matched, 1/3 not matched by source
    val inputDF = createJoinOutput(N,
      matchedFraction = 1.0 / 3,
      notMatchedFraction = 1.0 / 3)
    val a = inputDF.queryExecution.analyzed.output

    val matchedInstr = Seq(Keep(Update, TrueLiteral, Seq(
      a(0), a(5), a(6), a(3)
    )))
    val notMatchedInstr = Seq(Keep(Insert, TrueLiteral, Seq(
      a(4), a(5), a(6), a(7)
    )))
    val notMatchedBySourceInstr = Seq(Discard(TrueLiteral))

    codegenBenchmark("merge - matched + not matched + not matched by source", N) {
      val df = buildMergeRowsDF(inputDF, matchedInstr, notMatchedInstr, notMatchedBySourceInstr)
      assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[MergeRowsExec]))
      df.noop()
    }
  }

  private def mergeSplitUpdate(): Unit = {
    val inputDF = createJoinOutput(N, matchedFraction = 1.0)
    val a = inputDF.queryExecution.analyzed.output

    // Split: update as delete + insert (delta-based merge)
    val matchedInstr = Seq(Split(TrueLiteral,
      Seq(a(0), a(1), a(2), a(3)),
      Seq(a(0), a(5), a(6), a(3))
    ))

    codegenBenchmark("merge - split update (delete + insert)", N) {
      val df = buildMergeRowsDF(inputDF, matchedInstr)
      assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[MergeRowsExec]))
      df.noop()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("MergeRowsExec Codegen Benchmark") {
      mergeMatchedUpdateOnly()
      mergeNotMatchedInsertOnly()
      mergeMatchedAndNotMatched()
      mergeMatchedDelete()
      mergeConditionalClauses()
      mergeAllThreeClauses()
      mergeSplitUpdate()
    }
  }
}
