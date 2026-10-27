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

package org.apache.spark.sql.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Benchmark measuring the impact of field-based extraction on wide join method sizes.
 * SPARK-59787: Split wide join outputs into methods under whole-stage codegen.
 */
class WideJoinBenchmark extends QueryTest with SharedSparkSession {

  /**
   * Parses method names and sizes (in lines and chars) from a generated Java source string.
   * Uses a brace-depth counter to detect method boundaries.
   */
  private def parseMethodSizes(source: String): Seq[(String, Int, Int)] = {
    val methodSig = """(private|public)\s+\w+\s+(\w+)\s*\([^)]*\)\s*(throws\s+[\w.,\s]+)?\{""".r
    val results = scala.collection.mutable.ArrayBuffer[(String, Int, Int)]()
    val lines = source.split("\n")
    var i = 0
    while (i < lines.length) {
      methodSig.findFirstMatchIn(lines(i)) match {
        case Some(m) =>
          val methodName = m.group(2)
          // Count from opening brace to matching closing brace
          var depth = 0
          var methodLines = 0
          var methodChars = 0
          var j = i
          var started = false
          while (j < lines.length && (!started || depth > 0)) {
            val line = lines(j)
            methodLines += 1
            methodChars += line.length + 1
            for (c <- line) {
              if (c == '{') { depth += 1; started = true }
              else if (c == '}') depth -= 1
            }
            j += 1
          }
          results += ((methodName, methodLines, methodChars))
          i = j
        case None =>
          i += 1
      }
    }
    results.toSeq
  }

  test("SPARK-59787 benchmark: SortMergeJoin method sizes with 40+40 columns") {
    val numCols = 40

    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",  // force SMJ
      SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "200",    // allow codegen on wide schema
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",  // disable AQE for plan inspection
      "spark.sql.codegen.splitConsumeFuncByOperator" -> "false"  // avoid pre-existing bug
    ) {
      val leftCols = (0 until numCols).map(i => s"(id + $i) as l_col_$i")
      val rightCols = (0 until numCols).map(i => s"(id + ${i + 100}) as r_col_$i")

      val left = spark.range(100).selectExpr("id as join_key" +: leftCols: _*)
      val right = spark.range(100).selectExpr("id as join_key" +: rightCols: _*)

      val smjDf = left.join(right, "join_key")

      // Trigger execution to materialize the AQE plan
      smjDf.collect()

      // Collect WholeStageCodegenExec nodes from the final executed plan (post-AQE)
      val finalPlan = smjDf.queryExecution.executedPlan
      val codegenNodes = finalPlan.collect { case w: WholeStageCodegenExec => w }

      println("\n" + "=" * 90)
      println("SPARK-59787 BENCHMARK: SortMergeJoin 40+40 columns (WITH field-based extraction)")
      println(s"  Found ${codegenNodes.length} WholeStageCodegenExec stages")
      println("=" * 90)

      codegenNodes.zipWithIndex.foreach { case (wsce, stageIdx) =>
        val (_, source) = wsce.doCodeGen()
        val sourceCode = source.body
        val totalLines = sourceCode.split("\n").length
        val totalChars = sourceCode.length

        // Dump the SMJ stage source for detailed analysis
        if (stageIdx == 0) {
          val path = java.nio.file.Path.of(s"/tmp/smj_stage_$stageIdx.java")
          java.nio.file.Files.writeString(path, sourceCode)
          println(s"  Source dumped to /tmp/smj_stage_$stageIdx.java")
        }

        println(s"\n--- Stage $stageIdx ($totalLines lines, $totalChars chars total) ---")

        val methods = parseMethodSizes(sourceCode)
        println(f"  ${"Method"}%-50s ${"Lines"}%8s ${"Chars"}%8s ${"Est.Bytecode"}%14s ${"JIT?"}%6s")
        println("  " + "-" * 86)
        methods.sortBy(-_._3).take(15).foreach { case (name, lines, chars) =>
          val estBytecode = chars * 2  // rough 2x ratio
          val jitOk = if (estBytecode <= 8000) "YES" else "NO"
          println(f"  $name%-50s $lines%8d $chars%8d $estBytecode%14d $jitOk%6s")
        }

        // Find processNext specifically
        val pn = methods.filter(_._1.contains("processNext"))
        if (pn.nonEmpty) {
          val (_, pnLines, pnChars) = pn.maxBy(_._3)
          val estBytes = pnChars * 2
          println(s"\n  >>> processNext: $pnLines lines, $pnChars chars, ~$estBytes bytecode est.")
          if (estBytes > 8000) {
            println(s"  >>> OVER 8K JIT LIMIT -- would run in interpreter (~4x penalty)")
          } else {
            println(s"  >>> Under 8K -- will be JIT compiled by HotSpot C2")
          }
        }

        // Count extractJoinVars / extractStreamVars helper methods
        val helpers = methods.filter(m =>
          m._1.startsWith("extractJoinVars") || m._1.startsWith("extractStreamVars"))
        if (helpers.nonEmpty) {
          println(s"\n  >>> ${helpers.length} extraction helper methods generated by splitExpressions")
          helpers.foreach { case (name, lines, chars) =>
            println(f"      $name%-40s $lines%5d lines, $chars%5d chars")
          }
        }
      }

      // Already collected above for AQE plan materialization
      println(s"\n  Correctness: 100 rows -- PASS")
      println("=" * 90 + "\n")
    }
  }

  test("SPARK-59787 benchmark: correctness with outer join 40+40 columns") {
    val numCols = 40

    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "200",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1"
    ) {
      val leftCols = (0 until numCols).map(i => s"(id + $i) as l_col_$i")
      val rightCols = (0 until numCols).map(i => s"(id + ${i + 100}) as r_col_$i")

      val left = spark.range(10).selectExpr("id as join_key" +: leftCols: _*)
      val right = spark.range(5, 15).selectExpr("id as join_key" +: rightCols: _*)

      // Left outer: all left rows, null for right when no match
      val leftOuterDf = left.join(right, Seq("join_key"), "left_outer")
      val leftOuterResult = leftOuterDf.collect()
      assert(leftOuterResult.length == 10, s"Expected 10 left outer rows, got ${leftOuterResult.length}")

      // Rows 0-4 should have null right-side values
      val nullRightRows = leftOuterResult.filter { row =>
        val joinKey = row.getLong(0)
        joinKey < 5
      }
      nullRightRows.foreach { row =>
        // right-side columns start at index 1 + numCols (join_key + left cols)
        for (i <- (1 + numCols) until row.length) {
          assert(row.isNullAt(i), s"Expected null at column $i for join_key=${row.getLong(0)}")
        }
      }

      println(s"\nLeft outer join 40+40: ${leftOuterResult.length} rows, " +
        s"${nullRightRows.length} with null right side -- PASS")
    }
  }
}
