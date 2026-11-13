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

import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.codegen.{ByteCodeStats, CodeAndComment, CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.execution.{InputAdapter, LocalTableScanExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, DisableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.debug
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

// Disable AQE because these suites only plan their queries, and the whole-stage codegen subtrees
// of an AdaptiveSparkPlanExec are created only when the query runs, so `checkGeneratedCode`
// would find none to check.
abstract class BenchmarkQueryTest
  extends QueryTest with SharedSparkSession with DisableAdaptiveExecutionSuite {

  // When Utils.isTesting is true, the RuleExecutor will issue an exception when hitting
  // the max iteration of analyzer/optimizer batches.
  assert(Utils.isTesting, s"${IS_TESTING.key} is not set to true")

  /**
   * Drop all the tables
   */
  protected override def afterAll(): Unit = {
    try {
      // For debugging dump some statistics about how much time was spent in various optimizer rules
      // code generation, and compilation.
      logWarning(RuleExecutor.dumpTimeSpent())
      val codeGenTime = WholeStageCodegenExec.codeGenTime.toDouble / NANOS_PER_SECOND
      val compileTime = CodeGenerator.compileTime.toDouble / NANOS_PER_SECOND
      val codegenInfo =
        s"""
           |=== Metrics of Whole-stage Codegen ===
           |Total code generation time: $codeGenTime seconds
           |Total compile time: $compileTime seconds
         """.stripMargin
      logWarning(codegenInfo)
      spark.sessionState.catalog.reset()
    } finally {
      super.afterAll()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    RuleExecutor.resetMetrics()
    CodeGenerator.resetCompileTime()
    WholeStageCodegenExec.resetCodeGenTime()
  }

  protected def checkGeneratedCode(plan: SparkPlan, checkMethodCodeSize: Boolean = true): Unit = {
    def failOnAdaptivePlan(plan: SparkPlan): Unit = plan foreach {
      case a: AdaptiveSparkPlanExec =>
        fail("the plan to check has an AdaptiveSparkPlanExec, whose whole-stage codegen " +
          s"subtrees are not created until the query runs:\n${a.treeString}")
      case s => s.subqueries.foreach(failOnAdaptivePlan)
    }

    failOnAdaptivePlan(plan)
    val stages = BenchmarkQueryTest.generatedCode(plan)
    assert(stages.nonEmpty,
      s"no WholeStageCodegenExec subtree found to check in the plan:\n${plan.treeString}")
    stages.foreach { case (subtree, code) =>
      val (_, ByteCodeStats(maxMethodCodeSize, _, _)) = try {
        // Just check the generated code can be properly compiled
        CodeGenerator.compile(code)
      } catch {
        case e: Exception =>
          val msg =
            s"""
               |failed to compile:
               |Subtree:
               |$subtree
               |Generated code:
               |${CodeFormatter.format(code)}
             """.stripMargin
          throw new Exception(msg, e)
      }

      assert(!checkMethodCodeSize ||
          maxMethodCodeSize <= CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT,
        s"too long generated codes found in the WholeStageCodegenExec subtree (id=${subtree.id}) " +
          s"and JIT optimization might not work:\n${subtree.treeString}")
    }
  }
}

object BenchmarkQueryTest {

  /**
   * The whole-stage codegen subtrees of `plan` and of its subqueries, in stage id order, each
   * with the code it generates for data (see [[withRowBroadcasts]]). The subtrees are the ones
   * `debug.codegenStringSeq` reports; for an adaptive plan that has not run there are none yet.
   */
  def generatedCode(plan: SparkPlan): Seq[(WholeStageCodegenExec, CodeAndComment)] = {
    // One replacement per broadcast exchange of the plan, shared by the subtrees that read it, so
    // each runs its one-row broadcast once.
    val replacements = new java.util.IdentityHashMap[BroadcastExchangeExec, BroadcastExchangeExec]
    debug.codegenSubtrees(plan).map { s =>
      s -> withRowBroadcasts(s, replacements).doCodeGen()._2
    }
  }

  /**
   * Returns a copy of a whole-stage codegen subtree in which every broadcast exchange reads one
   * row of non-null default values instead of its child, to generate the subtree's code from.
   *
   * The benchmark queries are planned over empty tables. A broadcast join generates its code
   * from the value of its broadcast, and for an empty one it generates a stub in place of the
   * join, which leaves the rest of the subtree ungenerated too. Over one row it generates the
   * code it generates for data. A single row makes every join key unique, so a hash join
   * generates its unique-key form, which is what a join on a dimension table's key generates;
   * a join whose build side repeats keys also has a loop over the matches, which is not
   * measured. Only the copy's code is used: a build side is a subtree of its own, found and
   * measured in the original plan.
   *
   * Fails if a broadcast join of the copy still reads anything but such a row, so that no join
   * can generate its empty-build-side code unnoticed.
   */
  private def withRowBroadcasts(
      subtree: WholeStageCodegenExec,
      replacements: java.util.Map[BroadcastExchangeExec, BroadcastExchangeExec]
  ): WholeStageCodegenExec = {
    def rowBroadcast(b: BroadcastExchangeExec): BroadcastExchangeExec =
      replacements.computeIfAbsent(b, _ => b.copy(child = LocalTableScanExec(b.child.output,
        Seq(InternalRow.fromSeq(b.child.output.map(a => Literal.default(a.dataType).value))),
        None)))
    // Rewrites the stage only: below an input adapter is another stage's input, so only a
    // broadcast read right there is replaced, and nothing under it is copied.
    def inStage(p: SparkPlan): SparkPlan = p match {
      case r @ ReusedExchangeExec(_, b: BroadcastExchangeExec) => r.copy(child = rowBroadcast(b))
      case b: BroadcastExchangeExec => rowBroadcast(b)
      case a: InputAdapter => a.withNewChildren(a.children.map {
        case c @ (_: BroadcastExchangeExec | ReusedExchangeExec(_, _: BroadcastExchangeExec)) =>
          inStage(c)
        case c => c
      })
      case other => other.withNewChildren(other.children.map(inStage))
    }
    val copy = inStage(subtree).asInstanceOf[WholeStageCodegenExec]
    def stageNodes(p: SparkPlan): Seq[SparkPlan] = p +: (p match {
      case _: InputAdapter => Nil
      case other => other.children.flatMap(stageNodes)
    })
    def isRow(p: SparkPlan): Boolean = p match {
      case InputAdapter(child) => isRow(child)
      case BroadcastExchangeExec(_, _: LocalTableScanExec) => true
      case ReusedExchangeExec(_, BroadcastExchangeExec(_, _: LocalTableScanExec)) => true
      case _ => false
    }
    stageNodes(copy).foreach {
      case j: BroadcastHashJoinExec =>
        val build = if (j.buildSide == BuildLeft) j.left else j.right
        assert(isRow(build), s"a broadcast hash join reads ${build.nodeName}, not a one-row " +
          s"broadcast, in stage ${subtree.codegenStageId}:\n${subtree.treeString}")
      case j: BroadcastNestedLoopJoinExec =>
        val build = if (j.buildSide == BuildLeft) j.left else j.right
        assert(isRow(build), s"a broadcast nested loop join reads ${build.nodeName}, not a " +
          s"one-row broadcast, in stage ${subtree.codegenStageId}:\n${subtree.treeString}")
      case _ =>
    }
    copy
  }
}
