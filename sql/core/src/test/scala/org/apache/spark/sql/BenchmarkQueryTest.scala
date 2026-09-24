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

import scala.collection.mutable

import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.codegen.{ByteCodeStats, CodeAndComment, CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.execution.{InputAdapter, LocalTableScanExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

abstract class BenchmarkQueryTest extends SharedSparkSession {

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
    BenchmarkQueryTest.generatedCode(plan).foreach { case (subtree, code) =>
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
   * with the code it generates for data (see [[withRowBroadcasts]]). The walk follows an
   * adaptive plan into its current plan and query stages, as `debug.codegenStringSeq` does; for
   * a plan that has not run, that holds no whole-stage codegen subtree yet.
   */
  def generatedCode(plan: SparkPlan): Seq[(WholeStageCodegenExec, CodeAndComment)] = {
    val subtrees = new mutable.LinkedHashSet[WholeStageCodegenExec]()
    def findSubtrees(plan: SparkPlan): Unit = plan foreach {
      case s: WholeStageCodegenExec => subtrees += s
      case a: AdaptiveSparkPlanExec => findSubtrees(a.executedPlan)
      case s: QueryStageExec => findSubtrees(s.plan)
      case s => s.subqueries.foreach(findSubtrees)
    }
    findSubtrees(plan)
    // One replacement per broadcast exchange of the plan, shared by the subtrees that read it, so
    // each runs its one-row broadcast once.
    val replacements = new java.util.IdentityHashMap[BroadcastExchangeExec, BroadcastExchangeExec]
    subtrees.toSeq.sortBy(_.codegenStageId).map { s =>
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
  def withRowBroadcasts(
      subtree: WholeStageCodegenExec,
      replacements: java.util.Map[BroadcastExchangeExec, BroadcastExchangeExec] =
        new java.util.IdentityHashMap[BroadcastExchangeExec, BroadcastExchangeExec]
  ): WholeStageCodegenExec = {
    def rowBroadcast(b: BroadcastExchangeExec): BroadcastExchangeExec =
      replacements.computeIfAbsent(b, _ => b.copy(child = LocalTableScanExec(b.child.output,
        Seq(InternalRow.fromSeq(b.child.output.map(a => Literal.default(a.dataType).value))),
        None)))
    // Top down, so an exchange is replaced before its build side would be copied.
    val copy = subtree.transformDown {
      case r @ ReusedExchangeExec(_, b: BroadcastExchangeExec) => r.copy(child = rowBroadcast(b))
      case b: BroadcastExchangeExec => rowBroadcast(b)
    }.asInstanceOf[WholeStageCodegenExec]
    def isRow(p: SparkPlan): Boolean = p match {
      case InputAdapter(child) => isRow(child)
      case BroadcastExchangeExec(_, _: LocalTableScanExec) => true
      case ReusedExchangeExec(_, BroadcastExchangeExec(_, _: LocalTableScanExec)) => true
      case _ => false
    }
    copy.foreach {
      case j: BroadcastHashJoinExec =>
        val build = if (j.buildSide == BuildLeft) j.left else j.right
        assert(isRow(build), s"a broadcast hash join reads ${build.nodeName}, not a one-row " +
          s"broadcast:\n${subtree.treeString}")
      case j: BroadcastNestedLoopJoinExec =>
        val build = if (j.buildSide == BuildLeft) j.left else j.right
        assert(isRow(build), s"a broadcast nested loop join reads ${build.nodeName}, not a " +
          s"one-row broadcast:\n${subtree.treeString}")
      case _ =>
    }
    copy
  }
}
