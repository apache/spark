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

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.adaptive.InsertAdaptiveSparkPlan
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryExecution(
    val sparkSession: SparkSession,
    val logical: LogicalPlan,
    val tracker: QueryPlanningTracker = new QueryPlanningTracker) {

  case class Plans(
    val analized: LogicalPlan,
    val withCachedData: LogicalPlan,
    val optimizedPlan: LogicalPlan,
    val sparkPlan: SparkPlan,
    val executedPlan: SparkPlan)

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  def assertAnalyzed(): Unit = analyzed

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

  def calculateAnalyzedPlan(): LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    // We can't clone `logical` here, which will reset the `_analyzed` flag.
    sparkSession.sessionState.analyzer.executeAndCheck(logical, tracker)
  }

  def calculatePlanWithCachedData(plan: LogicalPlan): LogicalPlan = {
    assertAnalyzed()
    assertSupported()
    // clone the plan to avoid sharing the plan instance between different stages like analyzing,
    // optimizing and planning.
    sparkSession.sharedState.cacheManager.useCachedData(plan.clone())
  }

  def calculateOptimizedPlan(plan: LogicalPlan): LogicalPlan = {
    // clone the plan to avoid sharing the plan instance between different stages like analyzing,
    // optimizing and planning.
    sparkSession.sessionState.optimizer.executeAndTrack(plan.clone(), tracker)
  }

  def calculateSparkPlan(plan: LogicalPlan): SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    // Clone the logical plan here, in case the planner rules change the states of the logical plan.
    planner.plan(ReturnAnswer(plan.clone())).next()
  }

  def calculateExecutedPlan(plan: SparkPlan): SparkPlan = {
    // clone the plan to avoid sharing the plan instance between different stages like analyzing,
    // optimizing and planning.
    prepareForExecution(plan.clone())
  }

  /**
   * Some operations need any post-analyzed-plans even though
   * those plans are not materialized yet (plans are materialized lazily).
   * One of those operation is [[Dataset.explain()]], which needs executedPlan.
   * If explain accesses to executedPlan, pre-executed-plans including
   * withCachedData are also materialized.
   * After withCachedData is materialized and then [[Dataset.persist()]],
   * [[Dataset.explain()]] shows wrong resut.
   * To prevent materializing post-analyzed-plans accidentally, use this method.
   * This method returns a tuple of analyzedPlan, withCachedData, optimizedPlan,
   * sparkPlan and executedPlan in this order.
   * If some plans are already materialized, they are just returned otherwise calculated.
   */
  def getOrCalculatePlans(): Plans = {
    val phases = tracker.phases

    val analyzed = if (phases.contains(QueryPlanningTracker.ANALYSIS)) {
      this.analyzed
    } else {
      calculateAnalyzedPlan()
    }

    val withCachedData = if (phases.contains(QueryPlanningTracker.OPTIMIZATION)) {
      this.withCachedData
    } else {
      calculatePlanWithCachedData(analyzed)
    }

    val optimizedPlan = if (phases.contains(QueryPlanningTracker.OPTIMIZATION)) {
      this.optimizedPlan
    } else {
      calculateOptimizedPlan(withCachedData)
    }

    val sparkPlan = if (phases.contains(QueryPlanningTracker.PLANNING)) {
      this.sparkPlan
    } else {
      calculateSparkPlan(optimizedPlan)
    }

    val executedPlan = if (phases.contains(QueryPlanningTracker.PLANNING)) {
      this.executedPlan
    } else {
      calculateExecutedPlan(sparkPlan)
    }

    Plans(analyzed, withCachedData, optimizedPlan, sparkPlan, executedPlan)
  }

  lazy val analyzed: LogicalPlan = tracker.measurePhase(QueryPlanningTracker.ANALYSIS) {
    calculateAnalyzedPlan()
  }

  lazy val withCachedData: LogicalPlan = calculatePlanWithCachedData(analyzed)

  lazy val optimizedPlan: LogicalPlan = tracker.measurePhase(QueryPlanningTracker.OPTIMIZATION) {
    calculateOptimizedPlan(withCachedData)
  }

  lazy val sparkPlan: SparkPlan = tracker.measurePhase(QueryPlanningTracker.PLANNING) {
    calculateSparkPlan(optimizedPlan)
  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = tracker.measurePhase(QueryPlanningTracker.PLANNING) {
    calculateExecutedPlan(sparkPlan)
  }

  /**
   * Internal version of the RDD. Avoids copies and has no schema.
   * Note for callers: Spark may apply various optimization including reusing object: this means
   * the row is valid only for the iteration it is retrieved. You should avoid storing row and
   * accessing after iteration. (Calling `collect()` is one of known bad usage.)
   * If you want to store these rows into collection, please apply some converter or copy row
   * which produces new object per iteration.
   * Given QueryExecution is not a public class, end users are discouraged to use this: please
   * use `Dataset.rdd` instead where conversion will be applied.
   */
  lazy val toRdd: RDD[InternalRow] = executedPlan.execute()

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  /** A sequence of rules that will be applied in order to the physical plan before execution. */
  protected def preparations: Seq[Rule[SparkPlan]] = Seq(
    // `AdaptiveSparkPlanExec` is a leaf node. If inserted, all the following rules will be no-op
    // as the original plan is hidden behind `AdaptiveSparkPlanExec`.
    InsertAdaptiveSparkPlan(sparkSession),
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    ApplyColumnarRulesAndInsertTransitions(sparkSession.sessionState.conf,
      sparkSession.sessionState.columnarRules),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf))

  def simpleString: String = withRedaction {
    lazy val Plans(_, _, _, _, executedPlan) = getOrCalculatePlans()
    val concat = new PlanStringConcat()
    concat.append("== Physical Plan ==\n")
    QueryPlan.append(executedPlan, concat.append, verbose = false, addSuffix = false)
    concat.append("\n")
    concat.toString
  }

  private def writePlans(append: String => Unit, maxFields: Int): Unit = {
    lazy val Plans(analyzed, _, optimizedPlan, _, executedPlan) = getOrCalculatePlans()
    val (verbose, addSuffix) = (true, false)
    append("== Parsed Logical Plan ==\n")
    QueryPlan.append(logical, append, verbose, addSuffix, maxFields)
    append("\n== Analyzed Logical Plan ==\n")
    val analyzedOutput = try {
      truncatedString(
        analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}"), ", ", maxFields)
    } catch {
      case e: AnalysisException => e.toString
    }
    append(analyzedOutput)
    append("\n")
    QueryPlan.append(analyzed, append, verbose, addSuffix, maxFields)
    append("\n== Optimized Logical Plan ==\n")
    QueryPlan.append(optimizedPlan, append, verbose, addSuffix, maxFields)
    append("\n== Physical Plan ==\n")
    QueryPlan.append(executedPlan, append, verbose, addSuffix, maxFields)
  }

  override def toString: String = withRedaction {
    val concat = new PlanStringConcat()
    writePlans(concat.append, SQLConf.get.maxToStringFields)
    concat.toString
  }

  def stringWithStats: String = withRedaction {
    lazy val Plans(_, _, optimizedPlan, _, executedPlan) = getOrCalculatePlans()
    val concat = new PlanStringConcat()
    val maxFields = SQLConf.get.maxToStringFields

    // trigger to compute stats for logical plans
    optimizedPlan.stats

    // only show optimized logical plan and physical plan
    concat.append("== Optimized Logical Plan ==\n")
    QueryPlan.append(optimizedPlan, concat.append, verbose = true, addSuffix = true, maxFields)
    concat.append("\n== Physical Plan ==\n")
    QueryPlan.append(executedPlan, concat.append, verbose = true, addSuffix = false, maxFields)
    concat.append("\n")
    concat.toString
  }

  /**
   * Redact the sensitive information in the given string.
   */
  private def withRedaction(message: String): String = {
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, message)
  }

  /** A special namespace for commands that can be used to debug query execution. */
  // scalastyle:off
  object debug {
  // scalastyle:on

    /**
     * Prints to stdout all the generated code found in this plan (i.e. the output of each
     * WholeStageCodegen subtree).
     */
    def codegen(): Unit = {
      lazy val Plans(_, _, _, _, executedPlan) = getOrCalculatePlans()
      // scalastyle:off println
      println(org.apache.spark.sql.execution.debug.codegenString(executedPlan))
      // scalastyle:on println
    }

    /**
     * Get WholeStageCodegenExec subtrees and the codegen in a query plan
     *
     * @return Sequence of WholeStageCodegen subtrees and corresponding codegen
     */
    def codegenToSeq(): Seq[(String, String)] = {
      lazy val Plans(_, _, _, _, executedPlan) = getOrCalculatePlans()
      org.apache.spark.sql.execution.debug.codegenStringSeq(executedPlan)
    }

    /**
     * Dumps debug information about query execution into the specified file.
     *
     * @param maxFields maximum number of fields converted to string representation.
     */
    def toFile(path: String, maxFields: Int = Int.MaxValue): Unit = {
      lazy val Plans(_, _, _, _, executedPlan) = getOrCalculatePlans()
      val filePath = new Path(path)
      val fs = filePath.getFileSystem(sparkSession.sessionState.newHadoopConf())
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(filePath)))
      val append = (s: String) => {
        writer.write(s)
      }
      try {
        writePlans(append, maxFields)
        writer.write("\n== Whole Stage Codegen ==\n")
        org.apache.spark.sql.execution.debug.writeCodegen(writer.write, executedPlan)
      } finally {
        writer.close()
      }
    }
  }
}
