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
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.EXTENDED_EXPLAIN_GENERATOR
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, ExtendedExplainGenerator, Row, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.expressions.codegen.ByteCodeStats
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Command, CommandResult, CreateTableAsSelect, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, ReplaceTableAsSelect, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.adaptive.{AdaptiveExecutionContext, InsertAdaptiveSparkPlan}
import org.apache.spark.sql.execution.bucketing.{CoalesceBucketsInJoin, DisableUnnecessaryBucketedScan}
import org.apache.spark.sql.execution.dynamicpruning.PlanDynamicPruningFilters
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery
import org.apache.spark.sql.execution.streaming.{IncrementalExecution, OffsetSeqMetadata, WatermarkPropagator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.util.ArrayImplicits._
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
    val tracker: QueryPlanningTracker = new QueryPlanningTracker,
    val mode: CommandExecutionMode.Value = CommandExecutionMode.ALL,
    val shuffleCleanupMode: ShuffleCleanupMode = DoNotCleanup) extends Logging {

  val id: Long = QueryExecution.nextExecutionId

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  def assertAnalyzed(): Unit = {
    try {
      analyzed
    } catch {
      case e: AnalysisException =>
        // Because we do eager analysis for Dataframe, there will be no execution created after
        // AnalysisException occurs. So we need to explicitly create a new execution to post
        // start/end events to notify the listener and UI components.
        SQLExecution.withNewExecutionIdOnError(this, Some("analyze"))(e)
    }
  }

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

  lazy val analyzed: LogicalPlan = {
    val plan = executePhase(QueryPlanningTracker.ANALYSIS) {
      // We can't clone `logical` here, which will reset the `_analyzed` flag.
      sparkSession.sessionState.analyzer.executeAndCheck(logical, tracker)
    }
    tracker.setAnalyzed(plan)
    plan
  }

  lazy val commandExecuted: LogicalPlan = mode match {
    case CommandExecutionMode.NON_ROOT => analyzed.mapChildren(eagerlyExecuteCommands)
    case CommandExecutionMode.ALL => eagerlyExecuteCommands(analyzed)
    case CommandExecutionMode.SKIP => analyzed
  }

  private def commandExecutionName(command: Command): String = command match {
    case _: CreateTableAsSelect => "create"
    case _: ReplaceTableAsSelect => "replace"
    case _: AppendData => "append"
    case _: OverwriteByExpression => "overwrite"
    case _: OverwritePartitionsDynamic => "overwritePartitions"
    case _ => "command"
  }

  private def eagerlyExecuteCommands(p: LogicalPlan) = p transformDown {
    case c: Command =>
      // Since Command execution will eagerly take place here,
      // and in most cases be the bulk of time and effort,
      // with the rest of processing of the root plan being just outputting command results,
      // for eagerly executed commands we mark this place as beginning of execution.
      tracker.setReadyForExecution()
      val qe = sparkSession.sessionState.executePlan(c, CommandExecutionMode.NON_ROOT)
      val name = commandExecutionName(c)
      val result = QueryExecution.withInternalError(s"Eagerly executed $name failed.") {
        SQLExecution.withNewExecutionId(qe, Some(name)) {
          qe.executedPlan.executeCollect()
        }
      }
      CommandResult(
        qe.analyzed.output,
        qe.commandExecuted,
        qe.executedPlan,
        result.toImmutableArraySeq)
    case other => other
  }

  // The plan that has been normalized by custom rules, so that it's more likely to hit cache.
  lazy val normalized: LogicalPlan = {
    val normalizationRules = sparkSession.sessionState.planNormalizationRules
    if (normalizationRules.isEmpty) {
      commandExecuted
    } else {
      val planChangeLogger = new PlanChangeLogger[LogicalPlan]()
      val normalized = normalizationRules.foldLeft(commandExecuted) { (p, rule) =>
        val result = rule.apply(p)
        planChangeLogger.logRule(rule.ruleName, p, result)
        result
      }
      planChangeLogger.logBatch("Plan Normalization", commandExecuted, normalized)
      normalized
    }
  }

  lazy val withCachedData: LogicalPlan = sparkSession.withActive {
    assertAnalyzed()
    assertSupported()
    // clone the plan to avoid sharing the plan instance between different stages like analyzing,
    // optimizing and planning.
    sparkSession.sharedState.cacheManager.useCachedData(normalized.clone())
  }

  def assertCommandExecuted(): Unit = commandExecuted

  lazy val optimizedPlan: LogicalPlan = {
    // We need to materialize the commandExecuted here because optimizedPlan is also tracked under
    // the optimizing phase
    assertCommandExecuted()
    executePhase(QueryPlanningTracker.OPTIMIZATION) {
      // clone the plan to avoid sharing the plan instance between different stages like analyzing,
      // optimizing and planning.
      val plan =
        sparkSession.sessionState.optimizer.executeAndTrack(withCachedData.clone(), tracker)
      // We do not want optimized plans to be re-analyzed as literals that have been constant
      // folded and such can cause issues during analysis. While `clone` should maintain the
      // `analyzed` state of the LogicalPlan, we set the plan as analyzed here as well out of
      // paranoia.
      plan.setAnalyzed()
      plan
    }
  }

  def assertOptimized(): Unit = optimizedPlan

  lazy val sparkPlan: SparkPlan = {
    // We need to materialize the optimizedPlan here because sparkPlan is also tracked under
    // the planning phase
    assertOptimized()
    executePhase(QueryPlanningTracker.PLANNING) {
      // Clone the logical plan here, in case the planner rules change the states of the logical
      // plan.
      QueryExecution.createSparkPlan(sparkSession, planner, optimizedPlan.clone())
    }
  }

  def assertSparkPlanPrepared(): Unit = sparkPlan

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = {
    // We need to materialize the optimizedPlan here, before tracking the planning phase, to ensure
    // that the optimization time is not counted as part of the planning phase.
    assertOptimized()
    val plan = executePhase(QueryPlanningTracker.PLANNING) {
      // clone the plan to avoid sharing the plan instance between different stages like analyzing,
      // optimizing and planning.
      QueryExecution.prepareForExecution(preparations, sparkPlan.clone())
    }
    // Note: For eagerly executed command it might have already been called in
    // `eagerlyExecutedCommand` and is a noop here.
    tracker.setReadyForExecution()
    plan
  }

  def assertExecutedPlanPrepared(): Unit = executedPlan

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
  lazy val toRdd: RDD[InternalRow] = new SQLExecutionRDD(
    executedPlan.execute(), sparkSession.sessionState.conf)

  /** Get the metrics observed during the execution of the query plan. */
  def observedMetrics: Map[String, Row] = CollectMetricsExec.collect(executedPlan)

  protected def preparations: Seq[Rule[SparkPlan]] = {
    QueryExecution.preparations(sparkSession,
      Option(InsertAdaptiveSparkPlan(AdaptiveExecutionContext(sparkSession, this))), false)
  }

  protected def executePhase[T](phase: String)(block: => T): T = sparkSession.withActive {
    QueryExecution.withInternalError(s"The Spark SQL phase $phase failed with an internal error.") {
      tracker.measurePhase(phase)(block)
    }
  }

  def simpleString: String = {
    val concat = new PlanStringConcat()
    simpleString(false, SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def simpleString(
      formatted: Boolean,
      maxFields: Int,
      append: String => Unit): Unit = {
    append("== Physical Plan ==\n")
    if (formatted) {
      try {
        ExplainUtils.processPlan(executedPlan, append)
      } catch {
        case e: AnalysisException => append(e.toString)
        case e: IllegalArgumentException => append(e.toString)
      }
    } else {
      QueryPlan.append(executedPlan,
        append, verbose = false, addSuffix = false, maxFields = maxFields)
    }
    extendedExplainInfo(append, executedPlan)
    append("\n")
  }

  def explainString(mode: ExplainMode): String = {
    val concat = new PlanStringConcat()
    explainString(mode, SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def explainString(mode: ExplainMode, maxFields: Int, append: String => Unit): Unit = {
    val queryExecution = if (logical.isStreaming) {
      // This is used only by explaining `Dataset/DataFrame` created by `spark.readStream`, so the
      // output mode does not matter since there is no `Sink`.
      new IncrementalExecution(
        sparkSession, logical, OutputMode.Append(), "<unknown>",
        UUID.randomUUID, UUID.randomUUID, 0, None, OffsetSeqMetadata(0, 0),
        WatermarkPropagator.noop(), false)
    } else {
      this
    }

    mode match {
      case SimpleMode =>
        queryExecution.simpleString(false, maxFields, append)
      case ExtendedMode =>
        queryExecution.toString(maxFields, append)
      case CodegenMode =>
        try {
          org.apache.spark.sql.execution.debug.writeCodegen(append, queryExecution.executedPlan)
        } catch {
          case e: AnalysisException => append(e.toString)
        }
      case CostMode =>
        queryExecution.stringWithStats(maxFields, append)
      case FormattedMode =>
        queryExecution.simpleString(formatted = true, maxFields = maxFields, append)
    }
  }

  private def writePlans(append: String => Unit, maxFields: Int): Unit = {
    val (verbose, addSuffix) = (true, false)
    append("== Parsed Logical Plan ==\n")
    QueryPlan.append(logical, append, verbose, addSuffix, maxFields)
    append("\n== Analyzed Logical Plan ==\n")
    try {
      if (analyzed.output.nonEmpty) {
        append(
          truncatedString(
            analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}"), ", ", maxFields)
        )
        append("\n")
      }
      QueryPlan.append(analyzed, append, verbose, addSuffix, maxFields)
      append("\n== Optimized Logical Plan ==\n")
      QueryPlan.append(optimizedPlan, append, verbose, addSuffix, maxFields)
      append("\n== Physical Plan ==\n")
      QueryPlan.append(executedPlan, append, verbose, addSuffix, maxFields)
      extendedExplainInfo(append, executedPlan)
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }

  override def toString: String = withRedaction {
    val concat = new PlanStringConcat()
    toString(SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def toString(maxFields: Int, append: String => Unit): Unit = {
    writePlans(append, maxFields)
  }

  def stringWithStats: String = {
    val concat = new PlanStringConcat()
    stringWithStats(SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def stringWithStats(maxFields: Int, append: String => Unit): Unit = {
    // trigger to compute stats for logical plans
    try {
      // This will trigger to compute stats for all the nodes in the plan, including subqueries,
      // if the stats doesn't exist in the statsCache and update the statsCache corresponding
      // to the node.
      optimizedPlan.collectWithSubqueries {
        case plan => plan.stats
      }
    } catch {
      case e: AnalysisException => append(e.toString + "\n")
    }
    // only show optimized logical plan and physical plan
    append("== Optimized Logical Plan ==\n")
    QueryPlan.append(optimizedPlan, append, verbose = true, addSuffix = true, maxFields)
    append("\n== Physical Plan ==\n")
    QueryPlan.append(executedPlan, append, verbose = true, addSuffix = false, maxFields)
    append("\n")
  }

  /**
   * Redact the sensitive information in the given string.
   */
  private def withRedaction(message: String): String = {
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, message)
  }

  def extendedExplainInfo(append: String => Unit, plan: SparkPlan): Unit = {
    val generators = sparkSession.sessionState.conf.getConf(SQLConf.EXTENDED_EXPLAIN_PROVIDERS)
      .getOrElse(Seq.empty)
    val extensions = Utils.loadExtensions(classOf[ExtendedExplainGenerator],
      generators,
      sparkSession.sparkContext.conf)
    if (extensions.nonEmpty) {
      extensions.foreach(extension =>
        try {
          append(s"\n== Extended Information (${extension.title}) ==\n")
          append(extension.generateExtendedInfo(plan))
        } catch {
          case NonFatal(e) => logWarning(log"Cannot use " +
            log"${MDC(EXTENDED_EXPLAIN_GENERATOR, extension)} to get extended information.", e)
        })
    }
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
      // scalastyle:off println
      println(org.apache.spark.sql.execution.debug.codegenString(executedPlan))
      // scalastyle:on println
    }

    /**
     * Get WholeStageCodegenExec subtrees and the codegen in a query plan
     *
     * @return Sequence of WholeStageCodegen subtrees and corresponding codegen
     */
    def codegenToSeq(): Seq[(String, String, ByteCodeStats)] = {
      org.apache.spark.sql.execution.debug.codegenStringSeq(executedPlan)
    }

    /**
     * Dumps debug information about query execution into the specified file.
     *
     * @param path path of the file the debug info is written to.
     * @param maxFields maximum number of fields converted to string representation.
     * @param explainMode the explain mode to be used to generate the string
     *                    representation of the plan.
     */
    def toFile(
        path: String,
        maxFields: Int = Int.MaxValue,
        explainMode: Option[String] = None): Unit = {
      val filePath = new Path(path)
      val fs = filePath.getFileSystem(sparkSession.sessionState.newHadoopConf())
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(filePath)))
      try {
        val mode = explainMode.map(ExplainMode.fromString(_)).getOrElse(ExtendedMode)
        explainString(mode, maxFields, writer.write)
        if (mode != CodegenMode) {
          writer.write("\n== Whole Stage Codegen ==\n")
          org.apache.spark.sql.execution.debug.writeCodegen(writer.write, executedPlan)
        }
        log.info(s"Debug information was written at: $filePath")
      } finally {
        writer.close()
      }
    }
  }
}

/**
 * SPARK-35378: Commands should be executed eagerly so that something like `sql("INSERT ...")`
 * can trigger the table insertion immediately without a `.collect()`. To avoid end-less recursion
 * we should use `NON_ROOT` when recursively executing commands. Note that we can't execute
 * a query plan with leaf command nodes, because many commands return `GenericInternalRow`
 * and can't be put in a query plan directly, otherwise the query engine may cast
 * `GenericInternalRow` to `UnsafeRow` and fail. When running EXPLAIN, or commands inside other
 * command, we should use `SKIP` to not eagerly trigger the command execution.
 */
object CommandExecutionMode extends Enumeration {
  val SKIP, NON_ROOT, ALL = Value
}

/**
 * Modes for shuffle dependency cleanup.
 *
 * DoNotCleanup: Do not perform any cleanup.
 * SkipMigration: Shuffle dependencies will not be migrated at node decommissions.
 * RemoveShuffleFiles: Shuffle dependency files are removed at the end of SQL executions.
 */
sealed trait ShuffleCleanupMode

case object DoNotCleanup extends ShuffleCleanupMode

case object SkipMigration extends ShuffleCleanupMode

case object RemoveShuffleFiles extends ShuffleCleanupMode


object QueryExecution {
  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  /**
   * Construct a sequence of rules that are used to prepare a planned [[SparkPlan]] for execution.
   * These rules will make sure subqueries are planned, make sure the data partitioning and ordering
   * are correct, insert whole stage code gen, and try to reduce the work done by reusing exchanges
   * and subqueries.
   */
  private[execution] def preparations(
      sparkSession: SparkSession,
      adaptiveExecutionRule: Option[InsertAdaptiveSparkPlan] = None,
      subquery: Boolean): Seq[Rule[SparkPlan]] = {
    // `AdaptiveSparkPlanExec` is a leaf node. If inserted, all the following rules will be no-op
    // as the original plan is hidden behind `AdaptiveSparkPlanExec`.
    adaptiveExecutionRule.toSeq ++
    Seq(
      CoalesceBucketsInJoin,
      PlanDynamicPruningFilters(sparkSession),
      PlanSubqueries(sparkSession),
      RemoveRedundantProjects,
      EnsureRequirements(),
      // `ReplaceHashWithSortAgg` needs to be added after `EnsureRequirements` to guarantee the
      // sort order of each node is checked to be valid.
      ReplaceHashWithSortAgg,
      // `RemoveRedundantSorts` and `RemoveRedundantWindowGroupLimits` needs to be added after
      // `EnsureRequirements` to guarantee the same number of partitions when instantiating
      // PartitioningCollection.
      RemoveRedundantSorts,
      RemoveRedundantWindowGroupLimits,
      DisableUnnecessaryBucketedScan,
      ApplyColumnarRulesAndInsertTransitions(
        sparkSession.sessionState.columnarRules, outputsColumnar = false),
      CollapseCodegenStages()) ++
      (if (subquery) {
        Nil
      } else {
        Seq(ReuseExchangeAndSubquery)
      })
  }

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  private[execution] def prepareForExecution(
      preparations: Seq[Rule[SparkPlan]],
      plan: SparkPlan): SparkPlan = {
    val planChangeLogger = new PlanChangeLogger[SparkPlan]()
    val preparedPlan = preparations.foldLeft(plan) { case (sp, rule) =>
      val result = rule.apply(sp)
      planChangeLogger.logRule(rule.ruleName, sp, result)
      result
    }
    planChangeLogger.logBatch("Preparations", plan, preparedPlan)
    preparedPlan
  }

  /**
   * Transform a [[LogicalPlan]] into a [[SparkPlan]].
   *
   * Note that the returned physical plan still needs to be prepared for execution.
   */
  def createSparkPlan(
      sparkSession: SparkSession,
      planner: SparkPlanner,
      plan: LogicalPlan): SparkPlan = {
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(plan)).next()
  }

  /**
   * Prepare the [[SparkPlan]] for execution.
   */
  def prepareExecutedPlan(spark: SparkSession, plan: SparkPlan): SparkPlan = {
    prepareForExecution(preparations(spark, subquery = true), plan)
  }

  /**
   * Transform the subquery's [[LogicalPlan]] into a [[SparkPlan]] and prepare the resulting
   * [[SparkPlan]] for execution.
   */
  def prepareExecutedPlan(spark: SparkSession, plan: LogicalPlan): SparkPlan = {
    val sparkPlan = createSparkPlan(spark, spark.sessionState.planner, plan.clone())
    prepareExecutedPlan(spark, sparkPlan)
  }

  /**
   * Prepare the [[SparkPlan]] for execution using exists adaptive execution context.
   * This method is only called by [[PlanAdaptiveDynamicPruningFilters]].
   */
  def prepareExecutedPlan(
      session: SparkSession,
      plan: LogicalPlan,
      context: AdaptiveExecutionContext): SparkPlan = {
    val sparkPlan = createSparkPlan(session, session.sessionState.planner, plan.clone())
    val preparationRules = preparations(session, Option(InsertAdaptiveSparkPlan(context)), true)
    prepareForExecution(preparationRules, sparkPlan.clone())
  }

  /**
   * Marks null pointer exceptions, asserts and scala match errors as internal errors
   */
  private[sql] def isInternalError(e: Throwable): Boolean = e match {
    case _: java.lang.NullPointerException => true
    case _: java.lang.AssertionError => true
    case _: scala.MatchError => true
    case _ => false
  }

  /**
   * Converts marked exceptions from [[isInternalError]] to internal errors.
   */
  private[sql] def toInternalError(msg: String, e: Throwable): Throwable = {
    if (isInternalError(e)) {
      SparkException.internalError(
        msg + " You hit a bug in Spark or the Spark plugins you use. Please, report this bug " +
          "to the corresponding communities or vendors, and provide the full stack trace.",
        e)
    } else {
      e
    }
  }

  /**
   * Catches marked exceptions from [[isInternalError]], and converts them to internal errors.
   */
  private[sql] def withInternalError[T](msg: String)(block: => T): T = {
    try {
      block
    } catch {
      case e: Throwable => throw toInternalError(msg, e)
    }
  }
}
