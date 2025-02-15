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

package org.apache.spark.sql.catalyst.rules

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.MDC
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MILLIS
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

object RuleExecutor {
  protected val queryExecutionMeter = QueryExecutionMetering()

  /** Dump statistics about time spent running specific rules. */
  def dumpTimeSpent(): String = {
    queryExecutionMeter.dumpTimeSpent()
  }

  /** Resets statistics about time spent running specific rules */
  def resetMetrics(): Unit = {
    queryExecutionMeter.resetMetrics()
  }

  def getCurrentMetrics(): QueryExecutionMetrics = {
    queryExecutionMeter.getMetrics()
  }
}

class PlanChangeLogger[TreeType <: TreeNode[_]] extends Logging {

  private val logLevel = SQLConf.get.planChangeLogLevel

  private val logRules = SQLConf.get.planChangeRules.map(Utils.stringToSeq)

  private val logBatches = SQLConf.get.planChangeBatches.map(Utils.stringToSeq)

  def logRule(ruleName: String, oldPlan: TreeType, newPlan: TreeType): Unit = {
    if (!newPlan.fastEquals(oldPlan)) {
      if (logRules.isEmpty || logRules.get.contains(ruleName)) {
        def message(): MessageWithContext = {
          log"""
             |=== Applying Rule ${MDC(RULE_NAME, ruleName)} ===
             |${MDC(QUERY_PLAN, sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n"))}
           """.stripMargin
        }

        logBasedOnLevel(message())
      }
    }
  }

  def logBatch(batchName: String, oldPlan: TreeType, newPlan: TreeType): Unit = {
    if (logBatches.isEmpty || logBatches.get.contains(batchName)) {
      def message(): MessageWithContext = {
        if (!oldPlan.fastEquals(newPlan)) {
          log"""
             |=== Result of Batch ${MDC(BATCH_NAME, batchName)} ===
             |${MDC(QUERY_PLAN, sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n"))}
          """.stripMargin
        } else {
          log"Batch ${MDC(BATCH_NAME, batchName)} has no effect."
        }
      }

      logBasedOnLevel(message())
    }
  }

  def logMetrics(name: String, metrics: QueryExecutionMetrics): Unit = {
    val totalTime = metrics.time / NANOS_PER_MILLIS.toDouble
    val totalTimeEffective = metrics.timeEffective / NANOS_PER_MILLIS.toDouble
    // scalastyle:off line.size.limit
    val message: MessageWithContext =
      log"""
         |=== Metrics of Executed Rules ${MDC(RULE_EXECUTOR_NAME, name)} ===
         |Total number of runs: ${MDC(NUM_RULE_OF_RUNS, metrics.numRuns)}
         |Total time: ${MDC(TOTAL_TIME, totalTime)} ms
         |Total number of effective runs: ${MDC(NUM_EFFECTIVE_RULE_OF_RUNS, metrics.numEffectiveRuns)}
         |Total time of effective runs: ${MDC(TOTAL_EFFECTIVE_TIME, totalTimeEffective)} ms
      """.stripMargin
    // scalastyle:on line.size.limit

    logBasedOnLevel(message)
  }

  private def logBasedOnLevel(f: => MessageWithContext): Unit = {
    logLevel match {
      case "TRACE" => logTrace(f.message)
      case "DEBUG" => logDebug(f.message)
      case "INFO" => logInfo(f)
      case "WARN" => logWarning(f)
      case "ERROR" => logError(f)
      case _ => logTrace(f.message)
    }
  }
}

abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

  /** Name for this rule executor, automatically inferred based on class name. */
  protected def name: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  /**
   * An execution strategy for rules that indicates the maximum number of executions. If the
   * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
   */
  abstract class Strategy {

    /** The maximum number of executions. */
    def maxIterations: Int

    /** Whether to throw exception when exceeding the maximum number. */
    def errorOnExceed: Boolean = false

    /** The key of SQLConf setting to tune maxIterations */
    def maxIterationsSetting: String = null
  }

  /** A strategy that is run once and idempotent. */
  case object Once extends Strategy { val maxIterations = 1 }

  /**
   * A strategy that runs until fix point or maxIterations times, whichever comes first.
   * Especially, a FixedPoint(1) batch is supposed to run only once.
   */
  case class FixedPoint(
    override val maxIterations: Int,
    override val errorOnExceed: Boolean = false,
    override val maxIterationsSetting: String = null) extends Strategy

  /** A batch of rules. */
  protected case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  protected def batches: Seq[Batch]

  /** Once batches that are excluded in the idempotence checker */
  protected val excludedOnceBatches: Set[String] = Set.empty

  /**
   * Defines a validate function that validates the plan changes after the execution of each rule,
   * to make sure these rules make valid changes to the plan. For example, we can check whether
   * a plan is still resolved after each rule in `Optimizer`, so that we can catch rules that
   * turn the plan into unresolved.
   */
  protected def validatePlanChanges(
      previousPlan: TreeType,
      currentPlan: TreeType): Option[String] = None

  /**
   * Defines a validate function that validates the plan changes after the execution of each rule,
   * to make sure these rules make valid changes to the plan. Since this is enabled by default,
   * this should only consist of very lightweight checks.
   */
  protected def validatePlanChangesLightweight(
      previousPlan: TreeType,
      currentPlan: TreeType): Option[String] = None

  /**
   * Util method for checking whether a plan remains the same if re-optimized.
   */
  private def checkBatchIdempotence(batch: Batch, plan: TreeType): Unit = {
    val reOptimized = batch.rules.foldLeft(plan) { case (p, rule) => rule(p) }
    if (!plan.fastEquals(reOptimized)) {
      throw QueryExecutionErrors.onceStrategyIdempotenceIsBrokenForBatchError(
        batch.name, plan, reOptimized)
    }
  }

  /**
   * Executes the batches of rules defined by the subclass, and also tracks timing info for each
   * rule using the provided tracker.
   * @see [[execute]]
   */
  def executeAndTrack(plan: TreeType, tracker: QueryPlanningTracker): TreeType = {
    QueryPlanningTracker.withTracker(tracker) {
      execute(plan)
    }
  }

  /**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy. Within each batch, rules are also executed serially.
   */
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan
    val queryExecutionMetrics = RuleExecutor.queryExecutionMeter
    val planChangeLogger = new PlanChangeLogger[TreeType]()
    val tracker: Option[QueryPlanningTracker] = QueryPlanningTracker.get
    val beforeMetrics = RuleExecutor.getCurrentMetrics()

    val fullValidation = SQLConf.get.getConf(SQLConf.PLAN_CHANGE_VALIDATION)
    lazy val lightweightValidation = SQLConf.get.getConf(SQLConf.LIGHTWEIGHT_PLAN_CHANGE_VALIDATION)
    // Validate the initial input.
    if (fullValidation) {
      validatePlanChanges(plan, plan) match {
        case Some(msg) =>
          val ruleExecutorName = this.getClass.getName.stripSuffix("$")
          throw new SparkException(
            errorClass = "PLAN_VALIDATION_FAILED_RULE_EXECUTOR",
            messageParameters = Map("ruleExecutor" -> ruleExecutorName, "reason" -> msg),
            cause = null)
        case _ =>
      }
    }

    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // Run until fix point or the max number of iterations as specified in the strategy.
      while (continue) {
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            val result = rule(plan)
            val runTime = System.nanoTime() - startTime
            val effective = !result.fastEquals(plan)

            if (effective) {
              queryExecutionMetrics.incNumEffectiveExecution(rule.ruleName)
              queryExecutionMetrics.incTimeEffectiveExecutionBy(rule.ruleName, runTime)
              planChangeLogger.logRule(rule.ruleName, plan, result)
              // Run the plan changes validation after each rule.
              if (fullValidation || lightweightValidation) {
                // Only run the lightweight version of validation if full validation is disabled.
                val validationResult = if (fullValidation) {
                  validatePlanChanges(plan, result)
                } else {
                  validatePlanChangesLightweight(plan, result)
                }
                validationResult match {
                  case Some(msg) =>
                    throw new SparkException(
                      errorClass = "PLAN_VALIDATION_FAILED_RULE_IN_BATCH",
                      messageParameters = Map(
                        "rule" -> rule.ruleName,
                        "batch" -> batch.name,
                        "reason" -> msg),
                      cause = null)
                  case _ =>
                }
              }
            }
            queryExecutionMetrics.incExecutionTimeBy(rule.ruleName, runTime)
            queryExecutionMetrics.incNumExecution(rule.ruleName)

            // Record timing information using QueryPlanningTracker
            tracker.foreach(_.recordRuleInvocation(rule.ruleName, runTime, effective))

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            val endingMsg = if (batch.strategy.maxIterationsSetting == null) {
              log"."
            } else {
              log", please set '${MDC(NUM_ITERATIONS, batch.strategy.maxIterationsSetting)}' " +
                log"to a larger value."
            }
            val log = log"Max iterations (${MDC(NUM_ITERATIONS, iteration - 1)}) " +
              log"reached for batch ${MDC(BATCH_NAME, batch.name)}" +
              endingMsg
            if (Utils.isTesting || batch.strategy.errorOnExceed) {
              throw new RuntimeException(log.message)
            } else {
              logWarning(log)
            }
          }
          // Check idempotence for Once batches.
          if (batch.strategy == Once &&
            Utils.isTesting && !excludedOnceBatches.contains(batch.name)) {
            checkBatchIdempotence(batch, curPlan)
          }
          continue = false
        }

        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }

      planChangeLogger.logBatch(batch.name, batchStartPlan, curPlan)
    }
    planChangeLogger.logMetrics(name, RuleExecutor.getCurrentMetrics() - beforeMetrics)

    curPlan
  }
}
