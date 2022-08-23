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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
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
        def message(): String = {
          s"""
             |=== Applying Rule $ruleName ===
             |${sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n")}
           """.stripMargin
        }

        logBasedOnLevel(message)
      }
    }
  }

  def logBatch(batchName: String, oldPlan: TreeType, newPlan: TreeType): Unit = {
    if (logBatches.isEmpty || logBatches.get.contains(batchName)) {
      def message(): String = {
        if (!oldPlan.fastEquals(newPlan)) {
          s"""
             |=== Result of Batch $batchName ===
             |${sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n")}
          """.stripMargin
        } else {
          s"Batch $batchName has no effect."
        }
      }

      logBasedOnLevel(message)
    }
  }

  def logMetrics(metrics: QueryExecutionMetrics): Unit = {
    val totalTime = metrics.time / NANOS_PER_SECOND.toDouble
    val totalTimeEffective = metrics.timeEffective / NANOS_PER_SECOND.toDouble
    val message =
      s"""
         |=== Metrics of Executed Rules ===
         |Total number of runs: ${metrics.numRuns}
         |Total time: $totalTime seconds
         |Total number of effective runs: ${metrics.numEffectiveRuns}
         |Total time of effective runs: $totalTimeEffective seconds
      """.stripMargin

    logBasedOnLevel(message)
  }

  private def logBasedOnLevel(f: => String): Unit = {
    logLevel match {
      case "TRACE" => logTrace(f)
      case "DEBUG" => logDebug(f)
      case "INFO" => logInfo(f)
      case "WARN" => logWarning(f)
      case "ERROR" => logError(f)
      case _ => logTrace(f)
    }
  }
}

abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

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
   * Defines a check function that checks for structural integrity of the plan after the execution
   * of each rule. For example, we can check whether a plan is still resolved after each rule in
   * `Optimizer`, so we can catch rules that return invalid plans. The check function returns
   * `false` if the given plan doesn't pass the structural integrity check.
   */
  protected def isPlanIntegral(previousPlan: TreeType, currentPlan: TreeType): Boolean = true

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

    // Run the structural integrity checker against the initial input
    if (!isPlanIntegral(plan, plan)) {
      throw QueryExecutionErrors.structuralIntegrityOfInputPlanIsBrokenInClassError(
        this.getClass.getName.stripSuffix("$"))
    }

    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
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
            }
            queryExecutionMetrics.incExecutionTimeBy(rule.ruleName, runTime)
            queryExecutionMetrics.incNumExecution(rule.ruleName)

            // Record timing information using QueryPlanningTracker
            tracker.foreach(_.recordRuleInvocation(rule.ruleName, runTime, effective))

            // Run the structural integrity checker against the plan after each rule.
            if (effective && !isPlanIntegral(plan, result)) {
              throw QueryExecutionErrors.structuralIntegrityIsBrokenAfterApplyingRuleError(
                rule.ruleName, batch.name)
            }

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            val endingMsg = if (batch.strategy.maxIterationsSetting == null) {
              "."
            } else {
              s", please set '${batch.strategy.maxIterationsSetting}' to a larger value."
            }
            val message = s"Max iterations (${iteration - 1}) reached for batch ${batch.name}" +
              s"$endingMsg"
            if (Utils.isTesting || batch.strategy.errorOnExceed) {
              throw new RuntimeException(message)
            } else {
              logWarning(message)
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
    planChangeLogger.logMetrics(RuleExecutor.getCurrentMetrics() - beforeMetrics)

    curPlan
  }
}
