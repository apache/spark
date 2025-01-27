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

package org.apache.spark.sql.catalyst

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.util.BoundedPriorityQueue


/**
 * A simple utility for tracking runtime and associated stats in query planning.
 *
 * There are two separate concepts we track:
 *
 * 1. Phases: These are broad scope phases in query planning, as listed below, i.e. analysis,
 * optimization and physical planning (just planning).
 *
 * 2. Rules: These are the individual Catalyst rules that we track. In addition to time, we also
 * track the number of invocations and effective invocations.
 */
object QueryPlanningTracker {

  // Define a list of common phases here.
  val PARSING = "parsing"
  val ANALYSIS = "analysis"
  val OPTIMIZATION = "optimization"
  val PLANNING = "planning"

  /**
   * Summary for a rule.
   * @param totalTimeNs total amount of time, in nanosecs, spent in this rule.
   * @param numInvocations number of times the rule has been invoked.
   * @param numEffectiveInvocations number of times the rule has been invoked and
   *                                resulted in a plan change.
   */
  class RuleSummary(
    var totalTimeNs: Long, var numInvocations: Long, var numEffectiveInvocations: Long) {

    def this() = this(totalTimeNs = 0, numInvocations = 0, numEffectiveInvocations = 0)

    override def toString: String = {
      s"RuleSummary($totalTimeNs, $numInvocations, $numEffectiveInvocations)"
    }
  }

  /**
   * Summary of a phase, with start time and end time so we can construct a timeline.
   */
  class PhaseSummary(val startTimeMs: Long, val endTimeMs: Long) {

    def durationMs: Long = endTimeMs - startTimeMs

    override def toString: String = {
      s"PhaseSummary($startTimeMs, $endTimeMs)"
    }
  }

  /**
   * A thread local variable to implicitly pass the tracker around. This assumes the query planner
   * is single-threaded, and avoids passing the same tracker context in every function call.
   */
  private val localTracker = new ThreadLocal[QueryPlanningTracker]() {
    override def initialValue: QueryPlanningTracker = null
  }

  /** Returns the current tracker in scope, based on the thread local variable. */
  def get: Option[QueryPlanningTracker] = Option(localTracker.get())

  /** Sets the current tracker for the execution of function f. We assume f is single-threaded. */
  def withTracker[T](tracker: QueryPlanningTracker)(f: => T): T = {
    val originalTracker = localTracker.get()
    localTracker.set(tracker)
    try f finally { localTracker.set(originalTracker) }
  }
}

/**
 * Callbacks after planning phase completion.
 */
abstract class QueryPlanningTrackerCallback {
  /**
   * Called when query fails analysis
   *
   * @param tracker      tracker that triggered the callback.
   * @param parsedPlan   The plan prior to analysis
   *                     see @org.apache.spark.sql.catalyst.analysis.Analyzer
   */
  def analysisFailed(tracker: QueryPlanningTracker, parsedPlan: LogicalPlan): Unit = {
    // Noop by default for backward compatibility
  }
  /**
   * Called when query has been analyzed.
   *
   * @param tracker tracker that triggered the callback.
   * @param analyzedPlan The plan after analysis,
   *                     see @org.apache.spark.sql.catalyst.analysis.Analyzer
   */
  def analyzed(tracker: QueryPlanningTracker, analyzedPlan: LogicalPlan): Unit

  /**
   * Called when query is ready for execution.
   * This is after analysis for eager commands and after planning for other queries.
   * @param tracker tracker that triggered the callback.
   */
  def readyForExecution(tracker: QueryPlanningTracker): Unit
}

/**
 * @param trackerCallback Callback to be notified of planning phase completion.
 */
class QueryPlanningTracker(
    trackerCallback: Option[QueryPlanningTrackerCallback] = None) {

  import QueryPlanningTracker._

  // Mapping from the name of a rule to a rule's summary.
  // Use a Java HashMap for less overhead.
  private val rulesMap = new java.util.HashMap[String, RuleSummary]

  // From a phase to its start time and end time, in ms.
  private val phasesMap = new java.util.HashMap[String, PhaseSummary]

  private var readyForExecution = false

  /**
   * Measure the start and end time of a phase. Note that if this function is called multiple
   * times for the same phase, the recorded start time will be the start time of the first call,
   * and the recorded end time will be the end time of the last call.
   */
  def measurePhase[T](phase: String)(f: => T): T = {
    val startTime = System.currentTimeMillis()
    val ret = f
    val endTime = System.currentTimeMillis

    if (phasesMap.containsKey(phase)) {
      val oldSummary = phasesMap.get(phase)
      phasesMap.put(phase, new PhaseSummary(oldSummary.startTimeMs, endTime))
    } else {
      phasesMap.put(phase, new PhaseSummary(startTime, endTime))
    }
    ret
  }

  /**
   * Set when the query has been parsed but failed to be analyzed.
   * Can be called multiple times upon plan change.
   *
   * @param parsedPlan The plan prior analysis
   *                   see @org.apache.spark.sql.catalyst.analysis.Analyzer
   */
  private[sql] def setAnalysisFailed(parsedPlan: LogicalPlan): Unit = {
    trackerCallback.foreach(_.analysisFailed(this, parsedPlan))
  }

  /**
   * Set when the query has been analysed.
   * Can be called multiple times upon plan change.
   * @param analyzedPlan The plan after analysis,
   *                     see @org.apache.spark.sql.catalyst.analysis.Analyzer
   */
  private[sql] def setAnalyzed(analyzedPlan: LogicalPlan): Unit = {
    trackerCallback.foreach(_.analyzed(this, analyzedPlan))
  }

  /**
   * Set when the query is ready for execution. This is after analysis for
   * eager commands and after planning for other queries.
   * see @link org.apache.spark.sql.execution.CommandExecutionMode
   * When called multiple times, ignores subsequent call.
   */
  private[sql] def setReadyForExecution(): Unit = {
    if (!readyForExecution) {
      readyForExecution = true
      trackerCallback.foreach(_.readyForExecution(this))
    }
  }

  /**
   * Record a specific invocation of a rule.
   *
   * @param rule name of the rule
   * @param timeNs time taken to run this invocation
   * @param effective whether the invocation has resulted in a plan change
   */
  def recordRuleInvocation(rule: String, timeNs: Long, effective: Boolean): Unit = {
    var s = rulesMap.get(rule)
    if (s eq null) {
      s = new RuleSummary
      rulesMap.put(rule, s)
    }

    s.totalTimeNs += timeNs
    s.numInvocations += 1
    s.numEffectiveInvocations += (if (effective) 1 else 0)
  }

  // ------------ reporting functions below ------------

  def rules: Map[String, RuleSummary] = rulesMap.asScala.toMap

  def phases: Map[String, PhaseSummary] = phasesMap.asScala.toMap

  /**
   * Returns the top k most expensive rules (as measured by time). If k is larger than the rules
   * seen so far, return all the rules. If there is no rule seen so far or k <= 0, return empty seq.
   */
  def topRulesByTime(k: Int): Seq[(String, RuleSummary)] = {
    if (k <= 0) {
      Seq.empty
    } else {
      val orderingByTime: Ordering[(String, RuleSummary)] = Ordering.by(e => e._2.totalTimeNs)
      val q = new BoundedPriorityQueue(k)(orderingByTime)
      rulesMap.asScala.foreach(q.+=)
      q.toSeq.sortBy(r => -r._2.totalTimeNs)
    }
  }

}
