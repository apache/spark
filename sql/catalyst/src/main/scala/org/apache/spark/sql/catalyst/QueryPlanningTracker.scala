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

import scala.collection.JavaConverters._

import org.apache.spark.util.BoundedPriorityQueue


/**
 * A simple utility for tracking runtime and associated stats in query planning.
 *
 * There are two separate concepts we track:
 *
 * 1. Phases: These are broad scope phases in query planning, as listed below, i.e. analysis,
 * optimizationm and physical planning (just planning).
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

  class RuleSummary(
    var totalTimeNs: Long, var numInvocations: Long, var numEffectiveInvocations: Long) {

    def this() = this(totalTimeNs = 0, numInvocations = 0, numEffectiveInvocations = 0)

    override def toString: String = {
      s"RuleSummary($totalTimeNs, $numInvocations, $numEffectiveInvocations)"
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


class QueryPlanningTracker {

  import QueryPlanningTracker._

  // Mapping from the name of a rule to a rule's summary.
  // Use a Java HashMap for less overhead.
  private val rulesMap = new java.util.HashMap[String, RuleSummary]

  // From a phase to time in ns.
  private val phaseToTimeNs = new java.util.HashMap[String, Long]

  /** Measure the runtime of function f, and add it to the time for the specified phase. */
  def measureTime[T](phase: String)(f: => T): T = {
    val startTime = System.nanoTime()
    val ret = f
    val timeTaken = System.nanoTime() - startTime
    phaseToTimeNs.put(phase, phaseToTimeNs.getOrDefault(phase, 0) + timeTaken)
    ret
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

  def phases: Map[String, Long] = phaseToTimeNs.asScala.toMap

  /** Returns the top k most expensive rules (as measured by time). */
  def topRulesByTime(k: Int): Seq[(String, RuleSummary)] = {
    val orderingByTime: Ordering[(String, RuleSummary)] = Ordering.by(e => e._2.totalTimeNs)
    val q = new BoundedPriorityQueue(k)(orderingByTime)
    rulesMap.asScala.foreach(q.+=)
    q.toSeq.sortBy(r => -r._2.totalTimeNs)
  }

}
