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

import com.google.common.util.concurrent.AtomicLongMap
import scala.collection.JavaConverters._

case class QueryExecutionMetering() {
  private val timeMap = AtomicLongMap.create[String]()
  private val numRunsMap = AtomicLongMap.create[String]()
  private val numEffectiveRunsMap = AtomicLongMap.create[String]()
  private val timeEffectiveRunsMap = AtomicLongMap.create[String]()

  /** Resets statistics about time spent running specific rules */
  def resetMetrics(): Unit = {
    timeMap.clear()
    numRunsMap.clear()
    numEffectiveRunsMap.clear()
    timeEffectiveRunsMap.clear()
  }

  def totalTime: Long = {
    timeMap.sum()
  }

  def totalNumRuns: Long = {
    numRunsMap.sum()
  }

  def incExecutionTimeBy(ruleName: String, delta: Long): Unit = {
    timeMap.addAndGet(ruleName, delta)
  }

  def incTimeEffectiveExecutionBy(ruleName: String, delta: Long): Unit = {
    timeEffectiveRunsMap.addAndGet(ruleName, delta)
  }

  def incNumEffectiveExecution(ruleName: String): Unit = {
    numEffectiveRunsMap.incrementAndGet(ruleName)
  }

  def incNumExecution(ruleName: String): Unit = {
    numRunsMap.incrementAndGet(ruleName)
  }

  /** Dump statistics about time spent running specific rules. */
  def dumpTimeSpent(): String = {
    val map = timeMap.asMap().asScala
    val maxSize = map.keys.map(_.toString.length).max

    val colRuleName = "Rule".padTo(maxSize, " ").mkString
    val colRunTime = "Total Time".padTo(22, " ").mkString
    val colTimeEffectiveRuns = "Effective Time".padTo(22, " ").mkString
    val colNumRuns = "Total Runs".padTo(22, " ").mkString
    val colNumEffectiveRuns = "Effective Runs".padTo(22, " ").mkString

    val ruleMetrics = map.toSeq.sortBy(_._2).reverseMap { case (k, v) =>
      val ruleName = k.padTo(maxSize, " ").mkString
      val runtime = v.toString.padTo(len = 22, " ").mkString
      val numRuns = numRunsMap.get(k).toString.padTo(len = 22, " ").mkString
      val numEffectiveRuns = numEffectiveRunsMap.get(k).toString.padTo(len = 22, " ").mkString
      val timeEffectiveRuns = timeEffectiveRunsMap.get(k).toString.padTo(len = 22, " ").mkString
      s"$ruleName $runtime $timeEffectiveRuns $numRuns $numEffectiveRuns"
    }.mkString("\n", "\n", "")

    s"""
       |=== Metrics of Analyzer/Optimizer Rules ===
       |Total number of runs = $totalNumRuns
       |Total time: ${totalTime / 1000000000D} seconds
       |
       |$colRuleName $colRunTime $colTimeEffectiveRuns $colNumRuns $colNumEffectiveRuns
       |$ruleMetrics
     """.stripMargin
  }
}
