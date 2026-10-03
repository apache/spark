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

package org.apache.spark.sql.test

import scala.concurrent.duration._

import org.scalatest.Suite

import org.apache.spark.sql.{QueryTest, QueryTestBase, SparkSessionBinderBase}
import org.apache.spark.sql.classic

trait SharedSparkSession extends QueryTest with classic.SparkSessionBinder {

  // Runs func (which must trigger exactly one SQL execution), waits until the status store has
  // computed the SQL metrics of that execution and returns its execution id.
  def runAndWaitForExecution(func: => Unit): Long = {
    val statusStore = spark.sharedState.statusStore
    // The listener updates the status store asynchronously, so the latest execution in the store
    // may still be one from before func. Execution ids only grow, so wait for an execution with a
    // larger id than any existing one. Execution counts cannot be used for this, as old
    // executions are evicted once spark.sql.ui.retainedExecutions is reached.
    val lastExecId = statusStore.executionsList().lastOption.map(_.executionId).getOrElse(-1L)

    func

    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      val exec = statusStore.executionsList().lastOption
      assert(exec.exists(e => e.executionId > lastExecId && e.metricValues != null))
      exec.get.executionId
    }
  }

  // Runs func (which must trigger exactly one SQL execution) and returns the SQL metrics of that
  // execution as a map keyed by (planNodeId, planNodeName, metricName) -> metricValue.
  def runAndFetchMetrics(func: => Unit): Map[(Long, String, String), String] = {
    val execId = runAndWaitForExecution(func)
    val statusStore = spark.sharedState.statusStore
    val sqlMetrics = statusStore.planGraph(execId).allNodes
      .flatMap(n => n.metrics.map(m => (m.accumulatorId, (n.id, n.name, m.name))))
      .toMap
    statusStore.executionMetrics(execId).map { case (k, v) => sqlMetrics(k) -> v }
  }
}


/**
 * Helper trait for SQL test suites where all tests share a single [[TestSparkSession]].
 */
trait SharedSparkSessionBase extends QueryTestBase with SparkSessionBinderBase { self: Suite =>

  protected override def spark: classic.SparkSession =
    super.spark.asInstanceOf[classic.SparkSession]
}
