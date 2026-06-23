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

  // Runs func (which must trigger exactly one SQL execution) and returns the SQL metrics of that
  // execution as a map keyed by (planNodeId, planNodeName, metricName) -> metricValue.
  @deprecated("rarely used", "4.2.0")
  def runAndFetchMetrics(func: => Unit): Map[(Long, String, String), String] = {
    val statusStore = spark.sharedState.statusStore
    val oldCount = statusStore.executionsList().size

    func

    // Wait until the new execution is started and being tracked.
    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      assert(statusStore.executionsCount() >= oldCount)
    }

    // Wait for listener to finish computing the metrics for the execution.
    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      assert(statusStore.executionsList().nonEmpty &&
        statusStore.executionsList().last.metricValues != null)
    }

    val exec = statusStore.executionsList().last
    val execId = exec.executionId
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
