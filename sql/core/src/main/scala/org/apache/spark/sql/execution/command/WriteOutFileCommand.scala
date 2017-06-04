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

package org.apache.spark.sql.execution.command

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.ExecutedWriteSummary
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * A logical command specialized for writing data out. `WriteOutFileCommand`s are
 * wrapped in `WrittenFileCommandExec` during execution.
 */
trait WriteOutFileCommand extends logical.Command {

  /**
   * Those metrics will be updated once the command finishes writing data out. Those metrics will
   * be taken by `WrittenFileCommandExe` as its metrics when showing in UI.
   */
  def metrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      // General metrics.
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numDynamicParts" -> SQLMetrics.createMetric(sparkContext, "number of dynamic part"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of written files"),
      "numOutputBytes" -> SQLMetrics.createMetric(sparkContext, "bytes of written output"),
      "writingTime" -> SQLMetrics.createMetric(sparkContext, "average writing time (ms)")
    )

  def run(
      sparkSession: SparkSession,
      children: Seq[SparkPlan],
      metricsCallback: (Seq[ExecutedWriteSummary]) => Unit): Seq[Row] = {
    throw new NotImplementedError
  }
}
