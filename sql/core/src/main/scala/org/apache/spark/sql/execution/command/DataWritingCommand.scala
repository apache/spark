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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.SerializableConfiguration


/**
 * A special `RunnableCommand` which writes data out and updates metrics.
 */
trait DataWritingCommand extends RunnableCommand {

  /**
   * The input query plan that produces the data to be written.
   */
  def query: LogicalPlan

  // We make the input `query` an inner child instead of a child in order to hide it from the
  // optimizer. This is because optimizer may not preserve the output schema names' case, and we
  // have to keep the original analyzed plan here so that we can pass the corrected schema to the
  // writer. The schema of analyzed plan is what user expects(or specifies), so we should respect
  // it when writing.
  override protected def innerChildren: Seq[LogicalPlan] = query :: Nil

  override lazy val metrics: Map[String, SQLMetric] = {
    val sparkContext = SparkContext.getActive.get
    Map(
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of written files"),
      "numOutputBytes" -> SQLMetrics.createMetric(sparkContext, "bytes of written output"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numParts" -> SQLMetrics.createMetric(sparkContext, "number of dynamic part")
    )
  }

  def basicWriteJobStatsTracker(hadoopConf: Configuration): BasicWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)
  }
}
