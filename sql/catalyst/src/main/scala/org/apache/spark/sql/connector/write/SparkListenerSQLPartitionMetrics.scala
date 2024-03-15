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

package org.apache.spark.sql.connector.write

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.SparkListenerEvent

@DeveloperApi
case class SparkListenerSQLPartitionMetrics(executorId: Long,
                                            metrics: java.util.Map[String, PartitionMetrics])
  extends SparkListenerEvent

object SQLPartitionMetrics {

  /**
   * Post any aggregated partition write statistics to the listener bus using a
   * [[SparkListenerSQLPartitionMetrics]] event
   *
   * @param sc The Spark context
   * @param executionId The identifier for the SQL execution that resulted in the partition writes
   * @param writeInfo The aggregated partition writes for this SQL exectuion
   */
  def postDriverMetricUpdates(sc: SparkContext, executionId: String,
                              writeInfo: PartitionMetricsWriteInfo): Unit = {
    // Don't bother firing an event if there are no collected metrics
    if (writeInfo.isZero) {
      return
    }

    // There are some cases we don't care about the metrics and call `SparkPlan.doExecute`
    // directly without setting an execution id. We should be tolerant to it.
    if (executionId != null) {
      sc.listenerBus.post(
        SparkListenerSQLPartitionMetrics(executionId.toLong, writeInfo.toMap))
    }
  }

}
