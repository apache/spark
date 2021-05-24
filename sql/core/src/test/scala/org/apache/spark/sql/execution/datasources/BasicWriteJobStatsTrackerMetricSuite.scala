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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{LocalSparkSession, SparkSession}

class BasicWriteJobStatsTrackerMetricSuite extends SparkFunSuite with LocalSparkSession {

  test("SPARK-32978: make sure the number of dynamic part metric is correct") {
    try {
      val partitions = "50"
      spark = SparkSession.builder().master("local[4]").getOrCreate()
      val statusStore = spark.sharedState.statusStore

      spark.sql("create table dynamic_partition(i bigint, part bigint) " +
        "using parquet partitioned by (part)")
      val oldExecutionsSize = statusStore.executionsList().size
      spark.sql("insert overwrite table dynamic_partition partition(part) " +
        s"select id, id % $partitions as part from range(10000)")

      // Wait for listener to finish computing the metrics for the executions.
      while (statusStore.executionsList().size - oldExecutionsSize < 1 ||
        statusStore.executionsList().last.metricValues == null) {
        Thread.sleep(100)
      }

      // There should be 2 SQLExecutionUIData in executionsList and the 2nd item is we need,
      // but the executionId is indeterminate in maven test,
      // so the `statusStore.execution(executionId)` API is not used.
      assert(statusStore.executionsCount() == 2)
      val executionData = statusStore.executionsList()(1)
      val accumulatorIdOpt =
        executionData.metrics.find(_.name == "number of dynamic part").map(_.accumulatorId)
      assert(accumulatorIdOpt.isDefined)
      val numPartsOpt = executionData.metricValues.get(accumulatorIdOpt.get)
      assert(numPartsOpt.isDefined && numPartsOpt.get == partitions)

    } finally {
      spark.sql("drop table if exists dynamic_partition")
      spark.stop()
    }
  }
}
