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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecutionSuite
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.tags.SlowHiveTest

// Disable AQE because metric info is different with AQE on/off
@SlowHiveTest
class SQLMetricsSuite extends SQLMetricsTestUtils with TestHiveSingleton
  with DisableAdaptiveExecutionSuite {

  test("writing data out metrics: hive") {
    testMetricsNonDynamicPartition("hive", "t1")
  }

  test("writing data out metrics dynamic partition: hive") {
    withSQLConf(("hive.exec.dynamic.partition.mode", "nonstrict")) {
      testMetricsDynamicPartition("hive", "hive", "t1")
    }
  }

  test("SPARK-34567: Add metrics for CTAS operator") {
    Seq(false, true).foreach { canOptimized =>
      withSQLConf(HiveUtils.CONVERT_METASTORE_CTAS.key -> canOptimized.toString) {
        withTable("t") {
          val df = sql(s"CREATE TABLE t STORED AS PARQUET AS SELECT 1 as a")
          assert(df.queryExecution.executedPlan.isInstanceOf[CommandResultExec])
          val commandResultExec = df.queryExecution.executedPlan.asInstanceOf[CommandResultExec]
          val dataWritingCommandExec =
            commandResultExec.commandPhysicalPlan.asInstanceOf[DataWritingCommandExec]
          val createTableAsSelect = dataWritingCommandExec.cmd
          if (canOptimized) {
            assert(createTableAsSelect.isInstanceOf[OptimizedCreateHiveTableAsSelectCommand])
          } else {
            assert(createTableAsSelect.isInstanceOf[CreateHiveTableAsSelectCommand])
          }
          assert(createTableAsSelect.metrics.contains("numFiles"))
          assert(createTableAsSelect.metrics("numFiles").value == 1)
          assert(createTableAsSelect.metrics.contains("numOutputBytes"))
          assert(createTableAsSelect.metrics("numOutputBytes").value > 0)
          assert(createTableAsSelect.metrics.contains("numOutputRows"))
          assert(createTableAsSelect.metrics("numOutputRows").value == 1)
        }
      }
    }
  }
}
