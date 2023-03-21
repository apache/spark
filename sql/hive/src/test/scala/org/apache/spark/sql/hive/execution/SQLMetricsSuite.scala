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

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecutionSuite
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, WriteFilesExec}
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.util.QueryExecutionListener
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
    withPlannedWrite {
      checkWriteMetrics()
    }
  }

  private def checkWriteMetrics(): Unit = {
    Seq(false, true).foreach { canOptimized =>
      withSQLConf(HiveUtils.CONVERT_METASTORE_CTAS.key -> canOptimized.toString) {
        withTable("t") {
          var dataWriting: DataWritingCommandExec = null
          val listener = new QueryExecutionListener {
            override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {}
            override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
              qe.executedPlan match {
                case dataWritingCommandExec: DataWritingCommandExec =>
                  dataWriting = dataWritingCommandExec
                case _ =>
              }
            }
          }
          spark.listenerManager.register(listener)
          try {
            sql(s"CREATE TABLE t STORED AS PARQUET AS SELECT 1 as a")
            sparkContext.listenerBus.waitUntilEmpty()
            assert(dataWriting != null)
            val v1WriteCommand = if (canOptimized) {
              dataWriting.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]
            } else {
              dataWriting.cmd.asInstanceOf[InsertIntoHiveTable]
            }
            val metrics = if (conf.plannedWriteEnabled) {
              dataWriting.child.asInstanceOf[WriteFilesExec].metrics
            } else {
              v1WriteCommand.metrics
            }

            assert(metrics.contains("numFiles"))
            assert(metrics("numFiles").value == 1)
            assert(metrics.contains("numOutputBytes"))
            assert(metrics("numOutputBytes").value > 0)
            assert(metrics.contains("numOutputRows"))
            assert(metrics("numOutputRows").value == 1)
          } finally {
            spark.listenerManager.unregister(listener)
          }
        }
      }
    }
  }
}
