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
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, V1WriteCommand}
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
    Seq(false, true).foreach { canOptimized =>
      withSQLConf(HiveUtils.CONVERT_METASTORE_CTAS.key -> canOptimized.toString) {
        withTable("t") {
          var v1WriteCommand: V1WriteCommand = null
          val listener = new QueryExecutionListener {
            override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {}
            override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
              qe.executedPlan match {
                case dataWritingCommandExec: DataWritingCommandExec =>
                  val createTableAsSelect = dataWritingCommandExec.cmd
                  v1WriteCommand = if (canOptimized) {
                    createTableAsSelect.asInstanceOf[InsertIntoHadoopFsRelationCommand]
                  } else {
                    createTableAsSelect.asInstanceOf[InsertIntoHiveTable]
                  }
                case _ =>
              }
            }
          }
          spark.listenerManager.register(listener)
          try {
            sql(s"CREATE TABLE t STORED AS PARQUET AS SELECT 1 as a")
            sparkContext.listenerBus.waitUntilEmpty()
            assert(v1WriteCommand != null)
            assert(v1WriteCommand.metrics.contains("numFiles"))
            assert(v1WriteCommand.metrics("numFiles").value == 1)
            assert(v1WriteCommand.metrics.contains("numOutputBytes"))
            assert(v1WriteCommand.metrics("numOutputBytes").value > 0)
            assert(v1WriteCommand.metrics.contains("numOutputRows"))
            assert(v1WriteCommand.metrics("numOutputRows").value == 1)
          } finally {
            spark.listenerManager.unregister(listener)
          }
        }
      }
    }
  }
}
