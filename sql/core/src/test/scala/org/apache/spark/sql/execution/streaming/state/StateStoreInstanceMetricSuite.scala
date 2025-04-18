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

package org.apache.spark.sql.execution.streaming.state

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.MapHasAsScala

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._

// RocksDBSkipMaintenanceOnCertainPartitionsProvider is a test-only provider that skips running
// maintenance for partitions 0 and 1 (these are arbitrary choices). This is used to test
// snapshot upload lag can be observed through StreamingQueryProgress metrics.
class RocksDBSkipMaintenanceOnCertainPartitionsProvider extends RocksDBStateStoreProvider {
  override def doMaintenance(): Unit = {
    if (stateStoreId.partitionId == 0 || stateStoreId.partitionId == 1) {
      return
    }
    super.doMaintenance()
  }
}

// HDFSBackedSkipMaintenanceOnCertainPartitionsProvider is a test-only provider that skips running
// maintenance for partitions 0 and 1 (these are arbitrary choices). This is used to test
// snapshot upload lag can be observed through StreamingQueryProgress metrics.
class HDFSBackedSkipMaintenanceOnCertainPartitionsProvider extends HDFSBackedStateStoreProvider {
  override def doMaintenance(): Unit = {
    if (stateStoreId.partitionId == 0 || stateStoreId.partitionId == 1) {
      return
    }
    super.doMaintenance()
  }
}

class StateStoreInstanceMetricSuite extends StreamTest with AlsoTestWithRocksDBFeatures {
  import testImplicits._

  private val SNAPSHOT_LAG_METRIC_PREFIX = "SnapshotLastUploaded.partition_"

  private def snapshotLagMetricName(
      partitionId: Long,
      storeName: String = StateStoreId.DEFAULT_STORE_NAME): String = {
    s"$SNAPSHOT_LAG_METRIC_PREFIX${partitionId}_$storeName"
  }

  Seq(
    ("SPARK-51097", "RocksDBStateStoreProvider", classOf[RocksDBStateStoreProvider].getName),
    ("SPARK-51252", "HDFSBackedStateStoreProvider", classOf[HDFSBackedStateStoreProvider].getName)
  ).foreach {
    case (ticketPrefix, providerName, providerClassName) =>
      testWithChangelogCheckpointingEnabled(
        s"$ticketPrefix: Verify snapshot lag metrics are updated correctly with $providerName"
      ) {
        withSQLConf(
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName,
          SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
          SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10",
          SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
          SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
          SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT.key -> "3"
        ) {
          withTempDir { checkpointDir =>
            val inputData = MemoryStream[String]
            val result = inputData.toDS().dropDuplicates()

            testStream(result, outputMode = OutputMode.Update)(
              StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
              AddData(inputData, "a"),
              ProcessAllAvailable(),
              AddData(inputData, "b"),
              ProcessAllAvailable(),
              AddData(inputData, "b"),
              ProcessAllAvailable(),
              AddData(inputData, "b"),
              ProcessAllAvailable(),
              CheckNewAnswer("a", "b"),
              Execute { q =>
                // Make sure only smallest K active metrics are published
                eventually(timeout(10.seconds)) {
                  val instanceMetrics = q.lastProgress
                    .stateOperators(0)
                    .customMetrics
                    .asScala
                    .view
                    .filterKeys(_.startsWith(SNAPSHOT_LAG_METRIC_PREFIX))
                  // Determined by STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT
                  assert(
                    instanceMetrics.size == q.sparkSession.conf
                      .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT)
                  )
                  // All state store instances should have uploaded a version
                  assert(instanceMetrics.forall(_._2 >= 0))
                }
              },
              StopStream
            )
          }
        }
      }
  }

  Seq(
    (
      "SPARK-51097",
      "RocksDBSkipMaintenanceOnCertainPartitionsProvider",
      classOf[RocksDBSkipMaintenanceOnCertainPartitionsProvider].getName
    ),
    (
      "SPARK-51252",
      "HDFSBackedSkipMaintenanceOnCertainPartitionsProvider",
      classOf[HDFSBackedSkipMaintenanceOnCertainPartitionsProvider].getName
    )
  ).foreach {
    case (ticketPrefix, providerName, providerClassName) =>
      testWithChangelogCheckpointingEnabled(
        s"$ticketPrefix: Verify snapshot lag metrics are updated correctly with $providerName"
      ) {
        withSQLConf(
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName,
          SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
          SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10",
          SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
          SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
          SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT.key -> "3"
        ) {
          withTempDir { checkpointDir =>
            val inputData = MemoryStream[String]
            val result = inputData.toDS().dropDuplicates()

            testStream(result, outputMode = OutputMode.Update)(
              StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
              AddData(inputData, "a"),
              ProcessAllAvailable(),
              AddData(inputData, "b"),
              ProcessAllAvailable(),
              AddData(inputData, "b"),
              ProcessAllAvailable(),
              AddData(inputData, "b"),
              ProcessAllAvailable(),
              CheckNewAnswer("a", "b"),
              Execute { q =>
                // Partitions getting skipped (id 0 and 1) do not have an uploaded version, leaving
                // those instance metrics as -1.
                eventually(timeout(10.seconds)) {
                  assert(
                    q.lastProgress
                      .stateOperators(0)
                      .customMetrics
                      .get(snapshotLagMetricName(0)) === -1
                  )
                  assert(
                    q.lastProgress
                      .stateOperators(0)
                      .customMetrics
                      .get(snapshotLagMetricName(1)) === -1
                  )
                  // Make sure only smallest K active metrics are published
                  val instanceMetrics = q.lastProgress
                    .stateOperators(0)
                    .customMetrics
                    .asScala
                    .view
                    .filterKeys(_.startsWith(SNAPSHOT_LAG_METRIC_PREFIX))
                  // Determined by STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT
                  assert(
                    instanceMetrics.size == q.sparkSession.conf
                      .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT)
                  )
                  // Two metrics published are -1, but the remainder should all be set to a
                  // non-negative version as they uploaded properly.
                  assert(
                    instanceMetrics.count(_._2 >= 0) == q.sparkSession.conf
                      .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT) - 2
                  )
                }
              },
              StopStream
            )
          }
        }
      }
  }

  Seq(
    ("SPARK-51097", "RocksDBStateStoreProvider", classOf[RocksDBStateStoreProvider].getName),
    ("SPARK-51252", "HDFSBackedStateStoreProvider", classOf[HDFSBackedStateStoreProvider].getName)
  ).foreach {
    case (ticketPrefix, providerName, providerClassName) =>
      testWithChangelogCheckpointingEnabled(
        s"$ticketPrefix: Verify snapshot lag metrics are updated correctly for join queries with " +
        s"$providerName"
      ) {
        withSQLConf(
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName,
          SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
          SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10",
          SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
          SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
          SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT.key -> "10",
          SQLConf.SHUFFLE_PARTITIONS.key -> "3"
        ) {
          withTempDir { checkpointDir =>
            val input1 = MemoryStream[Int]
            val input2 = MemoryStream[Int]

            val df1 = input1.toDF().select($"value" as "leftKey", ($"value" * 2) as "leftValue")
            val df2 = input2
              .toDF()
              .select($"value" as "rightKey", ($"value" * 3) as "rightValue")
            val joined = df1.join(df2, expr("leftKey = rightKey"))

            testStream(joined)(
              StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
              AddData(input1, 1, 5),
              ProcessAllAvailable(),
              AddData(input2, 1, 5, 10),
              ProcessAllAvailable(),
              AddData(input1, 2, 3),
              ProcessAllAvailable(),
              AddData(input1, 2),
              ProcessAllAvailable(),
              AddData(input2, 3),
              ProcessAllAvailable(),
              AddData(input1, 4),
              ProcessAllAvailable(),
              Execute { q =>
                eventually(timeout(10.seconds)) {
                  // Make sure only smallest K active metrics are published.
                  // There are 3 * 4 = 12 metrics in total because of join, but only 10
                  // are published.
                  val instanceMetrics = q.lastProgress
                    .stateOperators(0)
                    .customMetrics
                    .asScala
                    .view
                    .filterKeys(_.startsWith(SNAPSHOT_LAG_METRIC_PREFIX))
                  // Determined by STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT
                  assert(
                    instanceMetrics.size == q.sparkSession.conf
                      .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT)
                  )
                  // All state store instances should have uploaded a version
                  assert(instanceMetrics.forall(_._2 >= 0))
                }
              },
              StopStream
            )
          }
        }
      }
  }

  Seq(
    (
      "SPARK-51097",
      "RocksDBSkipMaintenanceOnCertainPartitionsProvider",
      classOf[RocksDBSkipMaintenanceOnCertainPartitionsProvider].getName
    ),
    (
      "SPARK-51252",
      "HDFSBackedSkipMaintenanceOnCertainPartitionsProvider",
      classOf[HDFSBackedSkipMaintenanceOnCertainPartitionsProvider].getName
    )
  ).foreach {
    case (ticketPrefix, providerName, providerClassName) =>
      testWithChangelogCheckpointingEnabled(
        s"$ticketPrefix: Verify snapshot lag metrics are updated correctly for join queries with " +
        s"$providerName"
      ) {
        withSQLConf(
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName,
          SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
          SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10",
          SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
          SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
          SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT.key -> "10"
        ) {
          withTempDir { checkpointDir =>
            val input1 = MemoryStream[Int]
            val input2 = MemoryStream[Int]

            val df1 = input1.toDF().select($"value" as "leftKey", ($"value" * 2) as "leftValue")
            val df2 = input2
              .toDF()
              .select($"value" as "rightKey", ($"value" * 3) as "rightValue")
            val joined = df1.join(df2, expr("leftKey = rightKey"))

            testStream(joined)(
              StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
              AddData(input1, 1, 5),
              ProcessAllAvailable(),
              AddData(input2, 1, 5, 10),
              ProcessAllAvailable(),
              AddData(input1, 2, 3),
              ProcessAllAvailable(),
              AddData(input1, 2),
              ProcessAllAvailable(),
              AddData(input2, 3),
              ProcessAllAvailable(),
              AddData(input1, 4),
              ProcessAllAvailable(),
              Execute { q =>
                eventually(timeout(10.seconds)) {
                  // Make sure only smallest K active metrics are published.
                  // There are 5 * 4 = 20 metrics in total because of join, but only 10
                  // are published.
                  val allInstanceMetrics = q.lastProgress
                    .stateOperators(0)
                    .customMetrics
                    .asScala
                    .view
                    .filterKeys(_.startsWith(SNAPSHOT_LAG_METRIC_PREFIX))
                  val badInstanceMetrics = allInstanceMetrics.filterKeys(
                    k =>
                      k.startsWith(snapshotLagMetricName(0, "")) ||
                      k.startsWith(snapshotLagMetricName(1, ""))
                  )
                  // Determined by STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT
                  assert(
                    allInstanceMetrics.size == q.sparkSession.conf
                      .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT)
                  )
                  // Two ids are blocked, each with four state stores
                  assert(badInstanceMetrics.count(_._2 == -1) == 2 * 4)
                  // The rest should have uploaded a version
                  assert(
                    allInstanceMetrics.count(_._2 >= 0) == q.sparkSession.conf
                      .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT) - 2 * 4
                  )
                }
              },
              StopStream
            )
          }
        }
      }
  }

  testWithChangelogCheckpointingEnabled(
    "SPARK-51779 Verify snapshot lag metrics are updated correctly for join " +
    "using virtual column families with RocksDB"
  ) {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBSkipMaintenanceOnCertainPartitionsProvider].getName,
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10",
      SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT.key -> "4",
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "3"
    ) {
      withTempDir { checkpointDir =>
        val input1 = MemoryStream[Int]
        val input2 = MemoryStream[Int]

        val df1 = input1.toDF().select($"value" as "leftKey", ($"value" * 2) as "leftValue")
        val df2 = input2
          .toDF()
          .select($"value" as "rightKey", ($"value" * 3) as "rightValue")
        val joined = df1.join(df2, expr("leftKey = rightKey"))

        testStream(joined)(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(input1, 1, 5),
          ProcessAllAvailable(),
          AddData(input2, 1, 5, 10),
          ProcessAllAvailable(),
          AddData(input1, 2, 3),
          ProcessAllAvailable(),
          CheckNewAnswer((1, 2, 1, 3), (5, 10, 5, 15)),
          AddData(input1, 2),
          ProcessAllAvailable(),
          AddData(input2, 3),
          ProcessAllAvailable(),
          AddData(input1, 4),
          ProcessAllAvailable(),
          Execute { q =>
            eventually(timeout(10.seconds)) {
              // Make sure only smallest K active metrics are published.
              // There are 5 metrics in total, but only 4 are published.
              val allInstanceMetrics = q.lastProgress
                .stateOperators(0)
                .customMetrics
                .asScala
                .filter(_._1.startsWith(SNAPSHOT_LAG_METRIC_PREFIX))
              val badInstanceMetrics = allInstanceMetrics.filter {
                case (key, _) =>
                  key.startsWith(snapshotLagMetricName(0, "")) ||
                    key.startsWith(snapshotLagMetricName(1, ""))
              }
              // Determined by STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT
              assert(
                allInstanceMetrics.size == q.sparkSession.conf
                  .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT)
              )
              // However, creating a family column forces a snapshot regardless of maintenance
              // Thus, the version will be 1 for this case.
              // Since join queries only use one state store now, the number of instance metrics
              // is equal to the number of shuffle partitions as well.
              // Two partition ids are blocked, making two lagging stores in total instead
              // of 2 * 4 = 8 like previously.
              assert(badInstanceMetrics.count(_._2 == 1) == 2)
              // The rest should have uploaded a version greater than 1
              assert(
                allInstanceMetrics.count(_._2 >= 2) == q.sparkSession.conf
                  .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT) - 2
              )
            }
          },
          StopStream
        )
      }
    }
  }

  Seq(
    ("SPARK-51097", "RocksDBStateStoreProvider", classOf[RocksDBStateStoreProvider].getName),
    ("SPARK-51252", "HDFSBackedStateStoreProvider", classOf[HDFSBackedStateStoreProvider].getName)
  ).foreach {
    case (ticketPrefix, providerName, providerClassName) =>
      testWithChangelogCheckpointingEnabled(
        s"$ticketPrefix: Verify instance metrics for $providerName are not collected " +
        s"in execution plan"
      ) {
        withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName) {
          withTempDir { checkpointDir =>
            val input1 = MemoryStream[Int]
            val input2 = MemoryStream[Int]

            val df1 = input1.toDF().select($"value" as "leftKey", ($"value" * 2) as "leftValue")
            val df2 = input2
              .toDF()
              .select($"value" as "rightKey", ($"value" * 3) as "rightValue")
            val joined = df1.join(df2, expr("leftKey = rightKey"))

            testStream(joined)(
              StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
              AddData(input1, 1, 5),
              ProcessAllAvailable(),
              AddData(input2, 1, 5, 10),
              ProcessAllAvailable(),
              CheckNewAnswer((1, 2, 1, 3), (5, 10, 5, 15)),
              AssertOnQuery { q =>
                // Go through all elements in the execution plan and verify none of the metrics
                // are generated from RocksDB's snapshot lag instance metrics.
                q.lastExecution.executedPlan
                  .collect {
                    case node => node.metrics
                  }
                  .forall { nodeMetrics =>
                    nodeMetrics.forall(metric => !metric._1.startsWith(SNAPSHOT_LAG_METRIC_PREFIX))
                  }
              },
              StopStream
            )
          }
        }
      }
  }
}
