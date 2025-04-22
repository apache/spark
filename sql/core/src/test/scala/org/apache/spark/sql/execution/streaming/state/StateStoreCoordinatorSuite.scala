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

import java.util.UUID

import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SharedSparkContext, SparkContext, SparkFunSuite}
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.{LeftSide, RightSide}
import org.apache.spark.sql.functions.{count, expr}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQuery, StreamTest, Trigger}
import org.apache.spark.util.Utils

class StateStoreCoordinatorSuite extends SparkFunSuite with SharedSparkContext {

  import StateStoreCoordinatorSuite._

  test("report, verify, getLocation") {
    withCoordinatorRef(sc) { coordinatorRef =>
      val id = StateStoreProviderId(StateStoreId("x", 0, 0), UUID.randomUUID)

      assert(coordinatorRef.verifyIfInstanceActive(id, "exec1") === false)
      assert(coordinatorRef.getLocation(id) === None)

      coordinatorRef.reportActiveInstance(id, "hostX", "exec1", Seq.empty)
      eventually(timeout(5.seconds)) {
        assert(coordinatorRef.verifyIfInstanceActive(id, "exec1"))
        assert(
          coordinatorRef.getLocation(id) ===
            Some(ExecutorCacheTaskLocation("hostX", "exec1").toString))
      }

      coordinatorRef.reportActiveInstance(id, "hostX", "exec2", Seq.empty)

      eventually(timeout(5.seconds)) {
        assert(coordinatorRef.verifyIfInstanceActive(id, "exec1") === false)
        assert(coordinatorRef.verifyIfInstanceActive(id, "exec2"))

        assert(
          coordinatorRef.getLocation(id) ===
            Some(ExecutorCacheTaskLocation("hostX", "exec2").toString))
      }
    }
  }

  test("make inactive") {
    withCoordinatorRef(sc) { coordinatorRef =>
      val runId1 = UUID.randomUUID
      val runId2 = UUID.randomUUID
      val id1 = StateStoreProviderId(StateStoreId("x", 0, 0), runId1)
      val id2 = StateStoreProviderId(StateStoreId("y", 1, 0), runId2)
      val id3 = StateStoreProviderId(StateStoreId("x", 0, 1), runId1)
      val host = "hostX"
      val exec = "exec1"

      coordinatorRef.reportActiveInstance(id1, host, exec, Seq.empty)
      coordinatorRef.reportActiveInstance(id2, host, exec, Seq.empty)
      coordinatorRef.reportActiveInstance(id3, host, exec, Seq.empty)

      eventually(timeout(5.seconds)) {
        assert(coordinatorRef.verifyIfInstanceActive(id1, exec))
        assert(coordinatorRef.verifyIfInstanceActive(id2, exec))
        assert(coordinatorRef.verifyIfInstanceActive(id3, exec))
      }

      coordinatorRef.deactivateInstances(runId1)

      assert(coordinatorRef.verifyIfInstanceActive(id1, exec) === false)
      assert(coordinatorRef.verifyIfInstanceActive(id2, exec))
      assert(coordinatorRef.verifyIfInstanceActive(id3, exec) === false)

      assert(coordinatorRef.getLocation(id1) === None)
      assert(
        coordinatorRef.getLocation(id2) ===
          Some(ExecutorCacheTaskLocation(host, exec).toString))
      assert(coordinatorRef.getLocation(id3) === None)

      coordinatorRef.deactivateInstances(runId2)
      assert(coordinatorRef.verifyIfInstanceActive(id2, exec) === false)
      assert(coordinatorRef.getLocation(id2) === None)
    }
  }

  test("multiple references have same underlying coordinator") {
    withCoordinatorRef(sc) { coordRef1 =>
      val coordRef2 = StateStoreCoordinatorRef.forDriver(sc.env, new SQLConf)

      val id = StateStoreProviderId(StateStoreId("x", 0, 0), UUID.randomUUID)

      coordRef1.reportActiveInstance(id, "hostX", "exec1", Seq.empty)

      eventually(timeout(5.seconds)) {
        assert(coordRef2.verifyIfInstanceActive(id, "exec1"))
        assert(
          coordRef2.getLocation(id) ===
            Some(ExecutorCacheTaskLocation("hostX", "exec1").toString))
      }
    }
  }

  test("query stop deactivates related store providers") {
    var coordRef: StateStoreCoordinatorRef = null
    try {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      SparkSession.setActiveSession(spark)
      import spark.implicits._
      coordRef = spark.streams.stateStoreCoordinator
      implicit val sqlContext = spark.sqlContext
      spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "1")

      // Start a query and run a batch to load state stores
      val inputData = MemoryStream[Int]
      val aggregated = inputData.toDF().groupBy("value").agg(count("*")) // stateful query
      val checkpointLocation = Utils.createTempDir().getAbsoluteFile
      val query = aggregated.writeStream
        .format("memory")
        .outputMode("update")
        .queryName("query")
        .option("checkpointLocation", checkpointLocation.toString)
        .start()
      inputData.addData(1, 2, 3)
      query.processAllAvailable()

      // Verify state store has been loaded
      val stateCheckpointDir =
        query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.checkpointLocation
      val providerId = StateStoreProviderId(StateStoreId(stateCheckpointDir, 0, 0), query.runId)
      assert(coordRef.getLocation(providerId).nonEmpty)

      // Stop and verify whether the stores are deactivated in the coordinator
      query.stop()
      assert(coordRef.getLocation(providerId).isEmpty)
    } finally {
      SparkSession.getActiveSession.foreach(_.streams.active.foreach(_.stop()))
      if (coordRef != null) coordRef.stop()
      StateStore.stop()
    }
  }

  private val allJoinStateStoreNames: Seq[String] =
    SymmetricHashJoinStateManager.allStateStoreNames(LeftSide, RightSide)

  /** Lists the state store providers used for a test, and the set of lagging partition IDs */
  private val regularStateStoreProviders = Seq(
    ("RocksDBStateStoreProvider", classOf[RocksDBStateStoreProvider].getName, Set.empty[Int]),
    ("HDFSStateStoreProvider", classOf[HDFSBackedStateStoreProvider].getName, Set.empty[Int])
  )

  /** Lists the state store providers used for a test, and the set of lagging partition IDs */
  private val faultyStateStoreProviders = Seq(
    (
      "RocksDBSkipMaintenanceOnCertainPartitionsProvider",
      classOf[RocksDBSkipMaintenanceOnCertainPartitionsProvider].getName,
      Set(0, 1)
    ),
    (
      "HDFSBackedSkipMaintenanceOnCertainPartitionsProvider",
      classOf[HDFSBackedSkipMaintenanceOnCertainPartitionsProvider].getName,
      Set(0, 1)
    )
  )

  private val allStateStoreProviders =
    regularStateStoreProviders ++ faultyStateStoreProviders

  /**
   *  Verifies snapshot upload RPC messages from state stores are registered and verifies
   *  the coordinator detected the correct lagging partitions.
   */
  private def verifySnapshotUploadEvents(
      coordRef: StateStoreCoordinatorRef,
      query: StreamingQuery,
      badPartitions: Set[Int],
      storeNames: Seq[String] = Seq(StateStoreId.DEFAULT_STORE_NAME)): Unit = {
    val streamingQuery = query.asInstanceOf[StreamingQueryWrapper].streamingQuery
    val stateCheckpointDir = streamingQuery.lastExecution.checkpointLocation
    val latestVersion = streamingQuery.lastProgress.batchId + 1

    // Verify all stores have uploaded a snapshot and it's logged by the coordinator
    (0 until query.sparkSession.conf.get(SQLConf.SHUFFLE_PARTITIONS)).foreach {
      partitionId =>
        // Verify for every store name listed
        storeNames.foreach { storeName =>
          val storeId = StateStoreId(stateCheckpointDir, 0, partitionId, storeName)
          val providerId = StateStoreProviderId(storeId, query.runId)
          val latestSnapshotVersion = coordRef.getLatestSnapshotVersionForTesting(providerId)
          if (badPartitions.contains(partitionId)) {
            assert(latestSnapshotVersion.getOrElse(0) == 0)
          } else {
            assert(latestSnapshotVersion.get >= 0)
          }
        }
    }
    // Verify that only the bad partitions are all marked as lagging.
    // Join queries should have all their state stores marked as lagging,
    // which would be 4 stores per partition instead of 1.
    val laggingStores = coordRef.getLaggingStoresForTesting(query.runId, latestVersion)
    assert(laggingStores.size == badPartitions.size * storeNames.size)
    assert(laggingStores.map(_.storeId.partitionId).toSet == badPartitions)
  }

  /** Sets up a stateful dropDuplicate query for testing */
  private def setUpStatefulQuery(
      inputData: MemoryStream[Int], queryName: String): StreamingQuery = {
    // Set up a stateful drop duplicate query
    val aggregated = inputData.toDF().dropDuplicates()
    val checkpointLocation = Utils.createTempDir().getAbsoluteFile
    val query = aggregated.writeStream
      .format("memory")
      .outputMode("update")
      .queryName(queryName)
      .option("checkpointLocation", checkpointLocation.toString)
      .start()
    query
  }

  allStateStoreProviders.foreach { case (providerName, providerClassName, badPartitions) =>
    test(
      s"SPARK-51358: Snapshot uploads in $providerName are properly reported to the coordinator"
    ) {
      withCoordinatorAndSQLConf(
        sc,
        SQLConf.SHUFFLE_PARTITIONS.key -> "5",
        SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
        SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName,
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
        SQLConf.STATE_STORE_COORDINATOR_REPORT_SNAPSHOT_UPLOAD_LAG.key -> "true",
        SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_VERSION_DIFF_TO_LOG.key -> "2",
        SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_LAG_REPORT_INTERVAL.key -> "0"
      ) {
        case (coordRef, spark) =>
          import spark.implicits._
          implicit val sqlContext = spark.sqlContext
          val inputData = MemoryStream[Int]
          val query = setUpStatefulQuery(inputData, "query")
          // Add, commit, and wait multiple times to force snapshot versions and time difference
          (0 until 6).foreach { _ =>
            inputData.addData(1, 2, 3)
            query.processAllAvailable()
            Thread.sleep(500)
          }
          // Verify only the partitions in badPartitions are marked as lagging
          verifySnapshotUploadEvents(coordRef, query, badPartitions)
          query.stop()
      }
    }
  }

  allStateStoreProviders.foreach { case (providerName, providerClassName, badPartitions) =>
    test(
      s"SPARK-51358: Snapshot uploads for join queries with $providerName are properly " +
      s"reported to the coordinator"
    ) {
      withCoordinatorAndSQLConf(
        sc,
        SQLConf.SHUFFLE_PARTITIONS.key -> "3",
        SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
        SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName,
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
        SQLConf.STATE_STORE_COORDINATOR_REPORT_SNAPSHOT_UPLOAD_LAG.key -> "true",
        SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_VERSION_DIFF_TO_LOG.key -> "5",
        SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_LAG_REPORT_INTERVAL.key -> "0",
        SQLConf.STATE_STORE_COORDINATOR_MAX_LAGGING_STORES_TO_REPORT.key -> "5"
      ) {
        case (coordRef, spark) =>
          import spark.implicits._
          implicit val sqlContext = spark.sqlContext
          // Start a join query and run some data to force snapshot uploads
          val input1 = MemoryStream[Int]
          val input2 = MemoryStream[Int]
          val df1 = input1.toDF().select($"value" as "leftKey", ($"value" * 2) as "leftValue")
          val df2 = input2.toDF().select($"value" as "rightKey", ($"value" * 3) as "rightValue")
          val joined = df1.join(df2, expr("leftKey = rightKey"))
          val checkpointLocation = Utils.createTempDir().getAbsoluteFile
          val query = joined.writeStream
            .format("memory")
            .queryName("query")
            .option("checkpointLocation", checkpointLocation.toString)
            .start()
          // Add, commit, and wait multiple times to force snapshot versions and time difference
          (0 until 7).foreach { _ =>
            input1.addData(1, 5)
            input2.addData(1, 5, 10)
            query.processAllAvailable()
            Thread.sleep(500)
          }
          // Verify only the partitions in badPartitions are marked as lagging
          verifySnapshotUploadEvents(coordRef, query, badPartitions, allJoinStateStoreNames)
          query.stop()
      }
    }
  }

  test("SPARK-51358: Verify coordinator properly handles simultaneous query runs") {
    withCoordinatorAndSQLConf(
      sc,
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBSkipMaintenanceOnCertainPartitionsProvider].getName,
      RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
      SQLConf.STATE_STORE_COORDINATOR_REPORT_SNAPSHOT_UPLOAD_LAG.key -> "true",
      SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_VERSION_DIFF_TO_LOG.key -> "2",
      SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_LAG_REPORT_INTERVAL.key -> "0"
    ) {
      case (coordRef, spark) =>
        import spark.implicits._
        implicit val sqlContext = spark.sqlContext
        // Start and run two queries together with some data to force snapshot uploads
        val input1 = MemoryStream[Int]
        val input2 = MemoryStream[Int]
        val query1 = setUpStatefulQuery(input1, "query1")
        val query2 = setUpStatefulQuery(input2, "query2")

        // Go through several rounds of input to force snapshot uploads for both queries
        (0 until 2).foreach { _ =>
          input1.addData(1, 2, 3)
          input2.addData(1, 2, 3)
          query1.processAllAvailable()
          query2.processAllAvailable()
          // Process twice the amount of data for the first query
          input1.addData(1, 2, 3)
          query1.processAllAvailable()
          Thread.sleep(1000)
        }
        // Verify that the coordinator logged the correct lagging stores for the first query
        val streamingQuery1 = query1.asInstanceOf[StreamingQueryWrapper].streamingQuery
        val latestVersion1 = streamingQuery1.lastProgress.batchId + 1
        val laggingStores1 = coordRef.getLaggingStoresForTesting(query1.runId, latestVersion1)

        assert(laggingStores1.size == 2)
        assert(laggingStores1.forall(_.storeId.partitionId <= 1))
        assert(laggingStores1.forall(_.queryRunId == query1.runId))

        // Verify that the second query run hasn't reported anything yet due to lack of data
        val streamingQuery2 = query2.asInstanceOf[StreamingQueryWrapper].streamingQuery
        var latestVersion2 = streamingQuery2.lastProgress.batchId + 1
        var laggingStores2 = coordRef.getLaggingStoresForTesting(query2.runId, latestVersion2)
        assert(laggingStores2.isEmpty)

        // Process some more data for the second query to force lag reports
        input2.addData(1, 2, 3)
        query2.processAllAvailable()
        Thread.sleep(500)

        // Verify that the coordinator logged the correct lagging stores for the second query
        latestVersion2 = streamingQuery2.lastProgress.batchId + 1
        laggingStores2 = coordRef.getLaggingStoresForTesting(query2.runId, latestVersion2)

        assert(laggingStores2.size == 2)
        assert(laggingStores2.forall(_.storeId.partitionId <= 1))
        assert(laggingStores2.forall(_.queryRunId == query2.runId))
    }
  }

  test(
    "SPARK-51358: Snapshot uploads in RocksDB are not reported if changelog " +
    "checkpointing is disabled"
  ) {
    withCoordinatorAndSQLConf(
      sc,
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "false",
      SQLConf.STATE_STORE_COORDINATOR_REPORT_SNAPSHOT_UPLOAD_LAG.key -> "true",
      SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_TIME_DIFF_TO_LOG.key -> "1",
      SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_VERSION_DIFF_TO_LOG.key -> "1",
      SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_LAG_REPORT_INTERVAL.key -> "0"
    ) {
      case (coordRef, spark) =>
        import spark.implicits._
        implicit val sqlContext = spark.sqlContext
        // Start a query and run some data to force snapshot uploads
        val inputData = MemoryStream[Int]
        val query = setUpStatefulQuery(inputData, "query")

        // Go through two batches to force two snapshot uploads.
        // This would be enough to pass the version check for lagging stores.
        inputData.addData(1, 2, 3)
        query.processAllAvailable()
        inputData.addData(1, 2, 3)
        query.processAllAvailable()

        // Sleep for the duration of a maintenance interval - which should be enough
        // to pass the time check for lagging stores.
        Thread.sleep(100)

        val latestVersion =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastProgress.batchId + 1
        // Verify that no instances are marked as lagging, even when upload messages are sent.
        // Since snapshot uploads are tied to commit, the lack of version difference should prevent
        // the stores from being marked as lagging.
        assert(coordRef.getLaggingStoresForTesting(query.runId, latestVersion).isEmpty)
        query.stop()
    }
  }

  test("SPARK-51358: Snapshot lag reports properly detects when all state stores are lagging") {
    withCoordinatorAndSQLConf(
      sc,
      // Only use two partitions with the faulty store provider (both stores will skip uploads)
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBSkipMaintenanceOnCertainPartitionsProvider].getName,
      RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
      SQLConf.STATE_STORE_COORDINATOR_REPORT_SNAPSHOT_UPLOAD_LAG.key -> "true",
      SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_TIME_DIFF_TO_LOG.key -> "1",
      SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_VERSION_DIFF_TO_LOG.key -> "2",
      SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_LAG_REPORT_INTERVAL.key -> "0"
    ) {
      case (coordRef, spark) =>
        import spark.implicits._
        implicit val sqlContext = spark.sqlContext
        // Start a query and run some data to force snapshot uploads
        val inputData = MemoryStream[Int]
        val query = setUpStatefulQuery(inputData, "query")

        // Go through several rounds of input to force snapshot uploads
        (0 until 3).foreach { _ =>
          inputData.addData(1, 2, 3)
          query.processAllAvailable()
          Thread.sleep(500)
        }
        val latestVersion =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastProgress.batchId + 1
        // Verify that all instances are marked as lagging, since no upload messages are being sent
        assert(coordRef.getLaggingStoresForTesting(query.runId, latestVersion).size == 2)
        query.stop()
    }
  }
}

class StateStoreCoordinatorStreamingSuite extends StreamTest {
  import testImplicits._

  Seq(
    ("RocksDB", classOf[RocksDBSkipMaintenanceOnCertainPartitionsProvider].getName),
    ("HDFS", classOf[HDFSBackedSkipMaintenanceOnCertainPartitionsProvider].getName)
  ).foreach { case (providerName, providerClassName) =>
    test(
      s"SPARK-51358: Restarting queries do not mark state stores as lagging for $providerName"
    ) {
      withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "3",
        SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
        SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "2",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName,
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
        SQLConf.STATE_STORE_COORDINATOR_REPORT_SNAPSHOT_UPLOAD_LAG.key -> "true",
        SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_VERSION_DIFF_TO_LOG.key -> "2",
        SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_TIME_DIFF_TO_LOG.key -> "5",
        SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_LAG_REPORT_INTERVAL.key -> "0"
      ) {
        withTempDir { srcDir =>
          val inputData = MemoryStream[Int]
          val query = inputData.toDF().dropDuplicates()
          val numPartitions = query.sparkSession.conf.get(SQLConf.SHUFFLE_PARTITIONS)
          // Keep track of state checkpoint directory for the second run
          var stateCheckpoint = ""

          testStream(query)(
            StartStream(checkpointLocation = srcDir.getCanonicalPath),
            AddData(inputData, 1, 2, 3),
            ProcessAllAvailable(),
            AddData(inputData, 1, 2, 3),
            ProcessAllAvailable(),
            AddData(inputData, 1, 2, 3),
            ProcessAllAvailable(),
            AddData(inputData, 1, 2, 3),
            ProcessAllAvailable(),
            AddData(inputData, 1, 2, 3),
            ProcessAllAvailable(),
            Execute { query =>
              val coordRef =
                query.sparkSession.sessionState.streamingQueryManager.stateStoreCoordinator
              stateCheckpoint = query.lastExecution.checkpointLocation
              val latestVersion = query.lastProgress.batchId + 1

              // Verify the coordinator logged snapshot uploads
              (0 until numPartitions).map {
                partitionId =>
                  val storeId = StateStoreId(stateCheckpoint, 0, partitionId)
                  val providerId = StateStoreProviderId(storeId, query.runId)
                  if (partitionId <= 1) {
                    // Verify state stores in partition 0 and 1 are lagging and didn't upload
                    assert(
                      coordRef.getLatestSnapshotVersionForTesting(providerId).getOrElse(0) == 0
                    )
                  } else {
                    // Verify other stores have uploaded a snapshot and it's properly logged
                    assert(coordRef.getLatestSnapshotVersionForTesting(providerId).get >= 0)
                  }
              }
              // Verify that the normal state store (partitionId=2) is not lagging behind,
              // and the faulty stores are reported as lagging.
              val laggingStores =
                coordRef.getLaggingStoresForTesting(query.runId, latestVersion)
              assert(laggingStores.size == 2)
              assert(laggingStores.forall(_.storeId.partitionId <= 1))
            },
            // Stopping the streaming query should deactivate and clear snapshot uploaded events
            StopStream,
            Execute { query =>
              val coordRef =
                query.sparkSession.sessionState.streamingQueryManager.stateStoreCoordinator
              val latestVersion = query.lastProgress.batchId + 1

              // Verify we evicted the previous latest uploaded snapshots from the coordinator
              (0 until numPartitions).map { partitionId =>
                val storeId = StateStoreId(stateCheckpoint, 0, partitionId)
                val providerId = StateStoreProviderId(storeId, query.runId)
                assert(coordRef.getLatestSnapshotVersionForTesting(providerId).isEmpty)
              }
              // Verify that we are not reporting any lagging stores after eviction,
              // since none of these state stores are active anymore.
              assert(coordRef.getLaggingStoresForTesting(query.runId, latestVersion).isEmpty)
            }
          )
          // Restart the query, but do not add too much data so that we don't associate
          // the current StateStoreProviderId (store id + query run id) with any new uploads.
          testStream(query)(
            StartStream(checkpointLocation = srcDir.getCanonicalPath),
            // Perform one round of data, which is enough to activate instances and force a
            // lagging instance report, but not enough to trigger a snapshot upload yet.
            AddData(inputData, 1, 2, 3),
            ProcessAllAvailable(),
            Execute { query =>
              val coordRef =
                query.sparkSession.sessionState.streamingQueryManager.stateStoreCoordinator
              val latestVersion = query.lastProgress.batchId + 1
              // Verify that the state stores have restored their snapshot version from the
              // checkpoint and reported their current version
              (0 until numPartitions).map {
                partitionId =>
                  val storeId = StateStoreId(stateCheckpoint, 0, partitionId)
                  val providerId = StateStoreProviderId(storeId, query.runId)
                  val latestSnapshotVersion =
                    coordRef.getLatestSnapshotVersionForTesting(providerId)
                  if (partitionId <= 1) {
                    // Verify state stores in partition 0/1 are still lagging and didn't upload
                    assert(latestSnapshotVersion.getOrElse(0) == 0)
                  } else {
                    // Verify other stores have uploaded a snapshot and it's properly logged
                    assert(latestSnapshotVersion.get > 0)
                  }
              }
              // Sleep a bit to allow the coordinator to pass the time threshold and report lag
              Thread.sleep(5 * 100)
              // Verify that we're reporting the faulty state stores (partitionId 0 and 1)
              val laggingStores =
                coordRef.getLaggingStoresForTesting(query.runId, latestVersion)
              assert(laggingStores.size == 2)
              assert(laggingStores.forall(_.storeId.partitionId <= 1))
            },
            StopStream
          )
        }
      }
    }
  }

  test("SPARK-51358: Restarting queries with updated SQLConf get propagated to the coordinator") {
    withSQLConf(
      SQLConf.SHUFFLE_PARTITIONS.key -> "3",
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBSkipMaintenanceOnCertainPartitionsProvider].getName,
      RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
      SQLConf.STATE_STORE_COORDINATOR_REPORT_SNAPSHOT_UPLOAD_LAG.key -> "true",
      SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_VERSION_DIFF_TO_LOG.key -> "1",
      SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_TIME_DIFF_TO_LOG.key -> "5",
      SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_LAG_REPORT_INTERVAL.key -> "0"
    ) {
      withTempDir { srcDir =>
        val inputData = MemoryStream[Int]
        val query = inputData.toDF().dropDuplicates()

        testStream(query)(
          StartStream(checkpointLocation = srcDir.getCanonicalPath),
          // Process multiple batches so that the coordinator can start reporting lagging instances
          AddData(inputData, 1, 2, 3),
          ProcessAllAvailable(),
          AddData(inputData, 1, 2, 3),
          ProcessAllAvailable(),
          AddData(inputData, 1, 2, 3),
          ProcessAllAvailable(),
          Execute { query =>
            val coordRef =
              query.sparkSession.sessionState.streamingQueryManager.stateStoreCoordinator
            val latestVersion = query.lastProgress.batchId + 1
            // Sleep a bit to allow the coordinator to pass the time threshold and report lag
            Thread.sleep(5 * 100)
            // Verify that only the faulty stores are reported as lagging
            val laggingStores =
              coordRef.getLaggingStoresForTesting(query.runId, latestVersion)
            assert(laggingStores.size == 2)
            assert(laggingStores.forall(_.storeId.partitionId <= 1))
          },
          // Stopping the streaming query should deactivate and clear snapshot uploaded events
          StopStream
        )
        // Bump up version multiplier, which would stop the coordinator from reporting
        // lagging stores for the next few versions
        spark.conf
            .set(SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_VERSION_DIFF_TO_LOG.key, "10")
        // Restart the query, and verify the conf change reflects in the coordinator
        testStream(query)(
          StartStream(checkpointLocation = srcDir.getCanonicalPath),
          // Process the same amount of data as the first run
          AddData(inputData, 1, 2, 3),
          ProcessAllAvailable(),
          AddData(inputData, 1, 2, 3),
          ProcessAllAvailable(),
          AddData(inputData, 1, 2, 3),
          ProcessAllAvailable(),
          Execute { query =>
            val coordRef =
              query.sparkSession.sessionState.streamingQueryManager.stateStoreCoordinator
            val latestVersion = query.lastProgress.batchId + 1
            // Sleep the same amount to mimic conditions from first run
            Thread.sleep(5 * 100)
            // Verify that we are not reporting any lagging stores despite restarting
            // because of the higher version multiplier
            assert(coordRef.getLaggingStoresForTesting(query.runId, latestVersion).isEmpty)
          },
          StopStream
        )
      }
    }
  }

  Seq(
    ("RocksDB", classOf[RocksDBStateStoreProvider].getName),
    ("HDFS", classOf[HDFSBackedStateStoreProvider].getName)
  ).foreach { case (providerName, providerClassName) =>
    test(
      s"SPARK-51358: Infrequent maintenance with $providerName using Trigger.AvailableNow " +
      s"should be reported"
    ) {
      withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
        SQLConf.STATE_STORE_MAINTENANCE_SHUTDOWN_TIMEOUT.key -> "3",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName,
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
        SQLConf.STATE_STORE_COORDINATOR_REPORT_SNAPSHOT_UPLOAD_LAG.key -> "true",
        SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_VERSION_DIFF_TO_LOG.key -> "2",
        SQLConf.STATE_STORE_COORDINATOR_MULTIPLIER_FOR_MIN_TIME_DIFF_TO_LOG.key -> "50",
        SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_LAG_REPORT_INTERVAL.key -> "0"
      ) {
        withTempDir { srcDir =>
          val inputData = MemoryStream[Int]
          val query = inputData.toDF().dropDuplicates()

          // Populate state stores with an initial snapshot, so that timestamp isn't marked
          // as the default 0ms.
          testStream(query)(
            StartStream(checkpointLocation = srcDir.getCanonicalPath),
            AddData(inputData, 1, 2, 3),
            ProcessAllAvailable(),
            AddData(inputData, 1, 2, 3),
            ProcessAllAvailable(),
            AddData(inputData, 1, 2, 3),
            ProcessAllAvailable()
          )
          // Increase maintenance interval to a much larger value to stop snapshot uploads
          spark.conf.set(SQLConf.STREAMING_MAINTENANCE_INTERVAL.key, "60000")
          // Execute a few batches in a short span
          testStream(query)(
            AddData(inputData, 1, 2, 3),
            StartStream(Trigger.AvailableNow, checkpointLocation = srcDir.getCanonicalPath),
            Execute { query =>
              query.awaitTermination()
              // Verify the query ran with the AvailableNow trigger
              assert(query.lastExecution.isTerminatingTrigger)
            },
            AddData(inputData, 1, 2, 3),
            StartStream(Trigger.AvailableNow, checkpointLocation = srcDir.getCanonicalPath),
            Execute { query =>
              query.awaitTermination()
            },
            // Start without available now, otherwise the stream closes too quickly for the
            // testing RPC call to report lagging state stores
            StartStream(checkpointLocation = srcDir.getCanonicalPath),
            // Process data to activate state stores, but not enough to trigger snapshot uploads
            AddData(inputData, 1, 2, 3),
            ProcessAllAvailable(),
            Execute { query =>
              val coordRef =
                query.sparkSession.sessionState.streamingQueryManager.stateStoreCoordinator
              val latestVersion = query.lastProgress.batchId + 1
              // Verify that all faulty stores are reported as lagging despite the short burst.
              // This test scenario mimics cases where snapshots have not been uploaded for
              // a while due to the short running duration of AvailableNow.
              val laggingStores = coordRef.getLaggingStoresForTesting(
                query.runId,
                latestVersion,
                isTerminatingTrigger = true
              )
              assert(laggingStores.size == 2)
              assert(laggingStores.forall(_.storeId.partitionId <= 1))
            },
            StopStream
          )
        }
      }
    }
  }
}

object StateStoreCoordinatorSuite {
  def withCoordinatorRef(sc: SparkContext)(body: StateStoreCoordinatorRef => Unit): Unit = {
    var coordinatorRef: StateStoreCoordinatorRef = null
    try {
      coordinatorRef = StateStoreCoordinatorRef.forDriver(sc.env, new SQLConf)
      body(coordinatorRef)
    } finally {
      if (coordinatorRef != null) coordinatorRef.stop()
    }
  }

  def withCoordinatorAndSQLConf(sc: SparkContext, pairs: (String, String)*)(
      body: (StateStoreCoordinatorRef, SparkSession) => Unit): Unit = {
    var spark: SparkSession = null
    var coordinatorRef: StateStoreCoordinatorRef = null
    try {
      spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      SparkSession.setActiveSession(spark)
      coordinatorRef = spark.streams.stateStoreCoordinator
      // Set up SQLConf entries
      pairs.foreach { case (key, value) => spark.conf.set(key, value) }
      body(coordinatorRef, spark)
    } finally {
      SparkSession.getActiveSession.foreach(_.streams.active.foreach(_.stop()))
      // Unset all custom SQLConf entries
      if (spark != null) pairs.foreach { case (key, _) => spark.conf.unset(key) }
      if (coordinatorRef != null) coordinatorRef.stop()
      StateStore.stop()
    }
  }
}
