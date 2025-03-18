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

  test(
    "SPARK-51358: Snapshot uploads in RocksDB are not reported if changelog " +
    "checkpointing is disabled"
  ) {
    withCoordinatorAndSQLConf(
      sc,
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "false",
      SQLConf.STATE_STORE_COORDINATOR_REPORT_UPLOAD_ENABLED.key -> "true"
    ) {
      case (coordRef, spark) =>
        import spark.implicits._
        implicit val sqlContext = spark.sqlContext

        // Start a query and run some data to force snapshot uploads
        val inputData = MemoryStream[Int]
        val aggregated = inputData.toDF().dropDuplicates()
        val checkpointLocation = Utils.createTempDir().getAbsoluteFile
        val query = aggregated.writeStream
          .format("memory")
          .outputMode("update")
          .queryName("query")
          .option("checkpointLocation", checkpointLocation.toString)
          .start()
        inputData.addData(1, 2, 3)
        query.processAllAvailable()
        inputData.addData(1, 2, 3)
        query.processAllAvailable()
        val stateCheckpointDir =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.checkpointLocation

        // Verify stores do not report snapshot upload events to the coordinator.
        // As a result, all stores will return nothing as the latest version
        (0 until query.sparkSession.conf.get(SQLConf.SHUFFLE_PARTITIONS)).foreach { partitionId =>
          val providerId =
            StateStoreProviderId(StateStoreId(stateCheckpointDir, 0, partitionId), query.runId)
          assert(coordRef.getLatestSnapshotVersionForTesting(providerId).isEmpty)
        }
        query.stop()
    }
  }

  test("SPARK-51358: Snapshot uploads in RocksDB are properly reported to the coordinator") {
    withCoordinatorAndSQLConf(
      sc,
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
      SQLConf.STATE_STORE_COORDINATOR_REPORT_UPLOAD_ENABLED.key -> "true",
      SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_DELTA_MULTIPLIER_FOR_MIN_VERSION_DELTA_TO_LOG.key ->
        "2"
    ) {
      case (coordRef, spark) =>
        import spark.implicits._
        implicit val sqlContext = spark.sqlContext

        // Start a query and run some data to force snapshot uploads
        val inputData = MemoryStream[Int]
        val aggregated = inputData.toDF().dropDuplicates()
        val checkpointLocation = Utils.createTempDir().getAbsoluteFile
        val query = aggregated.writeStream
          .format("memory")
          .outputMode("update")
          .queryName("query")
          .option("checkpointLocation", checkpointLocation.toString)
          .start()
        // Add, commit, and wait multiple times to force snapshot versions and time difference
        (0 until 2).foreach { _ =>
          inputData.addData(1, 2, 3)
          query.processAllAvailable()
          Thread.sleep(1000)
        }
        val stateCheckpointDir =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.checkpointLocation
        val batchId =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastProgress.batchId
        val timestamp = System.currentTimeMillis()

        // Verify all stores have uploaded a snapshot and it's logged by the coordinator
        (0 until query.sparkSession.conf.get(SQLConf.SHUFFLE_PARTITIONS)).foreach { partitionId =>
          val providerId =
            StateStoreProviderId(StateStoreId(stateCheckpointDir, 0, partitionId), query.runId)
          assert(coordRef.getLatestSnapshotVersionForTesting(providerId).get >= 0)
        }
        // Verify that we should not have any state stores lagging behind
        assert(coordRef.getLaggingStoresForTesting(query.runId, batchId, timestamp).isEmpty)
        query.stop()
    }
  }

  test(
    "SPARK-51358: Snapshot uploads in RocksDBSkipMaintenanceOnCertainPartitionsProvider " +
    "are properly reported to the coordinator"
  ) {
    withCoordinatorAndSQLConf(
      sc,
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[SkipMaintenanceOnCertainPartitionsProvider].getName,
      RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
      SQLConf.STATE_STORE_COORDINATOR_REPORT_UPLOAD_ENABLED.key -> "true",
      SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_DELTA_MULTIPLIER_FOR_MIN_VERSION_DELTA_TO_LOG.key ->
        "2"
    ) {
      case (coordRef, spark) =>
        import spark.implicits._
        implicit val sqlContext = spark.sqlContext

        // Start a query and run some data to force snapshot uploads
        val inputData = MemoryStream[Int]
        val aggregated = inputData.toDF().dropDuplicates()
        val checkpointLocation = Utils.createTempDir().getAbsoluteFile
        val query = aggregated.writeStream
          .format("memory")
          .outputMode("update")
          .queryName("query")
          .option("checkpointLocation", checkpointLocation.toString)
          .start()
        // Add, commit, and wait multiple times to force snapshot versions and time difference
        (0 until 2).foreach { _ =>
          inputData.addData(1, 2, 3)
          query.processAllAvailable()
          Thread.sleep(1000)
        }
        val stateCheckpointDir =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.checkpointLocation
        val batchId =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastProgress.batchId
        val timestamp = System.currentTimeMillis()

        (0 until query.sparkSession.conf.get(SQLConf.SHUFFLE_PARTITIONS)).foreach { partitionId =>
          val providerId =
            StateStoreProviderId(StateStoreId(stateCheckpointDir, 0, partitionId), query.runId)
          if (partitionId <= 1) {
            // Verify state stores in partition 0 and 1 are lagging and did not upload anything
            assert(coordRef.getLatestSnapshotVersionForTesting(providerId).isEmpty)
          } else {
            // Verify other stores have uploaded a snapshot and it's logged by the coordinator
            assert(coordRef.getLatestSnapshotVersionForTesting(providerId).get >= 0)
          }
        }
        // We should have two state stores (id 0 and 1) that are lagging behind at this point
        val laggingStores = coordRef.getLaggingStoresForTesting(query.runId, batchId, timestamp)
        assert(laggingStores.size == 2)
        assert(laggingStores.forall(_.storeId.partitionId <= 1))
        query.stop()
    }
  }

  private val allJoinStateStoreNames: Seq[String] =
    SymmetricHashJoinStateManager.allStateStoreNames(LeftSide, RightSide)

  test(
    "SPARK-51358: Snapshot uploads for join queries with RocksDBStateStoreProvider " +
    "are properly reported to the coordinator"
  ) {
    withCoordinatorAndSQLConf(
      sc,
      SQLConf.SHUFFLE_PARTITIONS.key -> "3",
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
      SQLConf.STATE_STORE_COORDINATOR_REPORT_UPLOAD_ENABLED.key -> "true",
      SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_DELTA_MULTIPLIER_FOR_MIN_VERSION_DELTA_TO_LOG.key ->
        "5"
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
        (0 until 5).foreach { _ =>
          input1.addData(1, 5)
          input2.addData(1, 5, 10)
          query.processAllAvailable()
          Thread.sleep(500)
        }
        val stateCheckpointDir =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.checkpointLocation
        val batchId =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastProgress.batchId
        val timestamp = System.currentTimeMillis()

        // Verify all state stores for join queries are reporting snapshot uploads
        (0 until query.sparkSession.conf.get(SQLConf.SHUFFLE_PARTITIONS)).foreach { partitionId =>
          allJoinStateStoreNames.foreach { storeName =>
            val providerId =
              StateStoreProviderId(
                StateStoreId(stateCheckpointDir, 0, partitionId, storeName),
                query.runId
              )
            assert(coordRef.getLatestSnapshotVersionForTesting(providerId).get >= 0)
          }
        }
        // Verify that we should not have any state stores lagging behind
        assert(coordRef.getLaggingStoresForTesting(query.runId, batchId, timestamp).isEmpty)
        query.stop()
    }
  }

  test(
    "SPARK-51358: Snapshot uploads for join queries with " +
    "RocksDBSkipMaintenanceOnCertainPartitionsProvider are properly reported to the coordinator"
  ) {
    withCoordinatorAndSQLConf(
      sc,
      SQLConf.SHUFFLE_PARTITIONS.key -> "3",
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[SkipMaintenanceOnCertainPartitionsProvider].getName,
      RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
      SQLConf.STATE_STORE_COORDINATOR_REPORT_UPLOAD_ENABLED.key -> "true",
      SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_DELTA_MULTIPLIER_FOR_MIN_VERSION_DELTA_TO_LOG.key ->
        "5"
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
        (0 until 5).foreach { _ =>
          input1.addData(1, 5)
          input2.addData(1, 5, 10)
          query.processAllAvailable()
          Thread.sleep(500)
        }
        val stateCheckpointDir =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.checkpointLocation
        val batchId =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastProgress.batchId
        val timestamp = System.currentTimeMillis()

        // Verify all state stores for join queries are reporting snapshot uploads
        (0 until query.sparkSession.conf.get(SQLConf.SHUFFLE_PARTITIONS)).foreach { partitionId =>
          allJoinStateStoreNames.foreach { storeName =>
            val providerId =
              StateStoreProviderId(
                StateStoreId(stateCheckpointDir, 0, partitionId, storeName),
                query.runId
              )
            if (partitionId <= 1) {
              // Verify state stores in partition 0 and 1 are lagging and did not upload anything
              assert(coordRef.getLatestSnapshotVersionForTesting(providerId).isEmpty)
            } else {
              // Verify other stores have uploaded a snapshot and it's logged by the coordinator
              assert(coordRef.getLatestSnapshotVersionForTesting(providerId).get >= 0)
            }
          }
        }
        // Verify that only stores from partition id 0 and 1 are lagging behind.
        // Each partition has 4 stores for join queries, so there are 2 * 4 = 8 lagging stores.
        val laggingStores = coordRef.getLaggingStoresForTesting(query.runId, batchId, timestamp)
        assert(laggingStores.size == 2 * 4)
        assert(laggingStores.forall(_.storeId.partitionId <= 1))
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
