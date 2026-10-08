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

package org.apache.spark.sql.execution.streaming

import java.io.File

import org.apache.hadoop.fs.Path
import org.scalatest.Tag

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.util.stringToFile
import org.apache.spark.sql.execution.datasources.v2.state.metadata.StateMetadataPartitionReader
import org.apache.spark.sql.execution.streaming.checkpointing.{
  CommitLog, OffsetMap, OffsetSeq, OffsetSeqBase, OffsetSeqLog, OffsetSeqMetadata,
  OffsetSeqMetadataV2}
import org.apache.spark.sql.execution.streaming.runtime.{
  LongOffset, MemoryStream, SerializedOffset, StreamingQueryWrapper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.{SerializableConfiguration, Utils}

class OffsetSeqLogSuite extends SharedSparkSession {
  import testImplicits._

  test("SPARK-59919: v1 and v2 metadata persist rebound stateful shuffle partitions") {
    withSQLConf(
      SQLConf.SHUFFLE_PARTITIONS.key -> "10",
      SQLConf.STATEFUL_SHUFFLE_PARTITIONS_INTERNAL.key -> "3") {
      val v1 = OffsetSeqMetadata(0, 0, spark.conf)
      val v2 = OffsetSeqMetadataV2(0, 0, spark.conf)

      assert(v1.conf.get(SQLConf.SHUFFLE_PARTITIONS.key).contains("3"))
      assert(v2.conf.get(SQLConf.SHUFFLE_PARTITIONS.key).contains("3"))
    }
  }

  /** test string offset type */
  case class StringOffset(override val json: String) extends Offset

  test("OffsetSeqMetadata - deserialization") {
    val key = SQLConf.SHUFFLE_PARTITIONS.key

    def getConfWith(shufflePartitions: Int): Map[String, String] = {
      Map(key -> shufflePartitions.toString)
    }

    // None set
    assert(new OffsetSeqMetadata(0, 0, Map.empty) === OffsetSeqMetadata("""{}"""))

    // One set
    assert(new OffsetSeqMetadata(1, 0, Map.empty) ===
      OffsetSeqMetadata("""{"batchWatermarkMs":1}"""))
    assert(new OffsetSeqMetadata(0, 2, Map.empty) ===
      OffsetSeqMetadata("""{"batchTimestampMs":2}"""))
    assert(OffsetSeqMetadata(0, 0, getConfWith(shufflePartitions = 2)) ===
      OffsetSeqMetadata(s"""{"conf": {"$key":2}}"""))

    // Two set
    assert(new OffsetSeqMetadata(1, 2, Map.empty) ===
      OffsetSeqMetadata("""{"batchWatermarkMs":1,"batchTimestampMs":2}"""))
    assert(OffsetSeqMetadata(1, 0, getConfWith(shufflePartitions = 3)) ===
      OffsetSeqMetadata(s"""{"batchWatermarkMs":1,"conf": {"$key":3}}"""))
    assert(OffsetSeqMetadata(0, 2, getConfWith(shufflePartitions = 3)) ===
      OffsetSeqMetadata(s"""{"batchTimestampMs":2,"conf": {"$key":3}}"""))

    // All set
    assert(OffsetSeqMetadata(1, 2, getConfWith(shufflePartitions = 3)) ===
      OffsetSeqMetadata(s"""{"batchWatermarkMs":1,"batchTimestampMs":2,"conf": {"$key":3}}"""))

    // Drop unknown fields
    assert(OffsetSeqMetadata(1, 2, getConfWith(shufflePartitions = 3)) ===
      OffsetSeqMetadata(
        s"""{"batchWatermarkMs":1,"batchTimestampMs":2,"conf": {"$key":3}},"unknown":1"""))
  }

  test("OffsetSeqLog - serialization - deserialization") {
    withTempDir { temp =>
      val dir = new File(temp, "dir") // use non-existent directory to test whether log make the dir
      val metadataLog = new OffsetSeqLog(spark, dir.getAbsolutePath)
      val batch0 = OffsetSeq.fill(LongOffset(0), LongOffset(1), LongOffset(2))
      val batch1 = OffsetSeq.fill(StringOffset("one"), StringOffset("two"), StringOffset("three"))

      val batch0Serialized = OffsetSeq.fill(batch0.offsets.flatMap(_.map(o =>
        SerializedOffset(o.json))): _*)

      val batch1Serialized = OffsetSeq.fill(batch1.offsets.flatMap(_.map(o =>
        SerializedOffset(o.json))): _*)

      assert(metadataLog.add(0, batch0))
      assert(metadataLog.getLatest() === Some(0 -> batch0Serialized))
      assert(metadataLog.get(0) === Some(batch0Serialized))

      assert(metadataLog.add(1, batch1))
      assert(metadataLog.get(0) === Some(batch0Serialized))
      assert(metadataLog.get(1) === Some(batch1Serialized))
      assert(metadataLog.getLatest() === Some(1 -> batch1Serialized))
      assert(metadataLog.get(None, Some(1)) ===
        Array(0 -> batch0Serialized, 1 -> batch1Serialized))

      // Adding the same batch does nothing
      metadataLog.add(1, OffsetSeq.fill(LongOffset(3)))
      assert(metadataLog.get(0) === Some(batch0Serialized))
      assert(metadataLog.get(1) === Some(batch1Serialized))
      assert(metadataLog.getLatest() === Some(1 -> batch1Serialized))
      assert(metadataLog.get(None, Some(1)) ===
        Array(0 -> batch0Serialized, 1 -> batch1Serialized))
    }
  }

  test("deserialization log written by future version") {
    withTempDir { dir =>
      stringToFile(new File(dir, "0"), "v99999")
      val log = new OffsetSeqLog(spark, dir.getCanonicalPath)
      val e = intercept[IllegalStateException] {
        log.get(0)
      }
      Seq(
        s"maximum supported log version is v${OffsetSeqLog.MAX_VERSION}, but encountered v99999",
        "produced by a newer version of Spark and cannot be read by this version"
      ).foreach { message =>
        assert(e.getMessage.contains(message))
      }
    }
  }

  test("read Spark 2.1.0 log format") {
    val (batchId, offsetSeq) = readFromResource("offset-log-version-2.1.0")
    assert(batchId === 0)
    assert(offsetSeq.offsets === Seq(
      Some(SerializedOffset("""{"logOffset":345}""")),
      Some(SerializedOffset("""{"topic-0":{"0":1}}"""))
    ))
    assert(offsetSeq.metadataOpt === Some(OffsetSeqMetadata(0L, 1480981499528L)))
  }

  private def readFromResource(dir: String): (Long, OffsetSeqBase) = {
    val input = getClass.getResource(s"/structured-streaming/$dir")
    val log = new OffsetSeqLog(spark, input.toString)
    log.getLatest().get
  }

  // SPARK-50526 - sanity tests to ensure that values are set correctly for state store
  // encoding format within OffsetSeqMetadata
  test("offset log records defaults to unsafeRow for store encoding format") {
    val offsetSeqMetadata = OffsetSeqMetadata.apply(batchWatermarkMs = 0, batchTimestampMs = 0,
      spark.conf)
    assert(offsetSeqMetadata.conf.get(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key) ===
      Some("unsaferow"))
  }

  test("offset log uses the store encoding format set in the conf") {
    val offsetSeqMetadata = OffsetSeqMetadata.apply(batchWatermarkMs = 0, batchTimestampMs = 0,
      Map(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> "avro"))
    assert(offsetSeqMetadata.conf.get(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key) ===
      Some("avro"))
  }

  // Verify whether entry exists within the offset log and has the right value or that we pick up
  // the correct default values when populating the session conf.
  private def verifyOffsetLogEntry(
      checkpointDir: String,
      entryExists: Boolean,
      encodingFormat: String): Unit = {
    val log = new OffsetSeqLog(spark, s"$checkpointDir/offsets")
    val latestBatchId = log.getLatestBatchId()
    assert(latestBatchId.isDefined, "No offset log entries found in the checkpoint location")

    // Read the latest offset log
    val offsetSeq = log.get(latestBatchId.get).get
    val offsetSeqMetadata = offsetSeq.metadataOpt.get

    if (entryExists) {
      val encodingFormatOpt = offsetSeqMetadata.conf.get(
        SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key)
      assert(encodingFormatOpt.isDefined, "No store encoding format found in the offset log entry")
      assert(encodingFormatOpt.get == encodingFormat)
    }

    val clonedSqlConf = spark.sessionState.conf.clone()
    OffsetSeqMetadata.setSessionConf(offsetSeqMetadata, clonedSqlConf)
    assert(clonedSqlConf.stateStoreEncodingFormat == encodingFormat)
  }

  // verify that checkpoint created with different store encoding formats are read correctly
  Seq("unsaferow", "avro").foreach { storeEncodingFormat =>
    test(s"verify format values from checkpoint loc - $storeEncodingFormat") {
      withTempDir { checkpointDir =>
        val resourceUri = this.getClass.getResource(
        "/structured-streaming/checkpoint-version-4.0.0-tws-" + storeEncodingFormat + "/").toURI
        Utils.copyDirectory(new File(resourceUri), checkpointDir.getCanonicalFile)
        verifyOffsetLogEntry(checkpointDir.getAbsolutePath, entryExists = true,
          storeEncodingFormat)
      }
    }
  }

  test("verify format values from old checkpoint with Spark version 3.5.1") {
    withTempDir { checkpointDir =>
      val resourceUri = this.getClass.getResource(
        "/structured-streaming/checkpoint-version-3.5.1-streaming-deduplication/").toURI
      Utils.copyDirectory(new File(resourceUri), checkpointDir.getCanonicalFile)
      verifyOffsetLogEntry(checkpointDir.getAbsolutePath, entryExists = false,
        "unsaferow")
    }
  }

  test("Row checksum disabled by default") {
    val offsetSeqMetadata = OffsetSeqMetadata.apply(batchWatermarkMs = 0, batchTimestampMs = 0,
      spark.conf)
    assert(offsetSeqMetadata.conf.get(SQLConf.STATE_STORE_ROW_CHECKSUM_ENABLED.key) ===
      Some(false.toString))
  }

  test("Row checksum disabled for existing checkpoint even if conf is enabled") {
    val rowChecksumConf = SQLConf.STATE_STORE_ROW_CHECKSUM_ENABLED.key
    withSQLConf(rowChecksumConf -> true.toString) {
      val existingChkpt = "offset-log-version-2.1.0"
      val (_, offsetSeq) = readFromResource(existingChkpt)
      val offsetSeqMetadata = offsetSeq.metadataOpt.get
      // Not present in existing checkpoint
      assert(offsetSeqMetadata.conf.get(rowChecksumConf) === None)

      val clonedSqlConf = spark.sessionState.conf.clone()
      OffsetSeqMetadata.setSessionConf(offsetSeqMetadata, clonedSqlConf)
      assert(!clonedSqlConf.stateStoreRowChecksumEnabled)
    }
  }

  test("OffsetMap golden file compatibility test - VERSION_2 format") {
    val (batchId, offsetSeq) = readFromResource("offset-map")
    assert(batchId === 3)

    // Verify it's an OffsetMap (VERSION_2)
    assert(offsetSeq.isInstanceOf[OffsetMap])
    val offsetMap = offsetSeq.asInstanceOf[OffsetMap]

    // Verify the offset data
    assert(offsetMap.offsetsMap === Map("0" -> Some(SerializedOffset("3"))))

    // Verify metadata
    assert(offsetSeq.metadataOpt.isDefined)
    val metadata = offsetSeq.metadataOpt.get
    assert(metadata.batchWatermarkMs === 0)
    assert(metadata.batchTimestampMs === 1758651405232L)
  }

  def getConfWith(shufflePartitions: Int): Map[String, String] = {
    Map(SQLConf.SHUFFLE_PARTITIONS.key -> shufflePartitions.toString)
  }

  test("STREAMING_OFFSET_LOG_FORMAT_VERSION config - new query with VERSION_2") {
    withTempDir { checkpointDir =>
      withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2") {
        val inputData = MemoryStream[Int]
        val query = inputData.toDF()
          .writeStream
          .format("memory")
          .queryName("offsetlog_v2_test")
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .start()

        try {
          inputData.addData(1, 2, 3)
          query.processAllAvailable()

          val offsetLog = new OffsetSeqLog(spark, s"${checkpointDir.getAbsolutePath}/offsets")
          val latestBatch = offsetLog.getLatest()
          assert(latestBatch.isDefined, "Offset log should have at least one entry")

          val (batchId, offsetSeq) = latestBatch.get
          assert(offsetSeq.isInstanceOf[OffsetMap],
            s"Expected OffsetMap but got ${offsetSeq.getClass.getSimpleName}")

          assert(offsetSeq.version === 2, s"Expected version 2 but got ${offsetSeq.version}")
        } finally {
          query.stop()
        }
      }
    }
  }

  private def startStatefulQuery(
      aggregated: DataFrame,
      checkpointDir: File,
      shufflePartitions: Int,
      queryName: String): StreamingQueryWrapper = {
    withSQLConf(
      SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2",
      SQLConf.SHUFFLE_PARTITIONS.key -> shufflePartitions.toString) {
      aggregated.writeStream
        .format("memory")
        .queryName(queryName)
        .outputMode("complete")
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .start()
        .asInstanceOf[StreamingQueryWrapper]
    }
  }

  private def createStatefulCheckpoint(
      checkpointDir: File,
      queryName: String): (MemoryStream[(Int, Int)], DataFrame) = {
    val inputData = MemoryStream[(Int, Int)]
    val aggregated = inputData.toDS().groupBy("_1").count()
    val query = startStatefulQuery(aggregated, checkpointDir, 5, queryName)
    try {
      inputData.addData((1, 0), (2, 0))
      query.processAllAvailable()
      inputData.addData((3, 0))
      query.processAllAvailable()
      assert(query.streamingQuery.lastExecution.numStateStores === 5)
    } finally {
      query.stop()
    }
    (inputData, aggregated)
  }

  private def assertStatefulPartitionRecoveryFails(
      aggregated: DataFrame,
      checkpointDir: File,
      queryName: String): Unit = {
    val query = startStatefulQuery(aggregated, checkpointDir, 10, queryName)
    try {
      val exception = intercept[StreamingQueryException] {
        query.processAllAvailable()
      }
      val message = exception.getCause.getMessage
      assert(message.contains(
        "Failed to recover the state-store partition count from checkpoint metadata"))
      assert(message.contains("Delete the checkpoint and restart the query"))
    } finally {
      query.stop()
    }
  }

  private def updateShufflePartitionsInOffset(
      offsetLog: OffsetSeqLog,
      checkpointDir: File,
      batchId: Long,
      numPartitions: Option[Int]): Unit = {
    val offsetMap = offsetLog.get(batchId).get match {
      case offset: OffsetMap => offset
      case offset => fail(s"Expected a v2 offset, but found ${offset.getClass.getSimpleName}")
    }
    val conf = numPartitions match {
      case Some(value) => offsetMap.metadata.conf.updated(
        SQLConf.SHUFFLE_PARTITIONS.key, value.toString)
      case None => offsetMap.metadata.conf - SQLConf.SHUFFLE_PARTITIONS.key
    }
    val updatedOffset = offsetMap.copy(metadata = offsetMap.metadata.copy(conf = conf))

    val offsetFile = new Path(
      new Path(checkpointDir.getAbsolutePath, "offsets"), batchId.toString)
    val fileSystem = offsetFile.getFileSystem(spark.sessionState.newHadoopConf())
    assert(fileSystem.delete(offsetFile, false))
    assert(offsetLog.add(batchId, updatedOffset))
    assert(offsetLog.get(batchId).get.metadataOpt.get.conf
      .get(SQLConf.SHUFFLE_PARTITIONS.key) === numPartitions.map(_.toString))
  }

  test("SPARK-59919: VERSION_2 uses offset partitions when state metadata differs") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "1") {
      withTempDir { checkpointDir =>
        val (inputData, aggregated) = createStatefulCheckpoint(
          checkpointDir, "offsetlog_v2_stateful_restart_test")

        val offsetLog = new OffsetSeqLog(spark, s"${checkpointDir.getAbsolutePath}/offsets")
        val metadata = offsetLog.getLatest().get._2.metadataOpt.get
        assert(metadata.version === OffsetSeqLog.VERSION_2)
        assert(metadata.conf.get(SQLConf.SHUFFLE_PARTITIONS.key).contains("5"))
        val latestBatchId = offsetLog.getLatestBatchId().get
        assert(latestBatchId > 0L)
        updateShufflePartitionsInOffset(offsetLog, checkpointDir, latestBatchId - 1, None)

        // Keep the state metadata at 5 and change the latest offset to 10
        updateShufflePartitionsInOffset(offsetLog, checkpointDir, latestBatchId, Some(10))
        assert(offsetLog.getLatest().get._2.metadataOpt.get.conf
          .get(SQLConf.SHUFFLE_PARTITIONS.key).contains("10"))
        val stateMetadataReader = new StateMetadataPartitionReader(
          checkpointDir.getAbsolutePath,
          new SerializableConfiguration(spark.sessionState.newHadoopConf()),
          latestBatchId)
        assert(stateMetadataReader.stateStoreNumPartitions.contains(5))

        val query2 = startStatefulQuery(
          aggregated, checkpointDir, shufflePartitions = 10,
          queryName = "offsetlog_v2_stateful_restart_test")
        try {
          inputData.addData((2, 0), (3, 0))
          // The failure is expected because physical state exists for only five partitions.
          intercept[StreamingQueryException] {
            query2.processAllAvailable()
          }
          // Planning 10 stores proves the latest offset value won over state metadata.
          assert(query2.streamingQuery.lastExecution.numStateStores === 10)
        } finally {
          query2.stop()
        }
      }
    }
  }

  test("SPARK-59919: VERSION_2 recovers state-store partitions when the commit log is missing") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "1") {
      withTempDir { checkpointDir =>
        val (_, aggregated) = createStatefulCheckpoint(
          checkpointDir, "offsetlog_v2_stateful_missing_commit_log_test")

        val offsetLog = new OffsetSeqLog(spark, s"${checkpointDir.getAbsolutePath}/offsets")
        updateShufflePartitionsInOffset(
          offsetLog, checkpointDir, offsetLog.getLatestBatchId().get, None)
        val hadoopConf = spark.sessionState.newHadoopConf()

        val commitsPath = new Path(checkpointDir.getAbsolutePath, "commits")
        val commitsFileSystem = commitsPath.getFileSystem(hadoopConf)
        assert(commitsFileSystem.exists(commitsPath))
        assert(commitsFileSystem.delete(commitsPath, true))
        assert(!commitsFileSystem.exists(commitsPath))

        val query2 = startStatefulQuery(
          aggregated, checkpointDir, shufflePartitions = 10,
          queryName = "offsetlog_v2_stateful_missing_commit_log_test")
        try {
          query2.processAllAvailable()
          assert(query2.streamingQuery.lastExecution.numStateStores === 5)
        } finally {
          query2.stop()
        }
      }
    }
  }

  test("SPARK-59919: VERSION_2 recovers state-store partitions from metadata when offset " +
      "shuffle partitions are missing") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "1") {
      withTempDir { checkpointDir =>
        val (inputData, aggregated) = createStatefulCheckpoint(
          checkpointDir, "offsetlog_v2_stateful_missing_partition_test")

        val offsetLog = new OffsetSeqLog(spark, s"${checkpointDir.getAbsolutePath}/offsets")
        val latestBatchId = offsetLog.getLatestBatchId().get
        updateShufflePartitionsInOffset(offsetLog, checkpointDir, latestBatchId, None)
        assert(offsetLog.get(latestBatchId).get.metadataOpt.get.conf
          .get(SQLConf.SHUFFLE_PARTITIONS.key).isEmpty)

        val commitLog = new CommitLog(spark, s"${checkpointDir.getAbsolutePath}/commits")
        assert(commitLog.getLatestBatchId().contains(latestBatchId))
        val stateMetadataReader = new StateMetadataPartitionReader(
          checkpointDir.getAbsolutePath,
          new SerializableConfiguration(spark.sessionState.newHadoopConf()),
          latestBatchId)
        assert(stateMetadataReader.stateStoreNumPartitions.contains(5))

        // Restart with 10 partitions in session config. Recovery should restore
        // the value 5 recorded in state metadata.
        val query2 = startStatefulQuery(
          aggregated, checkpointDir, shufflePartitions = 10,
          queryName = "offsetlog_v2_stateful_missing_partition_test")
        try {
          inputData.addData((4, 0))
          query2.processAllAvailable()
          assert(query2.streamingQuery.lastExecution.numStateStores === 5)
        } finally {
          query2.stop()
        }
      }
    }
  }

  test("SPARK-59919: VERSION_2 fails when state metadata is corrupt during partition recovery") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "1") {
      withTempDir { checkpointDir =>
        val (_, aggregated) = createStatefulCheckpoint(
          checkpointDir, "offsetlog_v2_stateful_corrupt_metadata_test")

        val offsetLog = new OffsetSeqLog(spark, s"${checkpointDir.getAbsolutePath}/offsets")
        updateShufflePartitionsInOffset(
          offsetLog, checkpointDir, offsetLog.getLatestBatchId().get, None)

        val stateMetadataFile = new File(
          checkpointDir,
          "state/0/_metadata/metadata")
        assert(stateMetadataFile.isFile, s"Missing state metadata file: $stateMetadataFile")
        stringToFile(stateMetadataFile, "v1\ncorrupt state metadata")

        assertStatefulPartitionRecoveryFails(
          aggregated, checkpointDir, "offsetlog_v2_stateful_corrupt_metadata_test")
        assert(offsetLog.getLatest().get._2.metadataOpt.get.conf
          .get(SQLConf.SHUFFLE_PARTITIONS.key).isEmpty)
      }
    }
  }

  test("SPARK-59919: VERSION_2 fails when state metadata is missing during partition recovery") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "1") {
      withTempDir { checkpointDir =>
        val (_, aggregated) = createStatefulCheckpoint(
          checkpointDir, "offsetlog_v2_stateful_missing_metadata_test")

        val offsetLog = new OffsetSeqLog(spark, s"${checkpointDir.getAbsolutePath}/offsets")
        updateShufflePartitionsInOffset(
          offsetLog, checkpointDir, offsetLog.getLatestBatchId().get, None)

        val stateMetadataFile = new File(checkpointDir, "state/0/_metadata/metadata")
        assert(stateMetadataFile.isFile, s"Missing state metadata file: $stateMetadataFile")
        assert(
          stateMetadataFile.delete(),
          s"Failed to delete state metadata file: $stateMetadataFile")

        assertStatefulPartitionRecoveryFails(
          aggregated, checkpointDir, "offsetlog_v2_stateful_missing_metadata_test")
      }
    }
  }

  test("SPARK-59919: VERSION_2 uses session shuffle partitions for a stateless query") {
    withTempDir { checkpointDir =>
      withTempDir { outputDir =>
        val inputData = MemoryStream[Int]
        val input = inputData.toDF()
        val queryName = "offsetlog_v2_stateless_missing_partition_test"
        def startQuery(shufflePartitions: Int): StreamingQueryWrapper = {
          withSQLConf(
            SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2",
            SQLConf.SHUFFLE_PARTITIONS.key -> shufflePartitions.toString) {
            input.writeStream
              .format("parquet")
              .queryName(queryName)
              .option("path", outputDir.getAbsolutePath)
              .option("checkpointLocation", checkpointDir.getAbsolutePath)
              .start()
              .asInstanceOf[StreamingQueryWrapper]
          }
        }

        val query1 = startQuery(shufflePartitions = 5)
        try {
          inputData.addData(1, 2)
          query1.processAllAvailable()
        } finally {
          query1.stop()
        }

        val offsetLog = new OffsetSeqLog(spark, s"${checkpointDir.getAbsolutePath}/offsets")
        updateShufflePartitionsInOffset(
          offsetLog, checkpointDir, offsetLog.getLatestBatchId().get, None)

        val query2 = startQuery(shufflePartitions = 10)
        try {
          inputData.addData(3, 4)
          query2.processAllAvailable()
          assert(offsetLog.getLatest().get._2.metadataOpt.get.conf
            .get(SQLConf.SHUFFLE_PARTITIONS.key).contains("10"))
        } finally {
          query2.stop()
        }
      }
    }
  }

  test("STREAMING_OFFSET_LOG_FORMAT_VERSION config - default VERSION_1") {
    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]
      val query = inputData.toDF()
        .writeStream
        .format("memory")
        .queryName("offsetlog_v1_test")
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .start()

      try {
        inputData.addData(1, 2, 3)
        query.processAllAvailable()

        val offsetLog = new OffsetSeqLog(spark, s"${checkpointDir.getAbsolutePath}/offsets")
        val latestBatch = offsetLog.getLatest()
        assert(latestBatch.isDefined, "Offset log should have at least one entry")

        val (batchId, offsetSeq) = latestBatch.get
        assert(offsetSeq.isInstanceOf[OffsetSeq],
          s"Expected OffsetSeq but got ${offsetSeq.getClass.getSimpleName}")

        assert(offsetSeq.version === 1, s"Expected version 1 but got ${offsetSeq.version}")
      } finally {
        query.stop()
      }
    }
  }

  Seq(
    (1, 2, classOf[OffsetSeq]),
    (2, 1, classOf[OffsetMap])
  ).foreach { case (startingVersion, restartVersion, expectedClass) =>
    test(s"checkpoint version wins on restart (v$startingVersion to v$restartVersion)") {
      withTempDir { checkpointDir =>
        withTempDir { outputDir =>
          val inputData = MemoryStream[Int]

          // Start query with initial version
          withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key ->
              startingVersion.toString) {
            val query1 = inputData.toDF()
              .writeStream
              .format("parquet")
              .option("path", outputDir.getAbsolutePath)
              .option("checkpointLocation", checkpointDir.getAbsolutePath)
              .start()

            inputData.addData(1, 2)
            query1.processAllAvailable()
            query1.stop()
          }

          // Verify initial version was used
          val offsetLog = new OffsetSeqLog(spark, s"${checkpointDir.getAbsolutePath}/offsets")
          val batch1 = offsetLog.getLatest()
          assert(batch1.isDefined)
          assert(batch1.get._2.getClass === expectedClass)
          assert(batch1.get._2.version === startingVersion)

          // Restart query with different version config - should still use initial version
          withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key ->
              restartVersion.toString) {
            val query2 = inputData.toDF()
              .writeStream
              .format("parquet")
              .option("path", outputDir.getAbsolutePath)
              .option("checkpointLocation", checkpointDir.getAbsolutePath)
              .start()

            try {
              inputData.addData(3, 4)
              query2.processAllAvailable()

              val latestBatch = offsetLog.getLatest()
              assert(latestBatch.isDefined)

              val (batchId, offsetSeq) = latestBatch.get
              assert(offsetSeq.getClass === expectedClass,
                s"Query should continue using VERSION_$startingVersion format from checkpoint")

              assert(offsetSeq.version === startingVersion,
                s"Query should continue using version $startingVersion from checkpoint")
            } finally {
              query2.stop()
            }
          }
        }
      }
    }
  }

  test("enabling source evolution on an existing V1 checkpoint is rejected") {
    withTempDir { checkpointDir =>
      withTempDir { outputDir =>
        val inputData = MemoryStream[Int]

        // Start query without source evolution, writing V1 offset log entries.
        val query1 = inputData.toDF()
          .writeStream
          .format("parquet")
          .option("path", outputDir.getAbsolutePath)
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .start()
        inputData.addData(1, 2)
        query1.processAllAvailable()
        query1.stop()

        val offsetLog = new OffsetSeqLog(spark, s"${checkpointDir.getAbsolutePath}/offsets")
        val initialBatch = offsetLog.getLatest()
        assert(initialBatch.isDefined)
        assert(initialBatch.get._2.version === 1)
        assert(initialBatch.get._2.isInstanceOf[OffsetSeq])

        // Restart with the source evolution session flag enabled. The existing V1 checkpoint does
        // not support OffsetMap-based named source tracking, so the query must fail loudly rather
        // than silently downgrading the user's session config.
        withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
          val query2 = inputData.toDF()
            .writeStream
            .format("parquet")
            .option("path", outputDir.getAbsolutePath)
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .start()
          val ex = intercept[StreamingQueryException] {
            inputData.addData(3, 4)
            query2.processAllAvailable()
          }
          checkError(
            exception = ex.cause.asInstanceOf[AnalysisException],
            condition = "STREAMING_QUERY_EVOLUTION_ERROR.CANNOT_ENABLE_ON_EXISTING_CHECKPOINT",
            parameters = Map("existingVersion" -> "1"))
        }
      }
    }
  }

  test("SPARK-55131: offset log records defaults to merge operator version 2") {
    val offsetSeqMetadata = OffsetSeqMetadata.apply(batchWatermarkMs = 0, batchTimestampMs = 0,
      spark.conf)
    assert(offsetSeqMetadata.conf.get(SQLConf.STATE_STORE_ROCKSDB_MERGE_OPERATOR_VERSION.key) ===
      Some("2"))
  }

  test("SPARK-55131: offset log uses the merge operator version set in the conf") {
    val offsetSeqMetadata = OffsetSeqMetadata.apply(batchWatermarkMs = 0, batchTimestampMs = 0,
      // Trying to set it to non-default value, 1
      Map(SQLConf.STATE_STORE_ROCKSDB_MERGE_OPERATOR_VERSION.key -> "1"))
    assert(offsetSeqMetadata.conf.get(SQLConf.STATE_STORE_ROCKSDB_MERGE_OPERATOR_VERSION.key) ===
      Some("1"))
  }

  test("SPARK-55131: Backward compatibility test with merge operator version") {
    // Read from the checkpoint which does not have an entry for merge operator version
    // in its offset log. This should pick up the value to 1 instead of 2.
    withTempDir { checkpointDir =>
      val resourceUri = this.getClass.getResource(
        "/structured-streaming/checkpoint-version-4.0.0-tws-unsaferow/").toURI
      Utils.copyDirectory(new File(resourceUri), checkpointDir.getCanonicalFile)

      val log = new OffsetSeqLog(spark, s"$checkpointDir/offsets")
      val latestBatchId = log.getLatestBatchId()
      assert(latestBatchId.isDefined, "No offset log entries found in the checkpoint location")

      // Read the latest offset log
      val offsetSeq = log.get(latestBatchId.get).get
      val offsetSeqMetadata = offsetSeq.metadataOpt.get

      assert(!offsetSeqMetadata.conf
        .contains(SQLConf.STATE_STORE_ROCKSDB_MERGE_OPERATOR_VERSION.key),
        "Merge operator version should be absent in the offset log entry")

      val clonedSqlConf = spark.sessionState.conf.clone()
      OffsetSeqMetadata.setSessionConf(offsetSeqMetadata, clonedSqlConf)
      assert(clonedSqlConf.getConf(SQLConf.STATE_STORE_ROCKSDB_MERGE_OPERATOR_VERSION) == 1)
    }
  }

  def testWithOffsetV2(
      testName: String, testTags: Tag*)(testBody: => Any): Unit = {
    super.test(testName, testTags: _*) {
      // in case tests have any code that needs to execute before every test
      super.beforeEach()
      withSQLConf(
        SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2") {
        testBody
      }
      // in case tests have any code that needs to execute after every test
      super.afterEach()
    }
  }
}
