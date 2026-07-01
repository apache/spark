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
import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.execution.streaming.checkpointing.{CommitLog, CommitMetadata, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryCheckpointMetadata, StreamMetadata}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}

class StreamingQueryCheckpointMetadataSuite extends StreamTest {
  import testImplicits._

  /**
   * Creates checkpoint metadata with optional offset and commit log data.
   * Returns the initialized metadata and checkpoint root path.
   */
  private def createCheckpointWithLogs(
      dir: File,
      addOffsets: Boolean = false,
      addCommits: Boolean = false): (StreamingQueryCheckpointMetadata, String) = {
    val checkpointRoot = dir.getAbsolutePath
    val checkpointMetadata = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
    checkpointMetadata.streamMetadata // Initialize metadata

    if (addOffsets) {
      checkpointMetadata.offsetLog.add(0, OffsetSeq.fill(None))
      checkpointMetadata.offsetLog.add(1, OffsetSeq.fill(None))
    }

    if (addCommits) {
      checkpointMetadata.commitLog.add(0, CommitMetadata())
      checkpointMetadata.commitLog.add(1, CommitMetadata())
    }

    (checkpointMetadata, checkpointRoot)
  }

  /**
   * Deletes the metadata file from a checkpoint directory.
   */
  private def deleteMetadataFile(checkpointRoot: String): Unit = {
    val metadataPath = new Path(new Path(checkpointRoot), "metadata")
    val fs = metadataPath.getFileSystem(spark.sessionState.newHadoopConf())
    fs.delete(metadataPath, false)
  }

  /**
   * Validates that accessing streamMetadata throws the expected missing metadata error.
   */
  private def assertMissingMetadataError(checkpointRoot: String): Unit = {
    val checkpointMetadata = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
    val exception = intercept[SparkRuntimeException] {
      checkpointMetadata.streamMetadata
    }
    checkError(
      exception = exception,
      condition = "STREAMING_CHECKPOINT_MISSING_METADATA_FILE",
      parameters = Map("checkpointLocation" -> checkpointRoot)
    )
  }

  /**
   * Creates new checkpoint metadata and validates it has a valid UUID.
   */
  private def assertNewMetadataCreated(checkpointRoot: String): StreamMetadata = {
    val checkpointMetadata = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
    val metadata = checkpointMetadata.streamMetadata
    assert(metadata != null)
    assert(metadata.id != null)
    assert(UUID.fromString(metadata.id) != null) // Should be a valid UUID
    metadata
  }

  /**
   * Runs e2e test for streaming query with corrupted checkpoint.
   * @param validationEnabled if true, expects restart to fail and if false, expects success
   */
  private def testE2ECorruptedCheckpoint(validationEnabled: Boolean): Unit = {
    withTempDir { checkpointDir =>
      withTempDir { outputDir =>
        val inputData = MemoryStream[Int]
        var query = inputData.toDF()
          .writeStream
          .format("parquet")
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .outputMode(OutputMode.Append())
          .start(outputDir.getAbsolutePath)

        try {
          // Add data and process batches
          inputData.addData(1, 2, 3)
          query.processAllAvailable()

          // Stop the query
          query.stop()
          query = null

          // Simulate corrupted checkpoint by deleting only the metadata file
          deleteMetadataFile(checkpointDir.getAbsolutePath)

          if (validationEnabled) {
            // Should fail with validation error
            val metadataPath = new Path(new Path(checkpointDir.getAbsolutePath), "metadata")
            val fs = metadataPath.getFileSystem(spark.sessionState.newHadoopConf())
            val exception = intercept[SparkRuntimeException] {
              inputData.toDF()
                .writeStream
                .format("parquet")
                .option("checkpointLocation", checkpointDir.getAbsolutePath)
                .outputMode(OutputMode.Append())
                .start(outputDir.getAbsolutePath)
            }
            val qualifiedPath = fs.makeQualified(new Path(checkpointDir.getAbsolutePath))
            checkError(
              exception = exception,
              condition = "STREAMING_CHECKPOINT_MISSING_METADATA_FILE",
              parameters = Map("checkpointLocation" -> qualifiedPath.toString)
            )
          } else {
            // Should succeed - validation is disabled
            query = inputData.toDF()
              .writeStream
              .format("parquet")
              .option("checkpointLocation", checkpointDir.getAbsolutePath)
              .outputMode(OutputMode.Append())
              .start(outputDir.getAbsolutePath)

            assert(query.isActive, "Query should be active after restart")
            inputData.addData(7, 8, 9)
            query.processAllAvailable()
          }
        } finally {
          if (query != null && query.isActive) {
            query.stop()
          }
        }
      }
    }
  }

  test("valid case: new checkpoint with no metadata and no logs") {
    withTempDir { dir =>
      assertNewMetadataCreated(dir.getAbsolutePath)
    }
  }

  test("valid case: existing checkpoint with metadata and logs") {
    withTempDir { dir =>
      val (checkpointMetadata1, checkpointRoot) =
        createCheckpointWithLogs(dir, addOffsets = true, addCommits = true)
      val originalId = checkpointMetadata1.streamMetadata.id

      // Re-read checkpoint - should succeed and return the same ID
      val checkpointMetadata2 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      val metadata2 = checkpointMetadata2.streamMetadata
      assert(metadata2.id === originalId)
    }
  }

  test("invalid case: missing metadata with non-empty offset log") {
    withTempDir { dir =>
      val (_, checkpointRoot) = createCheckpointWithLogs(dir, addOffsets = true)
      deleteMetadataFile(checkpointRoot)
      assertMissingMetadataError(checkpointRoot)
    }
  }

  test("invalid case: missing metadata with non-empty commit log") {
    withTempDir { dir =>
      val (_, checkpointRoot) = createCheckpointWithLogs(dir, addCommits = true)
      deleteMetadataFile(checkpointRoot)
      assertMissingMetadataError(checkpointRoot)
    }
  }

  test("invalid case: missing metadata with both offset and commit logs non-empty") {
    withTempDir { dir =>
      val (_, checkpointRoot) = createCheckpointWithLogs(dir, addOffsets = true, addCommits = true)
      deleteMetadataFile(checkpointRoot)
      assertMissingMetadataError(checkpointRoot)
    }
  }

  test("valid case: missing metadata with empty logs should succeed") {
    withTempDir { dir =>
      val checkpointRoot = dir.getAbsolutePath

      // Create checkpoint directories but don't add any data
      val offsetLog = new OffsetSeqLog(spark, new File(dir, "offsets").toString)
      val commitLog = new CommitLog(spark, new File(dir, "commits").toString)

      // Verify logs are empty
      assert(offsetLog.getLatestBatchId().isEmpty)
      assert(commitLog.getLatestBatchId().isEmpty)

      // Try to create checkpoint metadata - should succeed
      assertNewMetadataCreated(checkpointRoot)
    }
  }

  test("sparkConf: validation is skipped when flag is disabled") {
    withSQLConf(SQLConf.STREAMING_CHECKPOINT_VERIFY_METADATA_EXISTS.key -> "false") {
      // Verify flag is disabled
      assert(!spark.conf.get(SQLConf.STREAMING_CHECKPOINT_VERIFY_METADATA_EXISTS))

      withTempDir { dir =>
        val (_, checkpointRoot) = createCheckpointWithLogs(dir, addOffsets = true)
        deleteMetadataFile(checkpointRoot)

        // Should succeed and create new metadata with new UUID (validation is skipped)
        assertNewMetadataCreated(checkpointRoot)
      }
    }
  }

  test("e2e: streaming query fails to restart when checkpoint metadata is corrupted") {
    testE2ECorruptedCheckpoint(validationEnabled = true)
  }

  test("e2e: streaming query restarts successfully when flag is disabled") {
    withSQLConf(SQLConf.STREAMING_CHECKPOINT_VERIFY_METADATA_EXISTS.key -> "false") {
      testE2ECorruptedCheckpoint(validationEnabled = false)
    }
  }
}
