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
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryCheckpointMetadata}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}

class StreamingQueryCheckpointMetadataSuite extends StreamTest {
  import testImplicits._

  test("valid case: new checkpoint with no metadata and no logs") {
    withTempDir { dir =>
      val checkpointRoot = dir.getAbsolutePath
      val checkpointMetadata = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)

      // Accessing streamMetadata should succeed and create a new UUID
      val metadata = checkpointMetadata.streamMetadata
      assert(metadata != null)
      assert(metadata.id != null)
      assert(UUID.fromString(metadata.id) != null) // Should be a valid UUID
    }
  }

  test("valid case: existing checkpoint with metadata and logs") {
    withTempDir { dir =>
      val checkpointRoot = dir.getAbsolutePath

      // Create metadata file first
      val checkpointMetadata1 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      val metadata1 = checkpointMetadata1.streamMetadata
      val originalId = metadata1.id

      // Add some offset data
      checkpointMetadata1.offsetLog.add(0, OffsetSeq.fill(None))

      // Add some commit data
      checkpointMetadata1.commitLog.add(0, CommitMetadata())

      // Re-read checkpoint - should succeed and return the same ID
      val checkpointMetadata2 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      val metadata2 = checkpointMetadata2.streamMetadata
      assert(metadata2.id === originalId)
    }
  }

  test("invalid case: missing metadata with non-empty offset log") {
    withTempDir { dir =>
      val checkpointRoot = dir.getAbsolutePath

      // Create checkpoint with metadata first
      val checkpointMetadata1 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      checkpointMetadata1.streamMetadata // Initialize metadata

      // Add offset data
      checkpointMetadata1.offsetLog.add(0, OffsetSeq.fill(None))
      checkpointMetadata1.offsetLog.add(1, OffsetSeq.fill(None))

      // Delete the metadata file
      val metadataPath = new Path(new Path(checkpointRoot), "metadata")
      val fs = metadataPath.getFileSystem(spark.sessionState.newHadoopConf())
      fs.delete(metadataPath, false)

      // Try to create a new checkpoint metadata - should throw error
      val checkpointMetadata2 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      val exception = intercept[SparkRuntimeException] {
        checkpointMetadata2.streamMetadata
      }
      checkError(
        exception = exception,
        condition = "STREAMING_CHECKPOINT_MISSING_METADATA_FILE",
        parameters = Map("checkpointLocation" -> checkpointRoot)
      )
    }
  }

  test("invalid case: missing metadata with non-empty commit log") {
    withTempDir { dir =>
      val checkpointRoot = dir.getAbsolutePath

      // Create checkpoint with metadata first
      val checkpointMetadata1 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      checkpointMetadata1.streamMetadata // Initialize metadata

      // Add commit data
      checkpointMetadata1.commitLog.add(0, CommitMetadata())
      checkpointMetadata1.commitLog.add(1, CommitMetadata())

      // Delete the metadata file
      val metadataPath = new Path(new Path(checkpointRoot), "metadata")
      val fs = metadataPath.getFileSystem(spark.sessionState.newHadoopConf())
      fs.delete(metadataPath, false)

      // Try to create a new checkpoint metadata - should throw error
      val checkpointMetadata2 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      val exception = intercept[SparkRuntimeException] {
        checkpointMetadata2.streamMetadata
      }
      checkError(
        exception = exception,
        condition = "STREAMING_CHECKPOINT_MISSING_METADATA_FILE",
        parameters = Map("checkpointLocation" -> checkpointRoot)
      )
    }
  }

  test("invalid case: missing metadata with both offset and commit logs non-empty") {
    withTempDir { dir =>
      val checkpointRoot = dir.getAbsolutePath

      // Create checkpoint with metadata first
      val checkpointMetadata1 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      checkpointMetadata1.streamMetadata // Initialize metadata

      // Add offset data
      checkpointMetadata1.offsetLog.add(0, OffsetSeq.fill(None))
      checkpointMetadata1.offsetLog.add(1, OffsetSeq.fill(None))

      // Add commit data
      checkpointMetadata1.commitLog.add(0, CommitMetadata())
      checkpointMetadata1.commitLog.add(1, CommitMetadata())

      // Delete the metadata file
      val metadataPath = new Path(new Path(checkpointRoot), "metadata")
      val fs = metadataPath.getFileSystem(spark.sessionState.newHadoopConf())
      fs.delete(metadataPath, false)

      // Try to create a new checkpoint metadata - should throw error
      val checkpointMetadata2 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      val exception = intercept[SparkRuntimeException] {
        checkpointMetadata2.streamMetadata
      }
      checkError(
        exception = exception,
        condition = "STREAMING_CHECKPOINT_MISSING_METADATA_FILE",
        parameters = Map("checkpointLocation" -> checkpointRoot)
      )
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
      val checkpointMetadata = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      val metadata = checkpointMetadata.streamMetadata
      assert(metadata != null)
      assert(metadata.id != null)
      assert(UUID.fromString(metadata.id) != null) // Should be a valid UUID
    }
  }

  test("feature flag: validation is performed when flag is enabled (default)") {
    // Verify flag is enabled by default
    assert(spark.conf.get(SQLConf.STREAMING_CHECKPOINT_VERIFY_METADATA_EXISTS) === true)

    withTempDir { dir =>
      val checkpointRoot = dir.getAbsolutePath

      // Create checkpoint with metadata and logs
      val checkpointMetadata1 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      checkpointMetadata1.streamMetadata
      checkpointMetadata1.offsetLog.add(0, OffsetSeq.fill(None))

      // Delete metadata file
      val metadataPath = new Path(new Path(checkpointRoot), "metadata")
      val fs = metadataPath.getFileSystem(spark.sessionState.newHadoopConf())
      fs.delete(metadataPath, false)

      // Should throw error because validation is enabled
      val checkpointMetadata2 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
      val exception = intercept[SparkRuntimeException] {
        checkpointMetadata2.streamMetadata
      }
      checkError(
        exception = exception,
        condition = "STREAMING_CHECKPOINT_MISSING_METADATA_FILE",
        parameters = Map("checkpointLocation" -> checkpointRoot)
      )
    }
  }

  test("feature flag: validation is skipped when flag is disabled") {
    withSQLConf(SQLConf.STREAMING_CHECKPOINT_VERIFY_METADATA_EXISTS.key -> "false") {
      // Verify flag is disabled
      assert(spark.conf.get(SQLConf.STREAMING_CHECKPOINT_VERIFY_METADATA_EXISTS) === false)

      withTempDir { dir =>
        val checkpointRoot = dir.getAbsolutePath

        // Create checkpoint with metadata and logs
        val checkpointMetadata1 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
        checkpointMetadata1.streamMetadata
        checkpointMetadata1.offsetLog.add(0, OffsetSeq.fill(None))

        // Delete metadata file
        val metadataPath = new Path(new Path(checkpointRoot), "metadata")
        val fs = metadataPath.getFileSystem(spark.sessionState.newHadoopConf())
        fs.delete(metadataPath, false)

        // Should succeed and create new metadata with new UUID (validation is skipped)
        val checkpointMetadata2 = new StreamingQueryCheckpointMetadata(spark, checkpointRoot)
        val metadata = checkpointMetadata2.streamMetadata
        assert(metadata != null)
        assert(metadata.id != null)
        assert(UUID.fromString(metadata.id) != null)
      }
    }
  }

  test("e2e: streaming query fails to restart when checkpoint metadata is corrupted") {
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

          inputData.addData(4, 5, 6)
          query.processAllAvailable()

          // Verify checkpoint files exist
          val metadataPath = new Path(new Path(checkpointDir.getAbsolutePath), "metadata")
          val fs = metadataPath.getFileSystem(spark.sessionState.newHadoopConf())
          assert(fs.exists(metadataPath), "Metadata file should exist")

          // Stop the query
          query.stop()
          query = null

          // Simulate corrupted checkpoint by deleting only the metadata file
          fs.delete(metadataPath, false)
          assert(!fs.exists(metadataPath), "Metadata file should be deleted")

          // Attempt to restart the query - should fail with validation error
          val exception = intercept[SparkRuntimeException] {
            inputData.toDF()
              .writeStream
              .format("parquet")
              .option("checkpointLocation", checkpointDir.getAbsolutePath)
              .outputMode(OutputMode.Append())
              .start(outputDir.getAbsolutePath)
          }

          // Verify the error is our checkpoint consistency error
          val qualifiedPath = fs.makeQualified(new Path(checkpointDir.getAbsolutePath))
          checkError(
            exception = exception,
            condition = "STREAMING_CHECKPOINT_MISSING_METADATA_FILE",
            parameters = Map("checkpointLocation" -> qualifiedPath.toString)
          )
        } finally {
          if (query != null && query.isActive) {
            query.stop()
          }
        }
      }
    }
  }

  test("e2e: streaming query restarts successfully when flag is disabled") {
    withSQLConf(SQLConf.STREAMING_CHECKPOINT_VERIFY_METADATA_EXISTS.key -> "false") {
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
            val metadataPath = new Path(new Path(checkpointDir.getAbsolutePath), "metadata")
            val fs = metadataPath.getFileSystem(spark.sessionState.newHadoopConf())
            fs.delete(metadataPath, false)

            // Restart the query - should succeed because validation is disabled
            query = inputData.toDF()
              .writeStream
              .format("parquet")
              .option("checkpointLocation", checkpointDir.getAbsolutePath)
              .outputMode(OutputMode.Append())
              .start(outputDir.getAbsolutePath)

            // Query should be active and can process more data
            assert(query.isActive, "Query should be active after restart")

            inputData.addData(7, 8, 9)
            query.processAllAvailable()
          } finally {
            if (query != null && query.isActive) {
              query.stop()
            }
          }
        }
      }
    }
  }
}
