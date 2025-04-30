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

import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession

/**
 * An interface for accessing the checkpoint metadata associated with a streaming query.
 * @param sparkSession Spark session
 * @param resolvedCheckpointRoot The resolved checkpoint root path
 */
class StreamingQueryCheckpointMetadata(sparkSession: SparkSession, resolvedCheckpointRoot: String) {

  /**
   * A write-ahead-log that records the offsets that are present in each batch. In order to ensure
   * that a given batch will always consist of the same data, we write to this log *before* any
   * processing is done.  Thus, the Nth record in this log indicated data that is currently being
   * processed and the N-1th entry indicates which offsets have been durably committed to the sink.
   */
  lazy val offsetLog =
    new OffsetSeqLog(sparkSession, checkpointFile(StreamingCheckpointConstants.DIR_NAME_OFFSETS))

  /**
   * A log that records the batch ids that have completed. This is used to check if a batch was
   * fully processed, and its output was committed to the sink, hence no need to process it again.
   * This is used (for instance) during restart, to help identify which batch to run next.
   */
  lazy val commitLog =
    new CommitLog(sparkSession, checkpointFile(StreamingCheckpointConstants.DIR_NAME_COMMITS))

  /** Metadata associated with the whole query */
  final lazy val streamMetadata: StreamMetadata = {
    val metadataPath = new Path(checkpointFile(StreamingCheckpointConstants.DIR_NAME_METADATA))
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    StreamMetadata.read(metadataPath, hadoopConf).getOrElse {
      val newMetadata = new StreamMetadata(UUID.randomUUID.toString)
      StreamMetadata.write(newMetadata, metadataPath, hadoopConf)
      newMetadata
    }
  }

  /** Returns the path of a file with `name` in the checkpoint directory. */
  final protected def checkpointFile(name: String): String =
    new Path(new Path(resolvedCheckpointRoot), name).toString

}
