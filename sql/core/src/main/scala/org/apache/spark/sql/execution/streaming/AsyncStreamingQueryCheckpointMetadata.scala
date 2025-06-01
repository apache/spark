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

import java.util.concurrent.ThreadPoolExecutor

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Clock

/**
 * A version of [[StreamingQueryCheckpointMetadata]] that supports async state checkpointing.
 *
 * @param sparkSession Spark session
 * @param resolvedCheckpointRoot The resolved checkpoint root path
 * @param asyncWritesExecutorService The executor service for async writes
 * @param asyncProgressTrackingCheckpointingIntervalMs The interval for async progress
 * @param triggerClock The clock to use for trigger time
 */
class AsyncStreamingQueryCheckpointMetadata(
    sparkSession: SparkSession,
    resolvedCheckpointRoot: String,
    asyncWritesExecutorService: ThreadPoolExecutor,
    asyncProgressTrackingCheckpointingIntervalMs: Long,
    triggerClock: Clock)
  extends StreamingQueryCheckpointMetadata(sparkSession, resolvedCheckpointRoot) {

  override lazy val offsetLog = new AsyncOffsetSeqLog(
    sparkSession,
    checkpointFile(StreamingCheckpointConstants.DIR_NAME_OFFSETS),
    asyncWritesExecutorService,
    asyncProgressTrackingCheckpointingIntervalMs,
    clock = triggerClock
  )

  override lazy val commitLog = new AsyncCommitLog(
    sparkSession,
    checkpointFile(StreamingCheckpointConstants.DIR_NAME_COMMITS),
    asyncWritesExecutorService
  )

}
