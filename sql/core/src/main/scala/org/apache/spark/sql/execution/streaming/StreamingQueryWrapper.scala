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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, StreamingQueryProgress, StreamingQueryStatus}

/**
 * Wrap non-serializable StreamExecution to make the query serializable as it's easy to for it to
 * get captured with normal usage. It's safe to capture the query but not use it in executors.
 * However, if the user tries to call its methods, it will throw `IllegalStateException`.
 */
class StreamingQueryWrapper(
    @transient val streamingQuery: StreamExecution) extends StreamingQuery with Serializable {

  override def name: String = {
    assertRunInDriver()
    streamingQuery.name
  }

  override def id: UUID = {
    assertRunInDriver()
    streamingQuery.id
  }

  override def runId: UUID = {
    assertRunInDriver()
    streamingQuery.runId
  }

  override def awaitTermination(): Unit = {
    assertRunInDriver()
    streamingQuery.awaitTermination()
  }

  override def awaitTermination(timeoutMs: Long): Boolean = {
    assertRunInDriver()
    streamingQuery.awaitTermination(timeoutMs)
  }

  override def stop(): Unit = {
    assertRunInDriver()
    streamingQuery.stop()
  }

  override def processAllAvailable(): Unit = {
    assertRunInDriver()
    streamingQuery.processAllAvailable()
  }

  override def isActive: Boolean = {
    assertRunInDriver()
    streamingQuery.isActive
  }

  override def lastProgress: StreamingQueryProgress = {
    assertRunInDriver()
    streamingQuery.lastProgress
  }

  override def explain(): Unit = {
    assertRunInDriver()
    streamingQuery.explain()
  }

  override def explain(extended: Boolean): Unit = {
    assertRunInDriver()
    streamingQuery.explain(extended)
  }

  override def sparkSession: SparkSession = {
    assertRunInDriver()
    streamingQuery.sparkSession
  }

  override def recentProgress: Array[StreamingQueryProgress] = {
    assertRunInDriver()
    streamingQuery.recentProgress
  }

  override def status: StreamingQueryStatus = {
    assertRunInDriver()
    streamingQuery.status
  }

  override def exception: Option[StreamingQueryException] = {
    assertRunInDriver()
    streamingQuery.exception
  }

  /** Assert the codes run in the driver. */
  private def assertRunInDriver(): Unit = {
    if (streamingQuery == null) {
      throw new IllegalStateException("StreamingQuery cannot be used in executors")
    }
  }
}
