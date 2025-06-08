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
package org.apache.spark.sql.pipelines.logging

import java.util.UUID

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.pipelines.graph.{DataflowGraph, GraphIdentifierManager, PipelineUpdateContext, ResolvedFlow}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}

/**
 * A streaming listener that converts streaming events into pipeline events for the relevant flows.
 */
class StreamListener(
    env: PipelineUpdateContext,
    graphForExecution: DataflowGraph
) extends StreamingQueryListener
    with Logging {

  private val queries = new java.util.concurrent.ConcurrentHashMap[UUID, StreamingQuery]()

  private def spark = SparkSession.active

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    val stream = spark.streams.get(event.id)
    queries.put(event.runId, stream)
    env.flowProgressEventLogger.recordRunning(getFlowFromStreamName(stream.name))
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {}

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    // if the non-pipelines managed stream is started before flow execution started,
    // onQueryStarted would not have captured the stream and it will not be in the queries map
    if (!queries.containsKey(event.runId)) return

    val stream = queries.remove(event.runId)
    env.flowProgressEventLogger.recordCompletion(getFlowFromStreamName(stream.name))
  }

  private def getFlowFromStreamName(streamName: String): ResolvedFlow = {
    val flowIdentifier = GraphIdentifierManager.parseTableIdentifier(streamName, env.spark)
    graphForExecution.resolvedFlow(flowIdentifier)
  }
}
