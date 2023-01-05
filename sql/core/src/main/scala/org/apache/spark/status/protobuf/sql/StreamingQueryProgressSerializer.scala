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

package org.apache.spark.status.protobuf.sql

import java.util.{HashMap => JHashMap, Map => JMap, UUID}

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.streaming.StreamingQueryProgress
import org.apache.spark.status.protobuf.StoreTypes

private[protobuf] object StreamingQueryProgressSerializer {

  private val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  def serialize(process: StreamingQueryProgress): StoreTypes.StreamingQueryProgress = {
    val builder = StoreTypes.StreamingQueryProgress.newBuilder()
    builder.setId(process.id.toString)
    builder.setRunId(process.runId.toString)
    builder.setName(process.name)
    builder.setTimestamp(process.timestamp)
    builder.setBatchId(process.batchId)
    builder.setBatchDuration(process.batchDuration)
    process.durationMs.forEach {
      case (k, v) => builder.putDurationMs(k, v)
    }
    process.eventTime.forEach {
      case (k, v) => builder.putEventTime(k, v)
    }
    process.stateOperators.foreach(
      s => builder.addStateOperators(StateOperatorProgressSerializer.serialize(s)))
    process.sources.foreach(
      s => builder.addSources(SourceProgressSerializer.serialize(s))
    )
    builder.setSink(SinkProgressSerializer.serialize(process.sink))
    process.observedMetrics.forEach {
      case (k, v) => builder.putObservedMetrics(k, mapper.writeValueAsString(v))
    }
    builder.build()
  }

  def deserialize(process: StoreTypes.StreamingQueryProgress): StreamingQueryProgress = {
    new StreamingQueryProgress(
      id = UUID.fromString(process.getId),
      runId = UUID.fromString(process.getRunId),
      name = process.getName,
      timestamp = process.getTimestamp,
      batchId = process.getBatchId,
      batchDuration = process.getBatchDuration,
      durationMs = process.getDurationMsMap,
      eventTime = process.getEventTimeMap,
      stateOperators =
        StateOperatorProgressSerializer.deserializeToArray(process.getStateOperatorsList),
      sources = SourceProgressSerializer.deserializeToArray(process.getSourcesList),
      sink = SinkProgressSerializer.deserialize(process.getSink),
      observedMetrics = convertToObservedMetrics(process.getObservedMetricsMap)
    )
  }

  private def convertToObservedMetrics(input: JMap[String, String]): JHashMap[String, Row] = {
    val observedMetrics = new JHashMap[String, Row](input.size())
    val classType = classOf[GenericRowWithSchema]
    input.forEach {
      case (k, v) =>
        observedMetrics.put(k, mapper.readValue(v, classType))
    }
    observedMetrics
  }
}
