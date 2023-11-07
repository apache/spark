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
import org.apache.spark.status.protobuf.Utils.{getStringField, setJMapField, setStringField}

private[protobuf] object StreamingQueryProgressSerializer {

  private val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  def serialize(process: StreamingQueryProgress): StoreTypes.StreamingQueryProgress = {
    val builder = StoreTypes.StreamingQueryProgress.newBuilder()
    if (process.id != null) {
      builder.setId(process.id.toString)
    }
    if (process.runId != null) {
      builder.setRunId(process.runId.toString)
    }
    setStringField(process.name, builder.setName)
    setStringField(process.timestamp, builder.setTimestamp)
    builder.setBatchId(process.batchId)
    builder.setBatchDuration(process.batchDuration)
    setJMapField(process.durationMs, builder.putAllDurationMs)
    setJMapField(process.eventTime, builder.putAllEventTime)
    process.stateOperators.foreach(
      s => builder.addStateOperators(StateOperatorProgressSerializer.serialize(s)))
    process.sources.foreach(
      s => builder.addSources(SourceProgressSerializer.serialize(s))
    )
    builder.setSink(SinkProgressSerializer.serialize(process.sink))
    setJMapField(process.observedMetrics, putAllObservedMetrics(builder, _))
    builder.build()
  }

  def deserialize(process: StoreTypes.StreamingQueryProgress): StreamingQueryProgress = {
    val id = if (process.hasId) {
      UUID.fromString(process.getId)
    } else null
    val runId = if (process.hasId) {
      UUID.fromString(process.getRunId)
    } else null
    new StreamingQueryProgress(
      id = id,
      runId = runId,
      name = getStringField(process.hasName, () => process.getName),
      timestamp = getStringField(process.hasTimestamp, () => process.getTimestamp),
      batchId = process.getBatchId,
      batchDuration = process.getBatchDuration,
      durationMs = new JHashMap(process.getDurationMsMap),
      eventTime = new JHashMap(process.getEventTimeMap),
      stateOperators =
        StateOperatorProgressSerializer.deserializeToArray(process.getStateOperatorsList),
      sources = SourceProgressSerializer.deserializeToArray(process.getSourcesList),
      sink = SinkProgressSerializer.deserialize(process.getSink),
      observedMetrics = convertToObservedMetrics(process.getObservedMetricsMap)
    )
  }

  private def putAllObservedMetrics(
      builder: StoreTypes.StreamingQueryProgress.Builder,
      observedMetrics: JMap[String, Row]): Unit = {
    // Encode Row as Json to handle it as a string type in protobuf and this way
    // is simpler than defining a message type corresponding to Row in protobuf.
    observedMetrics.forEach {
      case (k, v) => builder.putObservedMetrics(k, mapper.writeValueAsString(v))
    }
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
