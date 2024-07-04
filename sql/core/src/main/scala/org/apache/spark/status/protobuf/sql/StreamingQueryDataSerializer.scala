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

import java.util.UUID

import org.apache.spark.sql.streaming.ui.StreamingQueryData
import org.apache.spark.status.protobuf.{ProtobufSerDe, StoreTypes}
import org.apache.spark.status.protobuf.Utils._

private[protobuf] class StreamingQueryDataSerializer extends ProtobufSerDe[StreamingQueryData] {

  override def serialize(data: StreamingQueryData): Array[Byte] = {
    val builder = StoreTypes.StreamingQueryData.newBuilder()
    setStringField(data.name, builder.setName)
    if (data.id != null) {
      builder.setId(data.id.toString)
    }
    setStringField(data.runId, builder.setRunId)
    builder.setIsActive(data.isActive)
    data.exception.foreach(builder.setException)
    builder.setStartTimestamp(data.startTimestamp)
    data.endTimestamp.foreach(builder.setEndTimestamp)
    builder.build().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): StreamingQueryData = {
    val data = StoreTypes.StreamingQueryData.parseFrom(bytes)
    val exception =
      getOptional(data.hasException, () => data.getException)
    val endTimestamp =
      getOptional(data.hasEndTimestamp, () => data.getEndTimestamp)
    val id = if (data.hasId) {
      UUID.fromString(data.getId)
    } else null
    new StreamingQueryData(
      name = getStringField(data.hasName, () => data.getName),
      id = id,
      runId = getStringField(data.hasRunId, () => data.getRunId),
      isActive = data.getIsActive,
      exception = exception,
      startTimestamp = data.getStartTimestamp,
      endTimestamp = endTimestamp
    )
  }
}
