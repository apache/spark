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

package org.apache.spark.sql.streaming.protobuf

import java.util.UUID

import org.apache.spark.sql.streaming.ui.StreamingQueryData
import org.apache.spark.status.protobuf.ProtobufSerDe
import org.apache.spark.status.protobuf.StoreTypes
import org.apache.spark.status.protobuf.Utils.getOptional

class StreamingQueryDataSerializer extends ProtobufSerDe {

  override val supportClass: Class[_] = classOf[StreamingQueryData]

  override def serialize(input: Any): Array[Byte] = {
    val data = input.asInstanceOf[StreamingQueryData]
    val builder = StoreTypes.StreamingQueryData.newBuilder()
      .setName(data.name)
      .setId(data.id.toString)
      .setRunId(data.runId)
      .setIsActive(data.isActive)
    data.exception.foreach(builder.setException)
    builder.setStartTimestamp(data.startTimestamp)
    data.endTimestamp.foreach(builder.setEndTimestamp)
    builder.build().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): Any = {
    val data = StoreTypes.StreamingQueryData.parseFrom(bytes)
    val exception =
      getOptional(data.hasException, () => data.getException)
    val endTimestamp =
      getOptional(data.hasEndTimestamp, () => data.getEndTimestamp)
    new StreamingQueryData(
      name = data.getName,
      id = UUID.fromString(data.getId),
      runId = data.getRunId,
      isActive = data.getIsActive,
      exception = exception,
      startTimestamp = data.getStartTimestamp,
      endTimestamp = endTimestamp
    )
  }
}
