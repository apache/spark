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

package org.apache.spark.status.protobuf

import org.apache.spark.status.StreamBlockData
import org.apache.spark.status.protobuf.Utils.{getStringField, setStringField}
import org.apache.spark.util.Utils.weakIntern

private[protobuf] class StreamBlockDataSerializer extends ProtobufSerDe[StreamBlockData] {

  override def serialize(data: StreamBlockData): Array[Byte] = {
    val builder = StoreTypes.StreamBlockData.newBuilder()
    setStringField(data.name, builder.setName)
    setStringField(data.executorId, builder.setExecutorId)
    setStringField(data.hostPort, builder.setHostPort)
    setStringField(data.storageLevel, builder.setStorageLevel)
    builder.setUseMemory(data.useMemory)
      .setUseDisk(data.useDisk)
      .setDeserialized(data.deserialized)
      .setMemSize(data.memSize)
      .setDiskSize(data.diskSize)
    builder.build().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): StreamBlockData = {
    val binary = StoreTypes.StreamBlockData.parseFrom(bytes)
    new StreamBlockData(
      name = getStringField(binary.hasName, () => binary.getName),
      executorId = getStringField(binary.hasExecutorId, () => weakIntern(binary.getExecutorId)),
      hostPort = getStringField(binary.hasHostPort, () => weakIntern(binary.getHostPort)),
      storageLevel =
        getStringField(binary.hasStorageLevel, () => weakIntern(binary.getStorageLevel)),
      useMemory = binary.getUseMemory,
      useDisk = binary.getUseDisk,
      deserialized = binary.getDeserialized,
      memSize = binary.getMemSize,
      diskSize = binary.getDiskSize)
  }
}
