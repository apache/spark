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

package org.apache.spark.sql.connect.common

import org.apache.spark.connect.proto
import org.apache.spark.storage.StorageLevel

/**
 * Helper class for conversions between [[StorageLevel]] and [[proto.StorageLevel]].
 */
object StorageLevelProtoConverter {
  def toStorageLevel(sl: proto.StorageLevel): StorageLevel = {
    StorageLevel(
      useDisk = sl.getUseDisk,
      useMemory = sl.getUseMemory,
      useOffHeap = sl.getUseOffHeap,
      deserialized = sl.getDeserialized,
      replication = sl.getReplication)
  }

  def toConnectProtoType(sl: StorageLevel): proto.StorageLevel = {
    proto.StorageLevel
      .newBuilder()
      .setUseDisk(sl.useDisk)
      .setUseMemory(sl.useMemory)
      .setUseOffHeap(sl.useOffHeap)
      .setDeserialized(sl.deserialized)
      .setReplication(sl.replication)
      .build()
  }
}
