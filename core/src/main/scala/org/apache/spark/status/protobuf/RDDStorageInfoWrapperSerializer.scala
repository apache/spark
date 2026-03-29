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

import scala.jdk.CollectionConverters._

import org.apache.spark.status.RDDStorageInfoWrapper
import org.apache.spark.status.api.v1.{RDDDataDistribution, RDDPartitionInfo, RDDStorageInfo}
import org.apache.spark.status.protobuf.Utils.{getOptional, getStringField, setStringField}
import org.apache.spark.util.Utils.weakIntern

private[protobuf] class RDDStorageInfoWrapperSerializer
  extends ProtobufSerDe[RDDStorageInfoWrapper] {

  override def serialize(input: RDDStorageInfoWrapper): Array[Byte] = {
    val builder = StoreTypes.RDDStorageInfoWrapper.newBuilder()
    builder.setInfo(serializeRDDStorageInfo(input.info))
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): RDDStorageInfoWrapper = {
    val wrapper = StoreTypes.RDDStorageInfoWrapper.parseFrom(bytes)
    new RDDStorageInfoWrapper(
      info = deserializeRDDStorageInfo(wrapper.getInfo)
    )
  }

  private def serializeRDDStorageInfo(info: RDDStorageInfo): StoreTypes.RDDStorageInfo = {
    val builder = StoreTypes.RDDStorageInfo.newBuilder()
    builder.setId(info.id)
    setStringField(info.name, builder.setName)
    builder.setNumPartitions(info.numPartitions)
    builder.setNumCachedPartitions(info.numCachedPartitions)
    setStringField(info.storageLevel, builder.setStorageLevel)
    builder.setMemoryUsed(info.memoryUsed)
    builder.setDiskUsed(info.diskUsed)

    if (info.dataDistribution.isDefined) {
      info.dataDistribution.get.foreach { dd =>
        val dataDistributionBuilder = StoreTypes.RDDDataDistribution.newBuilder()
        setStringField(dd.address, dataDistributionBuilder.setAddress)
        dataDistributionBuilder.setMemoryUsed(dd.memoryUsed)
        dataDistributionBuilder.setMemoryRemaining(dd.memoryRemaining)
        dataDistributionBuilder.setDiskUsed(dd.diskUsed)
        dd.onHeapMemoryUsed.foreach(dataDistributionBuilder.setOnHeapMemoryUsed)
        dd.offHeapMemoryUsed.foreach(dataDistributionBuilder.setOffHeapMemoryUsed)
        dd.onHeapMemoryRemaining.foreach(dataDistributionBuilder.setOnHeapMemoryRemaining)
        dd.offHeapMemoryRemaining.foreach(dataDistributionBuilder.setOffHeapMemoryRemaining)
        builder.addDataDistribution(dataDistributionBuilder.build())
      }
    }

    if (info.partitions.isDefined) {
      info.partitions.get.foreach { p =>
        val partitionsBuilder = StoreTypes.RDDPartitionInfo.newBuilder()
        setStringField(p.blockName, partitionsBuilder.setBlockName)
        setStringField(p.storageLevel, partitionsBuilder.setStorageLevel)
        partitionsBuilder.setMemoryUsed(p.memoryUsed)
        partitionsBuilder.setDiskUsed(p.diskUsed)
        p.executors.foreach(partitionsBuilder.addExecutors)
        builder.addPartitions(partitionsBuilder.build())
      }
    }

    builder.build()
  }

  private def deserializeRDDStorageInfo(info: StoreTypes.RDDStorageInfo): RDDStorageInfo = {
    new RDDStorageInfo(
      id = info.getId,
      name = getStringField(info.hasName, info.getName),
      numPartitions = info.getNumPartitions,
      numCachedPartitions = info.getNumCachedPartitions,
      storageLevel = getStringField(info.hasStorageLevel, info.getStorageLevel),
      memoryUsed = info.getMemoryUsed,
      diskUsed = info.getDiskUsed,
      dataDistribution =
        if (info.getDataDistributionList.isEmpty) {
          None
        } else {
          Some(info.getDataDistributionList.asScala.map(deserializeRDDDataDistribution))
        },
      partitions =
        Some(info.getPartitionsList.asScala.map(deserializeRDDPartitionInfo))
    )
  }

  private def deserializeRDDDataDistribution(info: StoreTypes.RDDDataDistribution):
    RDDDataDistribution = {

    new RDDDataDistribution(
      address = getStringField(info.hasAddress, info.getAddress),
      memoryUsed = info.getMemoryUsed,
      memoryRemaining = info.getMemoryRemaining,
      diskUsed = info.getDiskUsed,
      onHeapMemoryUsed = getOptional(info.hasOnHeapMemoryUsed, info.getOnHeapMemoryUsed),
      offHeapMemoryUsed = getOptional(info.hasOffHeapMemoryUsed, info.getOffHeapMemoryUsed),
      onHeapMemoryRemaining =
        getOptional(info.hasOnHeapMemoryRemaining, info.getOnHeapMemoryRemaining),
      offHeapMemoryRemaining =
        getOptional(info.hasOffHeapMemoryRemaining, info.getOffHeapMemoryRemaining)
    )
  }

  private def deserializeRDDPartitionInfo(info: StoreTypes.RDDPartitionInfo): RDDPartitionInfo = {
    new RDDPartitionInfo(
      blockName = getStringField(info.hasBlockName, info.getBlockName),
      storageLevel = getStringField(info.hasStorageLevel, () => weakIntern(info.getStorageLevel)),
      memoryUsed = info.getMemoryUsed,
      diskUsed = info.getDiskUsed,
      executors = info.getExecutorsList.asScala
    )
  }
}
