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

import scala.collection.JavaConverters._

import org.apache.spark.status.RDDStorageInfoWrapper
import org.apache.spark.status.api.v1.{RDDDataDistribution, RDDPartitionInfo, RDDStorageInfo}
import org.apache.spark.status.protobuf.Utils.getOptional

class RDDStorageInfoWrapperSerializer extends ProtobufSerDe {

  override val supportClass: Class[_] = classOf[RDDStorageInfoWrapper]

  override def serialize(input: Any): Array[Byte] =
    serialize(input.asInstanceOf[RDDStorageInfoWrapper])

  private def serialize(input: RDDStorageInfoWrapper): Array[Byte] = {
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
    builder.setName(info.name)
    builder.setNumPartitions(info.numPartitions)
    builder.setNumCachedPartitions(info.numCachedPartitions)
    builder.setStorageLevel(info.storageLevel)
    builder.setMemoryUsed(info.memoryUsed)
    builder.setDiskUsed(info.diskUsed)

    if (info.dataDistribution.isDefined) {
      info.dataDistribution.get.foreach { dd =>
        val dataDistributionBuilder = StoreTypes.RDDDataDistribution.newBuilder()
        dataDistributionBuilder.setAddress(dd.address)
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
        partitionsBuilder.setBlockName(p.blockName)
        partitionsBuilder.setStorageLevel(p.storageLevel)
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
      name = info.getName,
      numPartitions = info.getNumPartitions,
      numCachedPartitions = info.getNumCachedPartitions,
      storageLevel = info.getStorageLevel,
      memoryUsed = info.getMemoryUsed,
      diskUsed = info.getDiskUsed,
      dataDistribution =
        if (info.getDataDistributionList.isEmpty) {
          None
        } else {
          Some(info.getDataDistributionList.asScala.map(deserializeRDDDataDistribution).toSeq)
        },
      partitions =
        Some(info.getPartitionsList.asScala.map(deserializeRDDPartitionInfo).toSeq)
    )
  }

  private def deserializeRDDDataDistribution(info: StoreTypes.RDDDataDistribution):
    RDDDataDistribution = {

    new RDDDataDistribution(
      address = info.getAddress,
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
      blockName = info.getBlockName,
      storageLevel = info.getStorageLevel,
      memoryUsed = info.getMemoryUsed,
      diskUsed = info.getDiskUsed,
      executors = info.getExecutorsList.asScala.toSeq
    )
  }
}
