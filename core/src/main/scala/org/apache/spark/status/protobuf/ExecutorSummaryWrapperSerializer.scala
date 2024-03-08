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

import java.util.Date

import scala.jdk.CollectionConverters._

import org.apache.spark.resource.ResourceInformation
import org.apache.spark.status.ExecutorSummaryWrapper
import org.apache.spark.status.api.v1.{ExecutorSummary, MemoryMetrics}
import org.apache.spark.status.protobuf.Utils.{getOptional, getStringField, setStringField}
import org.apache.spark.util.Utils.weakIntern

private[protobuf] class ExecutorSummaryWrapperSerializer
  extends ProtobufSerDe[ExecutorSummaryWrapper] {

  override def serialize(input: ExecutorSummaryWrapper): Array[Byte] = {
    val info = serializeExecutorSummary(input.info)
    val builder = StoreTypes.ExecutorSummaryWrapper.newBuilder()
      .setInfo(info)
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): ExecutorSummaryWrapper = {
    val binary = StoreTypes.ExecutorSummaryWrapper.parseFrom(bytes)
    val info = deserializeExecutorSummary(binary.getInfo)
    new ExecutorSummaryWrapper(info = info)
  }

  private def serializeExecutorSummary(
      input: ExecutorSummary): StoreTypes.ExecutorSummary = {
    val builder = StoreTypes.ExecutorSummary.newBuilder()
      .setIsActive(input.isActive)
      .setRddBlocks(input.rddBlocks)
      .setMemoryUsed(input.memoryUsed)
      .setDiskUsed(input.diskUsed)
      .setTotalCores(input.totalCores)
      .setMaxTasks(input.maxTasks)
      .setActiveTasks(input.activeTasks)
      .setFailedTasks(input.failedTasks)
      .setCompletedTasks(input.completedTasks)
      .setTotalTasks(input.totalTasks)
      .setTotalDuration(input.totalDuration)
      .setTotalGcTime(input.totalGCTime)
      .setTotalInputBytes(input.totalInputBytes)
      .setTotalShuffleRead(input.totalShuffleRead)
      .setTotalShuffleWrite(input.totalShuffleWrite)
      .setIsBlacklisted(input.isBlacklisted)
      .setMaxMemory(input.maxMemory)
      .setAddTime(input.addTime.getTime)
    setStringField(input.id, builder.setId)
    setStringField(input.hostPort, builder.setHostPort)
    input.removeTime.foreach {
      date => builder.setRemoveTime(date.getTime)
    }
    input.removeReason.foreach(builder.setRemoveReason)
    input.executorLogs.foreach { case (k, v) =>
      builder.putExecutorLogs(k, v)
    }
    input.memoryMetrics.foreach { metrics =>
      builder.setMemoryMetrics(serializeMemoryMetrics(metrics))
    }
    input.blacklistedInStages.foreach { stage =>
      builder.addBlacklistedInStages(stage.toLong)
    }
    input.peakMemoryMetrics.foreach { metrics =>
      builder.setPeakMemoryMetrics(ExecutorMetricsSerializer.serialize(metrics))
    }
    input.attributes.foreach { case (k, v) =>
      builder.putAttributes(k, v)
    }
    input.resources.foreach { case (k, v) =>
      builder.putResources(k, serializeResourceInformation(v))
    }

    builder.setResourceProfileId(input.resourceProfileId)
    builder.setIsExcluded(input.isExcluded)

    input.excludedInStages.foreach { stage =>
      builder.addExcludedInStages(stage.toLong)
    }

    builder.build()
  }

  private def deserializeExecutorSummary(
      binary: StoreTypes.ExecutorSummary): ExecutorSummary = {
    val peakMemoryMetrics =
      getOptional(binary.hasPeakMemoryMetrics,
        () => ExecutorMetricsSerializer.deserialize(binary.getPeakMemoryMetrics))
    val removeTime = getOptional(binary.hasRemoveTime, () => new Date(binary.getRemoveTime))
    val removeReason = getOptional(binary.hasRemoveReason, () => binary.getRemoveReason)
    val memoryMetrics =
      getOptional(binary.hasMemoryMetrics,
        () => deserializeMemoryMetrics(binary.getMemoryMetrics))
    new ExecutorSummary(
      id = getStringField(binary.hasId, binary.getId),
      hostPort = getStringField(binary.hasHostPort, () => weakIntern(binary.getHostPort)),
      isActive = binary.getIsActive,
      rddBlocks = binary.getRddBlocks,
      memoryUsed = binary.getMemoryUsed,
      diskUsed = binary.getDiskUsed,
      totalCores = binary.getTotalCores,
      maxTasks = binary.getMaxTasks,
      activeTasks = binary.getActiveTasks,
      failedTasks = binary.getFailedTasks,
      completedTasks = binary.getCompletedTasks,
      totalTasks = binary.getTotalTasks,
      totalDuration = binary.getTotalDuration,
      totalGCTime = binary.getTotalGcTime,
      totalInputBytes = binary.getTotalInputBytes,
      totalShuffleRead = binary.getTotalShuffleRead,
      totalShuffleWrite = binary.getTotalShuffleWrite,
      isBlacklisted = binary.getIsBlacklisted,
      maxMemory = binary.getMaxMemory,
      addTime = new Date(binary.getAddTime),
      removeTime = removeTime,
      removeReason = removeReason,
      executorLogs = binary.getExecutorLogsMap.asScala.toMap,
      memoryMetrics = memoryMetrics,
      blacklistedInStages = binary.getBlacklistedInStagesList.asScala.map(_.toInt).toSet,
      peakMemoryMetrics = peakMemoryMetrics,
      attributes = binary.getAttributesMap.asScala.toMap,
      resources =
        binary.getResourcesMap.asScala.toMap.transform((_, v) => deserializeResourceInformation(v)),
      resourceProfileId = binary.getResourceProfileId,
      isExcluded = binary.getIsExcluded,
      excludedInStages = binary.getExcludedInStagesList.asScala.map(_.toInt).toSet)
  }

  private def serializeMemoryMetrics(metrics: MemoryMetrics): StoreTypes.MemoryMetrics = {
    val builder = StoreTypes.MemoryMetrics.newBuilder()
    builder.setUsedOnHeapStorageMemory(metrics.usedOnHeapStorageMemory)
    builder.setUsedOffHeapStorageMemory(metrics.usedOffHeapStorageMemory)
    builder.setTotalOnHeapStorageMemory(metrics.totalOnHeapStorageMemory)
    builder.setTotalOffHeapStorageMemory(metrics.totalOffHeapStorageMemory)
    builder.build()
  }

  private def deserializeMemoryMetrics(binary: StoreTypes.MemoryMetrics): MemoryMetrics = {
    new MemoryMetrics(
      usedOnHeapStorageMemory = binary.getUsedOnHeapStorageMemory,
      usedOffHeapStorageMemory = binary.getUsedOffHeapStorageMemory,
      totalOnHeapStorageMemory = binary.getTotalOnHeapStorageMemory,
      totalOffHeapStorageMemory = binary.getTotalOffHeapStorageMemory
    )
  }

  private def serializeResourceInformation(info: ResourceInformation):
    StoreTypes.ResourceInformation = {
    val builder = StoreTypes.ResourceInformation.newBuilder()
    setStringField(info.name, builder.setName)
    if (info.addresses != null) {
      info.addresses.foreach(builder.addAddresses)
    }
    builder.build()
  }

  private def deserializeResourceInformation(binary: StoreTypes.ResourceInformation):
    ResourceInformation = {
    new ResourceInformation(
      name = getStringField(binary.hasName, () => weakIntern(binary.getName)),
      addresses = binary.getAddressesList.asScala.map(weakIntern).toArray)
  }
}
