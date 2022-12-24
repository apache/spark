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

import java.util.{List => JList}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.status.api.v1.{AccumulableInfo, ExecutorStageSummary}

object Utils {
  def getOptional[T](condition: Boolean, result: () => T): Option[T] = if (condition) {
    Some(result())
  } else {
    None
  }

  private[protobuf] def serializeAccumulableInfo(
      input: AccumulableInfo): StoreTypes.AccumulableInfo = {
    val builder = StoreTypes.AccumulableInfo.newBuilder()
      .setId(input.id)
      .setName(input.name)
      .setValue(input.value)
    input.update.foreach(builder.setUpdate)
    builder.build()
  }

  private[protobuf] def deserializeAccumulableInfos(
      updates: JList[StoreTypes.AccumulableInfo]): ArrayBuffer[AccumulableInfo] = {
    val accumulatorUpdates = new ArrayBuffer[AccumulableInfo]()
    updates.forEach { update =>
      accumulatorUpdates.append(new AccumulableInfo(
        id = update.getId,
        name = update.getName,
        update = getOptional(update.hasUpdate, update.getUpdate),
        value = update.getValue))
    }
    accumulatorUpdates
  }

  private[protobuf] def serializeExecutorStageSummary(
      input: ExecutorStageSummary): StoreTypes.ExecutorStageSummary = {
    val builder = StoreTypes.ExecutorStageSummary.newBuilder()
      .setTaskTime(input.taskTime)
      .setFailedTasks(input.failedTasks)
      .setSucceededTasks(input.succeededTasks)
      .setKilledTasks(input.killedTasks)
      .setInputBytes(input.inputBytes)
      .setInputRecords(input.inputRecords)
      .setOutputBytes(input.outputBytes)
      .setOutputRecords(input.outputRecords)
      .setShuffleRead(input.shuffleRead)
      .setShuffleReadRecords(input.shuffleReadRecords)
      .setShuffleWrite(input.shuffleWrite)
      .setShuffleWriteRecords(input.shuffleWriteRecords)
      .setMemoryBytesSpilled(input.memoryBytesSpilled)
      .setDiskBytesSpilled(input.diskBytesSpilled)
      .setIsBlacklistedForStage(input.isBlacklistedForStage)
      .setIsExcludedForStage(input.isExcludedForStage)
    input.peakMemoryMetrics.map { m =>
      builder.setPeakMemoryMetrics(ExecutorMetricsSerializer.serialize(m))
    }
    builder.build()
  }

  private[protobuf] def deserializeExecutorStageSummary(
      binary: StoreTypes.ExecutorStageSummary): ExecutorStageSummary = {
    val peakMemoryMetrics =
      getOptional(binary.hasPeakMemoryMetrics,
        () => ExecutorMetricsSerializer.deserialize(binary.getPeakMemoryMetrics))
    new ExecutorStageSummary(
      taskTime = binary.getTaskTime,
      failedTasks = binary.getFailedTasks,
      succeededTasks = binary.getSucceededTasks,
      killedTasks = binary.getKilledTasks,
      inputBytes = binary.getInputBytes,
      inputRecords = binary.getInputRecords,
      outputBytes = binary.getOutputBytes,
      outputRecords = binary.getOutputRecords,
      shuffleRead = binary.getShuffleRead,
      shuffleReadRecords = binary.getShuffleReadRecords,
      shuffleWrite = binary.getShuffleWrite,
      shuffleWriteRecords = binary.getShuffleWriteRecords,
      memoryBytesSpilled = binary.getMemoryBytesSpilled,
      diskBytesSpilled = binary.getDiskBytesSpilled,
      isBlacklistedForStage = binary.getIsBlacklistedForStage,
      peakMemoryMetrics = peakMemoryMetrics,
      isExcludedForStage = binary.getIsExcludedForStage)
  }
}
