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

import org.apache.spark.status.CachedQuantile

class CachedQuantileSerializer extends ProtobufSerDe {
  override val supportClass: Class[_] = classOf[CachedQuantile]

  override def serialize(input: Any): Array[Byte] = {
    val data = input.asInstanceOf[CachedQuantile]
    val builder = StoreTypes.CachedQuantile.newBuilder()
      .setStageId(data.stageId.toLong)
      .setStageAttemptId(data.stageAttemptId)
      .setQuantile(data.quantile)
      .setTaskCount(data.taskCount)
      .setDuration(data.duration)
      .setExecutorDeserializeTime(data.executorDeserializeTime)
      .setExecutorDeserializeCpuTime(data.executorDeserializeCpuTime)
      .setExecutorRunTime(data.executorRunTime)
      .setExecutorCpuTime(data.executorCpuTime)
      .setResultSize(data.resultSize)
      .setJvmGcTime(data.jvmGcTime)
      .setResultSerializationTime(data.resultSerializationTime)
      .setGettingResultTime(data.gettingResultTime)
      .setSchedulerDelay(data.schedulerDelay)
      .setPeakExecutionMemory(data.peakExecutionMemory)
      .setMemoryBytesSpilled(data.memoryBytesSpilled)
      .setDiskBytesSpilled(data.diskBytesSpilled)
      .setBytesRead(data.bytesRead)
      .setRecordsRead(data.recordsRead)
      .setBytesWritten(data.bytesWritten)
      .setRecordsWritten(data.recordsWritten)
      .setShuffleReadBytes(data.shuffleReadBytes)
      .setShuffleRecordsRead(data.shuffleRecordsRead)
      .setShuffleRemoteBlocksFetched(data.shuffleRemoteBlocksFetched)
      .setShuffleLocalBlocksFetched(data.shuffleLocalBlocksFetched)
      .setShuffleFetchWaitTime(data.shuffleFetchWaitTime)
      .setShuffleRemoteBytesRead(data.shuffleRemoteBytesRead)
      .setShuffleRemoteBytesReadToDisk(data.shuffleRemoteBytesReadToDisk)
      .setShuffleTotalBlocksFetched(data.shuffleTotalBlocksFetched)
      .setShuffleWriteBytes(data.shuffleWriteBytes)
      .setShuffleWriteRecords(data.shuffleWriteRecords)
      .setShuffleWriteTime(data.shuffleWriteTime)
    builder.build().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): CachedQuantile = {
    val binary = StoreTypes.CachedQuantile.parseFrom(bytes)
    new CachedQuantile(
      stageId = binary.getStageId.toInt,
      stageAttemptId = binary.getStageAttemptId,
      quantile = binary.getQuantile,
      taskCount = binary.getTaskCount,
      duration = binary.getDuration,
      executorDeserializeTime = binary.getExecutorDeserializeTime,
      executorDeserializeCpuTime = binary.getExecutorDeserializeCpuTime,
      executorRunTime = binary.getExecutorRunTime,
      executorCpuTime = binary.getExecutorCpuTime,
      resultSize = binary.getResultSize,
      jvmGcTime = binary.getJvmGcTime,
      resultSerializationTime = binary.getResultSerializationTime,
      gettingResultTime = binary.getGettingResultTime,
      schedulerDelay = binary.getSchedulerDelay,
      peakExecutionMemory = binary.getPeakExecutionMemory,
      memoryBytesSpilled = binary.getMemoryBytesSpilled,
      diskBytesSpilled = binary.getDiskBytesSpilled,
      bytesRead = binary.getBytesRead,
      recordsRead = binary.getRecordsRead,
      bytesWritten = binary.getBytesWritten,
      recordsWritten = binary.getRecordsWritten,
      shuffleReadBytes = binary.getShuffleReadBytes,
      shuffleRecordsRead = binary.getShuffleRecordsRead,
      shuffleRemoteBlocksFetched = binary.getShuffleRemoteBlocksFetched,
      shuffleLocalBlocksFetched = binary.getShuffleLocalBlocksFetched,
      shuffleFetchWaitTime = binary.getShuffleFetchWaitTime,
      shuffleRemoteBytesRead = binary.getShuffleRemoteBytesRead,
      shuffleRemoteBytesReadToDisk = binary.getShuffleRemoteBytesReadToDisk,
      shuffleTotalBlocksFetched = binary.getShuffleTotalBlocksFetched,
      shuffleWriteBytes = binary.getShuffleWriteBytes,
      shuffleWriteRecords = binary.getShuffleWriteRecords,
      shuffleWriteTime = binary.getShuffleWriteTime)
  }
}
