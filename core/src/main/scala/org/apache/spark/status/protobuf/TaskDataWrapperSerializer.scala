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

import org.apache.spark.status.TaskDataWrapper
import org.apache.spark.status.protobuf.Utils.{getOptional, getStringField, setStringField}
import org.apache.spark.util.Utils.weakIntern

private[protobuf] class TaskDataWrapperSerializer extends ProtobufSerDe[TaskDataWrapper] {

  override def serialize(input: TaskDataWrapper): Array[Byte] = {
    val builder = StoreTypes.TaskDataWrapper.newBuilder()
      .setTaskId(input.taskId)
      .setIndex(input.index)
      .setAttempt(input.attempt)
      .setPartitionId(input.partitionId)
      .setLaunchTime(input.launchTime)
      .setResultFetchStart(input.resultFetchStart)
      .setDuration(input.duration)
      .setSpeculative(input.speculative)
      .setHasMetrics(input.hasMetrics)
      .setExecutorDeserializeTime(input.executorDeserializeTime)
      .setExecutorDeserializeCpuTime(input.executorDeserializeCpuTime)
      .setExecutorRunTime(input.executorRunTime)
      .setExecutorCpuTime(input.executorCpuTime)
      .setResultSize(input.resultSize)
      .setJvmGcTime(input.jvmGcTime)
      .setResultSerializationTime(input.resultSerializationTime)
      .setMemoryBytesSpilled(input.memoryBytesSpilled)
      .setDiskBytesSpilled(input.diskBytesSpilled)
      .setPeakExecutionMemory(input.peakExecutionMemory)
      .setInputBytesRead(input.inputBytesRead)
      .setInputRecordsRead(input.inputRecordsRead)
      .setOutputBytesWritten(input.outputBytesWritten)
      .setOutputRecordsWritten(input.outputRecordsWritten)
      .setShuffleRemoteBlocksFetched(input.shuffleRemoteBlocksFetched)
      .setShuffleLocalBlocksFetched(input.shuffleLocalBlocksFetched)
      .setShuffleFetchWaitTime(input.shuffleFetchWaitTime)
      .setShuffleRemoteBytesRead(input.shuffleRemoteBytesRead)
      .setShuffleRemoteBytesReadToDisk(input.shuffleRemoteBytesReadToDisk)
      .setShuffleLocalBytesRead(input.shuffleLocalBytesRead)
      .setShuffleRecordsRead(input.shuffleRecordsRead)
      .setShuffleCorruptMergedBlockChunks(input.shuffleCorruptMergedBlockChunks)
      .setShuffleMergedFetchFallbackCount(input.shuffleMergedFetchFallbackCount)
      .setShuffleMergedRemoteBlocksFetched(input.shuffleMergedRemoteBlocksFetched)
      .setShuffleMergedLocalBlocksFetched(input.shuffleMergedLocalBlocksFetched)
      .setShuffleMergedRemoteChunksFetched(input.shuffleMergedRemoteChunksFetched)
      .setShuffleMergedLocalChunksFetched(input.shuffleMergedLocalChunksFetched)
      .setShuffleMergedRemoteBytesRead(input.shuffleMergedRemoteBytesRead)
      .setShuffleMergedLocalBytesRead(input.shuffleMergedLocalBytesRead)
      .setShuffleRemoteReqsDuration(input.shuffleRemoteReqsDuration)
      .setShuffleMergedRemoteReqDuration(input.shuffleMergedRemoteReqDuration)
      .setShuffleBytesWritten(input.shuffleBytesWritten)
      .setShuffleWriteTime(input.shuffleWriteTime)
      .setShuffleRecordsWritten(input.shuffleRecordsWritten)
      .setStageId(input.stageId)
      .setStageAttemptId(input.stageAttemptId)
    setStringField(input.executorId, builder.setExecutorId)
    setStringField(input.host, builder.setHost)
    setStringField(input.status, builder.setStatus)
    setStringField(input.taskLocality, builder.setTaskLocality)
    input.errorMessage.foreach(builder.setErrorMessage)
    input.accumulatorUpdates.foreach { update =>
      builder.addAccumulatorUpdates(AccumulableInfoSerializer.serialize(update))
    }
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): TaskDataWrapper = {
    val binary = StoreTypes.TaskDataWrapper.parseFrom(bytes)
    val accumulatorUpdates = AccumulableInfoSerializer.deserialize(binary.getAccumulatorUpdatesList)
    new TaskDataWrapper(
      taskId = binary.getTaskId,
      index = binary.getIndex,
      attempt = binary.getAttempt,
      partitionId = binary.getPartitionId,
      launchTime = binary.getLaunchTime,
      resultFetchStart = binary.getResultFetchStart,
      duration = binary.getDuration,
      executorId = getStringField(binary.hasExecutorId, () => weakIntern(binary.getExecutorId)),
      host = getStringField(binary.hasHost, () => weakIntern(binary.getHost)),
      status = getStringField(binary.hasStatus, () => weakIntern(binary.getStatus)),
      taskLocality =
        getStringField(binary.hasTaskLocality, () => weakIntern(binary.getTaskLocality)),
      speculative = binary.getSpeculative,
      accumulatorUpdates = accumulatorUpdates,
      errorMessage = getOptional(binary.hasErrorMessage, binary.getErrorMessage),
      hasMetrics = binary.getHasMetrics,
      executorDeserializeTime = binary.getExecutorDeserializeTime,
      executorDeserializeCpuTime = binary.getExecutorDeserializeCpuTime,
      executorRunTime = binary.getExecutorRunTime,
      executorCpuTime = binary.getExecutorCpuTime,
      resultSize = binary.getResultSize,
      jvmGcTime = binary.getJvmGcTime,
      resultSerializationTime = binary.getResultSerializationTime,
      memoryBytesSpilled = binary.getMemoryBytesSpilled,
      diskBytesSpilled = binary.getDiskBytesSpilled,
      peakExecutionMemory = binary.getPeakExecutionMemory,
      inputBytesRead = binary.getInputBytesRead,
      inputRecordsRead = binary.getInputRecordsRead,
      outputBytesWritten = binary.getOutputBytesWritten,
      outputRecordsWritten = binary.getOutputRecordsWritten,
      shuffleRemoteBlocksFetched = binary.getShuffleRemoteBlocksFetched,
      shuffleLocalBlocksFetched = binary.getShuffleLocalBlocksFetched,
      shuffleFetchWaitTime = binary.getShuffleFetchWaitTime,
      shuffleRemoteBytesRead = binary.getShuffleRemoteBytesRead,
      shuffleRemoteBytesReadToDisk = binary.getShuffleRemoteBytesReadToDisk,
      shuffleLocalBytesRead = binary.getShuffleLocalBytesRead,
      shuffleRecordsRead = binary.getShuffleRecordsRead,
      shuffleCorruptMergedBlockChunks = binary.getShuffleCorruptMergedBlockChunks,
      shuffleMergedFetchFallbackCount = binary.getShuffleMergedFetchFallbackCount,
      shuffleMergedRemoteBlocksFetched = binary.getShuffleMergedRemoteBlocksFetched,
      shuffleMergedLocalBlocksFetched = binary.getShuffleMergedLocalBlocksFetched,
      shuffleMergedRemoteChunksFetched = binary.getShuffleMergedRemoteChunksFetched,
      shuffleMergedLocalChunksFetched = binary.getShuffleMergedLocalChunksFetched,
      shuffleMergedRemoteBytesRead = binary.getShuffleMergedRemoteBytesRead,
      shuffleMergedLocalBytesRead = binary.getShuffleMergedLocalBytesRead,
      shuffleRemoteReqsDuration = binary.getShuffleRemoteReqsDuration,
      shuffleMergedRemoteReqDuration = binary.getShuffleMergedRemoteReqDuration,
      shuffleBytesWritten = binary.getShuffleBytesWritten,
      shuffleWriteTime = binary.getShuffleWriteTime,
      shuffleRecordsWritten = binary.getShuffleRecordsWritten,
      stageId = binary.getStageId.toInt,
      stageAttemptId = binary.getStageAttemptId
    )
  }
}
