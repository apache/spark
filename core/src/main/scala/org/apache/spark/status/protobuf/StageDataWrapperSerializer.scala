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

import org.apache.commons.collections4.MapUtils

import org.apache.spark.status.StageDataWrapper
import org.apache.spark.status.api.v1.{ExecutorMetricsDistributions, ExecutorPeakMetricsDistributions, InputMetricDistributions, InputMetrics, OutputMetricDistributions, OutputMetrics, ShufflePushReadMetricDistributions, ShufflePushReadMetrics, ShuffleReadMetricDistributions, ShuffleReadMetrics, ShuffleWriteMetricDistributions, ShuffleWriteMetrics, SpeculationStageSummary, StageData, TaskData, TaskMetricDistributions, TaskMetrics}
import org.apache.spark.status.protobuf.Utils._
import org.apache.spark.util.Utils.weakIntern

private[protobuf] class StageDataWrapperSerializer extends ProtobufSerDe[StageDataWrapper] {

  override def serialize(input: StageDataWrapper): Array[Byte] = {
    val builder = StoreTypes.StageDataWrapper.newBuilder()
    builder.setInfo(serializeStageData(input.info))
    input.jobIds.foreach(id => builder.addJobIds(id.toLong))
    input.locality.foreach { entry =>
      builder.putLocality(entry._1, entry._2)
    }
    builder.build().toByteArray
  }

  private def serializeStageData(stageData: StageData): StoreTypes.StageData = {
    val stageDataBuilder = StoreTypes.StageData.newBuilder()
    stageDataBuilder
      .setStatus(StageStatusSerializer.serialize(stageData.status))
      .setStageId(stageData.stageId.toLong)
      .setAttemptId(stageData.attemptId)
      .setNumTasks(stageData.numTasks)
      .setNumActiveTasks(stageData.numActiveTasks)
      .setNumCompleteTasks(stageData.numCompleteTasks)
      .setNumFailedTasks(stageData.numFailedTasks)
      .setNumKilledTasks(stageData.numKilledTasks)
      .setNumCompletedIndices(stageData.numCompletedIndices)
      .setExecutorDeserializeTime(stageData.executorDeserializeTime)
      .setExecutorDeserializeCpuTime(stageData.executorDeserializeCpuTime)
      .setExecutorRunTime(stageData.executorRunTime)
      .setExecutorCpuTime(stageData.executorCpuTime)
      .setResultSize(stageData.resultSize)
      .setJvmGcTime(stageData.jvmGcTime)
      .setResultSerializationTime(stageData.resultSerializationTime)
      .setMemoryBytesSpilled(stageData.memoryBytesSpilled)
      .setDiskBytesSpilled(stageData.diskBytesSpilled)
      .setPeakExecutionMemory(stageData.peakExecutionMemory)
      .setPeakOnHeapExecutionMemory(stageData.peakOnHeapExecutionMemory)
      .setPeakOffHeapExecutionMemory(stageData.peakOffHeapExecutionMemory)
      .setInputBytes(stageData.inputBytes)
      .setInputRecords(stageData.inputRecords)
      .setOutputBytes(stageData.outputBytes)
      .setOutputRecords(stageData.outputRecords)
      .setShuffleRemoteBlocksFetched(stageData.shuffleRemoteBlocksFetched)
      .setShuffleLocalBlocksFetched(stageData.shuffleLocalBlocksFetched)
      .setShuffleFetchWaitTime(stageData.shuffleFetchWaitTime)
      .setShuffleRemoteBytesRead(stageData.shuffleRemoteBytesRead)
      .setShuffleRemoteBytesReadToDisk(stageData.shuffleRemoteBytesReadToDisk)
      .setShuffleLocalBytesRead(stageData.shuffleLocalBytesRead)
      .setShuffleReadBytes(stageData.shuffleReadBytes)
      .setShuffleReadRecords(stageData.shuffleReadRecords)
      .setShuffleCorruptMergedBlockChunks(stageData.shuffleCorruptMergedBlockChunks)
      .setShuffleMergedFetchFallbackCount(stageData.shuffleMergedFetchFallbackCount)
      .setShuffleMergedRemoteBlocksFetched(stageData.shuffleMergedRemoteBlocksFetched)
      .setShuffleMergedLocalBlocksFetched(stageData.shuffleMergedLocalBlocksFetched)
      .setShuffleMergedRemoteChunksFetched(stageData.shuffleMergedRemoteChunksFetched)
      .setShuffleMergedLocalChunksFetched(stageData.shuffleMergedLocalChunksFetched)
      .setShuffleMergedRemoteBytesRead(stageData.shuffleMergedRemoteBytesRead)
      .setShuffleMergedLocalBytesRead(stageData.shuffleMergedLocalBytesRead)
      .setShuffleRemoteReqsDuration(stageData.shuffleRemoteReqsDuration)
      .setShuffleMergedRemoteReqsDuration(stageData.shuffleMergedRemoteReqsDuration)
      .setShuffleWriteBytes(stageData.shuffleWriteBytes)
      .setShuffleWriteTime(stageData.shuffleWriteTime)
      .setShuffleWriteRecords(stageData.shuffleWriteRecords)
      .setResourceProfileId(stageData.resourceProfileId)
      .setIsShufflePushEnabled(stageData.isShufflePushEnabled)
      .setShuffleMergersCount(stageData.shuffleMergersCount)
    setStringField(stageData.name, stageDataBuilder.setName)
    setStringField(stageData.details, stageDataBuilder.setDetails)
    setStringField(stageData.schedulingPool, stageDataBuilder.setSchedulingPool)
    stageData.submissionTime.foreach { d =>
      stageDataBuilder.setSubmissionTime(d.getTime)
    }
    stageData.firstTaskLaunchedTime.foreach { d =>
      stageDataBuilder.setFirstTaskLaunchedTime(d.getTime)
    }
    stageData.completionTime.foreach { d =>
      stageDataBuilder.setCompletionTime(d.getTime)
    }
    stageData.failureReason.foreach { fr =>
      stageDataBuilder.setFailureReason(fr)
    }
    stageData.description.foreach { d =>
      stageDataBuilder.setDescription(d)
    }
    stageData.rddIds.foreach(id => stageDataBuilder.addRddIds(id.toLong))
    stageData.accumulatorUpdates.foreach { update =>
      stageDataBuilder.addAccumulatorUpdates(
        AccumulableInfoSerializer.serialize(update))
    }
    stageData.tasks.foreach { t =>
      t.foreach { entry =>
        stageDataBuilder.putTasks(entry._1, serializeTaskData(entry._2))
      }
    }
    stageData.executorSummary.foreach { es =>
      es.foreach { entry =>
        stageDataBuilder.putExecutorSummary(entry._1,
          ExecutorStageSummarySerializer.serialize(entry._2))
      }
    }
    stageData.speculationSummary.foreach { ss =>
      stageDataBuilder.setSpeculationSummary(serializeSpeculationStageSummary(ss))
    }
    stageData.killedTasksSummary.foreach { entry =>
      stageDataBuilder.putKilledTasksSummary(entry._1, entry._2)
    }
    stageData.peakExecutorMetrics.foreach { pem =>
      stageDataBuilder.setPeakExecutorMetrics(ExecutorMetricsSerializer.serialize(pem))
    }
    stageData.taskMetricsDistributions.foreach { tmd =>
      stageDataBuilder.setTaskMetricsDistributions(serializeTaskMetricDistributions(tmd))
    }
    stageData.executorMetricsDistributions.foreach { emd =>
      stageDataBuilder.setExecutorMetricsDistributions(serializeExecutorMetricsDistributions(emd))
    }
    stageDataBuilder.build()
  }

  private def serializeTaskData(t: TaskData): StoreTypes.TaskData = {
    val taskDataBuilder = StoreTypes.TaskData.newBuilder()
    taskDataBuilder
      .setTaskId(t.taskId)
      .setIndex(t.index)
      .setAttempt(t.attempt)
      .setPartitionId(t.partitionId)
      .setLaunchTime(t.launchTime.getTime)
      .setSpeculative(t.speculative)
      .setSchedulerDelay(t.schedulerDelay)
      .setGettingResultTime(t.gettingResultTime)
    setStringField(t.executorId, taskDataBuilder.setExecutorId)
    setStringField(t.host, taskDataBuilder.setHost)
    setStringField(t.status, taskDataBuilder.setStatus)
    setStringField(t.taskLocality, taskDataBuilder.setTaskLocality)
    t.resultFetchStart.foreach { rfs =>
      taskDataBuilder.setResultFetchStart(rfs.getTime)
    }
    t.duration.foreach { d =>
      taskDataBuilder.setDuration(d)
    }
    t.accumulatorUpdates.foreach { update =>
      taskDataBuilder.addAccumulatorUpdates(
        AccumulableInfoSerializer.serialize(update))
    }
    t.errorMessage.foreach { em =>
      taskDataBuilder.setErrorMessage(em)
    }
    t.taskMetrics.foreach { tm =>
      taskDataBuilder.setTaskMetrics(serializeTaskMetrics(tm))
    }
    t.executorLogs.foreach { entry =>
      taskDataBuilder.putExecutorLogs(entry._1, entry._2)
    }
    taskDataBuilder.build()
  }

  private def serializeTaskMetrics(tm: TaskMetrics): StoreTypes.TaskMetrics = {
    val taskMetricsBuilder = StoreTypes.TaskMetrics.newBuilder()
    taskMetricsBuilder
      .setExecutorDeserializeTime(tm.executorDeserializeTime)
      .setExecutorDeserializeCpuTime(tm.executorDeserializeCpuTime)
      .setExecutorRunTime(tm.executorRunTime)
      .setExecutorCpuTime(tm.executorCpuTime)
      .setResultSize(tm.resultSize)
      .setJvmGcTime(tm.jvmGcTime)
      .setResultSerializationTime(tm.resultSerializationTime)
      .setMemoryBytesSpilled(tm.memoryBytesSpilled)
      .setDiskBytesSpilled(tm.diskBytesSpilled)
      .setPeakExecutionMemory(tm.peakExecutionMemory)
      .setPeakOnHeapExecutionMemory(tm.peakOnHeapExecutionMemory)
      .setPeakOffHeapExecutionMemory(tm.peakOffHeapExecutionMemory)
      .setInputMetrics(serializeInputMetrics(tm.inputMetrics))
      .setOutputMetrics(serializeOutputMetrics(tm.outputMetrics))
      .setShuffleReadMetrics(serializeShuffleReadMetrics(tm.shuffleReadMetrics))
      .setShuffleWriteMetrics(serializeShuffleWriteMetrics(tm.shuffleWriteMetrics))
    taskMetricsBuilder.build()
  }

  private def serializeInputMetrics(im: InputMetrics): StoreTypes.InputMetrics = {
    StoreTypes.InputMetrics.newBuilder()
      .setBytesRead(im.bytesRead)
      .setRecordsRead(im.recordsRead)
      .build()
  }

  private def serializeOutputMetrics(om: OutputMetrics): StoreTypes.OutputMetrics = {
    StoreTypes.OutputMetrics.newBuilder()
      .setBytesWritten(om.bytesWritten)
      .setRecordsWritten(om.recordsWritten)
      .build()
  }

  private def serializeShuffleReadMetrics(
      srm: ShuffleReadMetrics): StoreTypes.ShuffleReadMetrics = {
    StoreTypes.ShuffleReadMetrics.newBuilder()
      .setRemoteBlocksFetched(srm.remoteBlocksFetched)
      .setLocalBlocksFetched(srm.localBlocksFetched)
      .setFetchWaitTime(srm.fetchWaitTime)
      .setRemoteBytesRead(srm.remoteBytesRead)
      .setRemoteBytesReadToDisk(srm.remoteBytesReadToDisk)
      .setLocalBytesRead(srm.localBytesRead)
      .setRecordsRead(srm.recordsRead)
      .setRemoteReqsDuration(srm.remoteReqsDuration)
      .setShufflePushReadMetrics(serializeShufflePushReadMetrics(srm.shufflePushReadMetrics))
      .build()
  }

  private def serializeShufflePushReadMetrics(
      sprm: ShufflePushReadMetrics): StoreTypes.ShufflePushReadMetrics = {
    StoreTypes.ShufflePushReadMetrics.newBuilder()
      .setCorruptMergedBlockChunks(sprm.corruptMergedBlockChunks)
      .setMergedFetchFallbackCount(sprm.mergedFetchFallbackCount)
      .setRemoteMergedBlocksFetched(sprm.remoteMergedBlocksFetched)
      .setLocalMergedBlocksFetched(sprm.localMergedBlocksFetched)
      .setRemoteMergedChunksFetched(sprm.remoteMergedChunksFetched)
      .setLocalMergedChunksFetched(sprm.localMergedChunksFetched)
      .setRemoteMergedBytesRead(sprm.remoteMergedBytesRead)
      .setLocalMergedBytesRead(sprm.localMergedBytesRead)
      .setRemoteMergedReqsDuration(sprm.remoteMergedReqsDuration)
      .build()
  }

  private def serializeShuffleWriteMetrics(
      swm: ShuffleWriteMetrics): StoreTypes.ShuffleWriteMetrics = {
    StoreTypes.ShuffleWriteMetrics.newBuilder()
      .setBytesWritten(swm.bytesWritten)
      .setWriteTime(swm.writeTime)
      .setRecordsWritten(swm.recordsWritten)
      .build()
  }

  private def serializeSpeculationStageSummary(
      sss: SpeculationStageSummary): StoreTypes.SpeculationStageSummary = {
    StoreTypes.SpeculationStageSummary.newBuilder()
      .setNumTasks(sss.numTasks)
      .setNumActiveTasks(sss.numActiveTasks)
      .setNumCompletedTasks(sss.numCompletedTasks)
      .setNumFailedTasks(sss.numFailedTasks)
      .setNumKilledTasks(sss.numKilledTasks)
      .build()
  }

  private def serializeTaskMetricDistributions(
      tmd: TaskMetricDistributions): StoreTypes.TaskMetricDistributions = {
    val builder = StoreTypes.TaskMetricDistributions.newBuilder()
    tmd.quantiles.foreach(q => builder.addQuantiles(q))
    tmd.duration.foreach(d => builder.addDuration(d))
    tmd.executorDeserializeTime.foreach(edt => builder.addExecutorDeserializeTime(edt))
    tmd.executorDeserializeCpuTime.foreach(edct => builder.addExecutorDeserializeCpuTime(edct))
    tmd.executorRunTime.foreach(ert => builder.addExecutorRunTime(ert))
    tmd.executorCpuTime.foreach(ect => builder.addExecutorCpuTime(ect))
    tmd.resultSize.foreach(rs => builder.addResultSize(rs))
    tmd.jvmGcTime.foreach(jgt => builder.addJvmGcTime(jgt))
    tmd.resultSerializationTime.foreach(rst => builder.addResultSerializationTime(rst))
    tmd.gettingResultTime.foreach(grt => builder.addGettingResultTime(grt))
    tmd.schedulerDelay.foreach(sd => builder.addSchedulerDelay(sd))
    tmd.peakExecutionMemory.foreach(pem => builder.addPeakExecutionMemory(pem))
    tmd.peakOnHeapExecutionMemory.foreach(peonm => builder.addPeakOnHeapExecutionMemory(peonm))
    tmd.peakOffHeapExecutionMemory.foreach(peoffm => builder.addPeakOffHeapExecutionMemory(peoffm))
    tmd.memoryBytesSpilled.foreach(mbs => builder.addMemoryBytesSpilled(mbs))
    tmd.diskBytesSpilled.foreach(dbs => builder.addDiskBytesSpilled(dbs))
    builder
      .setInputMetrics(serializeInputMetricDistributions(tmd.inputMetrics))
      .setOutputMetrics(serializeOutputMetricDistributions(tmd.outputMetrics))
      .setShuffleReadMetrics(serializeShuffleReadMetricDistributions(tmd.shuffleReadMetrics))
      .setShuffleWriteMetrics(serializeShuffleWriteMetricDistributions(tmd.shuffleWriteMetrics))
      .build()
  }

  private def serializeInputMetricDistributions(
      imd: InputMetricDistributions): StoreTypes.InputMetricDistributions = {
    val builder = StoreTypes.InputMetricDistributions.newBuilder()
    imd.bytesRead.foreach(br => builder.addBytesRead(br))
    imd.recordsRead.foreach(rr => builder.addRecordsRead(rr))
    builder.build()
  }

  private def serializeOutputMetricDistributions(
      omd: OutputMetricDistributions): StoreTypes.OutputMetricDistributions = {
    val builder = StoreTypes.OutputMetricDistributions.newBuilder()
    omd.bytesWritten.foreach(bw => builder.addBytesWritten(bw))
    omd.recordsWritten.foreach(rw => builder.addRecordsWritten(rw))
    builder.build()
  }

  private def serializeShuffleReadMetricDistributions(
      srmd: ShuffleReadMetricDistributions): StoreTypes.ShuffleReadMetricDistributions = {
    val builder = StoreTypes.ShuffleReadMetricDistributions.newBuilder()
    srmd.readBytes.foreach(rb => builder.addReadBytes(rb))
    srmd.readRecords.foreach(rr => builder.addReadRecords(rr))
    srmd.remoteBlocksFetched.foreach(rbf => builder.addRemoteBlocksFetched(rbf))
    srmd.localBlocksFetched.foreach(lbf => builder.addLocalBlocksFetched(lbf))
    srmd.fetchWaitTime.foreach(fwt => builder.addFetchWaitTime(fwt))
    srmd.remoteBytesRead.foreach(rbr => builder.addRemoteBytesRead(rbr))
    srmd.remoteBytesReadToDisk.foreach(rbrtd => builder.addRemoteBytesReadToDisk(rbrtd))
    srmd.totalBlocksFetched.foreach(tbf => builder.addTotalBlocksFetched(tbf))
    srmd.remoteReqsDuration.foreach(rrd => builder.addRemoteReqsDuration(rrd))
    builder.setShufflePushReadMetricsDist(
      serializeShufflePushReadMetricDistributions(srmd.shufflePushReadMetricsDist))
    builder.build()
  }

  private def serializeShufflePushReadMetricDistributions(
      sprmd: ShufflePushReadMetricDistributions): StoreTypes.ShufflePushReadMetricDistributions = {
    val builder = StoreTypes.ShufflePushReadMetricDistributions.newBuilder()
    sprmd.corruptMergedBlockChunks.foreach(cmbc => builder.addCorruptMergedBlockChunks(cmbc))
    sprmd.mergedFetchFallbackCount.foreach(mffc => builder.addMergedFetchFallbackCount(mffc))
    sprmd.remoteMergedBlocksFetched.foreach(rmbf => builder.addRemoteMergedBlocksFetched(rmbf))
    sprmd.localMergedBlocksFetched.foreach(lmbf => builder.addLocalMergedBlocksFetched(lmbf))
    sprmd.remoteMergedChunksFetched.foreach(rmcf => builder.addRemoteMergedChunksFetched(rmcf))
    sprmd.localMergedChunksFetched.foreach(lmcf => builder.addLocalMergedChunksFetched(lmcf))
    sprmd.remoteMergedBytesRead.foreach(rmbr => builder.addRemoteMergedBytesRead(rmbr))
    sprmd.localMergedBytesRead.foreach(lmbr => builder.addLocalMergedBytesRead(lmbr))
    sprmd.remoteMergedReqsDuration.foreach(rmrd => builder.addRemoteMergedReqsDuration(rmrd))
    builder.build()
  }

  private def serializeShuffleWriteMetricDistributions(
      swmd: ShuffleWriteMetricDistributions): StoreTypes.ShuffleWriteMetricDistributions = {
    val builder = StoreTypes.ShuffleWriteMetricDistributions.newBuilder()
    swmd.writeBytes.foreach(wb => builder.addWriteBytes(wb))
    swmd.writeRecords.foreach(wr => builder.addWriteRecords(wr))
    swmd.writeTime.foreach(wt => builder.addWriteTime(wt))
    builder.build()
  }

  private def serializeExecutorMetricsDistributions(
      emd: ExecutorMetricsDistributions): StoreTypes.ExecutorMetricsDistributions = {
    val builder = StoreTypes.ExecutorMetricsDistributions.newBuilder()
    emd.quantiles.foreach(q => builder.addQuantiles(q))
    emd.taskTime.foreach(tt => builder.addTaskTime(tt))
    emd.failedTasks.foreach(ft => builder.addFailedTasks(ft))
    emd.succeededTasks.foreach(st => builder.addSucceededTasks(st))
    emd.killedTasks.foreach(kt => builder.addKilledTasks(kt))
    emd.inputBytes.foreach(ib => builder.addInputBytes(ib))
    emd.inputRecords.foreach(ir => builder.addInputRecords(ir))
    emd.outputBytes.foreach(ob => builder.addOutputBytes(ob))
    emd.outputRecords.foreach(or => builder.addOutputRecords(or))
    emd.shuffleRead.foreach(sr => builder.addShuffleRead(sr))
    emd.shuffleReadRecords.foreach(srr => builder.addShuffleReadRecords(srr))
    emd.shuffleWrite.foreach(sw => builder.addShuffleWrite(sw))
    emd.shuffleWriteRecords.foreach(swr => builder.addShuffleWriteRecords(swr))
    emd.memoryBytesSpilled.foreach(mbs => builder.addMemoryBytesSpilled(mbs))
    emd.diskBytesSpilled.foreach(dbs => builder.addDiskBytesSpilled(dbs))
    builder.setPeakMemoryMetrics(serializeExecutorPeakMetricsDistributions(emd.peakMemoryMetrics))
    builder.build()
  }

  private def serializeExecutorPeakMetricsDistributions(
      epmd: ExecutorPeakMetricsDistributions): StoreTypes.ExecutorPeakMetricsDistributions = {
    val builder = StoreTypes.ExecutorPeakMetricsDistributions.newBuilder()
    epmd.quantiles.foreach(q => builder.addQuantiles(q))
    epmd.executorMetrics.foreach(em => builder.addExecutorMetrics(
      ExecutorMetricsSerializer.serialize(em)))
    builder.build()
  }

  override def deserialize(bytes: Array[Byte]): StageDataWrapper = {
    val binary = StoreTypes.StageDataWrapper.parseFrom(bytes)
    val info = deserializeStageData(binary.getInfo)
    new StageDataWrapper(
      info = info,
      jobIds = binary.getJobIdsList.asScala.map(_.toInt).toSet,
      locality = binary.getLocalityMap.asScala.toMap.transform((_, v) => v.toLong)
    )
  }

  private def deserializeStageData(binary: StoreTypes.StageData): StageData = {
    val status = StageStatusSerializer.deserialize(binary.getStatus)
    val submissionTime =
      getOptional(binary.hasSubmissionTime, () => new Date(binary.getSubmissionTime))
    val firstTaskLaunchedTime =
      getOptional(binary.hasFirstTaskLaunchedTime, () => new Date(binary.getFirstTaskLaunchedTime))
    val completionTime =
      getOptional(binary.hasCompletionTime, () => new Date(binary.getCompletionTime))
    val failureReason = getOptional(binary.hasFailureReason, binary.getFailureReason)
    val description = getOptional(binary.hasDescription, binary.getDescription)
    val accumulatorUpdates = AccumulableInfoSerializer.deserialize(binary.getAccumulatorUpdatesList)
    val tasks = if (MapUtils.isNotEmpty(binary.getTasksMap)) {
      Some(binary.getTasksMap.asScala.map(
        entry => (entry._1.toLong, deserializeTaskData(entry._2))).toMap)
    } else None
    val executorSummary = if (MapUtils.isNotEmpty(binary.getExecutorSummaryMap)) {
      Some(binary.getExecutorSummaryMap.asScala.toMap
        .transform((_, v) => ExecutorStageSummarySerializer.deserialize(v)))
    } else None
    val speculationSummary =
      getOptional(binary.hasSpeculationSummary,
        () => deserializeSpeculationStageSummary(binary.getSpeculationSummary))
    val peakExecutorMetrics =
      getOptional(binary.hasPeakExecutorMetrics,
        () => ExecutorMetricsSerializer.deserialize(binary.getPeakExecutorMetrics))
    val taskMetricsDistributions =
      getOptional(binary.hasTaskMetricsDistributions,
        () => deserializeTaskMetricDistributions(binary.getTaskMetricsDistributions))
    val executorMetricsDistributions =
      getOptional(binary.hasExecutorMetricsDistributions,
        () => deserializeExecutorMetricsDistributions(binary.getExecutorMetricsDistributions))
    new StageData(
      status = status,
      stageId = binary.getStageId.toInt,
      attemptId = binary.getAttemptId,
      numTasks = binary.getNumTasks,
      numActiveTasks = binary.getNumActiveTasks,
      numCompleteTasks = binary.getNumCompleteTasks,
      numFailedTasks = binary.getNumFailedTasks,
      numKilledTasks = binary.getNumKilledTasks,
      numCompletedIndices = binary.getNumCompletedIndices,
      submissionTime = submissionTime,
      firstTaskLaunchedTime = firstTaskLaunchedTime,
      completionTime = completionTime,
      failureReason = failureReason,
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
      peakOnHeapExecutionMemory = binary.getPeakOnHeapExecutionMemory,
      peakOffHeapExecutionMemory = binary.getPeakOffHeapExecutionMemory,
      inputBytes = binary.getInputBytes,
      inputRecords = binary.getInputRecords,
      outputBytes = binary.getOutputBytes,
      outputRecords = binary.getOutputRecords,
      shuffleRemoteBlocksFetched = binary.getShuffleRemoteBlocksFetched,
      shuffleLocalBlocksFetched = binary.getShuffleLocalBlocksFetched,
      shuffleFetchWaitTime = binary.getShuffleFetchWaitTime,
      shuffleRemoteBytesRead = binary.getShuffleRemoteBytesRead,
      shuffleRemoteBytesReadToDisk = binary.getShuffleRemoteBytesReadToDisk,
      shuffleLocalBytesRead = binary.getShuffleLocalBytesRead,
      shuffleReadBytes = binary.getShuffleReadBytes,
      shuffleReadRecords = binary.getShuffleReadRecords,
      shuffleCorruptMergedBlockChunks = binary.getShuffleCorruptMergedBlockChunks,
      shuffleMergedFetchFallbackCount = binary.getShuffleMergedFetchFallbackCount,
      shuffleMergedRemoteBlocksFetched = binary.getShuffleMergedRemoteBlocksFetched,
      shuffleMergedLocalBlocksFetched = binary.getShuffleMergedLocalBlocksFetched,
      shuffleMergedRemoteChunksFetched = binary.getShuffleMergedRemoteChunksFetched,
      shuffleMergedLocalChunksFetched = binary.getShuffleMergedLocalChunksFetched,
      shuffleMergedRemoteBytesRead = binary.getShuffleMergedRemoteBytesRead,
      shuffleMergedLocalBytesRead = binary.getShuffleMergedLocalBytesRead,
      shuffleRemoteReqsDuration = binary.getShuffleRemoteReqsDuration,
      shuffleMergedRemoteReqsDuration = binary.getShuffleMergedRemoteReqsDuration,
      shuffleWriteBytes = binary.getShuffleWriteBytes,
      shuffleWriteTime = binary.getShuffleWriteTime,
      shuffleWriteRecords = binary.getShuffleWriteRecords,
      name = getStringField(binary.hasName, () => binary.getName),
      description = description,
      details = getStringField(binary.hasDetails, () => binary.getDetails),
      schedulingPool = getStringField(binary.hasSchedulingPool, () => binary.getSchedulingPool),
      rddIds = binary.getRddIdsList.asScala.map(_.toInt),
      accumulatorUpdates = accumulatorUpdates,
      tasks = tasks,
      executorSummary = executorSummary,
      speculationSummary = speculationSummary,
      killedTasksSummary =
        binary.getKilledTasksSummaryMap.asScala.toMap.transform((_, v) => v.toInt),
      resourceProfileId = binary.getResourceProfileId,
      peakExecutorMetrics = peakExecutorMetrics,
      taskMetricsDistributions = taskMetricsDistributions,
      executorMetricsDistributions = executorMetricsDistributions,
      isShufflePushEnabled = binary.getIsShufflePushEnabled,
      shuffleMergersCount = binary.getShuffleMergersCount
    )
  }

  private def deserializeSpeculationStageSummary(
      binary: StoreTypes.SpeculationStageSummary): SpeculationStageSummary = {
    new SpeculationStageSummary(
      binary.getNumTasks,
      binary.getNumActiveTasks,
      binary.getNumCompletedTasks,
      binary.getNumFailedTasks,
      binary.getNumKilledTasks
    )
  }

  private def deserializeTaskMetricDistributions(
      binary: StoreTypes.TaskMetricDistributions): TaskMetricDistributions = {
    new TaskMetricDistributions(
      quantiles = binary.getQuantilesList.asScala.map(_.toDouble).toIndexedSeq,
      duration = binary.getDurationList.asScala.map(_.toDouble).toIndexedSeq,
      executorDeserializeTime =
        binary.getExecutorDeserializeTimeList.asScala.map(_.toDouble).toIndexedSeq,
      executorDeserializeCpuTime =
        binary.getExecutorDeserializeCpuTimeList.asScala.map(_.toDouble).toIndexedSeq,
      executorRunTime = binary.getExecutorRunTimeList.asScala.map(_.toDouble).toIndexedSeq,
      executorCpuTime = binary.getExecutorCpuTimeList.asScala.map(_.toDouble).toIndexedSeq,
      resultSize = binary.getResultSizeList.asScala.map(_.toDouble).toIndexedSeq,
      jvmGcTime = binary.getJvmGcTimeList.asScala.map(_.toDouble).toIndexedSeq,
      resultSerializationTime =
        binary.getResultSerializationTimeList.asScala.map(_.toDouble).toIndexedSeq,
      gettingResultTime = binary.getGettingResultTimeList.asScala.map(_.toDouble).toIndexedSeq,
      schedulerDelay = binary.getSchedulerDelayList.asScala.map(_.toDouble).toIndexedSeq,
      peakExecutionMemory = binary.getPeakExecutionMemoryList.asScala.map(_.toDouble).toIndexedSeq,
      peakOnHeapExecutionMemory = binary.getPeakOnHeapExecutionMemoryList.asScala
        .map(_.toDouble).toIndexedSeq,
      peakOffHeapExecutionMemory = binary.getPeakOffHeapExecutionMemoryList.asScala
        .map(_.toDouble).toIndexedSeq,
      memoryBytesSpilled = binary.getMemoryBytesSpilledList.asScala.map(_.toDouble).toIndexedSeq,
      diskBytesSpilled = binary.getDiskBytesSpilledList.asScala.map(_.toDouble).toIndexedSeq,
      inputMetrics = deserializeInputMetricDistributions(binary.getInputMetrics),
      outputMetrics = deserializeOutputMetricDistributions(binary.getOutputMetrics),
      shuffleReadMetrics = deserializeShuffleReadMetricDistributions(binary.getShuffleReadMetrics),
      shuffleWriteMetrics =
        deserializeShuffleWriteMetricDistributions(binary.getShuffleWriteMetrics)
    )
  }

  private def deserializeInputMetricDistributions(
      binary: StoreTypes.InputMetricDistributions): InputMetricDistributions = {
    new InputMetricDistributions(
      bytesRead = binary.getBytesReadList.asScala.map(_.toDouble).toIndexedSeq,
      recordsRead = binary.getRecordsReadList.asScala.map(_.toDouble).toIndexedSeq
    )
  }

  private def deserializeOutputMetricDistributions(
      binary: StoreTypes.OutputMetricDistributions): OutputMetricDistributions = {
    new OutputMetricDistributions(
      bytesWritten = binary.getBytesWrittenList.asScala.map(_.toDouble).toIndexedSeq,
      recordsWritten = binary.getRecordsWrittenList.asScala.map(_.toDouble).toIndexedSeq
    )
  }

  private def deserializeShuffleReadMetricDistributions(
      binary: StoreTypes.ShuffleReadMetricDistributions): ShuffleReadMetricDistributions = {
    new ShuffleReadMetricDistributions(
      readBytes = binary.getReadBytesList.asScala.map(_.toDouble).toIndexedSeq,
      readRecords = binary.getReadRecordsList.asScala.map(_.toDouble).toIndexedSeq,
      remoteBlocksFetched = binary.getRemoteBlocksFetchedList.asScala.map(_.toDouble).toIndexedSeq,
      localBlocksFetched = binary.getLocalBlocksFetchedList.asScala.map(_.toDouble).toIndexedSeq,
      fetchWaitTime = binary.getFetchWaitTimeList.asScala.map(_.toDouble).toIndexedSeq,
      remoteBytesRead = binary.getRemoteBytesReadList.asScala.map(_.toDouble).toIndexedSeq,
      remoteBytesReadToDisk =
        binary.getRemoteBytesReadToDiskList.asScala.map(_.toDouble).toIndexedSeq,
      totalBlocksFetched = binary.getTotalBlocksFetchedList.asScala.map(_.toDouble).toIndexedSeq,
      remoteReqsDuration = binary.getRemoteReqsDurationList.asScala.map(_.toDouble).toIndexedSeq,
      shufflePushReadMetricsDist =
        deserializeShufflePushReadMetricsDistributions(binary.getShufflePushReadMetricsDist)
    )
  }

  private def deserializeShufflePushReadMetricsDistributions(
      binary: StoreTypes.ShufflePushReadMetricDistributions): ShufflePushReadMetricDistributions = {
    new ShufflePushReadMetricDistributions(
      corruptMergedBlockChunks =
        binary.getCorruptMergedBlockChunksList.asScala.map(_.toDouble).toIndexedSeq,
      mergedFetchFallbackCount =
        binary.getMergedFetchFallbackCountList.asScala.map(_.toDouble).toIndexedSeq,
      remoteMergedBlocksFetched =
        binary.getRemoteMergedBlocksFetchedList.asScala.map(_.toDouble).toIndexedSeq,
      localMergedBlocksFetched =
        binary.getLocalMergedBlocksFetchedList.asScala.map(_.toDouble).toIndexedSeq,
      remoteMergedChunksFetched =
        binary.getRemoteMergedChunksFetchedList.asScala.map(_.toDouble).toIndexedSeq,
      localMergedChunksFetched =
        binary.getLocalMergedChunksFetchedList.asScala.map(_.toDouble).toIndexedSeq,
      remoteMergedBytesRead =
        binary.getRemoteMergedBytesReadList.asScala.map(_.toDouble).toIndexedSeq,
      localMergedBytesRead =
        binary.getLocalMergedBytesReadList.asScala.map(_.toDouble).toIndexedSeq,
      remoteMergedReqsDuration =
        binary.getRemoteMergedReqsDurationList.asScala.map(_.toDouble).toIndexedSeq
    )
  }

  private def deserializeShuffleWriteMetricDistributions(
      binary: StoreTypes.ShuffleWriteMetricDistributions): ShuffleWriteMetricDistributions = {
    new ShuffleWriteMetricDistributions(
      writeBytes = binary.getWriteBytesList.asScala.map(_.toDouble).toIndexedSeq,
      writeRecords = binary.getWriteRecordsList.asScala.map(_.toDouble).toIndexedSeq,
      writeTime = binary.getWriteTimeList.asScala.map(_.toDouble).toIndexedSeq
    )
  }

  private def deserializeExecutorMetricsDistributions(
      binary: StoreTypes.ExecutorMetricsDistributions): ExecutorMetricsDistributions = {
    new ExecutorMetricsDistributions(
      quantiles = binary.getQuantilesList.asScala.map(_.toDouble).toIndexedSeq,
      taskTime = binary.getTaskTimeList.asScala.map(_.toDouble).toIndexedSeq,
      failedTasks = binary.getFailedTasksList.asScala.map(_.toDouble).toIndexedSeq,
      succeededTasks = binary.getSucceededTasksList.asScala.map(_.toDouble).toIndexedSeq,
      killedTasks = binary.getKilledTasksList.asScala.map(_.toDouble).toIndexedSeq,
      inputBytes = binary.getInputBytesList.asScala.map(_.toDouble).toIndexedSeq,
      inputRecords = binary.getInputRecordsList.asScala.map(_.toDouble).toIndexedSeq,
      outputBytes = binary.getOutputBytesList.asScala.map(_.toDouble).toIndexedSeq,
      outputRecords = binary.getOutputRecordsList.asScala.map(_.toDouble).toIndexedSeq,
      shuffleRead = binary.getShuffleReadList.asScala.map(_.toDouble).toIndexedSeq,
      shuffleReadRecords = binary.getShuffleReadRecordsList.asScala.map(_.toDouble).toIndexedSeq,
      shuffleWrite = binary.getShuffleWriteList.asScala.map(_.toDouble).toIndexedSeq,
      shuffleWriteRecords = binary.getShuffleWriteRecordsList.asScala.map(_.toDouble).toIndexedSeq,
      memoryBytesSpilled = binary.getMemoryBytesSpilledList.asScala.map(_.toDouble).toIndexedSeq,
      diskBytesSpilled = binary.getDiskBytesSpilledList.asScala.map(_.toDouble).toIndexedSeq,
      peakMemoryMetrics = deserializeExecutorPeakMetricsDistributions(binary.getPeakMemoryMetrics)
    )
  }

  private def deserializeExecutorPeakMetricsDistributions(
      binary: StoreTypes.ExecutorPeakMetricsDistributions): ExecutorPeakMetricsDistributions = {
    new ExecutorPeakMetricsDistributions(
      quantiles = binary.getQuantilesList.asScala.map(_.toDouble).toIndexedSeq,
      executorMetrics = binary.getExecutorMetricsList.asScala.map(
        ExecutorMetricsSerializer.deserialize).toIndexedSeq
    )
  }

  private def deserializeTaskData(binary: StoreTypes.TaskData): TaskData = {
    val resultFetchStart = getOptional(binary.hasResultFetchStart,
      () => new Date(binary.getResultFetchStart))
    val duration = getOptional(binary.hasDuration, () => binary.getDuration)
    val accumulatorUpdates = AccumulableInfoSerializer.deserialize(binary.getAccumulatorUpdatesList)
    val taskMetrics = getOptional(binary.hasTaskMetrics,
      () => deserializeTaskMetrics(binary.getTaskMetrics))
    new TaskData(
      taskId = binary.getTaskId,
      index = binary.getIndex,
      attempt = binary.getAttempt,
      partitionId = binary.getPartitionId,
      launchTime = new Date(binary.getLaunchTime),
      resultFetchStart = resultFetchStart,
      duration = duration,
      executorId = getStringField(binary.hasExecutorId, () => weakIntern(binary.getExecutorId)),
      host = getStringField(binary.hasHost, () => weakIntern(binary.getHost)),
      status = getStringField(binary.hasStatus, () => weakIntern(binary.getStatus)),
      taskLocality =
        getStringField(binary.hasTaskLocality, () => weakIntern(binary.getTaskLocality)),
      speculative = binary.getSpeculative,
      accumulatorUpdates = accumulatorUpdates,
      errorMessage = getOptional(binary.hasErrorMessage, binary.getErrorMessage),
      taskMetrics = taskMetrics,
      executorLogs = binary.getExecutorLogsMap.asScala.toMap,
      schedulerDelay = binary.getSchedulerDelay,
      gettingResultTime = binary.getGettingResultTime)
  }

  private def deserializeTaskMetrics(binary: StoreTypes.TaskMetrics): TaskMetrics = {
    new TaskMetrics(
      binary.getExecutorDeserializeTime,
      binary.getExecutorDeserializeCpuTime,
      binary.getExecutorRunTime,
      binary.getExecutorCpuTime,
      binary.getResultSize,
      binary.getJvmGcTime,
      binary.getResultSerializationTime,
      binary.getMemoryBytesSpilled,
      binary.getDiskBytesSpilled,
      binary.getPeakExecutionMemory,
      binary.getPeakOnHeapExecutionMemory,
      binary.getPeakOffHeapExecutionMemory,
      deserializeInputMetrics(binary.getInputMetrics),
      deserializeOutputMetrics(binary.getOutputMetrics),
      deserializeShuffleReadMetrics(binary.getShuffleReadMetrics),
      deserializeShuffleWriteMetrics(binary.getShuffleWriteMetrics))
  }

  private def deserializeInputMetrics(binary: StoreTypes.InputMetrics): InputMetrics = {
    new InputMetrics(binary.getBytesRead, binary.getRecordsRead)
  }

  private def deserializeOutputMetrics(binary: StoreTypes.OutputMetrics): OutputMetrics = {
    new OutputMetrics(binary.getBytesWritten, binary.getRecordsWritten)
  }

  private def deserializeShuffleReadMetrics(
      binary: StoreTypes.ShuffleReadMetrics): ShuffleReadMetrics = {
    new ShuffleReadMetrics(
      binary.getRemoteBlocksFetched,
      binary.getLocalBlocksFetched,
      binary.getFetchWaitTime,
      binary.getRemoteBytesRead,
      binary.getRemoteBytesReadToDisk,
      binary.getLocalBytesRead,
      binary.getRecordsRead,
      binary.getRemoteReqsDuration,
      deserializeShufflePushReadMetrics(binary.getShufflePushReadMetrics))
  }

  private def deserializeShufflePushReadMetrics(
      binary: StoreTypes.ShufflePushReadMetrics): ShufflePushReadMetrics = {
    new ShufflePushReadMetrics(
      binary.getCorruptMergedBlockChunks,
      binary.getMergedFetchFallbackCount,
      binary.getRemoteMergedBlocksFetched,
      binary.getLocalMergedBlocksFetched,
      binary.getRemoteMergedChunksFetched,
      binary.getLocalMergedChunksFetched,
      binary.getRemoteMergedBytesRead,
      binary.getLocalMergedBytesRead,
      binary.getRemoteMergedReqsDuration
    )
  }

  private def deserializeShuffleWriteMetrics(
      binary: StoreTypes.ShuffleWriteMetrics): ShuffleWriteMetrics = {
    new ShuffleWriteMetrics(
      binary.getBytesWritten,
      binary.getWriteTime,
      binary.getRecordsWritten)
  }
}
