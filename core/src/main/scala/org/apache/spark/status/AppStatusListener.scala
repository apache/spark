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

package org.apache.spark.status

import java.util.Date

import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.status.api.v1
import org.apache.spark.storage._
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.kvstore.KVStore

/**
 * A Spark listener that writes application information to a data store. The types written to the
 * store are defined in the `storeTypes.scala` file and are based on the public REST API.
 */
private class AppStatusListener(kvstore: KVStore) extends SparkListener with Logging {

  private var sparkVersion = SPARK_VERSION
  private var appInfo: v1.ApplicationInfo = null
  private var coresPerTask: Int = 1

  // Keep track of live entities, so that task metrics can be efficiently updated (without
  // causing too many writes to the underlying store, and other expensive operations).
  private val liveStages = new HashMap[(Int, Int), LiveStage]()
  private val liveJobs = new HashMap[Int, LiveJob]()
  private val liveExecutors = new HashMap[String, LiveExecutor]()
  private val liveTasks = new HashMap[Long, LiveTask]()
  private val liveRDDs = new HashMap[Int, LiveRDD]()

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case SparkListenerLogStart(version) => sparkVersion = version
    case _ =>
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    assert(event.appId.isDefined, "Application without IDs are not supported.")

    val attempt = new v1.ApplicationAttemptInfo(
      event.appAttemptId,
      new Date(event.time),
      new Date(-1),
      new Date(event.time),
      -1L,
      event.sparkUser,
      false,
      sparkVersion)

    appInfo = new v1.ApplicationInfo(
      event.appId.get,
      event.appName,
      None,
      None,
      None,
      None,
      Seq(attempt))

    kvstore.write(new ApplicationInfoWrapper(appInfo))
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    val old = appInfo.attempts.head
    val attempt = new v1.ApplicationAttemptInfo(
      old.attemptId,
      old.startTime,
      new Date(event.time),
      new Date(event.time),
      event.time - old.startTime.getTime(),
      old.sparkUser,
      true,
      old.appSparkVersion)

    appInfo = new v1.ApplicationInfo(
      appInfo.id,
      appInfo.name,
      None,
      None,
      None,
      None,
      Seq(attempt))
    kvstore.write(new ApplicationInfoWrapper(appInfo))
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    // This needs to be an update in case an executor re-registers after the driver has
    // marked it as "dead".
    val exec = getOrCreateExecutor(event.executorId)
    exec.host = event.executorInfo.executorHost
    exec.isActive = true
    exec.totalCores = event.executorInfo.totalCores
    exec.maxTasks = event.executorInfo.totalCores / coresPerTask
    exec.executorLogs = event.executorInfo.logUrlMap
    update(exec)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    liveExecutors.remove(event.executorId).foreach { exec =>
      exec.isActive = false
      update(exec)
    }
  }

  override def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit = {
    updateBlackListStatus(event.executorId, true)
  }

  override def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit = {
    updateBlackListStatus(event.executorId, false)
  }

  override def onNodeBlacklisted(event: SparkListenerNodeBlacklisted): Unit = {
    updateNodeBlackList(event.hostId, true)
  }

  override def onNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Unit = {
    updateNodeBlackList(event.hostId, false)
  }

  private def updateBlackListStatus(execId: String, blacklisted: Boolean): Unit = {
    liveExecutors.get(execId).foreach { exec =>
      exec.isBlacklisted = blacklisted
      update(exec)
    }
  }

  private def updateNodeBlackList(host: String, blacklisted: Boolean): Unit = {
    // Implicitly (un)blacklist every executor associated with the node.
    liveExecutors.values.foreach { exec =>
      if (exec.hostname == host) {
        exec.isBlacklisted = blacklisted
        update(exec)
      }
    }
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    // Compute (a potential over-estimate of) the number of tasks that will be run by this job.
    // This may be an over-estimate because the job start event references all of the result
    // stages' transitive stage dependencies, but some of these stages might be skipped if their
    // output is available from earlier runs.
    // See https://github.com/apache/spark/pull/3009 for a more extensive discussion.
    val numTasks = {
      val missingStages = event.stageInfos.filter(_.completionTime.isEmpty)
      missingStages.map(_.numTasks).sum
    }

    val lastStageInfo = event.stageInfos.lastOption
    val lastStageName = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")

    val jobGroup = Option(event.properties)
      .flatMap { p => Option(p.getProperty(SparkContext.SPARK_JOB_GROUP_ID)) }

    val job = new LiveJob(
      event.jobId,
      lastStageName,
      Some(new Date(event.time)),
      event.stageIds,
      jobGroup,
      numTasks)
    liveJobs.put(event.jobId, job)
    update(job)

    event.stageInfos.foreach { stageInfo =>
      // A new job submission may re-use an existing stage, so this code needs to do an update
      // instead of just a write.
      val stage = getOrCreateStage(stageInfo)
      stage.jobs :+= job
      stage.jobIds += event.jobId
      update(stage)
    }
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    liveJobs.remove(event.jobId).foreach { job =>
      job.status = event.jobResult match {
        case JobSucceeded => JobExecutionStatus.SUCCEEDED
        case JobFailed(_) => JobExecutionStatus.FAILED
      }

      job.completionTime = Some(new Date(event.time))
      update(job)
    }
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val stage = getOrCreateStage(event.stageInfo)
    stage.status = v1.StageStatus.ACTIVE
    stage.schedulingPool = Option(event.properties).flatMap { p =>
      Option(p.getProperty("spark.scheduler.pool"))
    }.getOrElse(SparkUI.DEFAULT_POOL_NAME)

    // Look at all active jobs to find the ones that mention this stage.
    stage.jobs = liveJobs.values
      .filter(_.stageIds.contains(event.stageInfo.stageId))
      .toSeq
    stage.jobIds = stage.jobs.map(_.jobId).toSet

    stage.jobs.foreach { job =>
      job.completedStages = job.completedStages - event.stageInfo.stageId
      job.activeStages += 1
      update(job)
    }

    event.stageInfo.rddInfos.foreach { info =>
      if (info.storageLevel.isValid) {
        update(liveRDDs.getOrElseUpdate(info.id, new LiveRDD(info)))
      }
    }

    update(stage)
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    val task = new LiveTask(event.taskInfo, event.stageId, event.stageAttemptId)
    liveTasks.put(event.taskInfo.taskId, task)
    update(task)

    liveStages.get((event.stageId, event.stageAttemptId)).foreach { stage =>
      stage.activeTasks += 1
      stage.firstLaunchTime = math.min(stage.firstLaunchTime, event.taskInfo.launchTime)
      update(stage)

      stage.jobs.foreach { job =>
        job.activeTasks += 1
        update(job)
      }
    }

    liveExecutors.get(event.taskInfo.executorId).foreach { exec =>
      exec.activeTasks += 1
      exec.totalTasks += 1
      update(exec)
    }
  }

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = {
    // Call update on the task so that the "getting result" time is written to the store; the
    // value is part of the mutable TaskInfo state that the live entity already references.
    liveTasks.get(event.taskInfo.taskId).foreach { task =>
      update(task)
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    // TODO: can this really happen?
    if (event.taskInfo == null) {
      return
    }

    val metricsDelta = liveTasks.remove(event.taskInfo.taskId).map { task =>
      val errorMessage = event.reason match {
        case Success =>
          None
        case k: TaskKilled =>
          Some(k.reason)
        case e: ExceptionFailure => // Handle ExceptionFailure because we might have accumUpdates
          Some(e.toErrorString)
        case e: TaskFailedReason => // All other failure cases
          Some(e.toErrorString)
        case other =>
          logInfo(s"Unhandled task end reason: $other")
          None
      }
      task.errorMessage = errorMessage
      val delta = task.updateMetrics(event.taskMetrics)
      update(task)
      delta
    }.orNull

    val (completedDelta, failedDelta) = event.reason match {
      case Success =>
        (1, 0)
      case _ =>
        (0, 1)
    }

    liveStages.get((event.stageId, event.stageAttemptId)).foreach { stage =>
      if (metricsDelta != null) {
        stage.metrics.update(metricsDelta)
      }
      stage.activeTasks -= 1
      stage.completedTasks += completedDelta
      stage.failedTasks += failedDelta
      update(stage)

      stage.jobs.foreach { job =>
        job.activeTasks -= 1
        job.completedTasks += completedDelta
        job.failedTasks += failedDelta
        update(job)
      }

      val esummary = stage.executorSummary(event.taskInfo.executorId)
      esummary.taskTime += event.taskInfo.duration
      esummary.succeededTasks += completedDelta
      esummary.failedTasks += failedDelta
      if (metricsDelta != null) {
        esummary.metrics.update(metricsDelta)
      }
      update(esummary)
    }

    liveExecutors.get(event.taskInfo.executorId).foreach { exec =>
      if (event.taskMetrics != null) {
        val readMetrics = event.taskMetrics.shuffleReadMetrics
        exec.totalGcTime += event.taskMetrics.jvmGCTime
        exec.totalInputBytes += event.taskMetrics.inputMetrics.bytesRead
        exec.totalShuffleRead += readMetrics.localBytesRead + readMetrics.remoteBytesRead
        exec.totalShuffleWrite += event.taskMetrics.shuffleWriteMetrics.bytesWritten
      }

      exec.activeTasks -= 1
      exec.completedTasks += completedDelta
      exec.failedTasks += failedDelta
      exec.totalDuration += event.taskInfo.duration
      update(exec)
    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    liveStages.remove((event.stageInfo.stageId, event.stageInfo.attemptId)).foreach { stage =>
      stage.info = event.stageInfo

      // Because of SPARK-20205, old event logs may contain valid stages without a submission time
      // in their start event. In those cases, we can only detect whether a stage was skipped by
      // waiting until the completion event, at which point the field would have been set.
      stage.status = event.stageInfo.failureReason match {
        case Some(_) => v1.StageStatus.FAILED
        case _ if event.stageInfo.submissionTime.isDefined => v1.StageStatus.COMPLETE
        case _ => v1.StageStatus.SKIPPED
      }
      update(stage)

      stage.jobs.foreach { job =>
        stage.status match {
          case v1.StageStatus.COMPLETE =>
            job.completedStages += event.stageInfo.stageId
          case v1.StageStatus.SKIPPED =>
            job.skippedStages += event.stageInfo.stageId
            job.skippedTasks += event.stageInfo.numTasks
          case _ =>
            job.failedStages += 1
        }
        job.activeStages -= 1
        update(job)
      }

      stage.executorSummaries.values.foreach(update)
      update(stage)
    }
  }

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = {
    // This needs to set fields that are already set by onExecutorAdded because the driver is
    // considered an "executor" in the UI, but does not have a SparkListenerExecutorAdded event.
    val exec = getOrCreateExecutor(event.blockManagerId.executorId)
    exec.hostPort = event.blockManagerId.hostPort
    event.maxOnHeapMem.foreach { _ =>
      exec.totalOnHeap = event.maxOnHeapMem.get
      exec.totalOffHeap = event.maxOffHeapMem.get
    }
    exec.isActive = true
    exec.maxMemory = event.maxMem
    update(exec)
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = {
    // Nothing to do here. Covered by onExecutorRemoved.
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    liveRDDs.remove(event.rddId)
    kvstore.delete(classOf[RDDStorageInfoWrapper], event.rddId)
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    event.accumUpdates.foreach { case (taskId, sid, sAttempt, accumUpdates) =>
      liveTasks.get(taskId).foreach { task =>
        val metrics = TaskMetrics.fromAccumulatorInfos(accumUpdates)
        val delta = task.updateMetrics(metrics)
        update(task)

        liveStages.get((sid, sAttempt)).foreach { stage =>
          stage.metrics.update(delta)
          update(stage)

          val esummary = stage.executorSummary(event.execId)
          esummary.metrics.update(delta)
          update(esummary)
        }
      }
    }
  }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    event.blockUpdatedInfo.blockId match {
      case block: RDDBlockId => updateRDDBlock(event, block)
      case _ => // TODO: API only covers RDD storage.
    }
  }

  private def updateRDDBlock(event: SparkListenerBlockUpdated, block: RDDBlockId): Unit = {
    val executorId = event.blockUpdatedInfo.blockManagerId.executorId

    // Whether values are being added to or removed from the existing accounting.
    val storageLevel = event.blockUpdatedInfo.storageLevel
    val diskDelta = event.blockUpdatedInfo.diskSize * (if (storageLevel.useDisk) 1 else -1)
    val memoryDelta = event.blockUpdatedInfo.memSize * (if (storageLevel.useMemory) 1 else -1)

    // Function to apply a delta to a value, but ensure that it doesn't go negative.
    def newValue(old: Long, delta: Long): Long = math.max(0, old + delta)

    val updatedStorageLevel = if (storageLevel.isValid) {
      Some(storageLevel.description)
    } else {
      None
    }

    // We need information about the executor to update some memory accounting values in the
    // RDD info, so read that beforehand.
    val maybeExec = liveExecutors.get(executorId)
    var rddBlocksDelta = 0

    // Update the block entry in the RDD info, keeping track of the deltas above so that we
    // can update the executor information too.
    liveRDDs.get(block.rddId).foreach { rdd =>
      val partition = rdd.partition(block.name)

      val executors = if (updatedStorageLevel.isDefined) {
        if (!partition.executors.contains(executorId)) {
          rddBlocksDelta = 1
        }
        partition.executors + executorId
      } else {
        rddBlocksDelta = -1
        partition.executors - executorId
      }

      // Only update the partition if it's still stored in some executor, otherwise get rid of it.
      if (executors.nonEmpty) {
        if (updatedStorageLevel.isDefined) {
          partition.storageLevel = updatedStorageLevel.get
        }
        partition.memoryUsed = newValue(partition.memoryUsed, memoryDelta)
        partition.diskUsed = newValue(partition.diskUsed, diskDelta)
        partition.executors = executors
      } else {
        rdd.removePartition(block.name)
      }

      maybeExec.foreach { exec =>
        if (exec.rddBlocks + rddBlocksDelta > 0) {
          val dist = rdd.distribution(exec)
          dist.memoryRemaining = newValue(dist.memoryRemaining, -memoryDelta)
          dist.memoryUsed = newValue(dist.memoryUsed, memoryDelta)
          dist.diskUsed = newValue(dist.diskUsed, diskDelta)

          if (exec.hasMemoryInfo) {
            if (storageLevel.useOffHeap) {
              dist.offHeapUsed = newValue(dist.offHeapUsed, memoryDelta)
              dist.offHeapRemaining = newValue(dist.offHeapRemaining, -memoryDelta)
            } else {
              dist.onHeapUsed = newValue(dist.onHeapUsed, memoryDelta)
              dist.onHeapRemaining = newValue(dist.onHeapRemaining, -memoryDelta)
            }
          }
        } else {
          rdd.removeDistribution(exec)
        }
      }

      if (updatedStorageLevel.isDefined) {
        rdd.storageLevel = updatedStorageLevel.get
      }
      rdd.memoryUsed = newValue(rdd.memoryUsed, memoryDelta)
      rdd.diskUsed = newValue(rdd.diskUsed, diskDelta)
      update(rdd)
    }

    maybeExec.foreach { exec =>
      if (exec.hasMemoryInfo) {
        if (storageLevel.useOffHeap) {
          exec.usedOffHeap = newValue(exec.usedOffHeap, memoryDelta)
        } else {
          exec.usedOnHeap = newValue(exec.usedOnHeap, memoryDelta)
        }
      }
      exec.memoryUsed = newValue(exec.memoryUsed, memoryDelta)
      exec.diskUsed = newValue(exec.diskUsed, diskDelta)
      exec.rddBlocks += rddBlocksDelta
      if (exec.hasMemoryInfo || rddBlocksDelta != 0) {
        update(exec)
      }
    }
  }

  private def getOrCreateExecutor(executorId: String): LiveExecutor = {
    liveExecutors.getOrElseUpdate(executorId, new LiveExecutor(executorId))
  }

  private def getOrCreateStage(info: StageInfo): LiveStage = {
    val stage = liveStages.getOrElseUpdate((info.stageId, info.attemptId), new LiveStage())
    stage.info = info
    stage
  }

  private def update(entity: LiveEntity): Unit = {
    entity.write(kvstore)
  }

}
