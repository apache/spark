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
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.status.api.v1
import org.apache.spark.storage._
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.scope._

/**
 * A Spark listener that writes application information to a data store. The types written to the
 * store are defined in the `storeTypes.scala` file and are based on the public REST API.
 *
 * @param lastUpdateTime When replaying logs, the log's last update time, so that the duration of
 *                       unfinished tasks can be more accurately calculated (see SPARK-21922).
 */
private[spark] class AppStatusListener(
    kvstore: ElementTrackingStore,
    conf: SparkConf,
    live: Boolean,
    lastUpdateTime: Option[Long] = None) extends SparkListener with Logging {

  import config._

  private var sparkVersion = SPARK_VERSION
  private var appInfo: v1.ApplicationInfo = null
  private var appSummary = new AppSummary(0, 0)
  private var coresPerTask: Int = 1

  // How often to update live entities. -1 means "never update" when replaying applications,
  // meaning only the last write will happen. For live applications, this avoids a few
  // operations that we can live without when rapidly processing incoming task events.
  private val liveUpdatePeriodNs = if (live) conf.get(LIVE_ENTITY_UPDATE_PERIOD) else -1L

  private val maxTasksPerStage = conf.get(MAX_RETAINED_TASKS_PER_STAGE)
  private val maxGraphRootNodes = conf.get(MAX_RETAINED_ROOT_NODES)

  // Keep track of live entities, so that task metrics can be efficiently updated (without
  // causing too many writes to the underlying store, and other expensive operations).
  private val liveStages = new ConcurrentHashMap[(Int, Int), LiveStage]()
  private val liveJobs = new HashMap[Int, LiveJob]()
  private val liveExecutors = new HashMap[String, LiveExecutor]()
  private val liveTasks = new HashMap[Long, LiveTask]()
  private val liveRDDs = new HashMap[Int, LiveRDD]()
  private val pools = new HashMap[String, SchedulerPool]()
  // Keep the active executor count as a separate variable to avoid having to do synchronization
  // around liveExecutors.
  @volatile private var activeExecutorCount = 0

  kvstore.addTrigger(classOf[ExecutorSummaryWrapper], conf.get(MAX_RETAINED_DEAD_EXECUTORS))
    { count => cleanupExecutors(count) }

  kvstore.addTrigger(classOf[JobDataWrapper], conf.get(MAX_RETAINED_JOBS)) { count =>
    cleanupJobs(count)
  }

  kvstore.addTrigger(classOf[StageDataWrapper], conf.get(MAX_RETAINED_STAGES)) { count =>
    cleanupStages(count)
  }

  kvstore.onFlush {
    if (!live) {
      flush()
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case SparkListenerLogStart(version) => sparkVersion = version
    case _ =>
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    assert(event.appId.isDefined, "Application without IDs are not supported.")

    val attempt = v1.ApplicationAttemptInfo(
      event.appAttemptId,
      new Date(event.time),
      new Date(-1),
      new Date(event.time),
      -1L,
      event.sparkUser,
      false,
      sparkVersion)

    appInfo = v1.ApplicationInfo(
      event.appId.get,
      event.appName,
      None,
      None,
      None,
      None,
      Seq(attempt))

    kvstore.write(new ApplicationInfoWrapper(appInfo))
    kvstore.write(appSummary)

    // Update the driver block manager with logs from this event. The SparkContext initialization
    // code registers the driver before this event is sent.
    event.driverLogs.foreach { logs =>
      val driver = liveExecutors.get(SparkContext.DRIVER_IDENTIFIER)
        .orElse(liveExecutors.get(SparkContext.LEGACY_DRIVER_IDENTIFIER))
      driver.foreach { d =>
        d.executorLogs = logs.toMap
        update(d, System.nanoTime())
      }
    }
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    val details = event.environmentDetails

    val jvmInfo = Map(details("JVM Information"): _*)
    val runtime = new v1.RuntimeInfo(
      jvmInfo.get("Java Version").orNull,
      jvmInfo.get("Java Home").orNull,
      jvmInfo.get("Scala Version").orNull)

    val envInfo = new v1.ApplicationEnvironmentInfo(
      runtime,
      details.getOrElse("Spark Properties", Nil),
      details.getOrElse("System Properties", Nil),
      details.getOrElse("Classpath Entries", Nil))

    coresPerTask = envInfo.sparkProperties.toMap.get("spark.task.cpus").map(_.toInt)
      .getOrElse(coresPerTask)

    kvstore.write(new ApplicationEnvironmentInfoWrapper(envInfo))
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    val old = appInfo.attempts.head
    val attempt = v1.ApplicationAttemptInfo(
      old.attemptId,
      old.startTime,
      new Date(event.time),
      new Date(event.time),
      event.time - old.startTime.getTime(),
      old.sparkUser,
      true,
      old.appSparkVersion)

    appInfo = v1.ApplicationInfo(
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
    val exec = getOrCreateExecutor(event.executorId, event.time)
    exec.host = event.executorInfo.executorHost
    exec.isActive = true
    exec.totalCores = event.executorInfo.totalCores
    exec.maxTasks = event.executorInfo.totalCores / coresPerTask
    exec.executorLogs = event.executorInfo.logUrlMap
    liveUpdate(exec, System.nanoTime())
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    liveExecutors.remove(event.executorId).foreach { exec =>
      val now = System.nanoTime()
      activeExecutorCount = math.max(0, activeExecutorCount - 1)
      exec.isActive = false
      exec.removeTime = new Date(event.time)
      exec.removeReason = event.reason
      update(exec, now, last = true)

      // Remove all RDD distributions that reference the removed executor, in case there wasn't
      // a corresponding event.
      liveRDDs.values.foreach { rdd =>
        if (rdd.removeDistribution(exec)) {
          update(rdd, now)
        }
      }
    }
  }

  override def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit = {
    updateBlackListStatus(event.executorId, true)
  }

  override def onExecutorBlacklistedForStage(
      event: SparkListenerExecutorBlacklistedForStage): Unit = {
    val now = System.nanoTime()

    Option(liveStages.get((event.stageId, event.stageAttemptId))).foreach { stage =>
      setStageBlackListStatus(stage, now, event.executorId)
    }
    liveExecutors.get(event.executorId).foreach { exec =>
      addBlackListedStageTo(exec, event.stageId, now)
    }
  }

  override def onNodeBlacklistedForStage(event: SparkListenerNodeBlacklistedForStage): Unit = {
    val now = System.nanoTime()

    // Implicitly blacklist every available executor for the stage associated with this node
    Option(liveStages.get((event.stageId, event.stageAttemptId))).foreach { stage =>
      val executorIds = liveExecutors.values.filter(_.host == event.hostId).map(_.executorId).toSeq
      setStageBlackListStatus(stage, now, executorIds: _*)
    }
    liveExecutors.values.filter(_.hostname == event.hostId).foreach { exec =>
      addBlackListedStageTo(exec, event.stageId, now)
    }
  }

  private def addBlackListedStageTo(exec: LiveExecutor, stageId: Int, now: Long): Unit = {
    exec.blacklistedInStages += stageId
    liveUpdate(exec, now)
  }

  private def setStageBlackListStatus(stage: LiveStage, now: Long, executorIds: String*): Unit = {
    executorIds.foreach { executorId =>
      val executorStageSummary = stage.executorSummary(executorId)
      executorStageSummary.isBlacklisted = true
      maybeUpdate(executorStageSummary, now)
    }
    stage.blackListedExecutors ++= executorIds
    maybeUpdate(stage, now)
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
      liveUpdate(exec, System.nanoTime())
    }
  }

  private def updateNodeBlackList(host: String, blacklisted: Boolean): Unit = {
    val now = System.nanoTime()

    // Implicitly (un)blacklist every executor associated with the node.
    liveExecutors.values.foreach { exec =>
      if (exec.hostname == host) {
        exec.isBlacklisted = blacklisted
        liveUpdate(exec, now)
      }
    }
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val now = System.nanoTime()

    // Compute (a potential over-estimate of) the number of tasks that will be run by this job.
    // This may be an over-estimate because the job start event references all of the result
    // stages' transitive stage dependencies, but some of these stages might be skipped if their
    // output is available from earlier runs.
    // See https://github.com/apache/spark/pull/3009 for a more extensive discussion.
    val numTasks = {
      val missingStages = event.stageInfos.filter(_.completionTime.isEmpty)
      missingStages.map(_.numTasks).sum
    }

    val lastStageInfo = event.stageInfos.sortBy(_.stageId).lastOption
    val lastStageName = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
    val jobGroup = Option(event.properties)
      .flatMap { p => Option(p.getProperty(SparkContext.SPARK_JOB_GROUP_ID)) }

    val job = new LiveJob(
      event.jobId,
      lastStageName,
      if (event.time > 0) Some(new Date(event.time)) else None,
      event.stageIds,
      jobGroup,
      numTasks)
    liveJobs.put(event.jobId, job)
    liveUpdate(job, now)

    event.stageInfos.foreach { stageInfo =>
      // A new job submission may re-use an existing stage, so this code needs to do an update
      // instead of just a write.
      val stage = getOrCreateStage(stageInfo)
      stage.jobs :+= job
      stage.jobIds += event.jobId
      liveUpdate(stage, now)
    }

    // Create the graph data for all the job's stages.
    event.stageInfos.foreach { stage =>
      val graph = RDDOperationGraph.makeOperationGraph(stage, maxGraphRootNodes)
      val uigraph = new RDDOperationGraphWrapper(
        stage.stageId,
        graph.edges,
        graph.outgoingEdges,
        graph.incomingEdges,
        newRDDOperationCluster(graph.rootCluster))
      kvstore.write(uigraph)
    }
  }

  private def newRDDOperationCluster(cluster: RDDOperationCluster): RDDOperationClusterWrapper = {
    new RDDOperationClusterWrapper(
      cluster.id,
      cluster.name,
      cluster.childNodes,
      cluster.childClusters.map(newRDDOperationCluster))
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    liveJobs.remove(event.jobId).foreach { job =>
      val now = System.nanoTime()

      // Check if there are any pending stages that match this job; mark those as skipped.
      val it = liveStages.entrySet.iterator()
      while (it.hasNext()) {
        val e = it.next()
        if (job.stageIds.contains(e.getKey()._1)) {
          val stage = e.getValue()
          if (v1.StageStatus.PENDING.equals(stage.status)) {
            stage.status = v1.StageStatus.SKIPPED
            job.skippedStages += stage.info.stageId
            job.skippedTasks += stage.info.numTasks
            job.activeStages -= 1

            pools.get(stage.schedulingPool).foreach { pool =>
              pool.stageIds = pool.stageIds - stage.info.stageId
              update(pool, now)
            }

            it.remove()
            update(stage, now, last = true)
          }
        }
      }

      job.status = event.jobResult match {
        case JobSucceeded => JobExecutionStatus.SUCCEEDED
        case JobFailed(_) => JobExecutionStatus.FAILED
      }

      job.completionTime = if (event.time > 0) Some(new Date(event.time)) else None
      update(job, now, last = true)
      if (job.status == JobExecutionStatus.SUCCEEDED) {
        appSummary = new AppSummary(appSummary.numCompletedJobs + 1, appSummary.numCompletedStages)
        kvstore.write(appSummary)
      }
    }
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val now = System.nanoTime()
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

    stage.description = Option(event.properties).flatMap { p =>
      Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
    }

    stage.jobs.foreach { job =>
      job.completedStages = job.completedStages - event.stageInfo.stageId
      job.activeStages += 1
      liveUpdate(job, now)
    }

    val pool = pools.getOrElseUpdate(stage.schedulingPool, new SchedulerPool(stage.schedulingPool))
    pool.stageIds = pool.stageIds + event.stageInfo.stageId
    update(pool, now)

    event.stageInfo.rddInfos.foreach { info =>
      if (info.storageLevel.isValid) {
        liveUpdate(liveRDDs.getOrElseUpdate(info.id, new LiveRDD(info)), now)
      }
    }

    liveUpdate(stage, now)
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    val now = System.nanoTime()
    val task = new LiveTask(event.taskInfo, event.stageId, event.stageAttemptId, lastUpdateTime)
    liveTasks.put(event.taskInfo.taskId, task)
    liveUpdate(task, now)

    Option(liveStages.get((event.stageId, event.stageAttemptId))).foreach { stage =>
      stage.activeTasks += 1
      stage.firstLaunchTime = math.min(stage.firstLaunchTime, event.taskInfo.launchTime)

      val locality = event.taskInfo.taskLocality.toString()
      val count = stage.localitySummary.getOrElse(locality, 0L) + 1L
      stage.localitySummary = stage.localitySummary ++ Map(locality -> count)
      maybeUpdate(stage, now)

      stage.jobs.foreach { job =>
        job.activeTasks += 1
        maybeUpdate(job, now)
      }

      if (stage.savedTasks.incrementAndGet() > maxTasksPerStage && !stage.cleaning) {
        stage.cleaning = true
        kvstore.doAsync {
          cleanupTasks(stage)
        }
      }
    }

    liveExecutors.get(event.taskInfo.executorId).foreach { exec =>
      exec.activeTasks += 1
      exec.totalTasks += 1
      maybeUpdate(exec, now)
    }
  }

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = {
    // Call update on the task so that the "getting result" time is written to the store; the
    // value is part of the mutable TaskInfo state that the live entity already references.
    liveTasks.get(event.taskInfo.taskId).foreach { task =>
      maybeUpdate(task, System.nanoTime())
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    // TODO: can this really happen?
    if (event.taskInfo == null) {
      return
    }

    val now = System.nanoTime()

    val metricsDelta = liveTasks.remove(event.taskInfo.taskId).map { task =>
      task.info = event.taskInfo

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
      update(task, now, last = true)
      delta
    }.orNull

    val (completedDelta, failedDelta, killedDelta) = event.reason match {
      case Success =>
        (1, 0, 0)
      case _: TaskKilled =>
        (0, 0, 1)
      case _: TaskCommitDenied =>
        (0, 0, 1)
      case _ =>
        (0, 1, 0)
    }

    Option(liveStages.get((event.stageId, event.stageAttemptId))).foreach { stage =>
      if (metricsDelta != null) {
        stage.metrics = LiveEntityHelpers.addMetrics(stage.metrics, metricsDelta)
      }
      stage.activeTasks -= 1
      stage.completedTasks += completedDelta
      if (completedDelta > 0) {
        stage.completedIndices.add(event.taskInfo.index)
      }
      stage.failedTasks += failedDelta
      stage.killedTasks += killedDelta
      if (killedDelta > 0) {
        stage.killedSummary = killedTasksSummary(event.reason, stage.killedSummary)
      }
      // [SPARK-24415] Wait for all tasks to finish before removing stage from live list
      val removeStage =
        stage.activeTasks == 0 &&
          (v1.StageStatus.COMPLETE.equals(stage.status) ||
            v1.StageStatus.FAILED.equals(stage.status))
      if (removeStage) {
        update(stage, now, last = true)
      } else {
        maybeUpdate(stage, now)
      }

      // Store both stage ID and task index in a single long variable for tracking at job level.
      val taskIndex = (event.stageId.toLong << Integer.SIZE) | event.taskInfo.index
      stage.jobs.foreach { job =>
        job.activeTasks -= 1
        job.completedTasks += completedDelta
        if (completedDelta > 0) {
          job.completedIndices.add(taskIndex)
        }
        job.failedTasks += failedDelta
        job.killedTasks += killedDelta
        if (killedDelta > 0) {
          job.killedSummary = killedTasksSummary(event.reason, job.killedSummary)
        }
        conditionalLiveUpdate(job, now, removeStage)
      }

      val esummary = stage.executorSummary(event.taskInfo.executorId)
      esummary.taskTime += event.taskInfo.duration
      esummary.succeededTasks += completedDelta
      esummary.failedTasks += failedDelta
      esummary.killedTasks += killedDelta
      if (metricsDelta != null) {
        esummary.metrics = LiveEntityHelpers.addMetrics(esummary.metrics, metricsDelta)
      }
      conditionalLiveUpdate(esummary, now, removeStage)

      if (!stage.cleaning && stage.savedTasks.get() > maxTasksPerStage) {
        stage.cleaning = true
        kvstore.doAsync {
          cleanupTasks(stage)
        }
      }
      if (removeStage) {
        liveStages.remove((event.stageId, event.stageAttemptId))
      }
    }

    liveExecutors.get(event.taskInfo.executorId).foreach { exec =>
      exec.activeTasks -= 1
      exec.completedTasks += completedDelta
      exec.failedTasks += failedDelta
      exec.totalDuration += event.taskInfo.duration

      // Note: For resubmitted tasks, we continue to use the metrics that belong to the
      // first attempt of this task. This may not be 100% accurate because the first attempt
      // could have failed half-way through. The correct fix would be to keep track of the
      // metrics added by each attempt, but this is much more complicated.
      if (event.reason != Resubmitted) {
        if (event.taskMetrics != null) {
          val readMetrics = event.taskMetrics.shuffleReadMetrics
          exec.totalGcTime += event.taskMetrics.jvmGCTime
          exec.totalInputBytes += event.taskMetrics.inputMetrics.bytesRead
          exec.totalShuffleRead += readMetrics.localBytesRead + readMetrics.remoteBytesRead
          exec.totalShuffleWrite += event.taskMetrics.shuffleWriteMetrics.bytesWritten
        }
      }

      // Force an update on live applications when the number of active tasks reaches 0. This is
      // checked in some tests (e.g. SQLTestUtilsBase) so it needs to be reliably up to date.
      conditionalLiveUpdate(exec, now, exec.activeTasks == 0)
    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val maybeStage =
      Option(liveStages.get((event.stageInfo.stageId, event.stageInfo.attemptNumber)))
    maybeStage.foreach { stage =>
      val now = System.nanoTime()
      stage.info = event.stageInfo

      // Because of SPARK-20205, old event logs may contain valid stages without a submission time
      // in their start event. In those cases, we can only detect whether a stage was skipped by
      // waiting until the completion event, at which point the field would have been set.
      stage.status = event.stageInfo.failureReason match {
        case Some(_) => v1.StageStatus.FAILED
        case _ if event.stageInfo.submissionTime.isDefined => v1.StageStatus.COMPLETE
        case _ => v1.StageStatus.SKIPPED
      }

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
        liveUpdate(job, now)
      }

      pools.get(stage.schedulingPool).foreach { pool =>
        pool.stageIds = pool.stageIds - event.stageInfo.stageId
        update(pool, now)
      }

      stage.executorSummaries.values.foreach(update(_, now))

      val executorIdsForStage = stage.blackListedExecutors
      executorIdsForStage.foreach { executorId =>
        liveExecutors.get(executorId).foreach { exec =>
          removeBlackListedStageFrom(exec, event.stageInfo.stageId, now)
        }
      }

      // Remove stage only if there are no active tasks remaining
      val removeStage = stage.activeTasks == 0
      update(stage, now, last = removeStage)
      if (removeStage) {
        liveStages.remove((event.stageInfo.stageId, event.stageInfo.attemptNumber))
      }
      if (stage.status == v1.StageStatus.COMPLETE) {
        appSummary = new AppSummary(appSummary.numCompletedJobs, appSummary.numCompletedStages + 1)
        kvstore.write(appSummary)
      }
    }
  }

  private def removeBlackListedStageFrom(exec: LiveExecutor, stageId: Int, now: Long) = {
    exec.blacklistedInStages -= stageId
    liveUpdate(exec, now)
  }

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = {
    // This needs to set fields that are already set by onExecutorAdded because the driver is
    // considered an "executor" in the UI, but does not have a SparkListenerExecutorAdded event.
    val exec = getOrCreateExecutor(event.blockManagerId.executorId, event.time)
    exec.hostPort = event.blockManagerId.hostPort
    event.maxOnHeapMem.foreach { _ =>
      exec.totalOnHeap = event.maxOnHeapMem.get
      exec.totalOffHeap = event.maxOffHeapMem.get
    }
    exec.isActive = true
    exec.maxMemory = event.maxMem
    liveUpdate(exec, System.nanoTime())
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = {
    // Nothing to do here. Covered by onExecutorRemoved.
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    liveRDDs.remove(event.rddId).foreach { liveRDD =>
      val storageLevel = liveRDD.info.storageLevel

      // Use RDD partition info to update executor block info.
      liveRDD.getPartitions().foreach { case (_, part) =>
        part.executors.foreach { executorId =>
          liveExecutors.get(executorId).foreach { exec =>
            exec.rddBlocks = exec.rddBlocks - 1
          }
        }
      }

      val now = System.nanoTime()

      // Use RDD distribution to update executor memory and disk usage info.
      liveRDD.getDistributions().foreach { case (executorId, rddDist) =>
        liveExecutors.get(executorId).foreach { exec =>
          if (exec.hasMemoryInfo) {
            if (storageLevel.useOffHeap) {
              exec.usedOffHeap = addDeltaToValue(exec.usedOffHeap, -rddDist.offHeapUsed)
            } else {
              exec.usedOnHeap = addDeltaToValue(exec.usedOnHeap, -rddDist.onHeapUsed)
            }
          }
          exec.memoryUsed = addDeltaToValue(exec.memoryUsed, -rddDist.memoryUsed)
          exec.diskUsed = addDeltaToValue(exec.diskUsed, -rddDist.diskUsed)
          maybeUpdate(exec, now)
        }
      }
    }

    kvstore.delete(classOf[RDDStorageInfoWrapper], event.rddId)
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    val now = System.nanoTime()

    event.accumUpdates.foreach { case (taskId, sid, sAttempt, accumUpdates) =>
      liveTasks.get(taskId).foreach { task =>
        val metrics = TaskMetrics.fromAccumulatorInfos(accumUpdates)
        val delta = task.updateMetrics(metrics)
        maybeUpdate(task, now)

        Option(liveStages.get((sid, sAttempt))).foreach { stage =>
          stage.metrics = LiveEntityHelpers.addMetrics(stage.metrics, delta)
          maybeUpdate(stage, now)

          val esummary = stage.executorSummary(event.execId)
          esummary.metrics = LiveEntityHelpers.addMetrics(esummary.metrics, delta)
          maybeUpdate(esummary, now)
        }
      }
    }
  }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    event.blockUpdatedInfo.blockId match {
      case block: RDDBlockId => updateRDDBlock(event, block)
      case stream: StreamBlockId => updateStreamBlock(event, stream)
      case _ =>
    }
  }

  /** Flush all live entities' data to the underlying store. */
  private def flush(): Unit = {
    val now = System.nanoTime()
    liveStages.values.asScala.foreach { stage =>
      update(stage, now)
      stage.executorSummaries.values.foreach(update(_, now))
    }
    liveJobs.values.foreach(update(_, now))
    liveExecutors.values.foreach(update(_, now))
    liveTasks.values.foreach(update(_, now))
    liveRDDs.values.foreach(update(_, now))
    pools.values.foreach(update(_, now))
  }

  /**
   * Shortcut to get active stages quickly in a live application, for use by the console
   * progress bar.
   */
  def activeStages(): Seq[v1.StageData] = {
    liveStages.values.asScala
      .filter(_.info.submissionTime.isDefined)
      .map(_.toApi())
      .toList
      .sortBy(_.stageId)
  }

  /**
   * Apply a delta to a value, but ensure that it doesn't go negative.
   */
  private def addDeltaToValue(old: Long, delta: Long): Long = math.max(0, old + delta)

  private def updateRDDBlock(event: SparkListenerBlockUpdated, block: RDDBlockId): Unit = {
    val now = System.nanoTime()
    val executorId = event.blockUpdatedInfo.blockManagerId.executorId

    // Whether values are being added to or removed from the existing accounting.
    val storageLevel = event.blockUpdatedInfo.storageLevel
    val diskDelta = event.blockUpdatedInfo.diskSize * (if (storageLevel.useDisk) 1 else -1)
    val memoryDelta = event.blockUpdatedInfo.memSize * (if (storageLevel.useMemory) 1 else -1)

    val updatedStorageLevel = if (storageLevel.isValid) {
      Some(storageLevel.description)
    } else {
      None
    }

    // We need information about the executor to update some memory accounting values in the
    // RDD info, so read that beforehand.
    val maybeExec = liveExecutors.get(executorId)
    var rddBlocksDelta = 0

    // Update the executor stats first, since they are used to calculate the free memory
    // on tracked RDD distributions.
    maybeExec.foreach { exec =>
      if (exec.hasMemoryInfo) {
        if (storageLevel.useOffHeap) {
          exec.usedOffHeap = addDeltaToValue(exec.usedOffHeap, memoryDelta)
        } else {
          exec.usedOnHeap = addDeltaToValue(exec.usedOnHeap, memoryDelta)
        }
      }
      exec.memoryUsed = addDeltaToValue(exec.memoryUsed, memoryDelta)
      exec.diskUsed = addDeltaToValue(exec.diskUsed, diskDelta)
    }

    // Update the block entry in the RDD info, keeping track of the deltas above so that we
    // can update the executor information too.
    liveRDDs.get(block.rddId).foreach { rdd =>
      if (updatedStorageLevel.isDefined) {
        rdd.setStorageLevel(updatedStorageLevel.get)
      }

      val partition = rdd.partition(block.name)

      val executors = if (updatedStorageLevel.isDefined) {
        val current = partition.executors
        if (current.contains(executorId)) {
          current
        } else {
          rddBlocksDelta = 1
          current :+ executorId
        }
      } else {
        rddBlocksDelta = -1
        partition.executors.filter(_ != executorId)
      }

      // Only update the partition if it's still stored in some executor, otherwise get rid of it.
      if (executors.nonEmpty) {
        partition.update(executors, rdd.storageLevel,
          addDeltaToValue(partition.memoryUsed, memoryDelta),
          addDeltaToValue(partition.diskUsed, diskDelta))
      } else {
        rdd.removePartition(block.name)
      }

      maybeExec.foreach { exec =>
        if (exec.rddBlocks + rddBlocksDelta > 0) {
          val dist = rdd.distribution(exec)
          dist.memoryUsed = addDeltaToValue(dist.memoryUsed, memoryDelta)
          dist.diskUsed = addDeltaToValue(dist.diskUsed, diskDelta)

          if (exec.hasMemoryInfo) {
            if (storageLevel.useOffHeap) {
              dist.offHeapUsed = addDeltaToValue(dist.offHeapUsed, memoryDelta)
            } else {
              dist.onHeapUsed = addDeltaToValue(dist.onHeapUsed, memoryDelta)
            }
          }
          dist.lastUpdate = null
        } else {
          rdd.removeDistribution(exec)
        }

        // Trigger an update on other RDDs so that the free memory information is updated.
        liveRDDs.values.foreach { otherRdd =>
          if (otherRdd.info.id != block.rddId) {
            otherRdd.distributionOpt(exec).foreach { dist =>
              dist.lastUpdate = null
              update(otherRdd, now)
            }
          }
        }
      }

      rdd.memoryUsed = addDeltaToValue(rdd.memoryUsed, memoryDelta)
      rdd.diskUsed = addDeltaToValue(rdd.diskUsed, diskDelta)
      update(rdd, now)
    }

    // Finish updating the executor now that we know the delta in the number of blocks.
    maybeExec.foreach { exec =>
      exec.rddBlocks += rddBlocksDelta
      maybeUpdate(exec, now)
    }
  }

  private def getOrCreateExecutor(executorId: String, addTime: Long): LiveExecutor = {
    liveExecutors.getOrElseUpdate(executorId, {
      activeExecutorCount += 1
      new LiveExecutor(executorId, addTime)
    })
  }

  private def updateStreamBlock(event: SparkListenerBlockUpdated, stream: StreamBlockId): Unit = {
    val storageLevel = event.blockUpdatedInfo.storageLevel
    if (storageLevel.isValid) {
      val data = new StreamBlockData(
        stream.name,
        event.blockUpdatedInfo.blockManagerId.executorId,
        event.blockUpdatedInfo.blockManagerId.hostPort,
        storageLevel.description,
        storageLevel.useMemory,
        storageLevel.useDisk,
        storageLevel.deserialized,
        event.blockUpdatedInfo.memSize,
        event.blockUpdatedInfo.diskSize)
      kvstore.write(data)
    } else {
      kvstore.delete(classOf[StreamBlockData],
        Array(stream.name, event.blockUpdatedInfo.blockManagerId.executorId))
    }
  }

  private def getOrCreateStage(info: StageInfo): LiveStage = {
    val stage = liveStages.computeIfAbsent((info.stageId, info.attemptNumber),
      new Function[(Int, Int), LiveStage]() {
        override def apply(key: (Int, Int)): LiveStage = new LiveStage()
      })
    stage.info = info
    stage
  }

  private def killedTasksSummary(
      reason: TaskEndReason,
      oldSummary: Map[String, Int]): Map[String, Int] = {
    reason match {
      case k: TaskKilled =>
        oldSummary.updated(k.reason, oldSummary.getOrElse(k.reason, 0) + 1)
      case denied: TaskCommitDenied =>
        val reason = denied.toErrorString
        oldSummary.updated(reason, oldSummary.getOrElse(reason, 0) + 1)
      case _ =>
        oldSummary
    }
  }

  private def update(entity: LiveEntity, now: Long, last: Boolean = false): Unit = {
    entity.write(kvstore, now, checkTriggers = last)
  }

  /** Update a live entity only if it hasn't been updated in the last configured period. */
  private def maybeUpdate(entity: LiveEntity, now: Long): Unit = {
    if (live && liveUpdatePeriodNs >= 0 && now - entity.lastWriteTime > liveUpdatePeriodNs) {
      update(entity, now)
    }
  }

  /** Update an entity only if in a live app; avoids redundant writes when replaying logs. */
  private def liveUpdate(entity: LiveEntity, now: Long): Unit = {
    if (live) {
      update(entity, now)
    }
  }

  private def conditionalLiveUpdate(entity: LiveEntity, now: Long, condition: Boolean): Unit = {
    if (condition) {
      liveUpdate(entity, now)
    } else {
      maybeUpdate(entity, now)
    }
  }

  private def cleanupExecutors(count: Long): Unit = {
    // Because the limit is on the number of *dead* executors, we need to calculate whether
    // there are actually enough dead executors to be deleted.
    val threshold = conf.get(MAX_RETAINED_DEAD_EXECUTORS)
    val dead = count - activeExecutorCount

    if (dead > threshold) {
      val countToDelete = calculateNumberToRemove(dead, threshold)
      val toDelete = kvstore.view(classOf[ExecutorSummaryWrapper]).index("active")
        .max(countToDelete).first(false).last(false).asScala.toSeq
      toDelete.foreach { e => kvstore.delete(e.getClass(), e.info.id) }
    }
  }

  private def cleanupJobs(count: Long): Unit = {
    val countToDelete = calculateNumberToRemove(count, conf.get(MAX_RETAINED_JOBS))
    if (countToDelete <= 0L) {
      return
    }

    val view = kvstore.view(classOf[JobDataWrapper]).index("completionTime").first(0L)
    val toDelete = KVUtils.viewToSeq(view, countToDelete.toInt) { j =>
      j.info.status != JobExecutionStatus.RUNNING && j.info.status != JobExecutionStatus.UNKNOWN
    }
    toDelete.foreach { j => kvstore.delete(j.getClass(), j.info.jobId) }
  }

  private def cleanupStages(count: Long): Unit = {
    val countToDelete = calculateNumberToRemove(count, conf.get(MAX_RETAINED_STAGES))
    if (countToDelete <= 0L) {
      return
    }

    // As the completion time of a skipped stage is always -1, we will remove skipped stages first.
    // This is safe since the job itself contains enough information to render skipped stages in the
    // UI.
    val view = kvstore.view(classOf[StageDataWrapper]).index("completionTime")
    val stages = KVUtils.viewToSeq(view, countToDelete.toInt) { s =>
      s.info.status != v1.StageStatus.ACTIVE && s.info.status != v1.StageStatus.PENDING
    }

    stages.foreach { s =>
      val key = Array(s.info.stageId, s.info.attemptId)
      kvstore.delete(s.getClass(), key)

      val execSummaries = kvstore.view(classOf[ExecutorStageSummaryWrapper])
        .index("stage")
        .first(key)
        .last(key)
        .asScala
        .toSeq
      execSummaries.foreach { e =>
        kvstore.delete(e.getClass(), e.id)
      }

      val tasks = kvstore.view(classOf[TaskDataWrapper])
        .index("stage")
        .first(key)
        .last(key)
        .asScala

      tasks.foreach { t =>
        kvstore.delete(t.getClass(), t.taskId)
      }

      // Check whether there are remaining attempts for the same stage. If there aren't, then
      // also delete the RDD graph data.
      val remainingAttempts = kvstore.view(classOf[StageDataWrapper])
        .index("stageId")
        .first(s.info.stageId)
        .last(s.info.stageId)
        .closeableIterator()

      val hasMoreAttempts = try {
        remainingAttempts.asScala.exists { other =>
          other.info.attemptId != s.info.attemptId
        }
      } finally {
        remainingAttempts.close()
      }

      if (!hasMoreAttempts) {
        kvstore.delete(classOf[RDDOperationGraphWrapper], s.info.stageId)
      }

      cleanupCachedQuantiles(key)
    }
  }

  private def cleanupTasks(stage: LiveStage): Unit = {
    val countToDelete = calculateNumberToRemove(stage.savedTasks.get(), maxTasksPerStage).toInt
    if (countToDelete > 0) {
      val stageKey = Array(stage.info.stageId, stage.info.attemptNumber)
      val view = kvstore.view(classOf[TaskDataWrapper])
        .index(TaskIndexNames.COMPLETION_TIME)
        .parent(stageKey)

      // Try to delete finished tasks only.
      val toDelete = KVUtils.viewToSeq(view, countToDelete) { t =>
        !live || t.status != TaskState.RUNNING.toString()
      }
      toDelete.foreach { t => kvstore.delete(t.getClass(), t.taskId) }
      stage.savedTasks.addAndGet(-toDelete.size)

      // If there are more running tasks than the configured limit, delete running tasks. This
      // should be extremely rare since the limit should generally far exceed the number of tasks
      // that can run in parallel.
      val remaining = countToDelete - toDelete.size
      if (remaining > 0) {
        val runningTasksToDelete = view.max(remaining).iterator().asScala.toList
        runningTasksToDelete.foreach { t => kvstore.delete(t.getClass(), t.taskId) }
        stage.savedTasks.addAndGet(-remaining)
      }

      // On live applications, cleanup any cached quantiles for the stage. This makes sure that
      // quantiles will be recalculated after tasks are replaced with newer ones.
      //
      // This is not needed in the SHS since caching only happens after the event logs are
      // completely processed.
      if (live) {
        cleanupCachedQuantiles(stageKey)
      }
    }
    stage.cleaning = false
  }

  private def cleanupCachedQuantiles(stageKey: Array[Int]): Unit = {
    val cachedQuantiles = kvstore.view(classOf[CachedQuantile])
      .index("stage")
      .first(stageKey)
      .last(stageKey)
      .asScala
      .toList
    cachedQuantiles.foreach { q =>
      kvstore.delete(q.getClass(), q.id)
    }
  }

  /**
   * Remove at least (retainedSize / 10) items to reduce friction. Because tracking may be done
   * asynchronously, this method may return 0 in case enough items have been deleted already.
   */
  private def calculateNumberToRemove(dataSize: Long, retainedSize: Long): Long = {
    if (dataSize > retainedSize) {
      math.max(retainedSize / 10L, dataSize - retainedSize)
    } else {
      0L
    }
  }

}
