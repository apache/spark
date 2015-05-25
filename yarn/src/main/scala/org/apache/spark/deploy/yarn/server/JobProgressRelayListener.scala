package org.apache.spark.deploy.yarn.server

import java.util.Date

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerUnpersistRDD, _}
import org.apache.spark.ui.jobs.JobProgressListener

/**
 *
 * User: chester
 * Date: 7/9/14
 * Time: 11:40 AM
 */
class JobProgressRelayListener(appCtx: ApplicationContext)
  extends JobProgressListener(appCtx.sparkCtx.getConf) with SparkListener {

  private val logger = new ChannelMessageLogger(appCtx.appName, Some(appCtx))
  private val sc = appCtx.sparkCtx
  private implicit val sparkCtx = Some(sc)
  private implicit val messenger = appCtx.messenger

  private def makeProgress(started: Int,
                           completed: Int,
                           failed: Int,
                           total: Int): (Double, String) = {
    val progress = (completed.toDouble / total) * 100
    val failedText = if (failed > 0) s"$failed failed" else ""
    val progressMessage = s"$completed/$total $failedText"

    (progress, progressMessage)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {

    super.onStageCompleted(stageCompleted)

    val info = stageCompleted.stageInfo
    val stageDataOption = stageIdToData.get((info.stageId, info.attemptId))

    val progressMessage: Option[ProgressMessage] = stageDataOption.map { stageData =>
      val (progress, message) = makeProgress(stageData.numActiveTasks, stageData.completedIndices.size,
        stageData.numFailedTasks, info.numTasks)
      val task = s"${info.name} - ${stageData.description.getOrElse(info.stageId.toString)}"
      val time = info.completionTime.getOrElse(new Date().getTime)
      ProgressMessage(task, message, progress.toFloat, time)
    }

    Console.err.println("onStageCompleted task message " + progressMessage)
    progressMessage.foreach(logger.sendVisualMessage)

  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    super.onStageSubmitted(stageSubmitted)

    val info = stageSubmitted.stageInfo
    val stageDataOption = stageIdToData.get((info.stageId, info.attemptId))
    val taskMessage: Option[StartTaskMessage] = stageDataOption.map { stageData =>
      val task = s"${info.name} - ${stageData.description.getOrElse(info.stageId.toString)}"
      val time = info.submissionTime.getOrElse(new Date().getTime)
      StartTaskMessage(task, "submitted", time)
    }

    Console.err.println("onStageSubmitted task message " + taskMessage)

    taskMessage.foreach(logger.sendVisualMessage)

  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) {
    super.onUnpersistRDD(unpersistRDD)
    Console.err.println(s"RDD [${unpersistRDD.rddId}] is un-persisted")
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    super.onApplicationStart(applicationStart)
    val taskMessage = StartTaskMessage(applicationStart.appName, "started", applicationStart.time)
    logger.sendVisualMessage(taskMessage)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    super.onApplicationEnd(applicationEnd)
    val taskMessage = EndTaskMessage(appCtx.appName, "end", applicationEnd.time)
    logger.sendVisualMessage(taskMessage)
  }
}
