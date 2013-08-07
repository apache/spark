package spark.ui.jobs

import scala.Seq
import scala.collection.mutable.{ListBuffer, HashMap, HashSet}

import spark.{ExceptionFailure, SparkContext, Success, Utils}
import spark.scheduler._
import spark.scheduler.cluster.TaskInfo
import spark.executor.TaskMetrics
import collection.mutable

private[spark] class JobProgressListener(val sc: SparkContext) extends SparkListener {
  // How many stages to remember
  val RETAINED_STAGES = System.getProperty("spark.ui.retained_stages", "1000").toInt
  val DEFAULT_POOL_NAME = "default"

  val stageToPool = new HashMap[Stage, String]()
  val stageToDescription = new HashMap[Stage, String]()
  val poolToActiveStages = new HashMap[String, HashSet[Stage]]()

  val activeStages = HashSet[Stage]()
  val completedStages = ListBuffer[Stage]()
  val failedStages = ListBuffer[Stage]()

  // Total metrics reflect metrics only for completed tasks
  var totalTime = 0L
  var totalShuffleRead = 0L
  var totalShuffleWrite = 0L

  val stageToTime = HashMap[Int, Long]()
  val stageToShuffleRead = HashMap[Int, Long]()
  val stageToShuffleWrite = HashMap[Int, Long]()
  val stageToTasksActive = HashMap[Int, HashSet[TaskInfo]]()
  val stageToTasksComplete = HashMap[Int, Int]()
  val stageToTasksFailed = HashMap[Int, Int]()
  val stageToTaskInfos =
    HashMap[Int, HashSet[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]]()

  override def onJobStart(jobStart: SparkListenerJobStart) {}

  override def onStageCompleted(stageCompleted: StageCompleted) = {
    val stage = stageCompleted.stageInfo.stage
    poolToActiveStages(stageToPool(stage)) -= stage
    activeStages -= stage
    completedStages += stage
    trimIfNecessary(completedStages)
  }

  /** If stages is too large, remove and garbage collect old stages */
  def trimIfNecessary(stages: ListBuffer[Stage]) {
    if (stages.size > RETAINED_STAGES) {
      val toRemove = RETAINED_STAGES / 10
      stages.takeRight(toRemove).foreach( s => {
        stageToTaskInfos.remove(s.id)
        stageToTime.remove(s.id)
        stageToShuffleRead.remove(s.id)
        stageToShuffleWrite.remove(s.id)
        stageToTasksActive.remove(s.id)
        stageToTasksComplete.remove(s.id)
        stageToTasksFailed.remove(s.id)
        stageToPool.remove(s)
        if (stageToDescription.contains(s)) {stageToDescription.remove(s)}
      })
      stages.trimEnd(toRemove)
    }
  }

  /** For FIFO, all stages are contained by "default" pool but "default" pool here is meaningless */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = {
    val stage = stageSubmitted.stage
    activeStages += stage

    val poolName = Option(stageSubmitted.properties).map {
      p => p.getProperty("spark.scheduler.cluster.fair.pool", DEFAULT_POOL_NAME)
    }.getOrElse(DEFAULT_POOL_NAME)
    stageToPool(stage) = poolName

    val description = Option(stageSubmitted.properties).flatMap {
      p => Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
    }
    description.map(d => stageToDescription(stage) = d)

    val stages = poolToActiveStages.getOrElseUpdate(poolName, new HashSet[Stage]())
    stages += stage
  }
  
  override def onTaskStart(taskStart: SparkListenerTaskStart) {
    val sid = taskStart.task.stageId
    val tasksActive = stageToTasksActive.getOrElseUpdate(sid, new HashSet[TaskInfo]())
    tasksActive += taskStart.taskInfo
    val taskList = stageToTaskInfos.getOrElse(
      sid, HashSet[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]())
    taskList += ((taskStart.taskInfo, None, None))
    stageToTaskInfos(sid) = taskList
  }
 
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val sid = taskEnd.task.stageId
    val tasksActive = stageToTasksActive.getOrElseUpdate(sid, new HashSet[TaskInfo]())
    tasksActive -= taskEnd.taskInfo
    val (failureInfo, metrics): (Option[ExceptionFailure], Option[TaskMetrics]) =
      taskEnd.reason match {
        case e: ExceptionFailure =>
          stageToTasksFailed(sid) = stageToTasksFailed.getOrElse(sid, 0) + 1
          (Some(e), e.metrics)
        case _ =>
          stageToTasksComplete(sid) = stageToTasksComplete.getOrElse(sid, 0) + 1
          (None, Option(taskEnd.taskMetrics))
      }

    stageToTime.getOrElseUpdate(sid, 0L)
    val time = metrics.map(m => m.executorRunTime).getOrElse(0)
    stageToTime(sid) += time
    totalTime += time

    stageToShuffleRead.getOrElseUpdate(sid, 0L)
    val shuffleRead = metrics.flatMap(m => m.shuffleReadMetrics).map(s =>
      s.remoteBytesRead).getOrElse(0L)
    stageToShuffleRead(sid) += shuffleRead
    totalShuffleRead += shuffleRead

    stageToShuffleWrite.getOrElseUpdate(sid, 0L)
    val shuffleWrite = metrics.flatMap(m => m.shuffleWriteMetrics).map(s =>
      s.shuffleBytesWritten).getOrElse(0L)
    stageToShuffleWrite(sid) += shuffleWrite
    totalShuffleWrite += shuffleWrite

    val taskList = stageToTaskInfos.getOrElse(
      sid, HashSet[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]())
    taskList -= ((taskEnd.taskInfo, None, None))
    taskList += ((taskEnd.taskInfo, metrics, failureInfo))
    stageToTaskInfos(sid) = taskList
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    jobEnd match {
      case end: SparkListenerJobEnd =>
        end.jobResult match {
          case JobFailed(ex, Some(stage)) =>
            activeStages -= stage
            poolToActiveStages(stageToPool(stage)) -= stage
            failedStages += stage
            trimIfNecessary(failedStages)
          case _ =>
        }
      case _ =>
    }
  }

  /** Is this stage's input from a shuffle read. */
  def hasShuffleRead(stageID: Int): Boolean = {
    // This is written in a slightly complicated way to avoid having to scan all tasks
    for (s <- stageToTaskInfos.get(stageID).getOrElse(Seq())) {
      if (s._2 != null) return s._2.flatMap(m => m.shuffleReadMetrics).isDefined
    }
    return false // No tasks have finished for this stage
  }

  /** Is this stage's output to a shuffle write. */
  def hasShuffleWrite(stageID: Int): Boolean = {
    // This is written in a slightly complicated way to avoid having to scan all tasks
    for (s <- stageToTaskInfos.get(stageID).getOrElse(Seq())) {
      if (s._2 != null) return s._2.flatMap(m => m.shuffleWriteMetrics).isDefined
    }
    return false // No tasks have finished for this stage
  }
}
