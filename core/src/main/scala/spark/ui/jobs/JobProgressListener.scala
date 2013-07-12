package spark.ui.jobs

import scala.Seq
import scala.collection.mutable.{HashSet, ListBuffer, HashMap, ArrayBuffer}

import spark.{ExceptionFailure, SparkContext, Success, Utils}
import spark.scheduler._
import spark.scheduler.cluster.TaskInfo
import spark.executor.TaskMetrics
import collection.mutable

private[spark] class FairJobProgressListener(val sparkContext: SparkContext)
  extends JobProgressListener(sparkContext) {

  val FAIR_SCHEDULER_PROPERTIES = "spark.scheduler.cluster.fair.pool"
  val DEFAULT_POOL_NAME = "default"

  override val stageToPool = HashMap[Stage, String]()
  override val poolToActiveStages = HashMap[String, HashSet[Stage]]()

  override def onStageCompleted(stageCompleted: StageCompleted) = {
    super.onStageCompleted(stageCompleted)
    val stage = stageCompleted.stageInfo.stage
    poolToActiveStages(stageToPool(stage)) -= stage
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = {
    super.onStageSubmitted(stageSubmitted)
    val stage = stageSubmitted.stage
    var poolName = DEFAULT_POOL_NAME
    if (stageSubmitted.properties != null) {
      poolName = stageSubmitted.properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME)
    }
    stageToPool(stage) = poolName
    val stages = poolToActiveStages.getOrElseUpdate(poolName, new HashSet[Stage]())
    stages += stage
  }
  
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    super.onJobEnd(jobEnd)
    jobEnd match {
      case end: SparkListenerJobEnd =>
        end.jobResult match {
          case JobFailed(ex, Some(stage)) =>
            poolToActiveStages(stageToPool(stage)) -= stage
          case _ =>
        }
      case _ =>
    }
  }
}

private[spark] class JobProgressListener(val sc: SparkContext) extends SparkListener {
  // How many stages to remember
  val RETAINED_STAGES = System.getProperty("spark.ui.retained_stages", "1000").toInt

  def stageToPool: HashMap[Stage, String] = null
  def poolToActiveStages: HashMap[String, HashSet[Stage]] =null

  val activeStages = HashSet[Stage]()
  val completedStages = ListBuffer[Stage]()
  val failedStages = ListBuffer[Stage]()

  val stageToTasksComplete = HashMap[Int, Int]()
  val stageToTasksFailed = HashMap[Int, Int]()
  val stageToTaskInfos =
    HashMap[Int, ArrayBuffer[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]]()

  override def onJobStart(jobStart: SparkListenerJobStart) {}

  override def onStageCompleted(stageCompleted: StageCompleted) = {
    val stage = stageCompleted.stageInfo.stage
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
      })
      stages.trimEnd(toRemove)
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) =
    activeStages += stageSubmitted.stage

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val sid = taskEnd.task.stageId
    val (failureInfo, metrics): (Option[ExceptionFailure], Option[TaskMetrics]) =
      taskEnd.reason match {
        case e: ExceptionFailure =>
          stageToTasksFailed(sid) = stageToTasksFailed.getOrElse(sid, 0) + 1
          (Some(e), e.metrics)
        case _ =>
          stageToTasksComplete(sid) = stageToTasksComplete.getOrElse(sid, 0) + 1
          (None, Some(taskEnd.taskMetrics))
      }
    val taskList = stageToTaskInfos.getOrElse(
      sid, ArrayBuffer[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]())
    taskList += ((taskEnd.taskInfo, metrics, failureInfo))
    stageToTaskInfos(sid) = taskList
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    jobEnd match {
      case end: SparkListenerJobEnd =>
        end.jobResult match {
          case JobFailed(ex, Some(stage)) =>
            activeStages -= stage
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
