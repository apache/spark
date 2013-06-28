package spark.ui.jobs

import akka.util.Duration

import java.text.SimpleDateFormat

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import scala.Seq
import scala.collection.mutable.{HashSet, ListBuffer, HashMap, ArrayBuffer}

import spark.ui.JettyUtils._
import spark.SparkContext
import spark.scheduler._
import spark.scheduler.cluster.TaskInfo
import spark.executor.TaskMetrics
import spark.Success
import spark.Utils

/** Web UI showing progress status of all jobs in the given SparkContext. */
private[spark] class JobProgressUI(val sc: SparkContext) {
  private var _listener: Option[JobProgressListener] = None
  def listener = _listener.get
  val dateFmt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

  private val indexPage = new IndexPage(this)
  private val stagePage = new StagePage(this)

  def start() {
    _listener = Some(new JobProgressListener)
    sc.addSparkListener(listener)
  }

  def formatDuration(ms: Long) = Utils.msDurationToString(ms)

  def getHandlers = Seq[(String, Handler)](
    ("/stages/stage", (request: HttpServletRequest) => stagePage.render(request)),
    ("/stages", (request: HttpServletRequest) => indexPage.render(request))
  )
}

private[spark] class JobProgressListener extends SparkListener {
  // How many stages to remember
  val RETAINED_STAGES = System.getProperty("spark.ui.retained_stages", "1000").toInt

  val activeStages = HashSet[Stage]()
  val completedStages = ListBuffer[Stage]()
  val failedStages = ListBuffer[Stage]()

  val stageToTasksComplete = HashMap[Int, Int]()
  val stageToTasksFailed = HashMap[Int, Int]()
  val stageToTaskInfos = HashMap[Int, ArrayBuffer[(TaskInfo, TaskMetrics)]]()

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
        stageToTasksComplete.remove(s.id)
        stageToTasksFailed.remove(s.id)
        stageToTaskInfos.remove(s.id)
      })
      stages.trimEnd(toRemove)
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) =
    activeStages += stageSubmitted.stage

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val sid = taskEnd.task.stageId
    taskEnd.reason match {
      case Success =>
        stageToTasksComplete(sid) = stageToTasksComplete.getOrElse(sid, 0) + 1
      case _ =>
        stageToTasksFailed(sid) = stageToTasksFailed.getOrElse(sid, 0) + 1
    }
    val taskList = stageToTaskInfos.getOrElse(sid, ArrayBuffer[(TaskInfo, TaskMetrics)]())
    taskList += ((taskEnd.taskInfo, taskEnd.taskMetrics))
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
      if (s._2 != null) return s._2.shuffleReadMetrics.isDefined
    }
    return false // No tasks have finished for this stage
  }

  /** Is this stage's output to a shuffle write. */
  def hasShuffleWrite(stageID: Int): Boolean = {
    // This is written in a slightly complicated way to avoid having to scan all tasks
    for (s <- stageToTaskInfos.get(stageID).getOrElse(Seq())) {
      if (s._2 != null) return s._2.shuffleWriteMetrics.isDefined
    }
    return false // No tasks have finished for this stage
  }
}