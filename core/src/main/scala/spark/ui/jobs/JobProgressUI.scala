package spark.ui.jobs

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

/** Web UI showing progress status of all jobs in the given SparkContext. */
private[spark] class JobProgressUI(sc: SparkContext) {
  val listener = new JobProgressListener
  val dateFmt = new SimpleDateFormat("EEE, MMM d yyyy HH:mm:ss")

  sc.addSparkListener(listener)

  private val indexPage = new IndexPage(this)
  private val stagePage = new StagePage(this)

  def getHandlers = Seq[(String, Handler)](
    ("/stages/stage", (request: HttpServletRequest) => stagePage.render(request)),
    ("/stages", (request: HttpServletRequest) => indexPage.render(request))
  )
}

private[spark] class JobProgressListener extends SparkListener {
  val activeStages = HashSet[Stage]()
  val stageToTasksComplete = HashMap[Int, Int]()
  val stageToTasksFailed = HashMap[Int, Int]()
  val stageToTaskInfos = HashMap[Int, ArrayBuffer[(TaskInfo, TaskMetrics)]]()
  val completedStages = ListBuffer[Stage]() // Todo (pwendell): Evict these over time

  override def onJobStart(jobStart: SparkListenerJobStart) { }

  override def onStageCompleted(stageCompleted: StageCompleted) = {
    val stage = stageCompleted.stageInfo.stage
    activeStages -= stage
    stage +=: completedStages
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) =
    activeStages += stageSubmitted.stage

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val sid = taskEnd.event.task.stageId
    taskEnd.event.reason match {
      case Success =>
        stageToTasksComplete(sid) = stageToTasksComplete.getOrElse(sid, 0) + 1
      case _ =>
        stageToTasksFailed(sid) = stageToTasksFailed.getOrElse(sid, 0) + 1
    }
    val taskList = stageToTaskInfos.getOrElse(sid, ArrayBuffer[(TaskInfo, TaskMetrics)]())
    taskList += ((taskEnd.event.taskInfo, taskEnd.event.taskMetrics))
    stageToTaskInfos(sid) = taskList
  }

  override def onJobEnd(jobEnd: SparkListenerEvents) { }
}