package spark.ui.jobs

import spark.{Utils, SparkContext}
import spark.scheduler._
import spark.scheduler.SparkListenerTaskEnd
import spark.scheduler.StageCompleted
import spark.scheduler.SparkListenerStageSubmitted
import org.eclipse.jetty.server.Handler
import javax.servlet.http.HttpServletRequest
import xml.Node
import collection.mutable._
import spark.Success
import akka.util.Duration
import java.text.SimpleDateFormat
import java.util.Date
import spark.scheduler.cluster.TaskInfo
import collection.mutable
import org.hsqldb.lib.HashMappedList
import spark.executor.TaskMetrics
import spark.scheduler.SparkListenerTaskEnd
import scala.Some
import spark.scheduler.SparkListenerStageSubmitted
import scala.Seq
import spark.scheduler.StageCompleted
import spark.scheduler.SparkListenerJobStart
import spark.ui.{WebUI, UIComponent}
import spark.ui.WebUI._
import spark.scheduler.SparkListenerTaskEnd
import scala.Some
import spark.scheduler.SparkListenerStageSubmitted
import spark.scheduler.StageCompleted
import spark.scheduler.SparkListenerJobStart

private[spark]
class JobProgressUI(sc: SparkContext) extends UIComponent {
  val listener = new JobProgressListener
  val dateFmt = new SimpleDateFormat("EEE, MMM d yyyy HH:mm:ss")

  sc.addSparkListener(listener)

  val indexPage = new IndexPage(this)
  val stagePage = new StagePage(this)

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