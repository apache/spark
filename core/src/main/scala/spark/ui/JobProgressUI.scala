package spark.ui

import spark.{Utils, SparkContext}
import spark.scheduler._
import spark.scheduler.SparkListenerTaskEnd
import spark.scheduler.StageCompleted
import spark.scheduler.SparkListenerStageSubmitted
import org.eclipse.jetty.server.Handler
import javax.servlet.http.HttpServletRequest
import xml.Node
import WebUI._
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

private[spark]
class JobProgressUI(sc: SparkContext) extends UIComponent {
  val listener = new JobProgressListener
  val fmt = new SimpleDateFormat("EEE, MMM d yyyy HH:mm:ss")

  sc.addSparkListener(listener)

  def getHandlers = Seq[(String, Handler)](
    ("/stages/stage", (request: HttpServletRequest) => stagePage(request)),
    ("/stages", (request: HttpServletRequest) => indexPage)
  )

  def stagePage(request: HttpServletRequest): Seq[Node] = {
    val stageId = request.getParameter("id").toInt

    val taskHeaders = Seq("Task ID", "Service Time (ms)", "Locality Level", "Worker", "Launch Time")
    val tasks = listener.stageToTaskInfos(stageId)
    val taskTable = listingTable(taskHeaders, taskRow, tasks)

    val content =
      <h2>Percentile Metrics</h2>
        <table class="table table-bordered table-striped table-condensed sortable">
          <thead>
            <tr>
              <th>Service Time</th>
              <th>Remote Bytes Read</th>
              <th>Shuffle Bytes Written</th>
            </tr>
          </thead>
          <tbody>
            {listener.stageToTaskInfos(stageId).map{ case(i, m) => taskRow(i, m) }}
          </tbody>
        </table>
      <h2>Tasks</h2> ++ {taskTable};

    WebUI.headerSparkPage(content, "Stage Details: %s".format(stageId))
  }

  def taskRow(taskData: (TaskInfo, TaskMetrics)): Seq[Node] = {
    val (info, metrics) = taskData
    <tr>
      <td>{info.taskId}</td>
      <td>{metrics.executorRunTime}</td>
      <td>{info.taskLocality}</td>
      <td>{info.hostPort}</td>
      <td>{fmt.format(new Date(info.launchTime))}</td>
    </tr>
  }

  def indexPage: Seq[Node] = {
    val stageHeaders = Seq("Stage ID", "Origin", "Submitted", "Duration", "Tasks: Complete/Total")
    val activeStages = listener.activeStages.toSeq
    val completedStages = listener.completedStages.toSeq

    val activeStageTable = listingTable(stageHeaders, stageRow, activeStages)
    val completedStageTable = listingTable(stageHeaders, stageRow, completedStages)

    val content =
      <h2>Active Stages</h2> ++ {activeStageTable}
      <h2>Completed Stages</h2> ++ {completedStageTable}

    WebUI.headerSparkPage(content, "Spark Stages")
  }

  def getElapsedTime(submitted: Option[Long], completed: Long): String = {
    submitted match {
      case Some(t) => Duration(completed - t, "milliseconds").printHMS
      case _ => "Unknown"
    }
  }

  def stageRow(s: Stage): Seq[Node] = {
    val submissionTime = s.submissionTime match {
      case Some(t) => fmt.format(new Date(t))
      case None => "Unknown"
    }
    <tr>
      <td><a href={"/stages/stage?id=%s".format(s.id)}>{s.id}</a></td>
      <td>{s.origin}</td>
      <td>{submissionTime}</td>
      <td>{getElapsedTime(s.submissionTime, s.completionTime.getOrElse(System.currentTimeMillis()))}</td>
      <td>{listener.stageToTasksComplete.getOrElse(s.id, 0)} / {s.numPartitions}
          {listener.stageToTasksFailed.getOrElse(s.id, 0) match {
            case f if f > 0 => "(%s failed)".format(f)
              case _ =>
          }}
      </td>
    </tr>
  }
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