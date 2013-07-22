package spark.ui.exec


import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.Properties

import spark.{ExceptionFailure, Logging, SparkContext, Success, Utils}
import spark.executor.TaskMetrics
import spark.scheduler.cluster.TaskInfo
import spark.scheduler._
import spark.SparkContext
import spark.storage.{StorageStatus, StorageUtils}
import spark.ui.JettyUtils._
import spark.ui.Page.Executors
import spark.ui.UIUtils.headerSparkPage
import spark.ui.UIUtils
import spark.Utils

import scala.xml.{Node, XML}

private[spark] class ExecutorsUI(val sc: SparkContext) {

  private var _listener: Option[ExecutorsListener] = None
  def listener = _listener.get

  def start() {
    _listener = Some(new ExecutorsListener)
    sc.addSparkListener(listener)
  }

  def getHandlers = Seq[(String, Handler)](
    ("/executors", (request: HttpServletRequest) => render(request))
  )

  def render(request: HttpServletRequest): Seq[Node] = {
    val storageStatusList = sc.getExecutorStorageStatus

    val maxMem = storageStatusList.map(_.maxMem).reduce(_+_)
    val memUsed = storageStatusList.map(_.memUsed()).reduce(_+_)
    val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize))
      .reduceOption(_+_).getOrElse(0L)

    val execHead = Seq("Executor ID", "Address", "RDD blocks", "Memory used", "Disk used",
      "Tasks: Complete/Total")
    def execRow(kv: Seq[String]) =
      <tr>
        <td>{kv(0)}</td>
        <td>{kv(1)}</td>
        <td>{kv(2)}</td>
        <td>{kv(3)}</td>
        <td>{kv(4)}</td>
        <td>{kv(5)}</td>
      </tr>
    val execInfo =
      for (b <- 0 until storageStatusList.size)
        yield getExecInfo(b)
    val execTable = UIUtils.listingTable(execHead, execRow, execInfo)

    val content =
      <div class="row">
        <div class="span12">
          <ul class="unstyled">
            <li><strong>Memory:</strong>
              {Utils.memoryBytesToString(memUsed)} Used
              ({Utils.memoryBytesToString(maxMem)} Total) </li>
            <li><strong>Disk:</strong> {Utils.memoryBytesToString(diskSpaceUsed)} Used </li>
          </ul>
        </div>
      </div>
      <div class = "row">
        <div class="span12">
          {execTable}
        </div>
      </div>;

    headerSparkPage(content, sc, "Executors", Executors)
  }

  def getExecInfo(a: Int): Seq[String] = {
    val execId = sc.getExecutorStorageStatus(a).blockManagerId.executorId
    val hostPort = sc.getExecutorStorageStatus(a).blockManagerId.hostPort
    val memUsed = Utils.memoryBytesToString(sc.getExecutorStorageStatus(a).memUsed())
    val maxMem = Utils.memoryBytesToString(sc.getExecutorStorageStatus(a).maxMem)
    val diskUsed = Utils.memoryBytesToString(sc.getExecutorStorageStatus(a).diskUsed())
    val rddBlocks = sc.getExecutorStorageStatus(a).blocks.size.toString
    val completedTasks = listener.executorToTasksComplete.getOrElse(a.toString, 0)
    val totalTasks = listener.executorToTaskInfos(a.toString).size
    val failedTasks = listener.executorToTasksFailed.getOrElse(a.toString, 0) match {
      case f if f > 0 => " (%s failed)".format(f)
      case _ => ""
    }

    Seq(
      execId,
      hostPort,
      rddBlocks,
      "%s / %s".format(memUsed, maxMem),
      diskUsed,
      "%s / %s".format(completedTasks, totalTasks) + failedTasks
    )
  }

  private[spark] class ExecutorsListener extends SparkListener with Logging {
    val executorToTasksComplete = HashMap[String, Int]()
    val executorToTasksFailed = HashMap[String, Int]()
    val executorToTaskInfos =
      HashMap[String, ArrayBuffer[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]]()

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
      val eid = taskEnd.taskMetrics.executorId
      val (failureInfo, metrics): (Option[ExceptionFailure], Option[TaskMetrics]) =
        taskEnd.reason match {
          case e: ExceptionFailure =>
            executorToTasksFailed(eid) = executorToTasksFailed.getOrElse(eid, 0) + 1
            logInfo("Executor %s has %s failed tasks.".format(eid, executorToTasksFailed(eid)))
            (Some(e), e.metrics)
          case _ =>
            executorToTasksComplete(eid) = executorToTasksComplete.getOrElse(eid, 0) + 1
            logInfo("Executor %s has %s completed tasks.".format(eid, executorToTasksComplete(eid)))
            (None, Some(taskEnd.taskMetrics))
        }
      val taskList = executorToTaskInfos.getOrElse(
        eid, ArrayBuffer[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]())
      taskList += ((taskEnd.taskInfo, metrics, failureInfo))
      executorToTaskInfos(eid) = taskList
    }
  }
}