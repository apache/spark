package spark.ui.exec

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.{HashMap, HashSet}
import scala.xml.Node

import org.eclipse.jetty.server.Handler

import spark.{ExceptionFailure, Logging, Utils, SparkContext}
import spark.executor.TaskMetrics
import spark.scheduler.cluster.TaskInfo
import spark.scheduler.{SparkListenerTaskStart, SparkListenerTaskEnd, SparkListener}
import spark.ui.JettyUtils._
import spark.ui.Page.Executors
import spark.ui.UIUtils


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

    val maxMem = storageStatusList.map(_.maxMem).fold(0L)(_+_)
    val memUsed = storageStatusList.map(_.memUsed()).fold(0L)(_+_)
    val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize)).fold(0L)(_+_)

    val execHead = Seq("Executor ID", "Address", "RDD blocks", "Memory used", "Disk used",
      "Active tasks", "Failed tasks", "Complete tasks", "Total tasks")

    def execRow(kv: Seq[String]) = {
      <tr>
        <td>{kv(0)}</td>
        <td>{kv(1)}</td>
        <td>{kv(2)}</td>
        <td sorttable_customkey={kv(3)}>
          {Utils.bytesToString(kv(3).toLong)} / {Utils.bytesToString(kv(4).toLong)}
        </td>
        <td sorttable_customkey={kv(5)}>
          {Utils.bytesToString(kv(5).toLong)}
        </td>
        <td>{kv(6)}</td>
        <td>{kv(7)}</td>
        <td>{kv(8)}</td>
        <td>{kv(9)}</td>
      </tr>
    }

    val execInfo = for (b <- 0 until storageStatusList.size) yield getExecInfo(b)
    val execTable = UIUtils.listingTable(execHead, execRow, execInfo)

    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li><strong>Memory:</strong>
              {Utils.bytesToString(memUsed)} Used
              ({Utils.bytesToString(maxMem)} Total) </li>
            <li><strong>Disk:</strong> {Utils.bytesToString(diskSpaceUsed)} Used </li>
          </ul>
        </div>
      </div>
      <div class = "row">
        <div class="span12">
          {execTable}
        </div>
      </div>;

    UIUtils.headerSparkPage(content, sc, "Executors (" + execInfo.size + ")", Executors)
  }

  def getExecInfo(a: Int): Seq[String] = {
    val execId = sc.getExecutorStorageStatus(a).blockManagerId.executorId
    val hostPort = sc.getExecutorStorageStatus(a).blockManagerId.hostPort
    val rddBlocks = sc.getExecutorStorageStatus(a).blocks.size.toString
    val memUsed = sc.getExecutorStorageStatus(a).memUsed().toString
    val maxMem = sc.getExecutorStorageStatus(a).maxMem.toString
    val diskUsed = sc.getExecutorStorageStatus(a).diskUsed().toString
    val activeTasks = listener.executorToTasksActive.get(a.toString).map(l => l.size).getOrElse(0)
    val failedTasks = listener.executorToTasksFailed.getOrElse(a.toString, 0)
    val completedTasks = listener.executorToTasksComplete.getOrElse(a.toString, 0)
    val totalTasks = activeTasks + failedTasks + completedTasks

    Seq(
      execId,
      hostPort,
      rddBlocks,
      memUsed,
      maxMem,
      diskUsed,
      activeTasks.toString,
      failedTasks.toString,
      completedTasks.toString,
      totalTasks.toString
    )
  }

  private[spark] class ExecutorsListener extends SparkListener with Logging {
    val executorToTasksActive = HashMap[String, HashSet[TaskInfo]]()
    val executorToTasksComplete = HashMap[String, Int]()
    val executorToTasksFailed = HashMap[String, Int]()

    override def onTaskStart(taskStart: SparkListenerTaskStart) {
      val eid = taskStart.taskInfo.executorId
      val activeTasks = executorToTasksActive.getOrElseUpdate(eid, new HashSet[TaskInfo]())
      activeTasks += taskStart.taskInfo
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
      val eid = taskEnd.taskInfo.executorId
      val activeTasks = executorToTasksActive.getOrElseUpdate(eid, new HashSet[TaskInfo]())
      activeTasks -= taskEnd.taskInfo
      val (failureInfo, metrics): (Option[ExceptionFailure], Option[TaskMetrics]) =
        taskEnd.reason match {
          case e: ExceptionFailure =>
            executorToTasksFailed(eid) = executorToTasksFailed.getOrElse(eid, 0) + 1
            (Some(e), e.metrics)
          case _ =>
            executorToTasksComplete(eid) = executorToTasksComplete.getOrElse(eid, 0) + 1
            (None, Option(taskEnd.taskMetrics))
        }
    }
  }
}
