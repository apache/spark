package spark.ui.jobs

import java.util.Date

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import spark.ui.UIUtils._
import spark.ui.Page._
import spark.util.Distribution
import spark.{ExceptionFailure, Utils}
import spark.scheduler.cluster.TaskInfo
import spark.executor.TaskMetrics

/** Page showing statistics and task list for a given stage */
private[spark] class StagePage(parent: JobProgressUI) {
  def listener = parent.listener
  val dateFmt = parent.dateFmt

  def render(request: HttpServletRequest): Seq[Node] = {
    val stageId = request.getParameter("id").toInt

    if (!listener.stageToTaskInfos.contains(stageId)) {
      val content =
        <div>
          <h2>Summary Metrics</h2> No tasks have finished yet
          <h2>Tasks</h2> No tasks have finished yet
        </div>
      return headerSparkPage(content, parent.sc, "Stage Details: %s".format(stageId), Jobs)
    }

    val tasks = listener.stageToTaskInfos(stageId)

    val shuffleRead = listener.hasShuffleRead(stageId)
    val shuffleWrite = listener.hasShuffleWrite(stageId)

    val taskHeaders: Seq[String] =
      Seq("Task ID", "Duration", "Locality Level", "Worker", "Launch Time") ++
        {if (shuffleRead) Seq("Shuffle Read")  else Nil} ++
        {if (shuffleWrite) Seq("Shuffle Write") else Nil} ++
      Seq("Details")

    val taskTable = listingTable(taskHeaders, taskRow, tasks)

    // Excludes tasks which failed and have incomplete metrics
    val validTasks = tasks.filter(t => Option(t._2).isDefined)

    val summaryTable: Option[Seq[Node]] =
      if (validTasks.size == 0) {
        None
      }
      else {
        val serviceTimes = validTasks.map{case (info, metrics, exception) =>
          metrics.executorRunTime.toDouble}
        val serviceQuantiles = "Duration" +: Distribution(serviceTimes).get.getQuantiles().map(
          ms => parent.formatDuration(ms.toLong))

        def getQuantileCols(data: Seq[Double]) =
          Distribution(data).get.getQuantiles().map(d => Utils.memoryBytesToString(d.toLong))

        val shuffleReadSizes = validTasks.map {
          case(info, metrics, exception) =>
            metrics.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L).toDouble
        }
        val shuffleReadQuantiles = "Shuffle Read (Remote)" +: getQuantileCols(shuffleReadSizes)

        val shuffleWriteSizes = validTasks.map {
          case(info, metrics, exception) =>
            metrics.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L).toDouble
        }
        val shuffleWriteQuantiles = "Shuffle Write" +: getQuantileCols(shuffleWriteSizes)

        val listings: Seq[Seq[String]] = Seq(serviceQuantiles,
          if (shuffleRead) shuffleReadQuantiles else Nil,
          if (shuffleWrite) shuffleWriteQuantiles else Nil)

        val quantileHeaders = Seq("Metric", "Min", "25%", "50%", "75%", "Max")
        def quantileRow(data: Seq[String]): Seq[Node] = <tr> {data.map(d => <td>{d}</td>)} </tr>
        Some(listingTable(quantileHeaders, quantileRow, listings))
      }

    val content =
      <h2>Summary Metrics</h2> ++ summaryTable.getOrElse(Nil) ++ <h2>Tasks</h2> ++ taskTable;

    headerSparkPage(content, parent.sc, "Stage Details: %s".format(stageId), Jobs)
  }


  def taskRow(taskData: (TaskInfo, TaskMetrics, Option[ExceptionFailure])): Seq[Node] = {
    def fmtStackTrace(trace: Seq[StackTraceElement]): Seq[Node] =
      trace.map(e => <span style="display:block;">{e.toString}</span>)
    val (info, metrics, exception) = taskData
    <tr>
      <td>{info.taskId}</td>
      <td>{Option(metrics).map{m => parent.formatDuration(m.executorRunTime)}.getOrElse("")}</td>
      <td>{info.taskLocality}</td>
      <td>{info.hostPort}</td>
      <td>{dateFmt.format(new Date(info.launchTime))}</td>
      {Option(metrics).flatMap{m => m.shuffleReadMetrics}.map{s =>
        <td>{Utils.memoryBytesToString(s.remoteBytesRead)}</td>}.getOrElse("")}
      {Option(metrics).flatMap{m => m.shuffleWriteMetrics}.map{s =>
        <td>{Utils.memoryBytesToString(s.shuffleBytesWritten)}</td>}.getOrElse("")}
      <td>{exception.map(e =>
             <span>{e.className}<br/>{fmtStackTrace(e.stackTrace)}</span>).getOrElse("")}</td>
    </tr>
  }
}
