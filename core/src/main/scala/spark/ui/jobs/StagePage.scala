package spark.ui.jobs

import java.util.Date

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import spark.ui.UIUtils._
import spark.ui.Page._
import spark.util.Distribution
import spark.Utils
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
        {if (shuffleWrite) Seq("Shuffle Write") else Nil}

    val taskTable = listingTable(taskHeaders, taskRow, tasks)

    val serviceTimes = tasks.map{case (info, metrics) => metrics.executorRunTime.toDouble}
    val serviceQuantiles = "Duration" +: Distribution(serviceTimes).get.getQuantiles().map(
      ms => parent.formatDuration(ms.toLong))

    def getQuantileCols(data: Seq[Double]) =
      Distribution(data).get.getQuantiles().map(d => Utils.memoryBytesToString(d.toLong))

    val shuffleReadSizes = tasks.map {
      case(info, metrics) =>
        metrics.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L).toDouble
    }
    val shuffleReadQuantiles = "Shuffle Read (Remote)" +: getQuantileCols(shuffleReadSizes)

    val shuffleWriteSizes = tasks.map {
      case(info, metrics) =>
        metrics.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L).toDouble
    }
    val shuffleWriteQuantiles = "Shuffle Write" +: getQuantileCols(shuffleWriteSizes)

    val listings: Seq[Seq[String]] = Seq(serviceQuantiles,
      if (shuffleRead) shuffleReadQuantiles else Nil,
      if (shuffleWrite) shuffleWriteQuantiles else Nil)

    val quantileHeaders = Seq("Metric", "Min", "25%", "50%", "75%", "Max")
    val quantileTable = listingTable(quantileHeaders, quantileRow, listings)

    val content =
      <h2>Summary Metrics</h2> ++ quantileTable ++ <h2>Tasks</h2> ++ taskTable;

    headerSparkPage(content, parent.sc, "Stage Details: %s".format(stageId), Jobs)
  }

  def quantileRow(data: Seq[String]): Seq[Node] = <tr> {data.map(d => <td>{d}</td>)} </tr>

  def taskRow(taskData: (TaskInfo, TaskMetrics)): Seq[Node] = {
    val (info, metrics) = taskData
    <tr>
      <td>{info.taskId}</td>
      <td>{parent.formatDuration(metrics.executorRunTime)}</td>
      <td>{info.taskLocality}</td>
      <td>{info.hostPort}</td>
      <td>{dateFmt.format(new Date(info.launchTime))}</td>
      {metrics.shuffleReadMetrics.map{m =>
        <td>{Utils.memoryBytesToString(m.remoteBytesRead)}</td>}.getOrElse("") }
      {metrics.shuffleWriteMetrics.map{m =>
        <td>{Utils.memoryBytesToString(m.shuffleBytesWritten)}</td>}.getOrElse("") }
    </tr>
  }
}
