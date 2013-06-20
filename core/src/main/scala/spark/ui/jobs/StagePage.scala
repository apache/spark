package spark.ui.jobs

import javax.servlet.http.HttpServletRequest
import xml.Node
import spark.ui.WebUI._
import spark.ui.WebUI
import spark.ui.View
import spark.util.Distribution
import spark.scheduler.cluster.TaskInfo
import spark.executor.TaskMetrics
import java.util.Date

class StagePage(parent: JobProgressUI) extends View[Seq[Node]] {
  val listener = parent.listener
  val dateFmt = parent.dateFmt

  def render(request: HttpServletRequest): Seq[Node] = {
    val stageId = request.getParameter("id").toInt

    val taskHeaders = Seq("Task ID", "Service Time (ms)", "Locality Level", "Worker", "Launch Time")
    val tasks = listener.stageToTaskInfos(stageId)
    val taskTable = listingTable(taskHeaders, taskRow, tasks)

    val serviceTimes = tasks.map{case (info, metrics) => metrics.executorRunTime.toDouble}
    val serviceQuantiles = "Service Time " ++ Distribution(serviceTimes).get.getQuantiles().map(_.toString)
    val quantileHeaders = Seq("Metric", "Min", "25%", "50%", "75%", "Max")

    val quantileTable = listingTable(quantileHeaders, Seq(serviceQuantiles), quantileRow)

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

  def quantileRow(data: Seq[String]) = <tr> {data.map(d => <td>d</td>)} </tr>

  def taskRow(taskData: (TaskInfo, TaskMetrics)): Seq[Node] = {
    val (info, metrics) = taskData
    <tr>
      <td>{info.taskId}</td>
      <td>{metrics.executorRunTime}</td>
      <td>{info.taskLocality}</td>
      <td>{info.hostPort}</td>
      <td>{dateFmt.format(new Date(info.launchTime))}</td>
    </tr>
  }


}
