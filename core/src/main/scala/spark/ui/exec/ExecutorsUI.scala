package spark.ui.exec


import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import scala.util.Properties

import spark.scheduler.cluster.TaskInfo
import spark.executor.TaskMetrics
import spark.ui.JettyUtils._
import spark.ui.UIUtils.headerSparkPage
import spark.ui.Page.Executors
import spark.storage.{StorageStatus, StorageUtils}
import spark.SparkContext
import spark.ui.UIUtils
import spark.Utils

import scala.xml.{Node, XML}

private[spark] class ExecutorsUI(val sc: SparkContext) {

  def getHandlers = Seq[(String, Handler)](
    ("/executors", (request: HttpServletRequest) => render(request))
  )

  def render(request: HttpServletRequest): Seq[Node] = {
    val storageStatusList = sc.getExecutorStorageStatus

    val maxMem = storageStatusList.map(_.maxMem).reduce(_+_)
    val remainingMem = storageStatusList.map(_.memRemaining).reduce(_+_)
    val memUsed = storageStatusList.map(_.memUsed()).reduce(_+_)
    val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize))
      .reduceOption(_+_).getOrElse(0L)

    val execHead = Seq("Executor ID", "Address", "RDD blocks", "Memory used/Memory total",
      "Disk used")
    def execRow(kv: Seq[String]) =
      <tr>
        <td>{kv(0)}</td>
        <td>{kv(1)}</td>
        <td>{kv(2)}</td>
        <td>{kv(3)}</td>
        <td>{kv(4)}</td>
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
    Seq(
      execId,
      hostPort,
      rddBlocks,
      "%s/%s".format(memUsed, maxMem),
      diskUsed
    )
  }
}