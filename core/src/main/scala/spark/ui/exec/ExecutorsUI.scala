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
    val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize))
      .reduceOption(_+_).getOrElse(0L)

    val execTables =
      for (a <- 0 until storageStatusList.size)
        yield getExecTable(a)

    val content =
      <div class="row">
        <div class="span12">
          <ul class="unstyled">
            <li><strong>Memory:</strong>
              {Utils.memoryBytesToString(maxMem - remainingMem)} Used
              ({Utils.memoryBytesToString(remainingMem)} Available) </li>
            <li><strong>Disk:</strong> {Utils.memoryBytesToString(diskSpaceUsed)} Used </li>
          </ul>
        </div>
      </div>
      <div class = "row">
        <div class="span12">
          {execTables}
        </div>
      </div>;

    headerSparkPage(content, sc, "Executors", Executors)
  }

  def getExecTable(a: Int): Seq[Node] = {
    val memUsed = Utils.memoryBytesToString(sc.getExecutorStorageStatus(a).maxMem)
    val maxMem = Utils.memoryBytesToString(sc.getExecutorStorageStatus(a).memUsed())
    val diskUsed = Utils.memoryBytesToString(sc.getExecutorStorageStatus(a).diskUsed())
    val rddBlocks = sc.getExecutorStorageStatus(a).blocks.size.toString
    val execInfo = Seq(
      ("RDD blocks", rddBlocks),
      ("Memory used", "%s/%s".format(memUsed, maxMem)),
      ("Disk used", diskUsed)
    )
    def execRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
    val table = UIUtils.listingTable(Seq("Name", "Value"), execRow, execInfo)
    val execId = sc.getExecutorStorageStatus(a).blockManagerId.executorId
    val hostPort = sc.getExecutorStorageStatus(a).blockManagerId.hostPort
    val header =
      <h3>Executor {execId}</h3>
        <h4>{hostPort}</h4>;
    header ++ table
  }
}