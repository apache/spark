package spark.ui

import akka.util.Duration
import javax.servlet.http.HttpServletRequest
import org.eclipse.jetty.server.Handler
import spark.{RDD, Logging, SparkContext, Utils}
import spark.ui.WebUI._
import xml.Node
import spark.storage.StorageUtils
import spark.storage.StorageStatus
import spark.storage.RDDInfo
import spark.storage.BlockManagerMasterActor.BlockStatus

/**
 * Web UI server for the BlockManager inside each SparkContext.
 */
private[spark]
class BlockManagerUI(sc: SparkContext)
    extends UIComponent with Logging  {
  implicit val timeout = Duration.create(
    System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")


  def getHandlers = Seq[(String, Handler)](
    ("/storage/rdd", (request: HttpServletRequest) => rddPage(request)),
    ("/storage", (request: HttpServletRequest) => indexPage)
  )

  def rddPage(request: HttpServletRequest): Seq[Node] = {
    val id = request.getParameter("id")
    val prefix = "rdd_" + id.toString
    val storageStatusList = sc.getExecutorStorageStatus
    val filteredStorageStatusList = StorageUtils.
      filterStorageStatusByPrefix(storageStatusList, prefix)
    val rddInfo = StorageUtils.rddInfoFromStorageStatus(filteredStorageStatusList, sc).head

    val workerHeaders = Seq("Host", "Memory Usage", "Disk Usage")
    val workers = filteredStorageStatusList.map((prefix, _))
    val workerTable = listingTable(workerHeaders, workerRow, workers)

    val blockHeaders = Seq("Block Name", "Storage Level", "Size in Memory", "Size on Disk")
    val blocks = filteredStorageStatusList.flatMap(_.blocks).toArray.sortWith(_._1 < _._1)
    val blockTable = listingTable(blockHeaders, blockRow, blocks)

    val content =
      <div class="row">
        <div class="span12">
          <ul class="unstyled">
            <li>
              <strong>Storage Level:</strong>
              {rddInfo.storageLevel.description}
            </li>
            <li>
              <strong>Cached Partitions:</strong>
              {rddInfo.numCachedPartitions}
            </li>
            <li>
              <strong>Total Partitions:</strong>
              {rddInfo.numPartitions}
            </li>
            <li>
              <strong>Memory Size:</strong>
              {Utils.memoryBytesToString(rddInfo.memSize)}
            </li>
            <li>
              <strong>Disk Size:</strong>
              {Utils.memoryBytesToString(rddInfo.diskSize)}
            </li>
          </ul>
        </div>
      </div>
      <hr/>
      <div class="row">
        <div class="span12">
          <h3> RDD Summary </h3>
          <br/> {blockTable}
        </div>
      </div>
      <hr/> ++ {workerTable};

    WebUI.headerSparkPage(content, "RDD Info: " + id)
  }

  def blockRow(blk: (String, BlockStatus)): Seq[Node] = {
    val (id, block) = blk
    <tr>
      <td>{id}</td>
      <td>
        {block.storageLevel.description}
      </td>
      <td>{Utils.memoryBytesToString(block.memSize)}</td>
      <td>{Utils.memoryBytesToString(block.diskSize)}</td>
    </tr>
  }

  def workerRow(worker: (String, StorageStatus)): Seq[Node] = {
    val (prefix, status) = worker
    <tr>
      <td>{status.blockManagerId.host + ":" + status.blockManagerId.port}</td>
      <td>
        {Utils.memoryBytesToString(status.memUsed(prefix))}
        ({Utils.memoryBytesToString(status.memRemaining)} Total Available)
      </td>
      <td>{Utils.memoryBytesToString(status.diskUsed(prefix))}</td>
    </tr>
  }

  def indexPage: Seq[Node] = {
    val storageStatusList = sc.getExecutorStorageStatus
    // Calculate macro-level statistics
    val maxMem = storageStatusList.map(_.maxMem).reduce(_+_)
    val remainingMem = storageStatusList.map(_.memRemaining).reduce(_+_)
    val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize))
      .reduceOption(_+_).getOrElse(0L)

    val rddHeaders = Seq(
      "RDD Name",
      "Storage Level",
      "Cached Partitions",
      "Fraction Partitions Cached",
      "Size in Memory",
      "Size on Disk")
    val rdds = StorageUtils.rddInfoFromStorageStatus(storageStatusList, sc)
    val rddTable = listingTable(rddHeaders, rddRow, rdds)

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
      </div> ++ {rddTable};

    WebUI.headerSparkPage(content, "Spark Storage ")
  }

  def rddRow(rdd: RDDInfo): Seq[Node] = {
    <tr>
      <td>
        <a href={"/storage/rdd?id=%s".format(rdd.id)}>
          {rdd.name}
        </a>
      </td>
      <td>{rdd.storageLevel.description}
      </td>
      <td>{rdd.numCachedPartitions}</td>
      <td>{rdd.numCachedPartitions / rdd.numPartitions.toDouble}</td>
      <td>{Utils.memoryBytesToString(rdd.memSize)}</td>
      <td>{Utils.memoryBytesToString(rdd.diskSize)}</td>
    </tr>
  }
}
