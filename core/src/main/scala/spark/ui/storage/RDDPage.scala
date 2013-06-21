package spark.ui.storage

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import spark.storage.{StorageStatus, StorageUtils}
import spark.ui.UIUtils._
import spark.Utils
import spark.storage.BlockManagerMasterActor.BlockStatus

/** Page showing storage details for a given RDD */
class RDDPage(parent: BlockManagerUI) {
  val sc = parent.sc

  def render(request: HttpServletRequest): Seq[Node] = {
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

    headerSparkPage(content, "RDD Info: " + rddInfo.name)
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
}
