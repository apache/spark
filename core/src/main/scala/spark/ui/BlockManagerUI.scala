package spark.ui

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Duration
import javax.servlet.http.HttpServletRequest
import org.eclipse.jetty.server.Handler
import spark.{Logging, SparkContext}
import spark.Utils
import WebUI._
import xml.Node
import spark.storage.StorageUtils

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
          <br/>
          <table class="table table-bordered table-striped table-condensed sortable">
            <thead>
              <tr>
                <th>Block Name</th>
                <th>Storage Level</th>
                <th>Size in Memory</th>
                <th>Size on Disk</th>
              </tr>
            </thead>
            <tbody>
              {filteredStorageStatusList.flatMap(_.blocks).toArray.sortWith(_._1 < _._1).map {
              case (k,v) =>
                <tr>
                  <td>{k}</td>
                  <td>
                    {v.storageLevel.description}
                  </td>
                  <td>{Utils.memoryBytesToString(v.memSize)}</td>
                  <td>{Utils.memoryBytesToString(v.diskSize)}</td>
                </tr>
                }
              }
            </tbody>
          </table>
        </div>
      </div>
      <hr/>
      <div class="row">
        <div class="span12">
        <h3> Worker Summary </h3>
        <br/>
        <table class="table table-bordered table-striped table-condensed sortable">
          <thead>
            <tr>
              <th>Host</th>
              <th>Memory Usage</th>
              <th>Disk Usage</th>
            </tr>
          </thead>
          <tbody>
            {filteredStorageStatusList.map {
              status =>
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
          </tbody>
        </table>
        </div>
      </div>;

    WebUI.headerSparkPage(content, "RDD Info: " + id)
  }

  def indexPage: Seq[Node] = {
    val storageStatusList = sc.getExecutorStorageStatus
    // Calculate macro-level statistics
    val maxMem = storageStatusList.map(_.maxMem).reduce(_+_)
    val remainingMem = storageStatusList.map(_.memRemaining).reduce(_+_)
    val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize))
      .reduceOption(_+_).getOrElse(0L)
    val rdds = StorageUtils.rddInfoFromStorageStatus(storageStatusList, sc)

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
      <hr/>
        <table class="table table-bordered table-striped table-condensed sortable">
          <thead>
            <tr>
              <th>RDD Name</th>
              <th>Storage Level</th>
              <th>Cached Partitions</th>
              <th>Fraction Partitions Cached</th>
              <th>Size in Memory</th>
              <th>Size on Disk</th>
            </tr>
          </thead>
          <tbody>
            {for (rdd <- rdds) yield
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
          </tbody>
        </table>;

    WebUI.headerSparkPage(content, "Spark Storage ")
  }
}
