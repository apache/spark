package spark.storage

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Duration
import akka.util.duration._
import cc.spray.typeconversion.TwirlSupport._
import cc.spray.Directives
import spark.{Logging, SparkContext}
import spark.util.AkkaUtils
import spark.Utils


/**
 * Web UI server for the BlockManager inside each SparkContext.
 */
private[spark]
class BlockManagerUI(val actorSystem: ActorSystem, blockManagerMaster: ActorRef, sc: SparkContext)
  extends Directives with Logging {

  val STATIC_RESOURCE_DIR = "spark/deploy/static"

  implicit val timeout = Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")
  val host = Utils.localHostName()
  val port = if (System.getProperty("spark.ui.port") != null) {
    System.getProperty("spark.ui.port").toInt
  } else {
    // TODO: Unfortunately, it's not possible to pass port 0 to spray and figure out which
    // random port it bound to, so we have to try to find a local one by creating a socket.
    Utils.findFreePort()
  }

  /** Start a HTTP server to run the Web interface */
  def start() {
    try {
      AkkaUtils.startSprayServer(actorSystem, "0.0.0.0", port, handler, "BlockManagerHTTPServer")
      logInfo("Started BlockManager web UI at http://%s:%d".format(host, port))
    } catch {
      case e: Exception =>
        logError("Failed to create BlockManager WebUI", e)
        System.exit(1)
    }
  }

  val handler = {
    get {
      path("") {
        completeWith {
          // Request the current storage status from the Master
          val storageStatusList = sc.getExecutorStorageStatus
          // Calculate macro-level statistics
          val maxMem = storageStatusList.map(_.maxMem).reduce(_+_)
          val remainingMem = storageStatusList.map(_.memRemaining).reduce(_+_)
          val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize))
            .reduceOption(_+_).getOrElse(0L)
          val rdds = StorageUtils.rddInfoFromStorageStatus(storageStatusList, sc)
          spark.storage.html.index.
            render(maxMem, remainingMem, diskSpaceUsed, rdds, storageStatusList)
        }
      } ~
      path("rdd") {
        parameter("id") { id =>
          completeWith {
            val prefix = "rdd_" + id.toString
            val storageStatusList = sc.getExecutorStorageStatus
            val filteredStorageStatusList = StorageUtils.
              filterStorageStatusByPrefix(storageStatusList, prefix)
            val rddInfo = StorageUtils.rddInfoFromStorageStatus(filteredStorageStatusList, sc).head
            spark.storage.html.rdd.render(rddInfo, filteredStorageStatusList)
          }
        }
      } ~
      pathPrefix("static") {
        getFromResourceDirectory(STATIC_RESOURCE_DIR)
      }
    }
  }

  private[spark] def appUIAddress = "http://" + host + ":" + port
}
