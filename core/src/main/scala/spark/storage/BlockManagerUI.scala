package spark.storage

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import cc.spray.Directives
import cc.spray.directives._
import cc.spray.typeconversion.TwirlSupport._
import scala.collection.mutable.ArrayBuffer
import spark.{Logging, SparkContext, SparkEnv}
import spark.util.AkkaUtils


private[spark]
object BlockManagerUI extends Logging {

  /* Starts the Web interface for the BlockManager */
  def start(actorSystem : ActorSystem, masterActor: ActorRef, sc: SparkContext) {
    val webUIDirectives = new BlockManagerUIDirectives(actorSystem, masterActor, sc)
    try {
      logInfo("Starting BlockManager WebUI.")
      val port = Option(System.getenv("BLOCKMANAGER_UI_PORT")).getOrElse("9080").toInt
      AkkaUtils.startSprayServer(actorSystem, "0.0.0.0", port, 
        webUIDirectives.handler, "BlockManagerHTTPServer")
    } catch {
      case e: Exception =>
        logError("Failed to create BlockManager WebUI", e)
        System.exit(1)
    }
  }

}


private[spark]
class BlockManagerUIDirectives(val actorSystem: ActorSystem, master: ActorRef, 
  sc: SparkContext) extends Directives {  

  val STATIC_RESOURCE_DIR = "spark/deploy/static"
  implicit val timeout = Timeout(1 seconds)

  val handler = {
    
    get { path("") { completeWith {
      // Request the current storage status from the Master
      val future = master ? GetStorageStatus
      future.map { status =>
        val storageStatusList = status.asInstanceOf[ArrayBuffer[StorageStatus]].toArray
        
        // Calculate macro-level statistics
        val maxMem = storageStatusList.map(_.maxMem).reduce(_+_)
        val remainingMem = storageStatusList.map(_.memRemaining).reduce(_+_)
        val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize))
          .reduceOption(_+_).getOrElse(0L)

        val rdds = StorageUtils.rddInfoFromStorageStatus(storageStatusList, sc)

        spark.storage.html.index.render(maxMem, remainingMem, diskSpaceUsed, rdds, storageStatusList)
      }
    }}} ~
    get { path("rdd") { parameter("id") { id => { completeWith {
      val future = master ? GetStorageStatus
      future.map { status =>
        val prefix = "rdd_" + id.toString


        val storageStatusList = status.asInstanceOf[ArrayBuffer[StorageStatus]].toArray
        val filteredStorageStatusList = StorageUtils.filterStorageStatusByPrefix(storageStatusList, prefix)

        val rddInfo = StorageUtils.rddInfoFromStorageStatus(filteredStorageStatusList, sc).first

        spark.storage.html.rdd.render(rddInfo, filteredStorageStatusList)

      }
    }}}}} ~
    pathPrefix("static") {
      getFromResourceDirectory(STATIC_RESOURCE_DIR)
    }

  }

  

}
