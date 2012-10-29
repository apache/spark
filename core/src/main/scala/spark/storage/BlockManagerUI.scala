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
      AkkaUtils.startSprayServer(actorSystem, "0.0.0.0", port, webUIDirectives.handler, "BlockManagerHTTPServer")
    } catch {
      case e: Exception =>
        logError("Failed to create BlockManager WebUI", e)
        System.exit(1)
    }
  }

}

private[spark]
case class RDDInfo(id: Int, name: String, storageLevel: StorageLevel, numPartitions: Int, memSize: Long, diskSize: Long)

private[spark]
class BlockManagerUIDirectives(val actorSystem: ActorSystem, master: ActorRef, sc: SparkContext) extends Directives {  

  val STATIC_RESOURCE_DIR = "spark/deploy/static"
  implicit val timeout = Timeout(1 seconds)

  val handler = {
    
    get { path("") { completeWith {
      // Request the current storage status from the Master
      val future = master ? GetStorageStatus
      future.map { status =>
        val storageStati = status.asInstanceOf[ArrayBuffer[StorageStatus]]
        
        // Calculate macro-level statistics
        val maxMem = storageStati.map(_.maxMem).reduce(_+_)
        val remainingMem = storageStati.map(_.remainingMem).reduce(_+_)
        val diskSpaceUsed = storageStati.flatMap(_.blocks.values.map(_.diskSize))
          .reduceOption(_+_).getOrElse(0L)

        // Filter out everything that's not and rdd.
        val rddBlocks = storageStati.flatMap(_.blocks).filter { case(k,v) => k.startsWith("rdd") }.toMap
        val rdds = rddInfoFromBlockStati(rddBlocks)

        spark.storage.html.index.render(maxMem, remainingMem, diskSpaceUsed, rdds.toList)
      }
    }}} ~
    get { path("rdd") { parameter("id") { id => { completeWith {
      val future = master ? GetStorageStatus
      future.map { status =>
        val prefix = "rdd_" + id.toString

        val storageStati = status.asInstanceOf[ArrayBuffer[StorageStatus]]
        val rddBlocks = storageStati.flatMap(_.blocks).filter { case(k,v) => k.startsWith(prefix) }.toMap
        val rddInfo = rddInfoFromBlockStati(rddBlocks).first

        spark.storage.html.rdd.render(rddInfo, rddBlocks)

      }
    }}}}} ~
    pathPrefix("static") {
      getFromResourceDirectory(STATIC_RESOURCE_DIR)
    }

  }

  private def rddInfoFromBlockStati(infos: Map[String, BlockStatus]) : Array[RDDInfo] = {
    infos.groupBy { case(k,v) =>
      // Group by rdd name, ignore the partition name
      k.substring(0,k.lastIndexOf('_'))
    }.map { case(k,v) =>
      val blockStati = v.map(_._2).toArray
      // Add up memory and disk sizes
      val tmp = blockStati.map { x => (x.memSize, x.diskSize)}.reduce { (x,y) => 
        (x._1 + y._1, x._2 + y._2)
      }
      // Get the friendly name for the rdd, if available.
      // This is pretty hacky, is there a better way?
      val rddId = k.split("_").last.toInt
      val rddName : String = Option(sc.rddNames.get(rddId)).getOrElse(k)
      val rddStorageLevel = sc.persistentRdds.get(rddId).getStorageLevel
      RDDInfo(rddId, rddName, rddStorageLevel, blockStati.length, tmp._1, tmp._2)
    }.toArray
  }

}
