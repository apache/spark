package spark.deploy.worker

import scala.collection.mutable.HashMap

import akka.actor.{Terminated, ActorRef, Props, Actor}
import spark.{Logging, Utils}
import spark.util.AkkaUtils
import java.text.SimpleDateFormat
import java.util.Date
import spark.deploy.{RegisteredSlave, RegisterSlave}

class Worker(ip: String, port: Int, webUiPort: Int, cores: Int, memory: Int)
  extends Actor with Logging {

  var coresUsed = 0
  var memoryUsed = 0

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  override def preStart() {
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      ip, port, cores, Utils.memoryMegabytesToString(memory)))
    startWebUi()
  }

  def startWebUi() {
    val webUi = new WorkerWebUI(context.system, self)
    try {
      AkkaUtils.startSprayServer(context.system, ip, webUiPort, webUi.handler)
    } catch {
      case e: Exception =>
        logError("Failed to create web UI", e)
        System.exit(1)
    }
  }

  override def receive = {
    case RegisteredSlave(clusterId, slaveId) => {
      logInfo("Registered with cluster ID " + clusterId + ", slave ID " + slaveId)
    }

    case Terminated(actor) => {
      logError("Master disconnected!")
    }
  }
}

object Worker {
  def main(argStrings: Array[String]) {
    val args = new WorkerArguments(argStrings)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", args.ip, args.port)
    val actor = actorSystem.actorOf(
      Props(new Worker(args.ip, boundPort, args.webUiPort, args.cores, args.memory)),
      name = "Worker")
    actorSystem.awaitTermination()
  }
}
