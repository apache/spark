package spark.deploy.master

import scala.collection.mutable.HashMap

import akka.actor.{Terminated, ActorRef, Props, Actor}
import spark.{Logging, Utils}
import spark.util.AkkaUtils
import java.text.SimpleDateFormat
import java.util.Date
import spark.deploy.{RegisteredSlave, RegisterSlave}

class SlaveInfo(
                 val id: Int,
                 val host: String,
                 val port: Int,
                 val cores: Int,
                 val memory: Int,
                 val actor: ActorRef) {
  var coresUsed = 0
  var memoryUsed = 0

  def coresFree: Int = cores - coresUsed

  def memoryFree: Int = memory - memoryUsed
}

class Master(ip: String, port: Int, webUiPort: Int) extends Actor with Logging {
  val clusterId = newClusterId()
  var nextSlaveId = 0
  var nextJobId = 0
  val slaves = new HashMap[Int, SlaveInfo]
  val actorToSlave = new HashMap[ActorRef, SlaveInfo]

  override def preStart() {
    logInfo("Starting Spark master at spark://" + ip + ":" + port)
    logInfo("Cluster ID: " + clusterId)
    startWebUi()
  }

  def startWebUi() {
    val webUi = new MasterWebUI(context.system, self)
    try {
      AkkaUtils.startSprayServer(context.system, ip, webUiPort, webUi.handler)
    } catch {
      case e: Exception =>
        logError("Failed to create web UI", e)
        System.exit(1)
    }
  }

  override def receive = {
    case RegisterSlave(host, slavePort, cores, memory) => {
      logInfo("Registering slave %s:%d with %d cores, %s RAM".format(
        host, slavePort, cores, Utils.memoryMegabytesToString(memory)))
      val id = newSlaveId()
      slaves(id) = new SlaveInfo(id, host, slavePort, cores, memory, sender)
      actorToSlave(sender) = slaves(id)
      context.watch(sender)
      sender ! RegisteredSlave(clusterId, id)
    }

    case Terminated(actor) => {
      logInfo("Slave disconnected: " + actor)
      actorToSlave.get(actor) match {
        case Some(slave) =>
          logInfo("Removing slave " + slave.id)
          slaves -= slave.id
          actorToSlave -= actor
        case None =>
          logError("Did not have any slave registered for " + actor)
      }
    }
  }

  def newClusterId(): String = {
    val date = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date())
    "%s-%04d".format(date, (math.random * 10000).toInt)
  }

  def newSlaveId(): Int = {
    nextSlaveId += 1
    nextSlaveId - 1
  }
}

object Master {
  def main(argStrings: Array[String]) {
    val args = new MasterArguments(argStrings)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", args.ip, args.port)
    val actor = actorSystem.actorOf(
      Props(new Master(args.ip, boundPort, args.webUiPort)), name = "Master")
    actorSystem.awaitTermination()
  }
}
