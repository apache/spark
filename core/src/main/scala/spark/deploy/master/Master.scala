package spark.deploy.master

import scala.collection.mutable.HashMap

import akka.actor._
import spark.{Logging, Utils}
import spark.util.AkkaUtils
import java.text.SimpleDateFormat
import java.util.Date
import spark.deploy.{RegisteredSlave, RegisterSlave}
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientShutdown, RemoteClientDisconnected}
import akka.remote.RemoteClientShutdown
import spark.deploy.RegisteredSlave
import akka.remote.RemoteClientDisconnected
import akka.actor.Terminated
import scala.Some
import spark.deploy.RegisterSlave

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
  val addressToSlave = new HashMap[Address, SlaveInfo]

  override def preStart() {
    logInfo("Starting Spark master at spark://" + ip + ":" + port)
    logInfo("Cluster ID: " + clusterId)
    context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
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
      val slave = addSlave(host, slavePort, cores, memory)
      context.watch(sender)  // This doesn't work with remote actors but helps for testing
      sender ! RegisteredSlave(clusterId, slave.id)
    }

    case RemoteClientDisconnected(transport, address) =>
      logInfo("Remote client disconnected: " + address)
      addressToSlave.get(address).foreach(s => removeSlave(s)) // Remove slave, if any, at address

    case RemoteClientShutdown(transport, address) =>
      logInfo("Remote client shutdown: " + address)
      addressToSlave.get(address).foreach(s => removeSlave(s)) // Remove slave, if any, at address

    case Terminated(actor) =>
      logInfo("Slave disconnected: " + actor)
      actorToSlave.get(actor).foreach(s => removeSlave(s)) // Remove slave, if any, at actor
  }

  def addSlave(host: String, slavePort: Int, cores: Int, memory: Int): SlaveInfo = {
    val slave = new SlaveInfo(newSlaveId(), host, slavePort, cores, memory, sender)
    slaves(slave.id) = slave
    actorToSlave(sender) = slave
    addressToSlave(sender.path.address) = slave
    return slave
  }

  def removeSlave(slave: SlaveInfo) {
    logInfo("Removing slave " + slave.id + " on " + slave.host + ":" + slave.port)
    slaves -= slave.id
    actorToSlave -= slave.actor
    addressToSlave -= slave.actor.path.address
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
