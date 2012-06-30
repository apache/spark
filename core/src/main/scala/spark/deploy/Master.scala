package spark.deploy

import akka.actor.{ActorRef, Props, Actor, ActorSystem}
import spark.{Logging, Utils}
import scala.collection.immutable.{::, Nil}
import spark.util.{AkkaUtils, IntParam}
import cc.spray.Directives

sealed trait MasterMessage
case class RegisterSlave(host: String, port: Int, cores: Int, memory: Int) extends MasterMessage

class Master(ip: String, port: Int, webUiPort: Int) extends Actor with Logging {
  override def preStart() {
    logInfo("Starting Spark master at spark://" + ip + ":" + port)
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
    case RegisterSlave(host, slavePort, cores, memory) =>
      logInfo("Registering slave %s:%d with %d cores, %s RAM".format(
        host, slavePort, cores, Utils.memoryBytesToString(memory * 1024L)))
  }
}

object Master {
  def main(args: Array[String]) {
    val params = new MasterArguments(args)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", params.ip, params.port)
    val actor = actorSystem.actorOf(
      Props(new Master(params.ip, boundPort, params.webUiPort)), name = "Master")
    actorSystem.awaitTermination()
  }
}
