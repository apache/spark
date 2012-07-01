package spark.deploy.worker


import akka.actor.{ActorRef, Terminated, Props, Actor}
import akka.pattern.ask
import akka.util.duration._
import spark.{SparkException, Logging, Utils}
import spark.util.{IntParam, AkkaUtils}
import spark.deploy.{RegisterSlave, RegisteredSlave}
import akka.dispatch.Await
import akka.remote.{RemoteClientShutdown, RemoteClientDisconnected, RemoteClientLifeCycleEvent}

class Worker(ip: String, port: Int, webUiPort: Int, cores: Int, memory: Int, masterUrl: String)
  extends Actor with Logging {

  val MASTER_REGEX = "spark://([^:]+):([0-9]+)".r

  var master: ActorRef = null
  var clusterId: String = null
  var slaveId: Int = 0

  var coresUsed = 0
  var memoryUsed = 0

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  override def preStart() {
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      ip, port, cores, Utils.memoryMegabytesToString(memory)))
    connectToMaster()
    startWebUi()
  }

  def connectToMaster() {
    masterUrl match {
      case MASTER_REGEX(masterHost, masterPort) =>
        logInfo("Connecting to master spark://" + masterHost + ":" + masterPort)
        val akkaUrl = "akka://spark@%s:%s/user/Master".format(masterHost, masterPort)
        try {
          master = context.actorFor(akkaUrl)
          master ! RegisterSlave(ip, port, cores, memory)
          context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
          context.watch(master)  // Doesn't work with remote actors, but useful for testing
        } catch {
          case e: Exception =>
            logError("Failed to connect to master", e)
            System.exit(1)
        }

      case _ =>
        logError("Invalid master URL: " + masterUrl)
        System.exit(1)
    }
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
    case RegisteredSlave(clusterId_, slaveId_) =>
      this.clusterId = clusterId_
      this.slaveId = slaveId_
      logInfo("Registered with master, cluster ID = " + clusterId + ", slave ID = " + slaveId)

    case RemoteClientDisconnected(_, _) =>
      masterDisconnected()

    case RemoteClientShutdown(_, _) =>
      masterDisconnected()

    case Terminated(_) =>
      masterDisconnected()
  }

  def masterDisconnected() {
    // Not sure what to do here exactly, so just shut down for now.
    logError("Connection to master failed! Shutting down.")
    System.exit(1)
  }
}

object Worker {
  def main(argStrings: Array[String]) {
    val args = new WorkerArguments(argStrings)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", args.ip, args.port)
    val actor = actorSystem.actorOf(
      Props(new Worker(args.ip, boundPort, args.webUiPort, args.cores, args.memory, args.master)),
      name = "Worker")
    actorSystem.awaitTermination()
  }
}
