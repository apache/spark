package spark.deploy.worker


import akka.actor.{ActorRef, Terminated, Props, Actor}
import spark.{Logging, Utils}
import spark.util.AkkaUtils
import spark.deploy.{RegisterWorkerFailed, RegisterWorker, RegisteredWorker}
import akka.remote.{RemoteClientShutdown, RemoteClientDisconnected, RemoteClientLifeCycleEvent}
import java.text.SimpleDateFormat
import java.util.Date

class Worker(ip: String, port: Int, webUiPort: Int, cores: Int, memory: Int, masterUrl: String)
  extends Actor with Logging {

  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For worker and executor IDs
  val MASTER_REGEX = "spark://([^:]+):([0-9]+)".r

  var master: ActorRef = null

  val workerId = generateWorkerId()

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
      case MASTER_REGEX(masterHost, masterPort) => {
        logInfo("Connecting to master spark://" + masterHost + ":" + masterPort)
        val akkaUrl = "akka://spark@%s:%s/user/Master".format(masterHost, masterPort)
        try {
          master = context.actorFor(akkaUrl)
          master ! RegisterWorker(workerId, ip, port, cores, memory)
          context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
          context.watch(master) // Doesn't work with remote actors, but useful for testing
        } catch {
          case e: Exception =>
            logError("Failed to connect to master", e)
            System.exit(1)
        }
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
    case RegisteredWorker =>
      logInfo("Successfully registered with master")

    case RegisterWorkerFailed(message) =>
      logError("Worker registration failed: " + message)
      System.exit(1)

    case Terminated(_) | RemoteClientDisconnected(_, _) | RemoteClientShutdown(_, _) =>
      masterDisconnected()
  }

  def masterDisconnected() {
    // TODO: It would be nice to try to reconnect to the master, but just shut down for now.
    // (Note that if reconnecting we would also need to assign IDs differently.)
    logError("Connection to master failed! Shutting down.")
    System.exit(1)
  }

  def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(DATE_FORMAT.format(new Date), ip, port)
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
