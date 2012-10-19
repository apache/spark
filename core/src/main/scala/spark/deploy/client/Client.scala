package spark.deploy.client

import spark.deploy._
import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.pattern.AskTimeoutException
import spark.{SparkException, Logging}
import akka.remote.RemoteClientLifeCycleEvent
import akka.remote.RemoteClientShutdown
import spark.deploy.RegisterJob
import akka.remote.RemoteClientDisconnected
import akka.actor.Terminated
import akka.dispatch.Await

/**
 * The main class used to talk to a Spark deploy cluster. Takes a master URL, a job description,
 * and a listener for job events, and calls back the listener when various events occur.
 */
private[spark] class Client(
    actorSystem: ActorSystem,
    masterUrl: String,
    jobDescription: JobDescription,
    listener: ClientListener)
  extends Logging {

  val MASTER_REGEX = "spark://([^:]+):([0-9]+)".r

  var actor: ActorRef = null
  var jobId: String = null

  if (MASTER_REGEX.unapplySeq(masterUrl) == None) {
    throw new SparkException("Invalid master URL: " + masterUrl)
  }

  class ClientActor extends Actor with Logging {
    var master: ActorRef = null
    var alreadyDisconnected = false  // To avoid calling listener.disconnected() multiple times

    override def preStart() {
      val Seq(masterHost, masterPort) = MASTER_REGEX.unapplySeq(masterUrl).get
      logInfo("Connecting to master spark://" + masterHost + ":" + masterPort)
      val akkaUrl = "akka://spark@%s:%s/user/Master".format(masterHost, masterPort)
      try {
        master = context.actorFor(akkaUrl)
        master ! RegisterJob(jobDescription)
        context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
        context.watch(master)  // Doesn't work with remote actors, but useful for testing
      } catch {
        case e: Exception =>
          logError("Failed to connect to master", e)
          markDisconnected()
          context.stop(self)
      }
    }

    override def receive = {
      case RegisteredJob(jobId_) =>
        jobId = jobId_
        listener.connected(jobId)

      case ExecutorAdded(id: Int, workerId: String, host: String, cores: Int, memory: Int) =>
        val fullId = jobId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, host, cores))
        listener.executorAdded(fullId, workerId, host, cores, memory)

      case ExecutorUpdated(id, state, message) =>
        val fullId = jobId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""))
        }

      case Terminated(_) | RemoteClientDisconnected(_, _) | RemoteClientShutdown(_, _) =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case StopClient =>
        markDisconnected()
        sender ! true
        context.stop(self)
    }

    /**
     * Notify the listener that we disconnected, if we hadn't already done so before.
     */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }
  }

  def start() {
    // Just launch an actor; it will call back into the listener.
    actor = actorSystem.actorOf(Props(new ClientActor))
  }

  def stop() {
    if (actor != null) {
      try {
        val timeout = 1.seconds
        val future = actor.ask(StopClient)(timeout)
        Await.result(future, timeout)
      } catch {
        case e: AskTimeoutException =>  // Ignore it, maybe master went away
      }
      actor = null
    }
  }
}
