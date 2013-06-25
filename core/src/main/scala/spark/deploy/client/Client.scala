package spark.deploy.client

import spark.deploy._
import akka.actor._
import akka.pattern.ask
import akka.util.Duration
import akka.util.duration._
import akka.pattern.AskTimeoutException
import spark.{SparkException, Logging}
import akka.remote.RemoteClientLifeCycleEvent
import akka.remote.RemoteClientShutdown
import spark.deploy.RegisterApplication
import spark.deploy.master.Master
import akka.remote.RemoteClientDisconnected
import akka.actor.Terminated
import akka.dispatch.Await

/**
 * The main class used to talk to a Spark deploy cluster. Takes a master URL, an app description,
 * and a listener for cluster events, and calls back the listener when various events occur.
 */
private[spark] class Client(
    actorSystem: ActorSystem,
    masterUrl: String,
    appDescription: ApplicationDescription,
    listener: ClientListener)
  extends Logging {

  var actor: ActorRef = null
  var appId: String = null

  class ClientActor extends Actor with Logging {
    var master: ActorRef = null
    var masterAddress: Address = null
    var alreadyDisconnected = false  // To avoid calling listener.disconnected() multiple times

    override def preStart() {
      logInfo("Connecting to master " + masterUrl)
      try {
        master = context.actorFor(Master.toAkkaUrl(masterUrl))
        masterAddress = master.path.address
        master ! RegisterApplication(appDescription)
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
      case RegisteredApplication(appId_) =>
        appId = appId_
        listener.connected(appId)

      case ApplicationRemoved(message) =>
        logError("Master removed our application: %s; stopping client".format(message))
        markDisconnected()
        context.stop(self)

      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, hostPort, cores))
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

      case ExecutorUpdated(id, state, message, exitStatus) =>
        val fullId = appId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus)
        }

      case Terminated(actor_) if actor_ == master =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case RemoteClientDisconnected(transport, address) if address == masterAddress =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case RemoteClientShutdown(transport, address) if address == masterAddress =>
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
        val timeout = Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")
        val future = actor.ask(StopClient)(timeout)
        Await.result(future, timeout)
      } catch {
        case e: AskTimeoutException =>  // Ignore it, maybe master went away
      }
      actor = null
    }
  }
}
