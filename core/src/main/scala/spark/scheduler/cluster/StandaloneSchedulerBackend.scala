package spark.scheduler.cluster

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import akka.actor._
import akka.util.duration._
import akka.pattern.ask

import spark.{SparkException, Logging, TaskState}
import akka.dispatch.Await
import java.util.concurrent.atomic.AtomicInteger
import akka.remote.{RemoteClientShutdown, RemoteClientDisconnected, RemoteClientLifeCycleEvent}

/**
 * A standalone scheduler backend, which waits for standalone executors to connect to it through
 * Akka. These may be executed in a variety of ways, such as Mesos tasks for the coarse-grained
 * Mesos mode or standalone processes for Spark's standalone deploy mode (spark.deploy.*).
 */
private[spark]
class StandaloneSchedulerBackend(scheduler: ClusterScheduler, actorSystem: ActorSystem)
  extends SchedulerBackend with Logging {

  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  var totalCoreCount = new AtomicInteger(0)

  class MasterActor(sparkProperties: Seq[(String, String)]) extends Actor {
    val slaveActor = new HashMap[String, ActorRef]
    val slaveAddress = new HashMap[String, Address]
    val slaveHost = new HashMap[String, String]
    val freeCores = new HashMap[String, Int]
    val actorToExecutorId = new HashMap[ActorRef, String]
    val addressToExecutorId = new HashMap[Address, String]

    override def preStart() {
      // Listen for remote client disconnection events, since they don't go through Akka's watch()
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
    }

    def receive = {
      case RegisterSlave(executorId, host, cores) =>
        if (slaveActor.contains(executorId)) {
          sender ! RegisterSlaveFailed("Duplicate executor ID: " + executorId)
        } else {
          logInfo("Registered executor: " + sender + " with ID " + executorId)
          sender ! RegisteredSlave(sparkProperties)
          context.watch(sender)
          slaveActor(executorId) = sender
          slaveHost(executorId) = host
          freeCores(executorId) = cores
          slaveAddress(executorId) = sender.path.address
          actorToExecutorId(sender) = executorId
          addressToExecutorId(sender.path.address) = executorId
          totalCoreCount.addAndGet(cores)
          makeOffers()
        }

      case StatusUpdate(executorId, taskId, state, data) =>
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          freeCores(executorId) += 1
          makeOffers(executorId)
        }

      case ReviveOffers =>
        makeOffers()

      case StopMaster =>
        sender ! true
        context.stop(self)

      case Terminated(actor) =>
        actorToExecutorId.get(actor).foreach(removeSlave(_, "Akka actor terminated"))

      case RemoteClientDisconnected(transport, address) =>
        addressToExecutorId.get(address).foreach(removeSlave(_, "remote Akka client disconnected"))

      case RemoteClientShutdown(transport, address) =>
        addressToExecutorId.get(address).foreach(removeSlave(_, "remote Akka client shutdown"))
    }

    // Make fake resource offers on all slaves
    def makeOffers() {
      launchTasks(scheduler.resourceOffers(
        slaveHost.toArray.map {case (id, host) => new WorkerOffer(id, host, freeCores(id))}))
    }

    // Make fake resource offers on just one slave
    def makeOffers(executorId: String) {
      launchTasks(scheduler.resourceOffers(
        Seq(new WorkerOffer(executorId, slaveHost(executorId), freeCores(executorId)))))
    }

    // Launch tasks returned by a set of resource offers
    def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        freeCores(task.executorId) -= 1
        slaveActor(task.executorId) ! LaunchTask(task)
      }
    }

    // Remove a disconnected slave from the cluster
    def removeSlave(executorId: String, reason: String) {
      logInfo("Slave " + executorId + " disconnected, so removing it")
      val numCores = freeCores(executorId)
      actorToExecutorId -= slaveActor(executorId)
      addressToExecutorId -= slaveAddress(executorId)
      slaveActor -= executorId
      slaveHost -= executorId
      freeCores -= executorId
      slaveHost -= executorId
      totalCoreCount.addAndGet(-numCores)
      scheduler.executorLost(executorId, SlaveLost(reason))
    }
  }

  var masterActor: ActorRef = null
  val taskIdsOnSlave = new HashMap[String, HashSet[String]]

  def start() {
    val properties = new ArrayBuffer[(String, String)]
    val iterator = System.getProperties.entrySet.iterator
    while (iterator.hasNext) {
      val entry = iterator.next
      val (key, value) = (entry.getKey.toString, entry.getValue.toString)
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }
    masterActor = actorSystem.actorOf(
      Props(new MasterActor(properties)), name = StandaloneSchedulerBackend.ACTOR_NAME)
  }

  def stop() {
    try {
      if (masterActor != null) {
        val timeout = 5.seconds
        val future = masterActor.ask(StopMaster)(timeout)
        Await.result(future, timeout)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's master actor", e)
    }
  }

  def reviveOffers() {
    masterActor ! ReviveOffers
  }

  def defaultParallelism(): Int = math.max(totalCoreCount.get(), 2)
}

private[spark] object StandaloneSchedulerBackend {
  val ACTOR_NAME = "StandaloneScheduler"
}
