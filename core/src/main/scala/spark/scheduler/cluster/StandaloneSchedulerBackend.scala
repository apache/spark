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
    val actorToSlaveId = new HashMap[ActorRef, String]
    val addressToSlaveId = new HashMap[Address, String]

    override def preStart() {
      // Listen for remote client disconnection events, since they don't go through Akka's watch()
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
    }

    def receive = {
      case RegisterSlave(slaveId, host, cores) =>
        if (slaveActor.contains(slaveId)) {
          sender ! RegisterSlaveFailed("Duplicate slave ID: " + slaveId)
        } else {
          logInfo("Registered slave: " + sender + " with ID " + slaveId)
          sender ! RegisteredSlave(sparkProperties)
          context.watch(sender)
          slaveActor(slaveId) = sender
          slaveHost(slaveId) = host
          freeCores(slaveId) = cores
          slaveAddress(slaveId) = sender.path.address
          actorToSlaveId(sender) = slaveId
          addressToSlaveId(sender.path.address) = slaveId
          totalCoreCount.addAndGet(cores)
          makeOffers()
        }

      case StatusUpdate(slaveId, taskId, state, data) =>
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          freeCores(slaveId) += 1
          makeOffers(slaveId)
        }

      case ReviveOffers =>
        makeOffers()

      case StopMaster =>
        sender ! true
        context.stop(self)

      case Terminated(actor) =>
        actorToSlaveId.get(actor).foreach(removeSlave)

      case RemoteClientDisconnected(transport, address) =>
        addressToSlaveId.get(address).foreach(removeSlave)

      case RemoteClientShutdown(transport, address) =>
        addressToSlaveId.get(address).foreach(removeSlave)
    }

    // Make fake resource offers on all slaves
    def makeOffers() {
      launchTasks(scheduler.resourceOffers(
        slaveHost.toArray.map {case (id, host) => new WorkerOffer(id, host, freeCores(id))}))
    }

    // Make fake resource offers on just one slave
    def makeOffers(slaveId: String) {
      launchTasks(scheduler.resourceOffers(
        Seq(new WorkerOffer(slaveId, slaveHost(slaveId), freeCores(slaveId)))))
    }

    // Launch tasks returned by a set of resource offers
    def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        freeCores(task.slaveId) -= 1
        slaveActor(task.slaveId) ! LaunchTask(task)
      }
    }

    // Remove a disconnected slave from the cluster
    def removeSlave(slaveId: String) {
      logInfo("Slave " + slaveId + " disconnected, so removing it")
      val numCores = freeCores(slaveId)
      actorToSlaveId -= slaveActor(slaveId)
      addressToSlaveId -= slaveAddress(slaveId)
      slaveActor -= slaveId
      slaveHost -= slaveId
      freeCores -= slaveId
      slaveHost -= slaveId
      totalCoreCount.addAndGet(-numCores)
      scheduler.slaveLost(slaveId)
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
