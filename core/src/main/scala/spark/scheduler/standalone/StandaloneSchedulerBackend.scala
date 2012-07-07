package spark.scheduler.standalone

import scala.collection.mutable.{HashMap, HashSet}

import akka.actor.{Props, Actor, ActorRef, ActorSystem}
import akka.util.duration._
import akka.pattern.ask

import spark.{SparkException, Logging, TaskState}
import spark.TaskState.TaskState
import spark.scheduler.cluster.{WorkerOffer, ClusterScheduler, SchedulerBackend}
import akka.dispatch.Await
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

/**
 * A standalone scheduler backend, which waits for standalone executors to connect to it through
 * Akka. These may be executed in a variety of ways, such as Mesos tasks for the coarse-grained
 * Mesos mode or standalone processes for Spark's standalone deploy mode (spark.deploy.*).
 */
class StandaloneSchedulerBackend(scheduler: ClusterScheduler, actorSystem: ActorSystem)
  extends SchedulerBackend
  with Logging {

  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  var totalCoreCount = new AtomicInteger(0)

  class MasterActor extends Actor {
    val slaveActor = new HashMap[String, ActorRef]
    val slaveHost = new HashMap[String, String]
    val freeCores = new HashMap[String, Int]

    def receive = {
      case RegisterSlave(slaveId, host, cores) =>
        slaveActor(slaveId) = sender
        logInfo("Registered slave: " + sender + " with ID " + slaveId)
        slaveHost(slaveId) = host
        freeCores(slaveId) = cores
        totalCoreCount.addAndGet(cores)
        makeOffers()

      case StatusUpdate(slaveId, taskId, state, data) =>
        scheduler.statusUpdate(taskId, state, ByteBuffer.wrap(data))
        if (TaskState.isFinished(state)) {
          freeCores(slaveId) += 1
          makeOffers(slaveId)
        }

      case LaunchTask(slaveId, task) =>
        freeCores(slaveId) -= 1
        slaveActor(slaveId) ! LaunchTask(slaveId, task)

      case ReviveOffers =>
        makeOffers()

      case StopMaster =>
        sender ! true
        context.stop(self)

      // TODO: Deal with nodes disconnecting too! (Including decreasing totalCoreCount)
    }

    // Make fake resource offers on all slaves
    def makeOffers() {
      scheduler.resourceOffers(
        slaveHost.toArray.map {case (id, host) => new WorkerOffer(id, host, freeCores(id))})
    }

    // Make fake resource offers on just one slave
    def makeOffers(slaveId: String) {
      scheduler.resourceOffers(
        Seq(new WorkerOffer(slaveId, slaveHost(slaveId), freeCores(slaveId))))
    }
  }

  var masterActor: ActorRef = null
  val taskIdsOnSlave = new HashMap[String, HashSet[String]]

  def start() {
    masterActor = actorSystem.actorOf(
      Props(new MasterActor), name = StandaloneSchedulerBackend.ACTOR_NAME)
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
        throw new SparkException("Error stopping standalone scheduler master actor", e)
    }
  }

  def reviveOffers() {
    masterActor ! ReviveOffers
  }

  def defaultParallelism(): Int = totalCoreCount.get()
}

object StandaloneSchedulerBackend {
  val ACTOR_NAME = "StandaloneScheduler"
}
