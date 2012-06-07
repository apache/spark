package spark.scheduler.mesos

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.{ArrayList => JArrayList}
import java.util.{List => JList}
import java.util.{HashMap => JHashMap}
import java.util.concurrent._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Map
import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._
import scala.math.Ordering

import akka.actor._
import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.Channel
import akka.serialization.RemoteActorSerialization._

import com.google.protobuf.ByteString

import org.apache.mesos.{Scheduler => MScheduler}
import org.apache.mesos._
import org.apache.mesos.Protos.{TaskInfo => MTaskInfo, _}

import spark._
import spark.scheduler._

sealed trait CoarseMesosSchedulerMessage
case class RegisterSlave(slaveId: String, host: String, port: Int) extends CoarseMesosSchedulerMessage
case class StatusUpdate(slaveId: String, status: TaskStatus) extends CoarseMesosSchedulerMessage
case class LaunchTask(slaveId: String, task: MTaskInfo) extends CoarseMesosSchedulerMessage
case class ReviveOffers() extends CoarseMesosSchedulerMessage

case class FakeOffer(slaveId: String, host: String, cores: Int)

/**
 * Mesos scheduler that uses coarse-grained tasks and does its own fine-grained scheduling inside
 * them using Akka actors for messaging. Clients should first call start(), then submit task sets
 * through the runTasks method.
 *
 * TODO: This is a pretty big hack for now.
 */
class CoarseMesosScheduler(
    sc: SparkContext,
    master: String,
    frameworkName: String)
  extends MesosScheduler(sc, master, frameworkName) {

  val CORES_PER_SLAVE = System.getProperty("spark.coarseMesosScheduler.coresPerSlave", "4").toInt

  class MasterActor extends Actor {
    val slaveActor = new HashMap[String, ActorRef]
    val slaveHost = new HashMap[String, String]
    val freeCores = new HashMap[String, Int]
   
    def receive = {
      case RegisterSlave(slaveId, host, port) =>
        slaveActor(slaveId) = remote.actorFor("WorkerActor", host, port)
        logInfo("Slave actor: " + slaveActor(slaveId))
        slaveHost(slaveId) = host
        freeCores(slaveId) = CORES_PER_SLAVE
        makeFakeOffers()

      case StatusUpdate(slaveId, status) =>
        fakeStatusUpdate(status)
        if (isFinished(status.getState)) {
          freeCores(slaveId) += 1
          makeFakeOffers(slaveId)
        }

      case LaunchTask(slaveId, task) =>
        freeCores(slaveId) -= 1
        slaveActor(slaveId) ! LaunchTask(slaveId, task)

      case ReviveOffers() =>
        logInfo("Reviving offers")
        makeFakeOffers()
    }

    // Make fake resource offers for all slaves
    def makeFakeOffers() {
      fakeResourceOffers(slaveHost.toSeq.map{case (id, host) => FakeOffer(id, host, freeCores(id))})
    }

    // Make fake resource offers for all slaves
    def makeFakeOffers(slaveId: String) {
      fakeResourceOffers(Seq(FakeOffer(slaveId, slaveHost(slaveId), freeCores(slaveId))))
    }
  }

  val masterActor: ActorRef = actorOf(new MasterActor)
  remote.register("MasterActor", masterActor)
  masterActor.start()

  val taskIdsOnSlave = new HashMap[String, HashSet[String]]

  /**
   * Method called by Mesos to offer resources on slaves. We resond by asking our active task sets 
   * for tasks in order of priority. We fill each node with tasks in a round-robin manner so that
   * tasks are balanced across the cluster.
   */
  override def resourceOffers(d: SchedulerDriver, offers: JList[Offer]) {
    synchronized {
      val tasks = offers.map(o => new JArrayList[MTaskInfo])
      for (i <- 0 until offers.size) {
        val o = offers.get(i)
        val slaveId = o.getSlaveId.getValue
        if (!slaveIdToHost.contains(slaveId)) {
          slaveIdToHost(slaveId) = o.getHostname
          hostsAlive += o.getHostname
          taskIdsOnSlave(slaveId) = new HashSet[String]
          // Launch an infinite task on the node that will talk to the MasterActor to get fake tasks
          val cpuRes = Resource.newBuilder()
              .setName("cpus")
              .setType(Value.Type.SCALAR)
              .setScalar(Value.Scalar.newBuilder().setValue(1).build())
              .build()
          val task = new WorkerTask(slaveId, o.getHostname)
          val serializedTask = Utils.serialize(task)
          tasks(i).add(MTaskInfo.newBuilder()
              .setTaskId(newTaskId())
              .setSlaveId(o.getSlaveId)
              .setExecutor(executorInfo)
              .setName("worker task")
              .addResources(cpuRes)
              .setData(ByteString.copyFrom(serializedTask))
              .build())
        }
      }
      val filters = Filters.newBuilder().setRefuseSeconds(10).build()
      for (i <- 0 until offers.size) {
        d.launchTasks(offers(i).getId(), tasks(i), filters)
      }
    }
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus) {
    val tid = status.getTaskId.getValue
    var taskSetToUpdate: Option[TaskSetManager] = None
    var taskFailed = false
    synchronized {
      try {
        taskIdToTaskSetId.get(tid) match {
          case Some(taskSetId) =>
            if (activeTaskSets.contains(taskSetId)) {
              //activeTaskSets(taskSetId).statusUpdate(status)
              taskSetToUpdate = Some(activeTaskSets(taskSetId))
            }
            if (isFinished(status.getState)) {
              taskIdToTaskSetId.remove(tid)
              if (taskSetTaskIds.contains(taskSetId)) {
                taskSetTaskIds(taskSetId) -= tid
              }
              val slaveId = taskIdToSlaveId(tid)
              taskIdToSlaveId -= tid
              taskIdsOnSlave(slaveId) -= tid
            }
            if (status.getState == TaskState.TASK_FAILED) {
              taskFailed = true
            }
          case None =>
            logInfo("Ignoring update from TID " + tid + " because its task set is gone")
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the task set and DAGScheduler without holding a lock on this, because that can deadlock
    if (taskSetToUpdate != None) {
      taskSetToUpdate.get.statusUpdate(status)
    }
    if (taskFailed) {
      // Revive offers if a task had failed for some reason other than host lost
      reviveOffers()
    }
  }

  override def slaveLost(d: SchedulerDriver, s: SlaveID) {
    logInfo("Slave lost: " + s.getValue)
    var failedHost: Option[String] = None
    var lostTids: Option[HashSet[String]] = None
    synchronized {
      val slaveId = s.getValue
      val host = slaveIdToHost(slaveId)
      if (hostsAlive.contains(host)) {
        slaveIdsWithExecutors -= slaveId
        hostsAlive -= host
        failedHost = Some(host)
        lostTids = Some(taskIdsOnSlave(slaveId))
        logInfo("failedHost: " + host)
        logInfo("lostTids: " + lostTids)
        taskIdsOnSlave -= slaveId
        activeTaskSetsQueue.foreach(_.hostLost(host))
      }
    }
    if (failedHost != None) {
      // Report all the tasks on the failed host as lost, without holding a lock on this
      for (tid <- lostTids.get; taskSetId <- taskIdToTaskSetId.get(tid)) {
        // TODO: Maybe call our statusUpdate() instead to clean our internal data structures
        activeTaskSets(taskSetId).statusUpdate(TaskStatus.newBuilder()
          .setTaskId(TaskID.newBuilder().setValue(tid).build())
          .setState(TaskState.TASK_LOST)
          .build())
      }
      // Also report the loss to the DAGScheduler
      listener.hostLost(failedHost.get)
      reviveOffers();
    }
  }

  override def offerRescinded(d: SchedulerDriver, o: OfferID) {}

  // Check for speculatable tasks in all our active jobs.
  override def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      for (ts <- activeTaskSetsQueue) {
        shouldRevive |= ts.checkSpeculatableTasks()
      }
    }
    if (shouldRevive) {
      reviveOffers()
    }
  }


  val lock2 = new Object
  var firstWait = true

  override def waitForRegister() {
    lock2.synchronized {
      if (firstWait) {
        super.waitForRegister()
        Thread.sleep(5000)
        firstWait = false
      }
    }
  }

  def fakeStatusUpdate(status: TaskStatus) {
    statusUpdate(driver, status)
  }

  def fakeResourceOffers(offers: Seq[FakeOffer]) {
    logDebug("fakeResourceOffers: " + offers)
    val availableCpus = offers.map(_.cores.toDouble).toArray
    var launchedTask = false
    for (manager <- activeTaskSetsQueue.sortBy(m => (m.taskSet.priority, m.taskSet.stageId))) {
      do {
        launchedTask = false
        for (i <- 0 until offers.size if hostsAlive.contains(offers(i).host)) {
          manager.slaveOffer(offers(i).slaveId, offers(i).host, availableCpus(i)) match {
            case Some(task) => 
              val tid = task.getTaskId.getValue
              val sid = offers(i).slaveId
              taskIdToTaskSetId(tid) = manager.taskSet.id
              taskSetTaskIds(manager.taskSet.id) += tid
              taskIdToSlaveId(tid) = sid
              taskIdsOnSlave(sid) += tid
              slaveIdsWithExecutors += sid
              availableCpus(i) -= getResource(task.getResourcesList(), "cpus")
              launchedTask = true
              masterActor ! LaunchTask(sid, task)
              
            case None => {}
          }
        }
      } while (launchedTask)
    }
  }

  override def reviveOffers() {
    masterActor ! ReviveOffers()
  }
}

class WorkerTask(slaveId: String, host: String) extends Task[Unit](-1) {
  generation = 0

  def run(id: Int): Unit = {
    val actor = actorOf(new WorkerActor(slaveId, host))
    if (!remote.isRunning) {
      remote.start(Utils.localIpAddress, 7078)
    }
    remote.register("WorkerActor", actor)
    actor.start()
    while (true) {
      Thread.sleep(10000)
    }
  }
}

class WorkerActor(slaveId: String, host: String) extends Actor with Logging {
  val env = SparkEnv.get
  val classLoader = currentThread.getContextClassLoader
  val threadPool = new ThreadPoolExecutor(
    1, 128, 600, TimeUnit.SECONDS, new SynchronousQueue[Runnable])

  val masterIp: String = System.getProperty("spark.master.host", "localhost")
  val masterPort: Int = System.getProperty("spark.master.port", "7077").toInt
  val masterActor = remote.actorFor("MasterActor", masterIp, masterPort)

  class TaskRunner(desc: MTaskInfo)
  extends Runnable {
    override def run() = {
      val tid = desc.getTaskId.getValue
      logInfo("Running task ID " + tid)
      try {
        SparkEnv.set(env)
        Thread.currentThread.setContextClassLoader(classLoader)
        Accumulators.clear
        val task = Utils.deserialize[Task[Any]](desc.getData.toByteArray, classLoader)
        env.mapOutputTracker.updateGeneration(task.generation)
        val value = task.run(tid.toInt)
        val accumUpdates = Accumulators.values
        val result = new TaskResult(value, accumUpdates)
        masterActor ! StatusUpdate(slaveId, TaskStatus.newBuilder()
            .setTaskId(desc.getTaskId)
            .setState(TaskState.TASK_FINISHED)
            .setData(ByteString.copyFrom(Utils.serialize(result)))
            .build())
        logInfo("Finished task ID " + tid)
      } catch {
        case ffe: FetchFailedException => {
          val reason = ffe.toTaskEndReason
          masterActor ! StatusUpdate(slaveId, TaskStatus.newBuilder()
              .setTaskId(desc.getTaskId)
              .setState(TaskState.TASK_FAILED)
              .setData(ByteString.copyFrom(Utils.serialize(reason)))
              .build())
        }
        case t: Throwable => {
          val reason = ExceptionFailure(t)
          masterActor ! StatusUpdate(slaveId, TaskStatus.newBuilder()
              .setTaskId(desc.getTaskId)
              .setState(TaskState.TASK_FAILED)
              .setData(ByteString.copyFrom(Utils.serialize(reason)))
              .build())

          // TODO: Should we exit the whole executor here? On the one hand, the failed task may
          // have left some weird state around depending on when the exception was thrown, but on
          // the other hand, maybe we could detect that when future tasks fail and exit then.
          logError("Exception in task ID " + tid, t)
          //System.exit(1)
        }
      }
    }
  }

  override def preStart {
    val ref = toRemoteActorRefProtocol(self).toByteArray
    logInfo("Registering with master")
    masterActor ! RegisterSlave(slaveId, host, remote.address.getPort)
  }

  override def receive = {
    case LaunchTask(slaveId, task) =>
      threadPool.execute(new TaskRunner(task))    
  }
}
