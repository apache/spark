package spark.scheduler.cluster

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import spark.Logging
import spark.scheduler.cluster.SchedulingMode.SchedulingMode

/**
 * An Schedulable entity that represent collection of Pools or TaskSetManagers
 */

private[spark] class Pool(
    val poolName: String,
    val schedulingMode: SchedulingMode,
    initMinShare: Int,
    initWeight: Int)
  extends Schedulable
  with Logging {

  var schedulableQueue = new ArrayBuffer[Schedulable]
  var schedulableNameToSchedulable = new HashMap[String, Schedulable]

  var weight = initWeight
  var minShare = initMinShare
  var runningTasks = 0

  var priority = 0
  var stageId = 0
  var name = poolName
  var parent:Schedulable = null

  var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
    }
  }

  override def addSchedulable(schedulable: Schedulable) {
    schedulableQueue += schedulable
    schedulableNameToSchedulable(schedulable.name) = schedulable
    schedulable.parent= this
  }

  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue -= schedulable
    schedulableNameToSchedulable -= schedulable.name
  }

  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.contains(schedulableName)) {
      return schedulableNameToSchedulable(schedulableName)
    }
    for (schedulable <- schedulableQueue) {
      var sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    return null
  }

  override def executorLost(executorId: String, host: String) {
    schedulableQueue.foreach(_.executorLost(executorId, host))
  }

  override def checkSpeculatableTasks(): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue) {
      shouldRevive |= schedulable.checkSpeculatableTasks()
    }
    return shouldRevive
  }

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue = schedulableQueue.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue()
    }
    return sortedTaskSetQueue
  }

  override def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  override def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
