package spark.scheduler.cluster

import scala.collection.mutable.ArrayBuffer

import spark.Logging
import spark.scheduler.cluster.SchedulingMode.SchedulingMode
/**
 * An Schedulable entity that represent collection of TaskSetManager
 */
private[spark] class Pool(val poolName: String,val schedulingMode: SchedulingMode, initMinShare:Int, initWeight:Int) extends Schedulable with Logging
{

  var activeTaskSetsQueue = new ArrayBuffer[TaskSetManager]

  var weight = initWeight
  var minShare = initMinShare
  var runningTasks = 0

  val priority = 0
  val stageId = 0

  var taskSetSchedulingAlgorithm: SchedulingAlgorithm =
  {
    schedulingMode match
    {
      case SchedulingMode.FAIR =>
        val schedule = new FairSchedulingAlgorithm()
        schedule
      case SchedulingMode.FIFO =>
        val schedule = new FIFOSchedulingAlgorithm()
        schedule
    }
  }

  def addTaskSetManager(manager:TaskSetManager)
  {
    activeTaskSetsQueue += manager
  }

  def removeTaskSetManager(manager:TaskSetManager)
  {
    activeTaskSetsQueue -= manager
  }

  def removeExecutor(executorId: String, host: String)
  {
      activeTaskSetsQueue.foreach(_.executorLost(executorId,host))
  }

  def checkSpeculatableTasks(): Boolean =
  {
    var shouldRevive = false
    for(ts <- activeTaskSetsQueue)
    {
      shouldRevive |= ts.checkSpeculatableTasks()
    }
    return shouldRevive
  }

  def receiveOffer(execId:String,host:String,availableCpus:Double):Option[TaskDescription] =
  {
    val sortedActiveTasksSetQueue = activeTaskSetsQueue.sortWith(taskSetSchedulingAlgorithm.comparator)
    for(manager <- sortedActiveTasksSetQueue)
    {
    logDebug("poolname:%s,taskSetId:%s,taskNum:%d,minShares:%d,weight:%d,runningTasks:%d".format(poolName,manager.taskSet.id,manager.numTasks,manager.minShare,manager.weight,manager.runningTasks))
    }
    for(manager <- sortedActiveTasksSetQueue)
    {
        val task = manager.slaveOffer(execId,host,availableCpus)
        if (task != None)
        {
          manager.runningTasks += 1
          return task
        }
    }
    return None
  }
}
