package spark.scheduler.cluster

import scala.collection.mutable.ArrayBuffer

import spark.Logging
import spark.scheduler.cluster.SchedulingMode.SchedulingMode
/**
 * An interface for 
 * 
 */
private[spark] class Pool(val poolName: String, schedulingMode: SchedulingMode,val minShare:Int, val weight:Int) extends Schedulable with Logging {
  
  var activeTaskSetsQueue = new ArrayBuffer[TaskSetManager]
  var numRunningTasks: Int = 0
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
  
  override def getMinShare():Int =
  {
    return minShare
  }

  override def getRunningTasks():Int = 
  {
    return numRunningTasks
  }

  def setRunningTasks(taskNum : Int)
  {
    numRunningTasks = taskNum
  }

  override def getWeight(): Int = 
  {
    return weight
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
      
    logDebug("taskSetId:%s,taskNum:%d,minShares:%d,weight:%d,runningTasks:%d".format(manager.taskSet.id,manager.numTasks,manager.getMinShare(),manager.getWeight(),manager.getRunningTasks()))  
    }
    for(manager <- sortedActiveTasksSetQueue)
    {
        val task = manager.slaveOffer(execId,host,availableCpus)
        if (task != None)
        {
          manager.setRunningTasks(manager.getRunningTasks() + 1)
          return task
        }
    }
    return None
  }
}
