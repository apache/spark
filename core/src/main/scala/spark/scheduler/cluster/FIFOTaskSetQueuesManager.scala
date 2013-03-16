package spark.scheduler.cluster

import scala.collection.mutable.ArrayBuffer

import spark.Logging

/**
 * A FIFO Implementation of the TaskSetQueuesManager
 */
private[spark] class FIFOTaskSetQueuesManager extends TaskSetQueuesManager with Logging {
  
  var activeTaskSetsQueue = new ArrayBuffer[TaskSetManager]
  val tasksetSchedulingAlgorithm = new FIFOSchedulingAlgorithm()
  
  override def addTaskSetManager(manager: TaskSetManager) {
    activeTaskSetsQueue += manager
  }
  
  override def removeTaskSetManager(manager: TaskSetManager) {
    activeTaskSetsQueue -= manager
  }
  
  override def taskFinished(manager: TaskSetManager) {
    //do nothing
  }
  
  override def removeExecutor(executorId: String, host: String) {
    activeTaskSetsQueue.foreach(_.executorLost(executorId, host))
  }
  
  override def receiveOffer(execId:String, host:String,avaiableCpus:Double):Option[TaskDescription] =
  {
    for(manager <- activeTaskSetsQueue.sortWith(tasksetSchedulingAlgorithm.comparator))
    {
      val task = manager.slaveOffer(execId,host,avaiableCpus)
      if (task != None)
      {
        return task
      }
    }
    return None
  }

  override def checkSpeculatableTasks(): Boolean = {
    var shouldRevive = false
    for (ts <- activeTaskSetsQueue) {
      shouldRevive |= ts.checkSpeculatableTasks()
    }
    return shouldRevive
  }
  
}
