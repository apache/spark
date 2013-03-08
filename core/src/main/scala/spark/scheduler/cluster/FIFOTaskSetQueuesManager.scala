package spark.scheduler.cluster

import scala.collection.mutable.ArrayBuffer

import spark.Logging

/**
 * A FIFO Implementation of the TaskSetQueuesManager
 */
private[spark] class FIFOTaskSetQueuesManager extends TaskSetQueuesManager with Logging {
  
  var activeTaskSetsQueue = new ArrayBuffer[TaskSetManager]
  
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
  
  override def receiveOffer(tasks: Seq[ArrayBuffer[TaskDescription]], offers: Seq[WorkerOffer]): Seq[Seq[String]] = {
    val taskSetIds = offers.map(o => new ArrayBuffer[String](o.cores))
    val availableCpus = offers.map(o => o.cores).toArray
    var launchedTask = false
    for (manager <- activeTaskSetsQueue.sortBy(m => (m.taskSet.priority, m.taskSet.stageId))) {
      do {
        launchedTask = false
        for (i <- 0 until offers.size) {
          val execId = offers(i).executorId
          val host = offers(i).hostname
          manager.slaveOffer(execId, host, availableCpus(i)) match {
            case Some(task) =>
              tasks(i) += task              
              taskSetIds(i) += manager.taskSet.id
              availableCpus(i) -= 1
              launchedTask = true

            case None => {}
          }
        }
      } while (launchedTask)
    }
    return taskSetIds
  }
  
  override def checkSpeculatableTasks(): Boolean = {
    var shouldRevive = false
    for (ts <- activeTaskSetsQueue) {
      shouldRevive |= ts.checkSpeculatableTasks()
    }
    return shouldRevive
  }
  
}