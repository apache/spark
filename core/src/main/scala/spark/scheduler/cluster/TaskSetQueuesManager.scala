package spark.scheduler.cluster

import scala.collection.mutable.ArrayBuffer

/**
 * An interface for managing TaskSet queue/s that allows plugging different policy for
 * offering tasks to resources
 * 
 */
private[spark] trait TaskSetQueuesManager {
  def addTaskSetManager(manager: TaskSetManager): Unit
  def removeTaskSetManager(manager: TaskSetManager): Unit
  def taskFinished(manager: TaskSetManager): Unit
  def removeExecutor(executorId: String, host: String): Unit
  def receiveOffer(execId: String, host:String, avaiableCpus:Double):Option[TaskDescription]
  def checkSpeculatableTasks(): Boolean
}
