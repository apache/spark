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
  //The receiveOffers function, accepts tasks and offers. It populates the tasks to the actual task from TaskSet
  //It returns a list of TaskSet ID that corresponds to each assigned tasks
  def receiveOffer(tasks: Seq[ArrayBuffer[TaskDescription]], offers: Seq[WorkerOffer]): Seq[Seq[String]] 
  def checkSpeculatableTasks(): Boolean
}