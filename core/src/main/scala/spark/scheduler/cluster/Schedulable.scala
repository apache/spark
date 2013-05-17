package spark.scheduler.cluster

import scala.collection.mutable.ArrayBuffer

/**
 * An interface for schedulable entities.
 * there are two type of Schedulable entities(Pools and TaskSetManagers)
 */
private[spark] trait Schedulable {
  var parent: Schedulable
  def weight: Int
  def minShare: Int
  def runningTasks: Int
  def priority: Int
  def stageId: Int
  def name: String

  def increaseRunningTasks(taskNum: Int): Unit
  def decreaseRunningTasks(taskNum: Int): Unit
  def addSchedulable(schedulable: Schedulable): Unit
  def removeSchedulable(schedulable: Schedulable): Unit
  def getSchedulableByName(name: String): Schedulable
  def executorLost(executorId: String, host: String): Unit
  def checkSpeculatableTasks(): Boolean
  def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager]
}
