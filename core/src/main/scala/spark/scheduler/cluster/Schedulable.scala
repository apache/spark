package spark.scheduler.cluster

import spark.scheduler.cluster.SchedulingMode.SchedulingMode

import scala.collection.mutable.ArrayBuffer
/**
 * An interface for schedulable entities.
 * there are two type of Schedulable entities(Pools and TaskSetManagers)
 */
private[spark] trait Schedulable {
  var parent: Schedulable
  // child queues
  def schedulableQueue: ArrayBuffer[Schedulable]
  def schedulingMode: SchedulingMode
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
  def hasPendingTasks(): Boolean
}
