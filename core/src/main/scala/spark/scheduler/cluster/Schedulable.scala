package spark.scheduler.cluster

/**
 * An interface for schedulable entities.
 * there are two type of Schedulable entities(Pools and TaskSetManagers)
 */
private[spark] trait Schedulable {
  def weight:Int
  def minShare:Int
  def runningTasks:Int
  def priority:Int
  def stageId:Int
}
