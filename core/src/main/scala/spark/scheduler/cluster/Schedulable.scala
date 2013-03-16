package spark.scheduler.cluster

import scala.collection.mutable.ArrayBuffer

/**
 * An interface for schedulable entities, there are two type Schedulable entities(Pools and TaskSetManagers) 
 */
private[spark] trait Schedulable {
  
  def getMinShare(): Int
  def getRunningTasks(): Int
  def getPriority(): Int = 
  {
    return 0
  }
  def getWeight(): Int
  def getStageId(): Int =
  {
    return 0
  }
}
