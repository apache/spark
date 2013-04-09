package spark.scheduler.cluster

/**
 * An interface for sort algorithm
 * FIFO: FIFO algorithm for TaskSetManagers
 * FS: FS algorithm for Pools, and FIFO or FS for TaskSetManagers
 */
private[spark] trait SchedulingAlgorithm {
  def comparator(s1: Schedulable,s2: Schedulable): Boolean
}

private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val priority1 = s1.priority
    val priority2 = s2.priority
    var res = Math.signum(priority1 - priority2)
    if (res == 0) {
        val stageId1 = s1.stageId
        val stageId2 = s2.stageId
        res = Math.signum(stageId1 - stageId2)
    }
    if (res < 0)
      return true
    else
      return false
  }
}

private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  def comparator(s1: Schedulable, s2:Schedulable): Boolean = {
    val minShare1 = s1.minShare
    val minShare2 = s2.minShare
    val runningTasks1 = s1.runningTasks
    val runningTasks2 = s2.runningTasks
    val s1Needy = runningTasks1 < minShare1
    val s2Needy = runningTasks2 < minShare2
    val minShareRatio1 = runningTasks1.toDouble / Math.max(minShare1,1.0).toDouble
    val minShareRatio2 = runningTasks2.toDouble / Math.max(minShare2,1.0).toDouble
    val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble
    var res:Boolean = true

    if (s1Needy && !s2Needy)
      res = true
    else if(!s1Needy && s2Needy)
      res = false
    else if (s1Needy && s2Needy)
      res = minShareRatio1 <= minShareRatio2
    else
      res = taskToWeightRatio1 <= taskToWeightRatio2

    return res
  }
}

