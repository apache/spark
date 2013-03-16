package spark.scheduler.cluster

/**
 * An interface for sort algorithm 
 * FIFO: FIFO algorithm for TaskSetManagers
 * FS: FS algorithm for Pools, and FIFO or FS for TaskSetManagers
 */
private[spark] trait SchedulingAlgorithm {
  def comparator(s1: Schedulable,s2: Schedulable): Boolean
}

private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm
{
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean =
  {
    val priority1 = s1.getPriority()
    val priority2 = s2.getPriority()
    var res = Math.signum(priority1 - priority2)
    if (res == 0)
    {
        val stageId1 = s1.getStageId()
        val stageId2 = s2.getStageId()
        res = Math.signum(stageId1 - stageId2)
    }
    if (res < 0)
    {
      return true
    }
    else
    {
      return false
    }    
  }
}

private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm
{
  def comparator(s1: Schedulable, s2:Schedulable): Boolean =
  {
    val minShare1 = s1.getMinShare()
    val minShare2 = s2.getMinShare()
    val s1Needy = s1.getRunningTasks() < minShare1
    val s2Needy = s2.getRunningTasks() < minShare2
    val minShareRatio1 = s1.getRunningTasks().toDouble / Math.max(minShare1,1.0).toDouble
    val minShareRatio2 = s2.getRunningTasks().toDouble / Math.max(minShare2,1.0).toDouble
    val taskToWeightRatio1 = s1.getRunningTasks().toDouble / s1.getWeight().toDouble
    val taskToWeightRatio2 = s2.getRunningTasks().toDouble / s2.getWeight().toDouble
    var res:Boolean = true
    
    if(s1Needy && !s2Needy)
    {
      res = true
    }
    else if(!s1Needy && s2Needy)
    {
      res = false
    }
    else if (s1Needy && s2Needy)
    {
      res = minShareRatio1 <= minShareRatio2   
    }
    else
    {
      res = taskToWeightRatio1 <= taskToWeightRatio2
    }
    return res
  }
}

