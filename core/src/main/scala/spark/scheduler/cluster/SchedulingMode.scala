package spark.scheduler.cluster

object SchedulingMode extends Enumeration("FAIR","FIFO")
{
  type SchedulingMode = Value

  val FAIR,FIFO = Value
}
