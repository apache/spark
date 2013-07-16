package spark.scheduler.cluster

object SchedulingMode extends Enumeration{

  type SchedulingMode = Value
  val FAIR,FIFO = Value
}
