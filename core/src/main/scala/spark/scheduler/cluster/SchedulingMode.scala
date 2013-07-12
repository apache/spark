package spark.scheduler.cluster

object SchedulingMode extends Enumeration("FAIR", "FIFO", "NONE"){

  type SchedulingMode = Value
  val FAIR,FIFO,NONE = Value
}
