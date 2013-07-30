package spark.scheduler.cluster

/**
 *  "FAIR" and "FIFO" determines which policy is used to order tasks amongst a Schedulable's sub-queues
 *  "NONE" is used when the a Schedulable has no sub-queues.
 */
object SchedulingMode extends Enumeration("FAIR", "FIFO", "NONE") {

  type SchedulingMode = Value
  val FAIR,FIFO,NONE = Value
}
