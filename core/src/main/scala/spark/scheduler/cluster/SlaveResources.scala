package spark.scheduler.cluster

private[spark]
class SlaveResources(val slaveId: String, val hostname: String, val coresFree: Int) {}
