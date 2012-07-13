package spark.scheduler.cluster

/**
 * Represents free resources available on a worker node.
 */
class WorkerOffer(val slaveId: String, val hostname: String, val cores: Int) {
}
