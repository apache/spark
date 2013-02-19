package spark.scheduler.cluster

/**
 * Represents free resources available on an executor.
 */
private[spark]
class WorkerOffer(val executorId: String, val hostname: String, val cores: Int) {
}
