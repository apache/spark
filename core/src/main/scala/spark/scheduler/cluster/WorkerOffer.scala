package spark.scheduler.cluster

/**
 * Represents free resources available on an executor.
 */
private[spark]
class WorkerOffer(val executorId: String, val hostPort: String, val cores: Int) {
}
