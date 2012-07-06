package spark.scheduler.cluster

trait ClusterSchedulerContext {
  def start(): Unit
  def stop(): Unit
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  // TODO: Probably want to add a killTask too
}
