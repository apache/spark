package spark.scheduler

/**
 * A task to execute on a worker node.
 */
abstract class Task[T](val stageId: Int) extends Serializable {
  def run(attemptId: Long): T
  def preferredLocations: Seq[String] = Nil

  var generation: Long = -1   // Map output tracker generation. Will be set by TaskScheduler.
}
