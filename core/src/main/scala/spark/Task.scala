package spark

class TaskContext(val stageId: Int, val splitId: Int, val attemptId: Int) extends Serializable

abstract class Task[T] extends Serializable {
  def run(id: Int): T
  def preferredLocations: Seq[String] = Nil
  def generation: Option[Long] = None
}
