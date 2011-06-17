package spark

import mesos._

@serializable
class TaskContext(val stageId: Int, val splitId: Int, val attemptId: Int) {
}

@serializable
abstract class Task[T] {
  def run (id: Int): T
  def preferredLocations: Seq[String] = Nil
  def generation: Option[Long] = None
}
