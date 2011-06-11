package spark

import mesos._

@serializable
class TaskContext {
	var jobID: Int = 0
	var taskID: Int = 0
	var attemptID: Int = 0
}

@serializable
abstract class Task[T] {
  val context_ = new TaskContext()
  def run: T
  def preferredLocations: Seq[String] = Nil
  def generation: Option[Long] = None
  def context = context_
}
