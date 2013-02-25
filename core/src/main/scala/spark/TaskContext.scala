package spark

import executor.TaskMetrics
import scala.collection.mutable.ArrayBuffer

class TaskContext(
  val stageId: Int,
  val splitId: Int,
  val attemptId: Long,
  val taskMetrics: TaskMetrics = TaskMetrics.empty()
) extends Serializable {

  @transient val onCompleteCallbacks = new ArrayBuffer[() => Unit]

  // Add a callback function to be executed on task completion. An example use
  // is for HadoopRDD to register a callback to close the input stream.
  def addOnCompleteCallback(f: () => Unit) {
    onCompleteCallbacks += f
  }

  def executeOnCompleteCallbacks() {
    onCompleteCallbacks.foreach{_()}
  }
}
