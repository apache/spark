package spark

import scala.collection.mutable.ArrayBuffer
import spark.scheduler.Task

class TaskContext(val stageId: Int, val splitId: Int, val attemptId: Long, val task: Task[_]) extends Serializable {
  //by adding Task here, I'm destroying the separation between Task & TaskContext ... not sure why they need to
  // be separate

  @transient val onCompleteCallbacks = new ArrayBuffer[TaskContext => Unit]

  // Add a callback function to be executed on task completion. An example use
  // is for HadoopRDD to register a callback to close the input stream.
  def addOnCompleteCallback(f: TaskContext => Unit) {
    onCompleteCallbacks += f
  }

  def executeOnCompleteCallbacks() {
    onCompleteCallbacks.foreach{_.apply(this)}
  }
}
