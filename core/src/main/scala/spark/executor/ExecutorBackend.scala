package spark.executor

import java.nio.ByteBuffer
import spark.TaskState.TaskState

/**
 * A pluggable interface used by the Executor to send updates to the cluster scheduler.
 */
private[spark] trait ExecutorBackend {
  def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer)
}
