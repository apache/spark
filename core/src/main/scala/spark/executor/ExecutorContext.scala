package spark.executor

import java.nio.ByteBuffer
import spark.TaskState.TaskState

/**
 * Interface used by Executor to send back updates to the cluster scheduler.
 */
trait ExecutorContext {
  def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer)
}
