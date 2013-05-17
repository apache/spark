package spark.scheduler.cluster

import java.nio.ByteBuffer
import spark.util.SerializableBuffer

private[spark] class TaskDescription(
    val taskId: Long,
    val executorId: String,
    val name: String,
    _serializedTask: ByteBuffer)
  extends Serializable {

  // Because ByteBuffers are not serializable, wrap the task in a SerializableBuffer
  private val buffer = new SerializableBuffer(_serializedTask)

  def serializedTask: ByteBuffer = buffer.value
}
