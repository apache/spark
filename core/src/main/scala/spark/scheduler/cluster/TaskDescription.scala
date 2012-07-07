package spark.scheduler.cluster

import java.nio.channels.Channels
import java.nio.ByteBuffer
import java.io.{IOException, EOFException, ObjectOutputStream, ObjectInputStream}
import spark.util.SerializableByteBuffer

class TaskDescription(val taskId: Long, val name: String, _serializedTask: ByteBuffer)
  extends Serializable {

  // Because ByteBuffers are not serializable, we wrap the task in a SerializableByteBuffer
  private val buffer = new SerializableByteBuffer(_serializedTask)

  def serializedTask: ByteBuffer = buffer.value
}
