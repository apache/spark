package spark.network

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer


private[network]
class MessageChunk(val header: MessageChunkHeader, val buffer: ByteBuffer) {

  val size = if (buffer == null) 0 else buffer.remaining

  lazy val buffers = {
    val ab = new ArrayBuffer[ByteBuffer]()
    ab += header.buffer
    if (buffer != null) {
      ab += buffer
    }
    ab
  }

  override def toString = {
    "" + this.getClass.getSimpleName + " (id = " + header.id + ", size = " + size + ")"
  }
}
