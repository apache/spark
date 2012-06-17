package spark.util

import java.io.InputStream
import java.nio.ByteBuffer

class ByteBufferInputStream(buffer: ByteBuffer) extends InputStream {
  override def read(): Int = {
    if (buffer.remaining() == 0) {
      -1
    } else {
      buffer.get()
    }
  }

  override def read(dest: Array[Byte]): Int = {
    read(dest, 0, dest.length)
  }

  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    val amountToGet = math.min(buffer.remaining(), length)
    buffer.get(dest, offset, amountToGet)
    return amountToGet
  }

  override def skip(bytes: Long): Long = {
    val amountToSkip = math.min(bytes, buffer.remaining).toInt
    buffer.position(buffer.position + amountToSkip)
    return amountToSkip
  }
}
