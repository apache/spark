package spark.util

import java.io.InputStream
import java.nio.ByteBuffer
import spark.storage.BlockManager

/**
 * Reads data from a ByteBuffer, and optionally cleans it up using BlockManager.dispose()
 * at the end of the stream (e.g. to close a memory-mapped file).
 */
private[spark]
class ByteBufferInputStream(private var buffer: ByteBuffer, dispose: Boolean = false)
  extends InputStream {

  override def read(): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      buffer.get() & 0xFF
    }
  }

  override def read(dest: Array[Byte]): Int = {
    read(dest, 0, dest.length)
  }

  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      val amountToGet = math.min(buffer.remaining(), length)
      buffer.get(dest, offset, amountToGet)
      amountToGet
    }
  }

  override def skip(bytes: Long): Long = {
    if (buffer != null) {
      val amountToSkip = math.min(bytes, buffer.remaining).toInt
      buffer.position(buffer.position + amountToSkip)
      if (buffer.remaining() == 0) {
        cleanUp()
      }
      amountToSkip
    } else {
      0L
    }
  }

  /**
   * Clean up the buffer, and potentially dispose of it using BlockManager.dispose().
   */
  private def cleanUp() {
    if (buffer != null) {
      if (dispose) {
        BlockManager.dispose(buffer)
      }
      buffer = null
    }
  }
}
