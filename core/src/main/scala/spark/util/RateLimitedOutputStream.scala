package spark.util

import java.io.OutputStream

class RateLimitedOutputStream(out: OutputStream, bytesPerSec: Int) extends OutputStream {
  var lastSyncTime = System.nanoTime()
  var bytesWrittenSinceSync: Long = 0

  override def write(b: Int) {
    waitToWrite(1)
    out.write(b)
  }

  override def write(bytes: Array[Byte]) {
    write(bytes, 0, bytes.length)
  }

  override def write(bytes: Array[Byte], offset: Int, length: Int) {
    val CHUNK_SIZE = 8192
    var pos = 0
    while (pos < length) {
      val writeSize = math.min(length - pos, CHUNK_SIZE)
      waitToWrite(writeSize)
      out.write(bytes, offset + pos, length - pos)
      pos += writeSize
    }
  }

  def waitToWrite(numBytes: Int) {
    while (true) {
      val now = System.nanoTime()
      val elapsed = math.max(now - lastSyncTime, 1)
      val rate = bytesWrittenSinceSync.toDouble / (elapsed / 1.0e9)
      if (rate < bytesPerSec) {
        // It's okay to write; just update some variables and return
        bytesWrittenSinceSync += numBytes
        if (now > lastSyncTime + (1e10).toLong) {
          // Ten seconds have passed since lastSyncTime; let's resync
          lastSyncTime = now
          bytesWrittenSinceSync = numBytes
        }
        return
      } else {
        Thread.sleep(5)
      }
    }
  }

  override def flush() {
    out.flush()
  }

  override def close() {
    out.close()
  }
}