package spark.util

import java.io.OutputStream
import java.util.concurrent.TimeUnit._

class RateLimitedOutputStream(out: OutputStream, bytesPerSec: Int) extends OutputStream {
  val SyncIntervalNs = NANOSECONDS.convert(10, SECONDS)
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
      out.write(bytes, offset + pos, writeSize)
      pos += writeSize
    }
  }

  def waitToWrite(numBytes: Int) {
    while (true) {
      val now = System.nanoTime
      val elapsedSecs = SECONDS.convert(math.max(now - lastSyncTime, 1), NANOSECONDS)
      val rate = bytesWrittenSinceSync.toDouble / elapsedSecs
      if (rate < bytesPerSec) {
        // It's okay to write; just update some variables and return
        bytesWrittenSinceSync += numBytes
        if (now > lastSyncTime + SyncIntervalNs) {
          // Sync interval has passed; let's resync
          lastSyncTime = now
          bytesWrittenSinceSync = numBytes
        }
        return
      } else {
        // Calculate how much time we should sleep to bring ourselves to the desired rate.
        // Based on throttler in Kafka (https://github.com/kafka-dev/kafka/blob/master/core/src/main/scala/kafka/utils/Throttler.scala)
        val sleepTime = MILLISECONDS.convert((bytesWrittenSinceSync / bytesPerSec - elapsedSecs), SECONDS)
        if (sleepTime > 0) Thread.sleep(sleepTime)
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
