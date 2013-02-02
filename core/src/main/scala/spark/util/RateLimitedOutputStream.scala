package spark.util

import scala.annotation.tailrec

import java.io.OutputStream
import java.util.concurrent.TimeUnit._

class RateLimitedOutputStream(out: OutputStream, bytesPerSec: Int) extends OutputStream {
  val SYNC_INTERVAL = NANOSECONDS.convert(10, SECONDS)
  val CHUNK_SIZE = 8192
  var lastSyncTime = System.nanoTime
  var bytesWrittenSinceSync: Long = 0

  override def write(b: Int) {
    waitToWrite(1)
    out.write(b)
  }

  override def write(bytes: Array[Byte]) {
    write(bytes, 0, bytes.length)
  }

  @tailrec
  override final def write(bytes: Array[Byte], offset: Int, length: Int) {
    val writeSize = math.min(length - offset, CHUNK_SIZE)
    if (writeSize > 0) {
      waitToWrite(writeSize)
      out.write(bytes, offset, writeSize)
      write(bytes, offset + writeSize, length)
    }
  }

  override def flush() {
    out.flush()
  }

  override def close() {
    out.close()
  }

  @tailrec
  private def waitToWrite(numBytes: Int) {
    val now = System.nanoTime
    val elapsedSecs = SECONDS.convert(math.max(now - lastSyncTime, 1), NANOSECONDS)
    val rate = bytesWrittenSinceSync.toDouble / elapsedSecs
    if (rate < bytesPerSec) {
      // It's okay to write; just update some variables and return
      bytesWrittenSinceSync += numBytes
      if (now > lastSyncTime + SYNC_INTERVAL) {
        // Sync interval has passed; let's resync
        lastSyncTime = now
        bytesWrittenSinceSync = numBytes
      }
    } else {
      // Calculate how much time we should sleep to bring ourselves to the desired rate.
      // Based on throttler in Kafka (https://github.com/kafka-dev/kafka/blob/master/core/src/main/scala/kafka/utils/Throttler.scala)
      val sleepTime = MILLISECONDS.convert((bytesWrittenSinceSync / bytesPerSec - elapsedSecs), SECONDS)
      if (sleepTime > 0) Thread.sleep(sleepTime)
      waitToWrite(numBytes)
    }
  }
}
