/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

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
