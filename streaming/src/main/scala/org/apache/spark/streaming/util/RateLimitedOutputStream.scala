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

package org.apache.spark.streaming.util

import java.io.OutputStream
import java.util.concurrent.TimeUnit._

import scala.annotation.tailrec

import org.apache.spark.internal.Logging

private[streaming]
class RateLimitedOutputStream(out: OutputStream, desiredBytesPerSec: Int)
  extends OutputStream
  with Logging {

  require(desiredBytesPerSec > 0)

  private val SYNC_INTERVAL = NANOSECONDS.convert(10, SECONDS)
  private val CHUNK_SIZE = 8192
  private var lastSyncTime = System.nanoTime
  private var bytesWrittenSinceSync = 0L

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
    val elapsedNanosecs = math.max(now - lastSyncTime, 1)
    val rate = bytesWrittenSinceSync.toDouble * 1000000000 / elapsedNanosecs
    if (rate < desiredBytesPerSec) {
      // It's okay to write; just update some variables and return
      bytesWrittenSinceSync += numBytes
      if (now > lastSyncTime + SYNC_INTERVAL) {
        // Sync interval has passed; let's resync
        lastSyncTime = now
        bytesWrittenSinceSync = numBytes
      }
    } else {
      // Calculate how much time we should sleep to bring ourselves to the desired rate.
      val targetTimeInMillis = bytesWrittenSinceSync * 1000 / desiredBytesPerSec
      val elapsedTimeInMillis = elapsedNanosecs / 1000000
      val sleepTimeInMillis = targetTimeInMillis - elapsedTimeInMillis
      if (sleepTimeInMillis > 0) {
        logTrace("Natural rate is " + rate + " per second but desired rate is " +
          desiredBytesPerSec + ", sleeping for " + sleepTimeInMillis + " ms to compensate.")
        Thread.sleep(sleepTimeInMillis)
      }
      waitToWrite(numBytes)
    }
  }
}
