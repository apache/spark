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

package org.apache.spark.scheduler

import java.util.Locale
import java.util.zip.{Adler32, Checksum, CRC32}

/**
 * Computes integrity checksums over the set of non-empty partition indices of a MapStatus.
 * Targets metadata corruptions that flip a non-empty partition to size zero, which would cause
 * reducers to silently skip fetching valid on-disk data.
 */
private[spark] object MapStatusChecksum {

  val ADLER32 = "ADLER32"
  val CRC32_ALG = "CRC32"

  /** Return a fresh Checksum instance for the given algorithm name. */
  def newChecksum(algorithm: String): Checksum = {
    algorithm.toUpperCase(Locale.ROOT) match {
      case ADLER32 => new Adler32
      case CRC32_ALG => new CRC32
      case other =>
        throw new IllegalArgumentException(s"Unsupported MapStatus checksum algorithm: $other")
    }
  }

  /** Returns `None` when the array has no non-empty entries. */
  def compute(partitionLengths: Array[Long], algorithm: String): Option[Int] = {
    val checksum = newChecksum(algorithm)
    val buf = new Array[Byte](4)
    var hasNonEmpty = false
    var i = 0
    while (i < partitionLengths.length) {
      if (partitionLengths(i) != 0L) {
        writeIntBE(buf, i)
        checksum.update(buf, 0, 4)
        hasNonEmpty = true
      }
      i += 1
    }
    if (hasNonEmpty) Some(checksum.getValue.toInt) else None
  }

  def recompute(status: MapStatus, algorithm: String): Option[Int] = {
    val bound = status.numPartitions
    val checksum = newChecksum(algorithm)
    val buf = new Array[Byte](4)
    var hasNonEmpty = false
    var i = 0
    while (i < bound) {
      if (status.getSizeForBlock(i) != 0L) {
        writeIntBE(buf, i)
        checksum.update(buf, 0, 4)
        hasNonEmpty = true
      }
      i += 1
    }
    if (hasNonEmpty) Some(checksum.getValue.toInt) else None
  }

  private def writeIntBE(buf: Array[Byte], value: Int): Unit = {
    buf(0) = (value >>> 24).toByte
    buf(1) = (value >>> 16).toByte
    buf(2) = (value >>>  8).toByte
    buf(3) = value.toByte
  }
}
