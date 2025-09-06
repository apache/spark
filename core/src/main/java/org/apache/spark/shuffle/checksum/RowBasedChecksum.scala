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

package org.apache.spark.shuffle.checksum

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * A class for computing checksum for input (key, value) pairs. The checksum is independent of
 * the order of the input (key, value) pairs. It is done by computing a checksum for each row
 * first, then computing the XOR and SUM for all the row checksums and mixing these two values
 * as the final checksum.
 */
abstract class RowBasedChecksum() extends Serializable with Logging {
  private val ROTATE_POSITIONS = 27
  private var hasError: Boolean = false
  private var checksumXor: Long = 0
  private var checksumSum: Long = 0

  /**
   * Returns the checksum value. It returns the default checksum value (0) if there
   * are any errors encountered during the checksum computation.
   */
  def getValue: Long = {
    if (!hasError) {
      // Here we rotate the `checksumSum` to transforms these two values into a single, strong
      // composite checksum by ensuring their bit patterns are thoroughly mixed.
      checksumXor ^ rotateLeft(checksumSum)
    } else {
      0
    }
  }

  /** Updates the row-based checksum with the given (key, value) pair. Not thread safe. */
  def update(key: Any, value: Any): Unit = {
    if (!hasError) {
      try {
        val rowChecksumValue = calculateRowChecksum(key, value)
        checksumXor = checksumXor ^ rowChecksumValue
        checksumSum += rowChecksumValue
      } catch {
        case NonFatal(e) =>
          logError("Checksum computation encountered error: ", e)
          hasError = true
      }
    }
  }

  /** Computes and returns the checksum value for the given (key, value) pair */
  protected def calculateRowChecksum(key: Any, value: Any): Long

  // Rotate the value by shifting the bits by `ROTATE_POSITIONS` positions to the left.
  private def rotateLeft(value: Long): Long = {
    (value << ROTATE_POSITIONS) | (value >>> (64 - ROTATE_POSITIONS))
  }
}

object RowBasedChecksum {
  def getAggregatedChecksumValue(rowBasedChecksums: Array[RowBasedChecksum]): Long = {
    Option(rowBasedChecksums)
      .map(_.foldLeft(0L)((acc, c) => acc * 31L + c.getValue))
      .getOrElse(0L)
  }
}
