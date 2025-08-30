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

import java.io.ObjectOutputStream
import java.util.zip.Checksum

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.util.ExposedBufferByteArrayOutputStream

/**
 * A class for computing checksum for input (key, value) pairs. The checksum is independent of
 * the order of the input (key, value) pairs. It is done by computing a checksum for each row
 * first, and then computing the XOR for all the row checksums.
 */
abstract class RowBasedChecksum() extends Serializable with Logging {
  private var hasError: Boolean = false
  private var checksumValue: Long = 0
  /** Returns the checksum value computed. Tt returns the default checksum value (0) if there
   * are any errors encountered during the checksum computation.
   */
  def getValue: Long = {
    if (!hasError) checksumValue else 0
  }

  /** Updates the row-based checksum with the given (key, value) pair */
  def update(key: Any, value: Any): Unit = {
    if (!hasError) {
      try {
        val rowChecksumValue = calculateRowChecksum(key, value)
        checksumValue = checksumValue ^ rowChecksumValue
      } catch {
        case NonFatal(e) =>
          logError("Checksum computation encountered error: ", e)
          hasError = true
      }
    }
  }

  /** Computes and returns the checksum value for the given (key, value) pair */
  protected def calculateRowChecksum(key: Any, value: Any): Long
}

/**
 * A Concrete implementation of RowBasedChecksum. The checksum for each row is
 * computed by first converting the (key, value) pair to byte array using OutputStreams,
 * and then computing the checksum for the byte array.
 * Note that this checksum computation is very expensive, and it is used only in tests
 * in the core component. A much cheaper implementation of RowBasedChecksum is in
 * UnsafeRowChecksum.
 *
 * @param checksumAlgorithm the algorithm used for computing checksum.
 */
class OutputStreamRowBasedChecksum(checksumAlgorithm: String)
  extends RowBasedChecksum() {

  private val DEFAULT_INITIAL_SER_BUFFER_SIZE = 32 * 1024

  @transient private lazy val serBuffer =
    new ExposedBufferByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE)
  @transient private lazy val objOut = new ObjectOutputStream(serBuffer)

  @transient
  protected lazy val checksum: Checksum =
    ShuffleChecksumHelper.getChecksumByAlgorithm(checksumAlgorithm)

  override protected def calculateRowChecksum(key: Any, value: Any): Long = {
    assert(checksum != null, "Checksum is null")

    // Converts the (key, value) pair into byte array.
    objOut.reset()
    serBuffer.reset()
    objOut.writeObject((key, value))
    objOut.flush()
    serBuffer.flush()

    // Computes and returns the checksum for the byte array.
    checksum.reset()
    checksum.update(serBuffer.getBuf, 0, serBuffer.size())
    checksum.getValue
  }
}

object RowBasedChecksum {
  def getAggregatedChecksumValue(rowBasedChecksums: Array[RowBasedChecksum]): Long = {
    Option(rowBasedChecksums)
      .map(_.foldLeft(0L)((acc, c) => acc * 31L + c.getValue))
      .getOrElse(0L)
  }
}
