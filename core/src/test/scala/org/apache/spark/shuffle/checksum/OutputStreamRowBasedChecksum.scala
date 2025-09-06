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

import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.util.ExposedBufferByteArrayOutputStream

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
