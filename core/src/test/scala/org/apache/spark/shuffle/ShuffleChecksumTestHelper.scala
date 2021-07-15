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

package org.apache.spark.shuffle

import java.io.{DataInputStream, File, FileInputStream}
import java.util.zip.CheckedInputStream

import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.checksum.ShuffleChecksumHelper

trait ShuffleChecksumTestHelper {

  /**
   * Ensure that the checksum values are consistent between write and read side.
   */
  def compareChecksums(numPartition: Int, checksum: File, data: File, index: File): Unit = {
    assert(checksum.exists(), "Checksum file doesn't exist")
    assert(data.exists(), "Data file doesn't exist")
    assert(index.exists(), "Index file doesn't exist")

    var checksumIn: DataInputStream = null
    val expectChecksums = Array.ofDim[Long](numPartition)
    try {
      checksumIn = new DataInputStream(new FileInputStream(checksum))
      (0 until numPartition).foreach(i => expectChecksums(i) = checksumIn.readLong())
    } finally {
      if (checksumIn != null) {
        checksumIn.close()
      }
    }

    var dataIn: FileInputStream = null
    var indexIn: DataInputStream = null
    var checkedIn: CheckedInputStream = null
    try {
      dataIn = new FileInputStream(data)
      indexIn = new DataInputStream(new FileInputStream(index))
      var prevOffset = indexIn.readLong
      (0 until numPartition).foreach { i =>
        val curOffset = indexIn.readLong
        val limit = (curOffset - prevOffset).toInt
        val bytes = new Array[Byte](limit)
        val checksumCal = ShuffleChecksumHelper.getChecksumByFileExtension(checksum.getName)
        checkedIn = new CheckedInputStream(
          new LimitedInputStream(dataIn, curOffset - prevOffset), checksumCal)
        checkedIn.read(bytes, 0, limit)
        prevOffset = curOffset
        // checksum must be consistent at both write and read sides
        assert(checkedIn.getChecksum.getValue == expectChecksums(i))
      }
    } finally {
      if (dataIn != null) {
        dataIn.close()
      }
      if (indexIn != null) {
        indexIn.close()
      }
      if (checkedIn != null) {
        checkedIn.close()
      }
    }
  }
}
