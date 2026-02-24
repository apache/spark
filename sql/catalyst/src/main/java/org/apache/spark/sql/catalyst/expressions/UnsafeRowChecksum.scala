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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.shuffle.checksum.RowBasedChecksum

/**
 * A concrete implementation of RowBasedChecksum for computing checksum for UnsafeRow.
 * The checksum for each row is computed by first casting or converting the baseObject
 * in the UnsafeRow to a byte array, and then computing the checksum for the byte array.
 *
 * Note that the input key is ignored in the checksum computation. As the Spark shuffle
 * currently uses a PartitionIdPassthrough partitioner, the keys are already the partition
 * IDs for sending the data, and they are the same for all rows in the same partition.
 */
class UnsafeRowChecksum extends RowBasedChecksum() {

  override protected def calculateRowChecksum(key: Any, value: Any): Long = {
    assert(
      value.isInstanceOf[UnsafeRow],
      "Expecting UnsafeRow but got " + value.getClass.getName)

    // Casts or converts the baseObject in UnsafeRow to a byte array.
    val unsafeRow = value.asInstanceOf[UnsafeRow]
    XXH64.hashUnsafeBytes(
      unsafeRow.getBaseObject,
      unsafeRow.getBaseOffset,
      unsafeRow.getSizeInBytes,
      0
    )
  }
}

object UnsafeRowChecksum {
  def createUnsafeRowChecksums(numPartitions: Int): Array[RowBasedChecksum] = {
    Array.tabulate(numPartitions)(_ => new UnsafeRowChecksum())
  }
}
