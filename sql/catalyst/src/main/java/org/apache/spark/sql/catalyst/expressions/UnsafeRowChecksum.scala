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
import org.apache.spark.unsafe.Platform

/**
 * A concrete implementation of RowBasedChecksum for computing checksum for UnsafeRow.
 * The checksum for each row is computed by first casting or converting the baseObject
 * in the UnsafeRow to a byte array, and then computing the checksum for the byte array.
 *
 * Note that the input key is ignored in the checksum computation. As the Spark shuffle
 * currently uses a PartitionIdPassthrough partitioner, the keys are already the partition
 * IDs for sending the data, and they are the same for all rows in the same partition.
 */
class UnsafeRowChecksum(override protected val failOnInvalidRow: Boolean = true)
  extends RowBasedChecksum() {

  override protected def validateRow(value: Any): Option[String] = value match {
    case row: UnsafeRow => UnsafeRowChecksum.validate(row)
    // A non-UnsafeRow is unexpected here; leave it for calculateRowChecksum's assert to report.
    case _ => None
  }

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
  // Off-heap rows carry an absolute address in baseOffset. Real allocations sit far above the
  // first page; an address inside it is the null-region read behind the SIGSEGV (si_addr=0x0).
  private val MinNativeAddress: Long = 4096L

  /**
   * Checks that `row`'s backing memory can be safely hashed. Returns Some(description) only for a
   * row that a validly-constructed UnsafeRow can never be -- a negative size, a null baseObject
   * (off-heap) pointing into the first memory page, or an out-of-bounds byte[] offset -- so a
   * well-formed row is never flagged. Any other non-null on-heap base (e.g. a long[]) is accepted:
   * it is a live Java array, so a read within it cannot fault at a null address. These are the
   * cases that make XXH64's unchecked reads fault. Note: a stale off-heap pointer to a *freed* page
   * at a plausible (high) address cannot be distinguished from a live one here and is not caught;
   * nor is a corrupt-but-large size, a heuristic we deliberately avoid to keep false positives
   * impossible.
   */
  def validate(row: UnsafeRow): Option[String] = {
    val base = row.getBaseObject
    val offset = row.getBaseOffset
    val size = row.getSizeInBytes
    def desc(reason: String): Option[String] = {
      val baseStr = if (base == null) "null" else base.getClass.getName
      Some(s"$reason (baseObject=$baseStr, baseOffset=0x${java.lang.Long.toHexString(offset)}, " +
        s"sizeInBytes=$size)")
    }
    if (size < 0) {
      desc("negative sizeInBytes")
    } else if (size == 0) {
      None // empty row: XXH64 reads nothing
    } else {
      base match {
        case null =>
          if (offset < MinNativeAddress) {
            desc("off-heap row (null baseObject) points into the first memory page")
          } else None
        case bytes: Array[Byte] =>
          val start = offset - Platform.BYTE_ARRAY_OFFSET
          if (start < 0 || start + size > bytes.length) {
            desc(s"on-heap row spans [$start, ${start + size}) of a ${bytes.length}-byte array")
          } else None
        // Any other non-null on-heap base (e.g. a long[], the usual UnsafeRow buffer) is a live
        // Java array; a read within it cannot fault at a null address, so do not second-guess it.
        case _ => None
      }
    }
  }

  def createUnsafeRowChecksums(
      numPartitions: Int,
      failOnInvalidRow: Boolean): Array[RowBasedChecksum] = {
    Array.tabulate(numPartitions)(_ => new UnsafeRowChecksum(failOnInvalidRow))
  }
}
