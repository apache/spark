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

package org.apache.spark.shuffle.unsafe;

/**
 * Wrapper around an 8-byte word that holds a 24-bit partition number and 40-bit record pointer.
 */
final class PackedRecordPointer {

  /** Bit mask for the lower 40 bits of a long. */
  private static final long MASK_LONG_LOWER_40_BITS = 0xFFFFFFFFFFL;

  /** Bit mask for the upper 24 bits of a long */
  private static final long MASK_LONG_UPPER_24_BITS = ~MASK_LONG_LOWER_40_BITS;

  /** Bit mask for the lower 27 bits of a long. */
  private static final long MASK_LONG_LOWER_27_BITS = 0x7FFFFFFL;

  /** Bit mask for the lower 51 bits of a long. */
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  /** Bit mask for the upper 13 bits of a long */
  private static final long MASK_LONG_UPPER_13_BITS = ~MASK_LONG_LOWER_51_BITS;

  // TODO: this shifting is probably extremely inefficient; this is just for prototyping

  /**
   * Pack a record address and partition id into a single word.
   *
   * @param recordPointer a record pointer encoded by TaskMemoryManager.
   * @param partitionId a shuffle partition id (maximum value of 2^24).
   * @return a packed pointer that can be decoded using the {@link PackedRecordPointer} class.
   */
  public static long packPointer(long recordPointer, int partitionId) {
    // Note that without word alignment we can address 2^27 bytes = 128 megabytes per page.
    // Also note that this relies on some internals of how TaskMemoryManager encodes its addresses.
    final int pageNumber = (int) ((recordPointer & MASK_LONG_UPPER_13_BITS) >>> 51);
    final long compressedAddress =
      (((long) pageNumber) << 27) | (recordPointer & MASK_LONG_LOWER_27_BITS);
    return (((long) partitionId) << 40) | compressedAddress;
  }

  public long packedRecordPointer;

  public int getPartitionId() {
    return (int) ((packedRecordPointer & MASK_LONG_UPPER_24_BITS) >>> 40);
  }

  public long getRecordPointer() {
    final long compressedAddress = packedRecordPointer & MASK_LONG_LOWER_40_BITS;
    final long pageNumber = (compressedAddress << 24) & MASK_LONG_UPPER_13_BITS;
    final long offsetInPage = compressedAddress & MASK_LONG_LOWER_27_BITS;
    return pageNumber | offsetInPage;
  }

  public int getRecordLength() {
    return -1; // TODO
  }
}
