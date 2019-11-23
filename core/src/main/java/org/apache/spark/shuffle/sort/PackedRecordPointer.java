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

package org.apache.spark.shuffle.sort;

/**
 * Wrapper around an 8-byte word that holds a 24-bit partition number and 40-bit record pointer.
 * <p>
 * Within the long, the data is laid out as follows:
 * <pre>
 *   [24 bit partition number][13 bit memory page number][27 bit offset in page]
 * </pre>
 * This implies that the maximum addressable page size is 2^27 bits = 128 megabytes, assuming that
 * our offsets in pages are not 8-byte-word-aligned. Since we have 2^13 pages (based off the
 * 13-bit page numbers assigned by {@link org.apache.spark.memory.TaskMemoryManager}), this
 * implies that we can address 2^13 * 128 megabytes = 1 terabyte of RAM per task.
 * <p>
 * Assuming word-alignment would allow for a 1 gigabyte maximum page size, but we leave this
 * optimization to future work as it will require more careful design to ensure that addresses are
 * properly aligned (e.g. by padding records).
 */
final class PackedRecordPointer {

  static final int MAXIMUM_PAGE_SIZE_BYTES = 1 << 27;  // 128 megabytes

  /**
   * The maximum partition identifier that can be encoded. Note that partition ids start from 0.
   */
  static final int MAXIMUM_PARTITION_ID = (1 << 24) - 1;  // 16777215

  /**
   * The index of the first byte of the partition id, counting from the least significant byte.
   */
  static final int PARTITION_ID_START_BYTE_INDEX = 5;

  /**
   * The index of the last byte of the partition id, counting from the least significant byte.
   */
  static final int PARTITION_ID_END_BYTE_INDEX = 7;

  /** Bit mask for the lower 40 bits of a long. */
  private static final long MASK_LONG_LOWER_40_BITS = (1L << 40) - 1;

  /** Bit mask for the upper 24 bits of a long */
  private static final long MASK_LONG_UPPER_24_BITS = ~MASK_LONG_LOWER_40_BITS;

  /** Bit mask for the lower 27 bits of a long. */
  private static final long MASK_LONG_LOWER_27_BITS = (1L << 27) - 1;

  /** Bit mask for the lower 51 bits of a long. */
  private static final long MASK_LONG_LOWER_51_BITS = (1L << 51) - 1;

  /** Bit mask for the upper 13 bits of a long */
  private static final long MASK_LONG_UPPER_13_BITS = ~MASK_LONG_LOWER_51_BITS;

  /**
   * Pack a record address and partition id into a single word.
   *
   * @param recordPointer a record pointer encoded by TaskMemoryManager.
   * @param partitionId a shuffle partition id (maximum value of 2^24).
   * @return a packed pointer that can be decoded using the {@link PackedRecordPointer} class.
   */
  public static long packPointer(long recordPointer, int partitionId) {
    assert (partitionId <= MAXIMUM_PARTITION_ID);
    // Note that without word alignment we can address 2^27 bytes = 128 megabytes per page.
    // Also note that this relies on some internals of how TaskMemoryManager encodes its addresses.
    final long pageNumber = (recordPointer & MASK_LONG_UPPER_13_BITS) >>> 24;
    final long compressedAddress = pageNumber | (recordPointer & MASK_LONG_LOWER_27_BITS);
    return (((long) partitionId) << 40) | compressedAddress;
  }

  private long packedRecordPointer;

  public void set(long packedRecordPointer) {
    this.packedRecordPointer = packedRecordPointer;
  }

  public int getPartitionId() {
    return (int) ((packedRecordPointer & MASK_LONG_UPPER_24_BITS) >>> 40);
  }

  public long getRecordPointer() {
    final long pageNumber = (packedRecordPointer << 24) & MASK_LONG_UPPER_13_BITS;
    final long offsetInPage = packedRecordPointer & MASK_LONG_LOWER_27_BITS;
    return pageNumber | offsetInPage;
  }

}
