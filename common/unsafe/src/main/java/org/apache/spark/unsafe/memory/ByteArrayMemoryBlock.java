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

package org.apache.spark.unsafe.memory;

import org.apache.spark.unsafe.Platform;

/**
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 */
public class ByteArrayMemoryBlock extends MemoryBlock {

  private final long length;

  /**
   * Optional page number; used when this MemoryBlock represents a page allocated by a
   * TaskMemoryManager. This field can be updated using setPageNumber method so that
   * this can be modified by the TaskMemoryManage, which lives in a different package.
   */
  private int pageNumber = NO_PAGE_NUMBER;

  public ByteArrayMemoryBlock(byte[] obj, long offset, long length) {
    super(obj, offset);
    this.length = length;
  }

  /**
   * Returns the size of the memory block.
   */
  public long size() {
    return length;
  }

  public void fill(byte value) {
    Platform.setMemory(obj, offset, length, value);
  }

  public MemoryBlock allocate(long offset, long size) {
    return new ByteArrayMemoryBlock((byte[]) obj, offset, size);
  }

  @Override
  public void setPageNumber(int pageNum) {
    this.pageNumber = pageNum;
  }

  @Override
  public int getPageNumber() {
    return this.pageNumber;
  }

  public byte[] getByteArray() { return (byte[])this.obj; }

  /**
   * Creates a memory block pointing to the memory used by the byte array.
   */
  public static ByteArrayMemoryBlock fromArray(final byte[] array) {
    return new ByteArrayMemoryBlock(array, Platform.BYTE_ARRAY_OFFSET, array.length);
  }
}
