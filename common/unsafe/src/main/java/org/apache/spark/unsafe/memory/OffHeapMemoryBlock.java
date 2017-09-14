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

public class OffHeapMemoryBlock extends MemoryBlock {
  private Object directBuffer;
  private long address;
  private final long length;

  /**
   * Optional page number; used when this MemoryBlock represents a page allocated by a
   * TaskMemoryManager. This field can be updated using setPageNumber method so that
   * this can be modified by the TaskMemoryManage, which lives in a different package.
   */
  private int pageNumber = -1;

  static public final OffHeapMemoryBlock NULL = new OffHeapMemoryBlock(null, 0, 0);

  public OffHeapMemoryBlock(Object directBuffer, long address, long size) {
    super(null, address);
    this.address = address;
    this.length = size;
    this.directBuffer = directBuffer;
  }

  public void setBaseOffset(long address) {
    this.address = address;
  }

  @Override
  public long size() {
    return this.length;
  }

  @Override
  public void fill(byte value) {
    Platform.setMemory(null, address, length, value);
  }

  @Override
  public MemoryBlock allocate(long offset, long size) {
    return new OffHeapMemoryBlock(address, offset, size);
  }

  @Override
  public void setPageNumber(int pageNum) {
    this.pageNumber = pageNum;
  }

  @Override
  public int getPageNumber() {
    return this.pageNumber;
  }
}
