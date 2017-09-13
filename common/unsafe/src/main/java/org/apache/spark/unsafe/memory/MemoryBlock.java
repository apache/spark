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

import javax.annotation.Nullable;

import org.apache.spark.unsafe.Platform;

/**
 * A declaration of interfaces of MemoryBloock classes .
 */
public interface MemoryBlock {
  /** Special `pageNumber` value for pages which were not allocated by TaskMemoryManagers */
  public static final int NO_PAGE_NUMBER = -1;

  /**
   * Special `pageNumber` value for marking pages that have been freed in the TaskMemoryManager.
   * We set `pageNumber` to this value in TaskMemoryManager.freePage() so that MemoryAllocator
   * can detect if pages which were allocated by TaskMemoryManager have been freed in the TMM
   * before being passed to MemoryAllocator.free() (it is an error to allocate a page in
   * TaskMemoryManager and then directly free it in a MemoryAllocator without going through
   * the TMM freePage() call).
   */
  public static final int FREED_IN_TMM_PAGE_NUMBER = -2;

  /**
   * Special `pageNumber` value for pages that have been freed by the MemoryAllocator. This allows
   * us to detect double-frees.
   */
  public static final int FREED_IN_ALLOCATOR_PAGE_NUMBER = -3;

  /**
   * Returns the size of the memory block.
   */
  long size();

  Object getBaseObject();

  long getBaseOffset();

  void setPageNumber(int pageNum);

  int getPageNumber();

  /**
   * Fills the memory block with the specified byte value.
   */
  void fill(byte value);

  /**
   * Instantiate the same type of MemoryBlock with new offset and size
   */
  MemoryBlock allocate(long offset, long size);
}
