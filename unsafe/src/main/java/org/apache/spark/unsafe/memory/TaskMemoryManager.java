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

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the memory allocated by an individual task.
 * <p>
 * Most of the complexity in this class deals with encoding of off-heap addresses into 64-bit longs.
 * In off-heap mode, memory can be directly addressed with 64-bit longs. In on-heap mode, memory is
 * addressed by the combination of a base Object reference and a 64-bit offset within that object.
 * This is a problem when we want to store pointers to data structures inside of other structures,
 * such as record pointers inside hashmaps or sorting buffers. Even if we decided to use 128 bits
 * to address memory, we can't just store the address of the base object since it's not guaranteed
 * to remain stable as the heap gets reorganized due to GC.
 * <p>
 * Instead, we use the following approach to encode record pointers in 64-bit longs: for off-heap
 * mode, just store the raw address, and for on-heap mode use the upper 13 bits of the address to
 * store a "page number" and the lower 51 bits to store an offset within this page. These page
 * numbers are used to index into a "page table" array inside of the MemoryManager in order to
 * retrieve the base object.
 * <p>
 * This allows us to address 8192 pages. In on-heap mode, the maximum page size is limited by the
 * maximum size of a long[] array, allowing us to address 8192 * 2^32 * 8 bytes, which is
 * approximately 35 terabytes of memory.
 */
public final class TaskMemoryManager {

  private final Logger logger = LoggerFactory.getLogger(TaskMemoryManager.class);

  /**
   * The number of entries in the page table.
   */
  private static final int PAGE_TABLE_SIZE = 1 << 13;

  /** Bit mask for the lower 51 bits of a long. */
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  /** Bit mask for the upper 13 bits of a long */
  private static final long MASK_LONG_UPPER_13_BITS = ~MASK_LONG_LOWER_51_BITS;

  /**
   * Similar to an operating system's page table, this array maps page numbers into base object
   * pointers, allowing us to translate between the hashtable's internal 64-bit address
   * representation and the baseObject+offset representation which we use to support both in- and
   * off-heap addresses. When using an off-heap allocator, every entry in this map will be `null`.
   * When using an in-heap allocator, the entries in this map will point to pages' base objects.
   * Entries are added to this map as new data pages are allocated.
   */
  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

  /**
   * Bitmap for tracking free pages.
   */
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  /**
   * Tracks memory allocated with {@link TaskMemoryManager#allocate(long)}, used to detect / clean
   * up leaked memory.
   */
  private final HashSet<MemoryBlock> allocatedNonPageMemory = new HashSet<MemoryBlock>();

  private final ExecutorMemoryManager executorMemoryManager;

  /**
   * Tracks whether we're in-heap or off-heap. For off-heap, we short-circuit most of these methods
   * without doing any masking or lookups. Since this branching should be well-predicted by the JIT,
   * this extra layer of indirection / abstraction hopefully shouldn't be too expensive.
   */
  private final boolean inHeap;

  /**
   * Construct a new MemoryManager.
   */
  public TaskMemoryManager(ExecutorMemoryManager executorMemoryManager) {
    this.inHeap = executorMemoryManager.inHeap;
    this.executorMemoryManager = executorMemoryManager;
  }

  /**
   * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
   * intended for allocating large blocks of memory that will be shared between operators.
   */
  public MemoryBlock allocatePage(long size) {
    if (logger.isTraceEnabled()) {
      logger.trace("Allocating {} byte page", size);
    }
    if (size >= (1L << 51)) {
      throw new IllegalArgumentException("Cannot allocate a page with more than 2^51 bytes");
    }

    final int pageNumber;
    synchronized (this) {
      pageNumber = allocatedPages.nextClearBit(0);
      if (pageNumber >= PAGE_TABLE_SIZE) {
        throw new IllegalStateException(
          "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
      }
      allocatedPages.set(pageNumber);
    }
    final MemoryBlock page = executorMemoryManager.allocate(size);
    page.pageNumber = pageNumber;
    pageTable[pageNumber] = page;
    if (logger.isDebugEnabled()) {
      logger.debug("Allocate page number {} ({} bytes)", pageNumber, size);
    }
    return page;
  }

  /**
   * Free a block of memory allocated via {@link TaskMemoryManager#allocatePage(long)}.
   */
  public void freePage(MemoryBlock page) {
    if (logger.isTraceEnabled()) {
      logger.trace("Freeing page number {} ({} bytes)", page.pageNumber, page.size());
    }
    assert (page.pageNumber != -1) :
      "Called freePage() on memory that wasn't allocated with allocatePage()";
    executorMemoryManager.free(page);
    synchronized (this) {
      allocatedPages.clear(page.pageNumber);
    }
    pageTable[page.pageNumber] = null;
    if (logger.isDebugEnabled()) {
      logger.debug("Freed page number {} ({} bytes)", page.pageNumber, page.size());
    }
  }

  /**
   * Allocates a contiguous block of memory. Note that the allocated memory is not guaranteed
   * to be zeroed out (call `zero()` on the result if this is necessary). This method is intended
   * to be used for allocating operators' internal data structures. For data pages that you want to
   * exchange between operators, consider using {@link TaskMemoryManager#allocatePage(long)}, since
   * that will enable intra-memory pointers (see
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)} and this class's
   * top-level Javadoc for more details).
   */
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    final MemoryBlock memory = executorMemoryManager.allocate(size);
    allocatedNonPageMemory.add(memory);
    return memory;
  }

  /**
   * Free memory allocated by {@link TaskMemoryManager#allocate(long)}.
   */
  public void free(MemoryBlock memory) {
    assert (memory.pageNumber == -1) : "Should call freePage() for pages, not free()";
    executorMemoryManager.free(memory);
    final boolean wasAlreadyRemoved = !allocatedNonPageMemory.remove(memory);
    assert (!wasAlreadyRemoved) : "Called free() on memory that was already freed!";
  }

  /**
   * Given a memory page and offset within that page, encode this address into a 64-bit long.
   * This address will remain valid as long as the corresponding page has not been freed.
   */
  public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
    if (inHeap) {
      assert (page.pageNumber != -1) : "encodePageNumberAndOffset called with invalid page";
      return (((long) page.pageNumber) << 51) | (offsetInPage & MASK_LONG_LOWER_51_BITS);
    } else {
      return offsetInPage;
    }
  }

  /**
   * Get the page associated with an address encoded by
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   */
  public Object getPage(long pagePlusOffsetAddress) {
    if (inHeap) {
      final int pageNumber = (int) ((pagePlusOffsetAddress & MASK_LONG_UPPER_13_BITS) >>> 51);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      final Object page = pageTable[pageNumber].getBaseObject();
      assert (page != null);
      return page;
    } else {
      return null;
    }
  }

  /**
   * Get the offset associated with an address encoded by
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   */
  public long getOffsetInPage(long pagePlusOffsetAddress) {
    if (inHeap) {
      return (pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS);
    } else {
      return pagePlusOffsetAddress;
    }
  }

  /**
   * Clean up all allocated memory and pages. Returns the number of bytes freed. A non-zero return
   * value can be used to detect memory leaks.
   */
  public long cleanUpAllAllocatedMemory() {
    long freedBytes = 0;
    for (MemoryBlock page : pageTable) {
      if (page != null) {
        freedBytes += page.size();
        freePage(page);
      }
    }
    final Iterator<MemoryBlock> iter = allocatedNonPageMemory.iterator();
    while (iter.hasNext()) {
      final MemoryBlock memory = iter.next();
      freedBytes += memory.size();
      // We don't call free() here because that calls Set.remove, which would lead to a
      // ConcurrentModificationException here.
      executorMemoryManager.free(memory);
      iter.remove();
    }
    return freedBytes;
  }
}
