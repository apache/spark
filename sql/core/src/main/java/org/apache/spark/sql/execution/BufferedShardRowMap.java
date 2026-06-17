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

package org.apache.spark.sql.execution;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.map.HashMapGrowthStrategy;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * Probe-side key batching buffer for distributed map join.
 *
 * Accumulates streamed-side keys into per-shard batches backed by off-heap
 * Tungsten pages, then serializes them into Netty buffers for RPC lookup
 * against build-side executors.
 */
public final class BufferedShardRowMap extends MemoryConsumer {

  private static final SparkLogger logger = SparkLoggerFactory.getLogger(BufferedShardRowMap.class);

  private final TaskMemoryManager taskMemoryManager;

  private final long setId;
  private final int numShards;
  private final int batchCapacity;
  private final int mask;
  private final int uaoSize;

  private final int numFields;
  private final UnsafeRow valueUr;

  private final KeyValueBatch[] tailingBatches;
  private final LinkedList<KeyValueBatch> pendingBatches;

  private final LinkedList<KeyValuePage> pageList;

  private final PooledByteBufAllocator alloc;

  private KeyValuePage currentPage = null;

  public BufferedShardRowMap(TaskMemoryManager taskMemoryManager, long setId, int numShards,
                             int numFields, UnsafeRow valueUr, int batchSize) {
    super(taskMemoryManager, taskMemoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = taskMemoryManager;
    this.setId = setId;
    this.numShards = numShards;
    this.tailingBatches = new KeyValueBatch[numShards];
    this.pendingBatches = new LinkedList<>();
    this.pageList = new LinkedList<>();
    this.numFields = numFields;
    this.valueUr = valueUr;
    this.alloc = NettyUtils.getSharedPooledByteBufAllocator(true, true);
    this.batchCapacity = Math.max((int) ByteArrayMethods.nextPowerOf2(batchSize), 32);
    this.mask = HashMapGrowthStrategy.DOUBLING.nextCapacity(batchCapacity) - 1;
    this.uaoSize = UnsafeAlignedOffset.getUaoSize();
  }

  public void putRow(int shard,
                     Object kbase, long koff, int klen, int khash,
                     Object vbase, long voff, int vlen) {
    final int idx = shard % numShards;
    if (tailingBatches[idx] == null) {
      tailingBatches[idx] = new KeyValueBatch(shard);
    }
    KeyValueBatch batch = tailingBatches[idx];
    if (batch.capReached()) {
      pendingBatches.add(batch);
      tailingBatches[idx] = new KeyValueBatch(shard);
      batch = tailingBatches[idx];
    }
    batch.append(kbase, koff, klen, khash, vbase, voff, vlen);
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this) {
      synchronized (this) {
        final Iterator<KeyValuePage> iter = pageList.iterator();
        long released = 0L;
        while (iter.hasNext()) {
          final KeyValuePage page = iter.next();
          if (page.nonRef()) {
            iter.remove();
            if (page == currentPage) {
              currentPage = null;
            }
            released += page.getSize();
            page.free();
            if (released >= size) {
              break;
            }
          }
        }
        return released;
      }
    }
    return 0L;
  }

  public synchronized void free() {
    Iterator<KeyValuePage> valueIter = pageList.iterator();
    while (valueIter.hasNext()) {
      KeyValuePage valuePage = valueIter.next();
      valueIter.remove();
      valuePage.free();
    }
  }

  private synchronized KeyValuePage acquirePage(long required) {
    for (KeyValuePage page : pageList) {
      if (page.nonRef()) {
        page.resetCursor();
      }

      if (required <= page.getWritableSize()) {
        return page;
      }
    }
    KeyValuePage page = new KeyValuePage(allocatePage(uaoSize + required));
    page.resetCursor();
    pageList.add(page);
    return page;
  }

  public boolean hasPending() {
    return !pendingBatches.isEmpty();
  }

  public Iterator<KeyValueBatch> pendingIterator() {
    return pendingBatches.iterator();
  }

  public Iterator<KeyValueBatch> tailingIterator() {
    return new Iterator<KeyValueBatch>() {
      private int nextIdx = advance(0);
      private int lastIdx = -1;
      private boolean canRemove = false;

      @Override
      public boolean hasNext() {
        return nextIdx != -1;
      }

      @Override
      public KeyValueBatch next() {
        if (nextIdx == -1) throw new NoSuchElementException();
        lastIdx = nextIdx;
        KeyValueBatch v = tailingBatches[lastIdx];
        nextIdx = advance(lastIdx + 1);
        canRemove = true;
        return v;
      }

      @Override
      public void remove() {
        if (!canRemove) {
          throw new IllegalStateException("next() not called or remove() already called");
        }
        tailingBatches[lastIdx] = null;
        canRemove = false;
      }

      private int advance(int from) {
        for (int i = from; i < tailingBatches.length; i++) {
          if (tailingBatches[i] != null) return i;
        }
        return -1;
      }
    };
  }

  @FunctionalInterface
  private interface ObjectLongIntConsumer {
    void accept(Object base, long off, int len);
  }

  private ObjectLongIntConsumer makeAdvanceWrite(final ByteBuf buf) {
    if (buf.hasArray()) {
      return (base, off, len) -> {
        final int idx = buf.writerIndex();
        Platform.copyMemory(base, off, buf.array(),
          Platform.BYTE_ARRAY_OFFSET + buf.arrayOffset() + idx, len);
        buf.writerIndex(idx + len);
      };
    } else if (buf.hasMemoryAddress()) {
      return (base, off, len) -> {
        final int idx = buf.writerIndex();
        Platform.copyMemory(base, off, null, buf.memoryAddress() + idx, len);
        buf.writerIndex(idx + len);
      };
    } else {
      return (base, off, len) -> {
        final byte[] arr = new byte[len];
        Platform.copyMemory(base, off, arr, Platform.BYTE_ARRAY_OFFSET, len);
        buf.writeBytes(arr);
      };
    }
  }

  public final class KeyValueBatch {
    private final int shard;
    private final Map<Integer, Long> keyMap;
    private final LinkedList<KeyValuePage> innerPageList;

    private int numKeys;
    private int numValues;
    private int sizeKeys;
    private KeyValuePage lastWritePage;

    private KeyValueBatch(int shard) {
      this.shard = shard;
      this.keyMap = new HashMap<>();
      this.innerPageList = new LinkedList<>();
    }

    public long getSetId() {
      return setId;
    }

    public int getShard() {
      return shard;
    }

    public ManagedBuffer wrapKeysBuffer() {
      // header: setId(8) + shardId(4) + numKeyFields(4)
      final int headerSize = Long.BYTES + Integer.BYTES * 2;
      final int capacity = headerSize + Integer.BYTES * numKeys + sizeKeys;
      final ByteBuf buf = alloc.buffer(capacity);
      buf.writeLong(setId);
      buf.writeInt(shard);
      buf.writeInt(numFields);

      final ObjectLongIntConsumer advanceWrite = makeAdvanceWrite(buf);

      for (long address : keyMap.values()) {
        final Object base = taskMemoryManager.getPage(address);
        final long off = taskMemoryManager.getOffsetInPage(address);
        final int klen = UnsafeAlignedOffset.getSize(base, off);
        buf.writeInt(klen);
        advanceWrite.accept(base, off + (uaoSize * 2L), klen);
      }

      return new NettyManagedBuffer(buf);
    }

    public Iterator<Iterator<UnsafeRow>> multiValuesIterator() {
      return new Iterator<Iterator<UnsafeRow>>() {
        private final Iterator<Long> iter = keyMap.values().iterator();

        @Override
        public boolean hasNext() {
          return iter.hasNext();
        }

        @Override
        public Iterator<UnsafeRow> next() {
          long addr = iter.next();
          Object base = taskMemoryManager.getPage(addr);
          long off = taskMemoryManager.getOffsetInPage(addr);
          int len = UnsafeAlignedOffset.getSize(base, off);
          off += ((uaoSize * 2L) + len);
          final long headAddr = Platform.getLong(base, off);

          return new Iterator<UnsafeRow>() {

            private long address = headAddr;

            @Override
            public boolean hasNext() {
              return address > 0L;
            }

            @Override
            public UnsafeRow next() {
              // (value length) (value) (next)
              Object base = taskMemoryManager.getPage(address);
              long off = taskMemoryManager.getOffsetInPage(address);
              int vlen = UnsafeAlignedOffset.getSize(base, off);
              off += uaoSize;
              valueUr.pointTo(base, off, vlen);
              off += vlen;
              address = Platform.getLong(base, off);
              return valueUr;
            }
          };
        }
      };
    }

    private boolean capReached() {
      return numValues >= batchCapacity;
    }

    private void append(Object kbase, long koff, int klen, int khash,
                        Object vbase, long voff, int vlen) {
      assert numValues < batchCapacity : "capacity reached " + batchCapacity;
      // lookup key
      int pos = khash & mask;
      boolean hasKey = false;
      Object keyBaseObject = null;
      long keyNextOffset = 0;
      int step = 1;
      while (true) {
        long keyAddress = keyMap.getOrDefault(pos, 0L);
        if (keyAddress == 0L) {
          // fresh key
          break;
        } else {
          final Object base = taskMemoryManager.getPage(keyAddress);
          long offset = taskMemoryManager.getOffsetInPage(keyAddress);
          keyBaseObject = base;
          int keyLength = UnsafeAlignedOffset.getSize(base, offset);
          if (keyLength == klen) {
            offset += uaoSize;
            int keyHash = UnsafeAlignedOffset.getSize(base, offset);
            if (keyHash == khash) {
              offset += uaoSize;
              hasKey = ByteArrayMethods.arrayEquals(base, offset, kbase, koff, klen);
              if (hasKey) {
                // exists key
                offset += klen;
                keyNextOffset = offset;
                break;
              }
            }
          }
        }
        pos = (pos + step) & mask;
        step++;
      }

      long nextAddress = hasKey ? Platform.getLong(keyBaseObject, keyNextOffset) : 0L;
      // (value length) (value) (next)
      final long valueRecordLength = uaoSize + vlen + NEXT_POINTER_BYTES;
      if (currentPage == null || currentPage.getWritableSize() < valueRecordLength) {
        currentPage = acquirePage(valueRecordLength);
      }
      if (lastWritePage != currentPage) {
        ensurePageRef();
      }
      final long valueAddress = currentPage.putValue(vbase, voff, vlen, nextAddress);
      numValues++;
      if (hasKey) {
        Platform.putLong(keyBaseObject, keyNextOffset, valueAddress);
      } else {
        numKeys++;
        sizeKeys += klen;
        // (key length) (key hash) (key) (next)
        final long keyRecordLength = (uaoSize * 2L) + klen + NEXT_POINTER_BYTES;
        if (currentPage.getWritableSize() < keyRecordLength) {
          currentPage = acquirePage(keyRecordLength);
        }
        if (lastWritePage != currentPage) {
          ensurePageRef();
        }
        long keyAddress = currentPage.putKey(kbase, koff, klen, khash, valueAddress);
        keyMap.put(pos, keyAddress);
      }
    }

    public void release() throws IOException {
      Iterator<KeyValuePage> iter = innerPageList.iterator();
      while (iter.hasNext()) {
        KeyValuePage page = iter.next();
        iter.remove();
        page.release();
      }
    }

    private void ensurePageRef() {
      lastWritePage = currentPage;
      lastWritePage.retain();
      innerPageList.add(lastWritePage);
    }
  }

  private static final int NEXT_POINTER_BYTES = Long.BYTES;

  private final class KeyValuePage {

    private final MemoryBlock page;
    private final Object baseObject;
    private final long baseOffset;
    private int refCount;
    private long cursor;

    private KeyValuePage(MemoryBlock page) {
      this.page = page;
      this.baseObject = page.getBaseObject();
      this.baseOffset = page.getBaseOffset();
      UnsafeAlignedOffset.putSize(baseObject, baseOffset, 0);
      this.refCount = 0;
    }

    private long putKey(Object kbase, long koff, int klen, int khash, long next) {
      long offset = baseOffset + cursor;
      final long recordOffset = offset;
      UnsafeAlignedOffset.putSize(baseObject, offset, klen);
      offset += uaoSize;
      UnsafeAlignedOffset.putSize(baseObject, offset, khash);
      offset += uaoSize;
      Platform.copyMemory(kbase, koff, baseObject, offset, klen);
      offset += klen;
      Platform.putLong(baseObject, offset, next);
      cursor += ((uaoSize * 2L) + klen + NEXT_POINTER_BYTES);
      return taskMemoryManager.encodePageNumberAndOffset(page, recordOffset);
    }

    private long putValue(Object vbase, long voff, int vlen, long next) {
      long offset = baseOffset + cursor;
      final long recordOffset = offset;
      UnsafeAlignedOffset.putSize(baseObject, offset, vlen);
      offset += uaoSize;
      Platform.copyMemory(vbase, voff, baseObject, offset, vlen);
      offset += vlen;
      Platform.putLong(baseObject, offset, next);
      cursor += (uaoSize + vlen + NEXT_POINTER_BYTES);
      return taskMemoryManager.encodePageNumberAndOffset(page, recordOffset);
    }

    private void resetCursor() {
      assert refCount == 0 : "hasRef";
      this.cursor = uaoSize;
    }

    private long getSize() {
      return page.size();
    }

    private long getWritableSize() {
      return getSize() - cursor;
    }

    private boolean nonRef() {
      return refCount == 0;
    }

    private void retain() {
      refCount++;
    }

    private void release() {
      assert refCount > 0 : "refCount=" + refCount;
      refCount--;
    }

    private void free() {
      freePage(page);
    }
  }
}
