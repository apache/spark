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

package org.apache.spark.util.kvstore;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.iq80.leveldb.DBIterator;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

class LevelDBIterator<T> implements KVStoreIterator<T> {

  private static final Cleaner CLEANER = Cleaner.create();

  private final LevelDB db;
  private final boolean ascending;
  private final DBIterator it;
  private final Class<T> type;
  private final LevelDBTypeInfo ti;
  private final LevelDBTypeInfo.Index index;
  private final byte[] indexKeyPrefix;
  private final byte[] end;
  private final long max;

  private final ResourceCleaner resourceCleaner;
  private final Cleaner.Cleanable cleanable;

  private boolean checkedNext;
  private byte[] next;
  private boolean closed;
  private long count;

  LevelDBIterator(Class<T> type, LevelDB db, KVStoreView<T> params) throws Exception {
    this.db = db;
    this.ascending = params.ascending;
    this.it = db.db().iterator();
    this.type = type;
    this.ti = db.getTypeInfo(type);
    this.index = ti.index(params.index);
    this.max = params.max;
    this.resourceCleaner = new ResourceCleaner(it, db);
    this.cleanable = CLEANER.register(this, this.resourceCleaner);

    Preconditions.checkArgument(!index.isChild() || params.parent != null,
      "Cannot iterate over child index %s without parent value.", params.index);
    byte[] parent = index.isChild() ? index.parent().childPrefix(params.parent) : null;

    this.indexKeyPrefix = index.keyPrefix(parent);

    byte[] firstKey;
    if (params.first != null) {
      if (ascending) {
        firstKey = index.start(parent, params.first);
      } else {
        firstKey = index.end(parent, params.first);
      }
    } else if (ascending) {
      firstKey = index.keyPrefix(parent);
    } else {
      firstKey = index.end(parent);
    }
    it.seek(firstKey);

    byte[] end = null;
    if (ascending) {
      if (params.last != null) {
        end = index.end(parent, params.last);
      } else {
        end = index.end(parent);
      }
    } else {
      if (params.last != null) {
        end = index.start(parent, params.last);
      }
      if (it.hasNext()) {
        // When descending, the caller may have set up the start of iteration at a non-existent
        // entry that is guaranteed to be after the desired entry. For example, if you have a
        // compound key (a, b) where b is a, integer, you may seek to the end of the elements that
        // have the same "a" value by specifying Integer.MAX_VALUE for "b", and that value may not
        // exist in the database. So need to check here whether the next value actually belongs to
        // the set being returned by the iterator before advancing.
        byte[] nextKey = it.peekNext().getKey();
        if (compare(nextKey, indexKeyPrefix) <= 0) {
          it.next();
        }
      }
    }
    this.end = end;

    if (params.skip > 0) {
      skip(params.skip);
    }
  }

  @Override
  public boolean hasNext() {
    if (!checkedNext && !closed) {
      next = loadNext();
      checkedNext = true;
    }
    if (!closed && next == null) {
      try {
        close();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return next != null;
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    checkedNext = false;

    try {
      T ret;
      if (index == null || index.isCopy()) {
        ret = db.serializer.deserialize(next, type);
      } else {
        byte[] key = ti.buildKey(false, ti.naturalIndex().keyPrefix(null), next);
        ret = db.get(key, type);
      }
      next = null;
      return ret;
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<T> next(int max) {
    List<T> list = new ArrayList<>(max);
    while (hasNext() && list.size() < max) {
      list.add(next());
    }
    return list;
  }

  @Override
  public boolean skip(long n) {
    if (closed) return false;

    long skipped = 0;
    while (skipped < n) {
      if (next != null) {
        checkedNext = false;
        next = null;
        skipped++;
        continue;
      }

      boolean hasNext = ascending ? it.hasNext() : it.hasPrev();
      if (!hasNext) {
        checkedNext = true;
        return false;
      }

      Map.Entry<byte[], byte[]> e = ascending ? it.next() : it.prev();
      if (!isEndMarker(e.getKey())) {
        skipped++;
      }
    }

    return hasNext();
  }

  @Override
  public synchronized void close() throws IOException {
    db.notifyIteratorClosed(it);
    if (!closed) {
      try {
        it.close();
      } finally {
        closed = true;
        next = null;
        cancelResourceClean();
      }
    }
  }

  /**
   * Prevent ResourceCleaner from trying to release resources after close.
   */
  private void cancelResourceClean() {
    this.resourceCleaner.setStartedToFalse();
    this.cleanable.clean();
  }

  DBIterator internalIterator() {
    return it;
  }

  @VisibleForTesting
  ResourceCleaner getResourceCleaner() {
    return resourceCleaner;
  }

  private byte[] loadNext() {
    if (count >= max) {
      return null;
    }

    while (true) {
      boolean hasNext = ascending ? it.hasNext() : it.hasPrev();
      if (!hasNext) {
        return null;
      }

      Map.Entry<byte[], byte[]> nextEntry;
      try {
        // Avoid races if another thread is updating the DB.
        nextEntry = ascending ? it.next() : it.prev();
      } catch (NoSuchElementException e) {
        return null;
      }

      byte[] nextKey = nextEntry.getKey();
      // Next key is not part of the index, stop.
      if (!startsWith(nextKey, indexKeyPrefix)) {
        return null;
      }

      // If the next key is an end marker, then skip it.
      if (isEndMarker(nextKey)) {
        continue;
      }

      // If there's a known end key and iteration has gone past it, stop.
      if (end != null) {
        int comp = compare(nextKey, end) * (ascending ? 1 : -1);
        if (comp > 0) {
          return null;
        }
      }

      count++;

      // Next element is part of the iteration, return it.
      return nextEntry.getValue();
    }
  }

  @VisibleForTesting
  static boolean startsWith(byte[] key, byte[] prefix) {
    if (key.length < prefix.length) {
      return false;
    }

    for (int i = 0; i < prefix.length; i++) {
      if (key[i] != prefix[i]) {
        return false;
      }
    }

    return true;
  }

  private boolean isEndMarker(byte[] key) {
    return (key.length > 2 &&
        key[key.length - 2] == LevelDBTypeInfo.KEY_SEPARATOR &&
        key[key.length - 1] == LevelDBTypeInfo.END_MARKER[0]);
  }

  static int compare(byte[] a, byte[] b) {
    int diff = 0;
    int minLen = Math.min(a.length, b.length);
    for (int i = 0; i < minLen; i++) {
      diff += (a[i] - b[i]);
      if (diff != 0) {
        return diff;
      }
    }

    return a.length - b.length;
  }

  static class ResourceCleaner implements Runnable {
    private static final SparkLogger LOG = SparkLoggerFactory.getLogger(ResourceCleaner.class);

    private final DBIterator dbIterator;

    private final LevelDB levelDB;

    private final AtomicBoolean started = new AtomicBoolean(true);

    ResourceCleaner(DBIterator dbIterator, LevelDB levelDB) {
      this.dbIterator = dbIterator;
      this.levelDB = levelDB;
    }

    @Override
    public void run() {
      if (started.compareAndSet(true, false)) {
        try {
          levelDB.closeIterator(dbIterator);
        } catch (IOException e) {
          LOG.warn("Failed to close iterator", e);
        }
      }
    }

    void setStartedToFalse() {
      started.set(false);
    }

    @VisibleForTesting
    boolean isCompleted() {
      return !started.get();
    }
  }
}
