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
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.rocksdb.RocksIterator;

class RocksDBIterator<T> implements KVStoreIterator<T> {

  private final RocksDB db;
  private final boolean ascending;
  private final RocksIterator it;
  private final Class<T> type;
  private final RocksDBTypeInfo ti;
  private final RocksDBTypeInfo.Index index;
  private final byte[] indexKeyPrefix;
  private final byte[] end;
  private final long max;

  private boolean checkedNext;
  private byte[] next;
  private boolean closed;
  private long count;

  RocksDBIterator(Class<T> type, RocksDB db, KVStoreView<T> params) throws Exception {
    this.db = db;
    this.ascending = params.ascending;
    this.it = db.db().newIterator();
    this.type = type;
    this.ti = db.getTypeInfo(type);
    this.index = ti.index(params.index);
    this.max = params.max;

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
      if(!it.isValid()) {
        throw new NoSuchElementException();
      }
      if (compare(it.key(), indexKeyPrefix) > 0) {
        it.prev();
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
        throw Throwables.propagate(ioe);
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
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
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
    if(closed) return false;

    long skipped = 0;
    while (skipped < n) {
      if (next != null) {
        checkedNext = false;
        next = null;
        skipped++;
        continue;
      }

      if (!it.isValid()) {
        checkedNext = true;
        return false;
      }

      if (!isEndMarker(it.key())) {
        skipped++;
      }
      if (ascending) {
        it.next();
      } else {
        it.prev();
      }
    }

    return hasNext();
  }

  @Override
  public synchronized void close() throws IOException {
    db.notifyIteratorClosed(this);
    if (!closed) {
      it.close();
      closed = true;
      next = null;
    }
  }

  /**
   * Because it's tricky to expose closeable iterators through many internal APIs, especially
   * when Scala wrappers are used, this makes sure that, hopefully, the JNI resources held by
   * the iterator will eventually be released.
   */
  @Override
  protected void finalize() throws Throwable {
    db.closeIterator(this);
  }

  private byte[] loadNext() {
    if (count >= max) {
      return null;
    }

    while (it.isValid()) {
      Map.Entry<byte[], byte[]> nextEntry = new AbstractMap.SimpleEntry<>(it.key(), it.value());

      byte[] nextKey = nextEntry.getKey();
      // Next key is not part of the index, stop.
      if (!startsWith(nextKey, indexKeyPrefix)) {
        return null;
      }

      // If the next key is an end marker, then skip it.
      if (isEndMarker(nextKey)) {
        if (ascending) {
          it.next();
        } else {
          it.prev();
        }
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
      if (ascending) {
        it.next();
      } else {
        it.prev();
      }
      // Next element is part of the iteration, return it.
      return nextEntry.getValue();
    }
    return null;
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

}
