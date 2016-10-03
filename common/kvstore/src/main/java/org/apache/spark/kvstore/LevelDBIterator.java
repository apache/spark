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

package org.apache.spark.kvstore;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.iq80.leveldb.DBIterator;

class LevelDBIterator<T> implements KVStoreIterator<T> {

  private final LevelDB db;
  private final boolean ascending;
  private final DBIterator it;
  private final Class<T> type;
  private final LevelDBTypeInfo<T> ti;
  private final LevelDBTypeInfo<T>.Index index;
  private final byte[] indexKeyPrefix;
  private final byte[] end;

  private boolean checkedNext;
  private T next;
  private boolean closed;

  /**
   * Creates a simple iterator over db keys.
   */
  LevelDBIterator(LevelDB db, byte[] keyPrefix, Class<T> type) throws Exception {
    this.db = db;
    this.ascending = true;
    this.type = type;
    this.ti = null;
    this.index = null;
    this.it = db.db.iterator();
    this.indexKeyPrefix = keyPrefix;
    this.end = null;
    it.seek(keyPrefix);
  }

  /**
   * Creates an iterator for indexed types (i.e., those whose keys are managed by the library).
   */
  LevelDBIterator(LevelDB db, KVStoreView<T> params) throws Exception {
    this.db = db;
    this.ascending = params.ascending;
    this.it = db.db.iterator();
    this.type = params.type;
    this.ti = db.getTypeInfo(type);
    this.index = ti.index(params.index);
    this.indexKeyPrefix = index.keyPrefix();

    byte[] firstKey;
    if (params.first != null) {
      if (ascending) {
        firstKey = index.start(params.first);
      } else {
        firstKey = index.end(params.first);
      }
    } else if (ascending) {
      firstKey = index.keyPrefix();
    } else {
      firstKey = index.end();
    }
    it.seek(firstKey);

    if (ascending) {
      this.end = index.end();
    } else {
      this.end = null;
      if (it.hasNext()) {
        it.next();
      }
    }

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
    return next;
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
    long skipped = 0;
    while (skipped < n) {
      next = null;
      boolean hasNext = ascending ? it.hasNext() : it.hasPrev();
      if (!hasNext) {
        return false;
      }

      Map.Entry<byte[], byte[]> e = ascending ? it.next() : it.prev();
      if (!isEndMarker(e.getKey())) {
        skipped++;
      }
    }

    return true;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      it.close();
      closed = true;
    }
  }

  private T loadNext() {
    try {
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

        // If the next key is an end marker, then skip it.
        if (isEndMarker(nextKey)) {
          continue;
        }

        // Next key is not part of the index, stop.
        if (!startsWith(nextKey, indexKeyPrefix)) {
          return null;
        }

        // If there's a known end key and it's found, stop.
        if (end != null && Arrays.equals(nextKey, end)) {
          return null;
        }

        // Next element is part of the iteration, return it.
        if (index == null || index.isCopy()) {
          return db.serializer.deserialize(nextEntry.getValue(), type);
        } else {
          byte[] key = stitch(ti.naturalIndex().keyPrefix(), nextEntry.getValue());
          return db.get(key, type);
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
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
        key[key.length - 1] == (byte) LevelDBTypeInfo.END_MARKER.charAt(0));
  }

  private byte[] stitch(byte[]... comps) {
    int len = 0;
    for (byte[] comp : comps) {
      len += comp.length;
    }

    byte[] dest = new byte[len];
    int written = 0;
    for (byte[] comp : comps) {
      System.arraycopy(comp, 0, dest, written, comp.length);
      written += comp.length;
    }

    return dest;
  }

}
