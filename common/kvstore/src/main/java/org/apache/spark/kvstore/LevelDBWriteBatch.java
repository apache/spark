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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

/**
 * A wrapper around the LevelDB library's WriteBatch with some extra functionality for keeping
 * track of counts.
 */
class LevelDBWriteBatch implements AutoCloseable {

  private final LevelDB db;
  private final Map<KeyWrapper, Long> deltas;
  private final WriteBatch batch;

  LevelDBWriteBatch(LevelDB db) {
    this.db = db;
    this.batch = db.db().createWriteBatch();
    this.deltas = new HashMap<>();
  }

  void updateCount(byte[] key, long delta) {
    KeyWrapper kw = new KeyWrapper(key);
    Long fullDelta = deltas.get(kw);
    if (fullDelta != null) {
      fullDelta += delta;
    } else {
      fullDelta = delta;
    }
    deltas.put(kw, fullDelta);
  }

  void put(byte[] key, byte[] value) {
    batch.put(key, value);
  }

  void delete(byte[] key) {
    batch.delete(key);
  }

  void write(boolean sync) {
    for (Map.Entry<KeyWrapper, Long> e : deltas.entrySet()) {
      long delta = e.getValue();
      if (delta == 0) {
        continue;
      }

      byte[] key = e.getKey().key;
      byte[] data = db.db().get(key);
      long count = data != null ? db.serializer.deserializeLong(data) : 0L;
      long newCount = count + delta;

      if (newCount > 0) {
        batch.put(key, db.serializer.serialize(newCount));
      } else {
        batch.delete(key);
      }
    }

    db.db().write(batch, new WriteOptions().sync(sync));
  }

  public void close() throws Exception {
    batch.close();
  }

  private static class KeyWrapper {

    private final byte[] key;

    KeyWrapper(byte[] key) {
      this.key = key;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof KeyWrapper) {
        return Arrays.equals(key, ((KeyWrapper) other).key);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(key);
    }

  }

}
