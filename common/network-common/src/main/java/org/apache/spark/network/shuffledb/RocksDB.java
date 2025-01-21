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

package org.apache.spark.network.shuffledb;

import java.io.IOException;

import org.rocksdb.RocksDBException;

/**
 * RocksDB implementation of the local KV storage used to persist the shuffle state.
 */
public class RocksDB implements DB {
    private final org.rocksdb.RocksDB db;

    public RocksDB(org.rocksdb.RocksDB db) {
      this.db = db;
    }

    @Override
    public void put(byte[] key, byte[] value) {
      try {
        db.put(key, value);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public byte[] get(byte[] key) {
      try {
        return db.get(key);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void delete(byte[] key) {
      try {
        db.delete(key);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public DBIterator iterator() {
      return new RocksDBIterator(db.newIterator());
    }

    @Override
    public void close() throws IOException {
      db.close();
    }
}
