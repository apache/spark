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
package org.apache.spark.network.util;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.internal.Logger;
import org.apache.spark.internal.LoggerFactory;
import org.apache.spark.network.shuffledb.DB;
import org.apache.spark.network.shuffledb.DBBackend;
import org.apache.spark.network.shuffledb.LevelDB;
import org.apache.spark.network.shuffledb.RocksDB;
import org.apache.spark.network.shuffledb.StoreVersion;

public class DBProvider {
  private static final Logger logger = LoggerFactory.getLogger(DBProvider.class);
    public static DB initDB(
        DBBackend dbBackend,
        File dbFile,
        StoreVersion version,
        ObjectMapper mapper) throws IOException {
      if (dbFile != null) {
        return switch (dbBackend) {
          case LEVELDB -> {
            org.iq80.leveldb.DB levelDB = LevelDBProvider.initLevelDB(dbFile, version, mapper);
            logger.warn("The LEVELDB is deprecated. Please use ROCKSDB instead.");
            yield levelDB != null ? new LevelDB(levelDB) : null;
          }
          case ROCKSDB -> {
            org.rocksdb.RocksDB rocksDB = RocksDBProvider.initRockDB(dbFile, version, mapper);
            yield rocksDB != null ? new RocksDB(rocksDB) : null;
          }
        };
      }
      return null;
    }

    @VisibleForTesting
    public static DB initDB(DBBackend dbBackend, File file) throws IOException {
      if (file != null) {
        return switch (dbBackend) {
          case LEVELDB -> {
            logger.warn("The LEVELDB is deprecated. Please use ROCKSDB instead.");
            yield new LevelDB(LevelDBProvider.initLevelDB(file));
          }
          case ROCKSDB -> new RocksDB(RocksDBProvider.initRocksDB(file));
        };
      }
      return null;
    }
}
