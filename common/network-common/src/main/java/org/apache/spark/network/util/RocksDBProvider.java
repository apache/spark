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
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.shuffledb.StoreVersion;

/**
 * RocksDB utility class available in the network package.
 */
public class RocksDBProvider {

    static {
      org.rocksdb.RocksDB.loadLibrary();
    }

    private static final Logger logger = LoggerFactory.getLogger(RocksDBProvider.class);

    public static RocksDB initRockDB(File dbFile, StoreVersion version, ObjectMapper mapper) throws
        IOException {
      RocksDB tmpDb = null;
      if (dbFile != null) {
        BloomFilter fullFilter =
          new BloomFilter(10.0D /* BloomFilter.DEFAULT_BITS_PER_KEY */, false);
        BlockBasedTableConfig tableFormatConfig = new BlockBasedTableConfig()
          .setFilterPolicy(fullFilter)
          .setEnableIndexCompression(false)
          .setIndexBlockRestartInterval(8)
          .setFormatVersion(5);

        Options dbOptions = new Options();
        RocksDBLogger rocksDBLogger = new RocksDBLogger(dbOptions);

        dbOptions.setCreateIfMissing(false);
        dbOptions.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
        dbOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        dbOptions.setTableFormatConfig(tableFormatConfig);
        dbOptions.setLogger(rocksDBLogger);

        try {
          tmpDb = RocksDB.open(dbOptions, dbFile.toString());
        } catch (RocksDBException e) {
          if (e.getStatus().getCode() == Status.Code.NotFound) {
            logger.info("Creating state database at " + dbFile);
            dbOptions.setCreateIfMissing(true);
            try {
              tmpDb = RocksDB.open(dbOptions, dbFile.toString());
            } catch (RocksDBException dbExc) {
              throw new IOException("Unable to create state store", dbExc);
            }
          } else {
            // the RocksDB file seems to be corrupt somehow.  Let's just blow it away and create
            // a new one, so we can keep processing new apps
            logger.error("error opening rocksdb file {}. Creating new file, will not be able to " +
              "recover state for existing applications", dbFile, e);
            if (dbFile.isDirectory()) {
              for (File f : Objects.requireNonNull(dbFile.listFiles())) {
                if (!f.delete()) {
                  logger.warn("error deleting {}", f.getPath());
                }
              }
            }
            if (!dbFile.delete()) {
              logger.warn("error deleting {}", dbFile.getPath());
            }
            dbOptions.setCreateIfMissing(true);
            try {
              tmpDb = RocksDB.open(dbOptions, dbFile.toString());
            } catch (RocksDBException dbExc) {
              throw new IOException("Unable to create state store", dbExc);
            }
          }
        }
        try {
          // if there is a version mismatch, we throw an exception, which means the service
          // is unusable
          checkVersion(tmpDb, version, mapper);
        } catch (RocksDBException e) {
          throw new IOException(e.getMessage(), e);
        }
      }
      return tmpDb;
    }

    @VisibleForTesting
    static RocksDB initRocksDB(File file) throws IOException {
      BloomFilter fullFilter =
        new BloomFilter(10.0D /* BloomFilter.DEFAULT_BITS_PER_KEY */, false);
      BlockBasedTableConfig tableFormatConfig = new BlockBasedTableConfig()
        .setFilterPolicy(fullFilter)
        .setEnableIndexCompression(false)
        .setIndexBlockRestartInterval(8)
        .setFormatVersion(5);

      Options dbOptions = new Options();
      dbOptions.setCreateIfMissing(true);
      dbOptions.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
      dbOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
      dbOptions.setTableFormatConfig(tableFormatConfig);
      try {
        return RocksDB.open(dbOptions, file.toString());
      } catch (RocksDBException e) {
        throw new IOException("Unable to open state store", e);
      }
    }

    private static class RocksDBLogger extends org.rocksdb.Logger {
        private static final Logger LOG = LoggerFactory.getLogger(RocksDBLogger.class);

        RocksDBLogger(Options options) {
          super(options);
        }

        @Override
        protected void log(InfoLogLevel infoLogLevel, String message) {
          if (infoLogLevel == InfoLogLevel.INFO_LEVEL) {
            LOG.info(message);
          }
        }
    }

    /**
     * Simple major.minor versioning scheme.  Any incompatible changes should be across major
     * versions.  Minor version differences are allowed -- meaning we should be able to read
     * dbs that are either earlier *or* later on the minor version.
     */
    public static void checkVersion(RocksDB db, StoreVersion newversion, ObjectMapper mapper) throws
        IOException, RocksDBException {
      byte[] bytes = db.get(StoreVersion.KEY);
      if (bytes == null) {
        storeVersion(db, newversion, mapper);
      } else {
        StoreVersion version = mapper.readValue(bytes, StoreVersion.class);
        if (version.major != newversion.major) {
          throw new IOException("cannot read state DB with version " + version + ", incompatible " +
            "with current version " + newversion);
        }
        storeVersion(db, newversion, mapper);
      }
    }

    public static void storeVersion(RocksDB db, StoreVersion version, ObjectMapper mapper)
        throws IOException, RocksDBException {
      db.put(StoreVersion.KEY, mapper.writeValueAsBytes(version));
    }
}
