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
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.shuffledb.StoreVersion;

/**
 * LevelDB utility class available in the network package.
 */
public class LevelDBProvider {
  private static final Logger logger = LoggerFactory.getLogger(LevelDBProvider.class);

  public static DB initLevelDB(File dbFile, StoreVersion version, ObjectMapper mapper) throws
      IOException {
    DB tmpDb = null;
    if (dbFile != null) {
      Options options = new Options();
      options.createIfMissing(false);
      options.logger(new LevelDBLogger());
      try {
        tmpDb = JniDBFactory.factory.open(dbFile, options);
      } catch (NativeDB.DBException e) {
        if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
          logger.info("Creating state database at " + dbFile);
          options.createIfMissing(true);
          try {
            tmpDb = JniDBFactory.factory.open(dbFile, options);
          } catch (NativeDB.DBException dbExc) {
            throw new IOException("Unable to create state store", dbExc);
          }
        } else {
          // the leveldb file seems to be corrupt somehow.  Lets just blow it away and create a new
          // one, so we can keep processing new apps
          logger.error("error opening leveldb file {}.  Creating new file, will not be able to " +
              "recover state for existing applications", dbFile, e);
          if (dbFile.isDirectory()) {
            for (File f : dbFile.listFiles()) {
              if (!f.delete()) {
                logger.warn("error deleting {}", f.getPath());
              }
            }
          }
          if (!dbFile.delete()) {
            logger.warn("error deleting {}", dbFile.getPath());
          }
          options.createIfMissing(true);
          try {
            tmpDb = JniDBFactory.factory.open(dbFile, options);
          } catch (NativeDB.DBException dbExc) {
            throw new IOException("Unable to create state store", dbExc);
          }

        }
      }
      // if there is a version mismatch, we throw an exception, which means the service is unusable
      checkVersion(tmpDb, version, mapper);
    }
    return tmpDb;
  }

  @VisibleForTesting
  static DB initLevelDB(File file) throws IOException {
    Options options = new Options();
    options.createIfMissing(true);
    JniDBFactory factory = new JniDBFactory();
    return factory.open(file, options);
  }

  private static class LevelDBLogger implements org.iq80.leveldb.Logger {
    private static final Logger LOG = LoggerFactory.getLogger(LevelDBLogger.class);

    @Override
    public void log(String message) {
      LOG.info(message);
    }
  }

  /**
   * Simple major.minor versioning scheme.  Any incompatible changes should be across major
   * versions.  Minor version differences are allowed -- meaning we should be able to read
   * dbs that are either earlier *or* later on the minor version.
   */
  public static void checkVersion(DB db, StoreVersion newversion, ObjectMapper mapper) throws
      IOException {
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

  public static void storeVersion(DB db, StoreVersion version, ObjectMapper mapper)
      throws IOException {
    db.put(StoreVersion.KEY, mapper.writeValueAsBytes(version));
  }
}
