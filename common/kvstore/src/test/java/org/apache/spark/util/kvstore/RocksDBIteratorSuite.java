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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;

public class RocksDBIteratorSuite extends DBIteratorSuite {

  private static File dbpath;
  private static RocksDB db;

  @AfterAll
  public static void cleanup() throws Exception {
    if (db != null) {
      db.close();
    }
    if (dbpath != null) {
      FileUtils.deleteQuietly(dbpath);
    }
  }

  @Override
  protected KVStore createStore() throws Exception {
    dbpath = File.createTempFile("test.", ".rdb");
    dbpath.delete();
    db = new RocksDB(dbpath);
    return db;
  }

}
