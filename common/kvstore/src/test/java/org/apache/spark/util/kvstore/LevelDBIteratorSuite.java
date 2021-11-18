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
import org.apache.commons.lang3.SystemUtils;
import org.junit.AfterClass;
import static org.junit.Assume.assumeFalse;

public class LevelDBIteratorSuite extends DBIteratorSuite {

  private static File dbpath;
  private static LevelDB db;

  @AfterClass
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
    assumeFalse(SystemUtils.IS_OS_MAC_OSX && SystemUtils.OS_ARCH.equals("aarch64"));
    dbpath = File.createTempFile("test.", ".ldb");
    dbpath.delete();
    db = new LevelDB(dbpath);
    return db;
  }

}
