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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SystemUtils;
import org.apache.spark.network.shuffledb.DBBackend;
import org.apache.spark.network.shuffledb.StoreVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class DBProviderSuite {

  @Test
  public void testRockDBCheckVersionFailed() throws IOException, InterruptedException {
    testCheckVersionFailed(DBBackend.ROCKSDB, "rocksdb");
  }

  @Test
  public void testLevelDBCheckVersionFailed() throws IOException, InterruptedException {
    assumeFalse(SystemUtils.IS_OS_MAC_OSX && SystemUtils.OS_ARCH.equals("aarch64"));
    testCheckVersionFailed(DBBackend.LEVELDB, "leveldb");
  }

  private void testCheckVersionFailed(DBBackend dbBackend, String namePrefix)
      throws IOException, InterruptedException {
    String root = System.getProperty("java.io.tmpdir");
    File dbFile = JavaUtils.createDirectory(root, namePrefix);
    try {
      StoreVersion v1 = new StoreVersion(1, 0);
      ObjectMapper mapper = new ObjectMapper();
      DBProvider.initDB(dbBackend, dbFile, v1, mapper).close();
      StoreVersion v2 = new StoreVersion(2, 0);
      IOException ioe = Assertions.assertThrows(IOException.class, () ->
        DBProvider.initDB(dbBackend, dbFile, v2, mapper));
      Assertions.assertTrue(
        ioe.getMessage().contains("incompatible with current version StoreVersion[2.0]"));
    } finally {
      JavaUtils.deleteRecursively(dbFile);
    }
  }
}
