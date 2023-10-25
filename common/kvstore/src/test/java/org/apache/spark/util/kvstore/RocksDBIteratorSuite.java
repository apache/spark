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
import java.lang.ref.Reference;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksIterator;

import static org.junit.jupiter.api.Assertions.*;

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

  @Test
  public void testResourceCleaner() throws Exception {
    File dbPathForCleanerTest = File.createTempFile(
      "test_db_cleaner.", ".rdb");
    dbPathForCleanerTest.delete();

    RocksDB dbForCleanerTest = new RocksDB(dbPathForCleanerTest);
    for (int i = 0; i < 8192; i++) {
      dbForCleanerTest.write(createCustomType1(i));
    }

    RocksDBIterator<CustomType1> rocksDBIterator = (RocksDBIterator<CustomType1>) dbForCleanerTest.view(CustomType1.class).iterator();
    Optional<Reference<RocksDBIterator<?>>> referenceOpt = dbForCleanerTest.iteratorReference(rocksDBIterator);
    assertTrue(referenceOpt.isPresent());
    RocksIterator it = rocksDBIterator.internalIterator();
    // it has not been closed yet, isOwningHandle should be true.
    assertTrue(it.isOwningHandle());
    // Manually set rocksDBIterator to null, to be GC.
    rocksDBIterator = null;
    Reference<RocksDBIterator<?>> ref = referenceOpt.get();
    // 100 times gc, the rocksDBIterator should be GCed.
    int count = 0;
    while (count < 100 && !ref.refersTo(null)) {
      System.gc();
      count++;
      Thread.sleep(100);
    }
    assertTrue(ref.refersTo(null)); // check rocksDBIterator should be GCed
    // Verify that the Cleaner will be executed after a period of time, and it.isOwningHandle() will become false.
    assertTimeout(java.time.Duration.ofSeconds(5), () -> assertFalse(it.isOwningHandle()));

    dbForCleanerTest.close();
  }
}
