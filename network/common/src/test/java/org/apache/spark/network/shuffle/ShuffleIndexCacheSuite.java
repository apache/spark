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

package org.apache.spark.network.shuffle;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class ShuffleIndexCacheSuite {
  private static long[] offsets;
  private static File tempDir;
  private static File indexFile;
  private static int partitionsPerMap = 1000;
  private static int mapTasks = 100;
  private static int shuffleId = 0;

  @BeforeClass
  public static void beforeAll() throws IOException {
    offsets = new long[partitionsPerMap + 1];
    long offset = 0;
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = offset;
      offset += i;
    }
    tempDir = Files.createTempDir();
    indexFile = File.createTempFile("shuffle-index-file", "index", tempDir);
    DataOutputStream indexStream = new DataOutputStream(new FileOutputStream(indexFile));
    try {
      for (int i = 0; i < offsets.length; i++) {
        indexStream.writeLong(offsets[i]);
      }
    } finally {
      indexStream.close();
    }
  }

  @Test
  public void testLRUCachePolicy() throws Exception {
    ShuffleIndexCache indexCache1 = new ShuffleIndexCache(1024 * 1024);
    for (int i = 0; i < mapTasks; i ++) {
      for (int j = 0; j < partitionsPerMap; j++) {
        ShuffleIndexRecord indexInfo = indexCache1.getIndexInformation(indexFile, shuffleId, i, j);
        assertEquals(indexInfo.offset, offsets[j]);
        assertEquals(indexInfo.nextOffset, offsets[j + 1]);
      }
    }

    ShuffleIndexCache indexCache2 = new ShuffleIndexCache(0);
    for (int i = 0; i < mapTasks; i ++) {
      for (int j = 0; j < partitionsPerMap; j++) {
        ShuffleIndexRecord indexInfo = indexCache2.getIndexInformation(indexFile, shuffleId, i, j);
        assertEquals(indexInfo.offset, offsets[j]);
        assertEquals(indexInfo.nextOffset, offsets[j + 1]);
      }
    };
  }

  @Test
  public void testMultiThreadsGetIndexInformation() throws Exception {
    final ShuffleIndexCache cache = new ShuffleIndexCache(1024 * 1024);
    // Run multiple instances
    Thread[] getInfoThreads = new Thread[50];
    for (int i = 0; i < 50; i++) {
      getInfoThreads[i] = new Thread() {
        @Override
        public void run() {
          try {
            int mapId = new Random().nextInt(mapTasks);
            int reduceId = new Random().nextInt(partitionsPerMap);
            cache.getIndexInformation(indexFile, shuffleId, mapId, reduceId);
            cache.removeMap(shuffleId, mapId);
          } catch (Exception e) {
          }
        }
      };
    }

    for (int i = 0; i < 50; i++) {
      getInfoThreads[i].start();
    }

    final Thread mainTestThread = Thread.currentThread();

    Thread timeoutThread = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(15000);
          mainTestThread.interrupt();
        } catch (InterruptedException ie) {
        }
      }
    };

    for (int i = 0; i < 50; i++) {
      try {
        getInfoThreads[i].join();
      } catch (InterruptedException ie) {
        // Haven't finished in time. Potential deadlock/race.
        fail("Unexpectedly long delay during concurrent cache entry creations");
      }
    }

    timeoutThread.interrupt();
  }

  @AfterClass
  public static void afterAll() {
    if (tempDir != null) {
      for (File f : tempDir.listFiles()) {
        f.delete();
      }
      tempDir.delete();
    }
  }
}
