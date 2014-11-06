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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.google.common.io.CharStreams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExternalShuffleBlockManagerSuite {
  static String sortBlock0 = "Hello!";
  static String sortBlock1 = "World!";

  static String hashBlock0 = "Elementary";
  static String hashBlock1 = "Tabular";

  static TestShuffleDataContext dataContext;

  @BeforeClass
  public static void beforeAll() throws IOException {
    dataContext = new TestShuffleDataContext(2, 5);

    dataContext.create();
    // Write some sort and hash data.
    dataContext.insertSortShuffleData(0, 0,
      new byte[][] { sortBlock0.getBytes(), sortBlock1.getBytes() } );
    dataContext.insertHashShuffleData(1, 0,
      new byte[][] { hashBlock0.getBytes(), hashBlock1.getBytes() } );
  }

  @AfterClass
  public static void afterAll() {
    dataContext.cleanup();
  }

  @Test
  public void testBadRequests() {
    ExternalShuffleBlockManager manager = new ExternalShuffleBlockManager();
    // Unregistered executor
    try {
      manager.getBlockData("app0", "exec1", "shuffle_1_1_0");
      fail("Should have failed");
    } catch (RuntimeException e) {
      assertTrue("Bad error message: " + e, e.getMessage().contains("not registered"));
    }

    // Invalid shuffle manager
    manager.registerExecutor("app0", "exec2", dataContext.createExecutorInfo("foobar"));
    try {
      manager.getBlockData("app0", "exec2", "shuffle_1_1_0");
      fail("Should have failed");
    } catch (UnsupportedOperationException e) {
      // pass
    }

    // Nonexistent shuffle block
    manager.registerExecutor("app0", "exec3",
      dataContext.createExecutorInfo("org.apache.spark.shuffle.sort.SortShuffleManager"));
    try {
      manager.getBlockData("app0", "exec3", "shuffle_1_1_0");
      fail("Should have failed");
    } catch (Exception e) {
      // pass
    }
  }

  @Test
  public void testSortShuffleBlocks() throws IOException {
    ExternalShuffleBlockManager manager = new ExternalShuffleBlockManager();
    manager.registerExecutor("app0", "exec0",
      dataContext.createExecutorInfo("org.apache.spark.shuffle.sort.SortShuffleManager"));

    InputStream block0Stream =
      manager.getBlockData("app0", "exec0", "shuffle_0_0_0").createInputStream();
    String block0 = CharStreams.toString(new InputStreamReader(block0Stream));
    block0Stream.close();
    assertEquals(sortBlock0, block0);

    InputStream block1Stream =
      manager.getBlockData("app0", "exec0", "shuffle_0_0_1").createInputStream();
    String block1 = CharStreams.toString(new InputStreamReader(block1Stream));
    block1Stream.close();
    assertEquals(sortBlock1, block1);
  }

  @Test
  public void testHashShuffleBlocks() throws IOException {
    ExternalShuffleBlockManager manager = new ExternalShuffleBlockManager();
    manager.registerExecutor("app0", "exec0",
      dataContext.createExecutorInfo("org.apache.spark.shuffle.hash.HashShuffleManager"));

    InputStream block0Stream =
      manager.getBlockData("app0", "exec0", "shuffle_1_0_0").createInputStream();
    String block0 = CharStreams.toString(new InputStreamReader(block0Stream));
    block0Stream.close();
    assertEquals(hashBlock0, block0);

    InputStream block1Stream =
      manager.getBlockData("app0", "exec0", "shuffle_1_0_1").createInputStream();
    String block1 = CharStreams.toString(new InputStreamReader(block1Stream));
    block1Stream.close();
    assertEquals(hashBlock1, block1);
  }
}
