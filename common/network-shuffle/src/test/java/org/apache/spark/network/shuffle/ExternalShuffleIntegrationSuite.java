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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class ExternalShuffleIntegrationSuite {

  private static final String APP_ID = "app-id";
  private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";

  // Executor 0 is sort-based
  static TestShuffleDataContext dataContext0;

  static ExternalShuffleBlockHandler handler;
  static TransportServer server;
  static TransportConf conf;

  static byte[][] exec0Blocks = new byte[][] {
    new byte[123],
    new byte[12345],
    new byte[1234567],
  };

  static byte[][] exec1Blocks = new byte[][] {
    new byte[321],
    new byte[54321],
  };

  @BeforeClass
  public static void beforeAll() throws IOException {
    Random rand = new Random();

    for (byte[] block : exec0Blocks) {
      rand.nextBytes(block);
    }
    for (byte[] block: exec1Blocks) {
      rand.nextBytes(block);
    }

    dataContext0 = new TestShuffleDataContext(2, 5);
    dataContext0.create();
    dataContext0.insertSortShuffleData(0, 0, exec0Blocks);

    conf = new TransportConf("shuffle", new SystemPropertyConfigProvider());
    handler = new ExternalShuffleBlockHandler(conf, null);
    TransportContext transportContext = new TransportContext(conf, handler);
    server = transportContext.createServer();
  }

  @AfterClass
  public static void afterAll() {
    dataContext0.cleanup();
    server.close();
  }

  @After
  public void afterEach() {
    handler.applicationRemoved(APP_ID, false /* cleanupLocalDirs */);
  }

  static class FetchResult {
    public Set<String> successBlocks;
    public Set<String> failedBlocks;
    public List<ManagedBuffer> buffers;

    public void releaseBuffers() {
      for (ManagedBuffer buffer : buffers) {
        buffer.release();
      }
    }
  }

  // Fetch a set of blocks from a pre-registered executor.
  private FetchResult fetchBlocks(String execId, String[] blockIds) throws Exception {
    return fetchBlocks(execId, blockIds, server.getPort());
  }

  // Fetch a set of blocks from a pre-registered executor. Connects to the server on the given port,
  // to allow connecting to invalid servers.
  private FetchResult fetchBlocks(String execId, String[] blockIds, int port) throws Exception {
    final FetchResult res = new FetchResult();
    res.successBlocks = Collections.synchronizedSet(new HashSet<String>());
    res.failedBlocks = Collections.synchronizedSet(new HashSet<String>());
    res.buffers = Collections.synchronizedList(new LinkedList<ManagedBuffer>());

    final Semaphore requestsRemaining = new Semaphore(0);

    ExternalShuffleClient client = new ExternalShuffleClient(conf, null, false, false);
    client.init(APP_ID);
    client.fetchBlocks(TestUtils.getLocalHost(), port, execId, blockIds,
      new BlockFetchingListener() {
        @Override
        public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
          synchronized (this) {
            if (!res.successBlocks.contains(blockId) && !res.failedBlocks.contains(blockId)) {
              data.retain();
              res.successBlocks.add(blockId);
              res.buffers.add(data);
              requestsRemaining.release();
            }
          }
        }

        @Override
        public void onBlockFetchFailure(String blockId, Throwable exception) {
          synchronized (this) {
            if (!res.successBlocks.contains(blockId) && !res.failedBlocks.contains(blockId)) {
              res.failedBlocks.add(blockId);
              requestsRemaining.release();
            }
          }
        }
      });

    if (!requestsRemaining.tryAcquire(blockIds.length, 5, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server");
    }
    client.close();
    return res;
  }

  @Test
  public void testFetchOneSort() throws Exception {
    registerExecutor("exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
    FetchResult exec0Fetch = fetchBlocks("exec-0", new String[] { "shuffle_0_0_0" });
    assertEquals(Sets.newHashSet("shuffle_0_0_0"), exec0Fetch.successBlocks);
    assertTrue(exec0Fetch.failedBlocks.isEmpty());
    assertBufferListsEqual(exec0Fetch.buffers, Lists.newArrayList(exec0Blocks[0]));
    exec0Fetch.releaseBuffers();
  }

  @Test
  public void testFetchThreeSort() throws Exception {
    registerExecutor("exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
    FetchResult exec0Fetch = fetchBlocks("exec-0",
      new String[] { "shuffle_0_0_0", "shuffle_0_0_1", "shuffle_0_0_2" });
    assertEquals(Sets.newHashSet("shuffle_0_0_0", "shuffle_0_0_1", "shuffle_0_0_2"),
      exec0Fetch.successBlocks);
    assertTrue(exec0Fetch.failedBlocks.isEmpty());
    assertBufferListsEqual(exec0Fetch.buffers, Lists.newArrayList(exec0Blocks));
    exec0Fetch.releaseBuffers();
  }

  @Test (expected = RuntimeException.class)
  public void testRegisterInvalidExecutor() throws Exception {
    registerExecutor("exec-1", dataContext0.createExecutorInfo("unknown sort manager"));
  }

  @Test
  public void testFetchWrongBlockId() throws Exception {
    registerExecutor("exec-1", dataContext0.createExecutorInfo(SORT_MANAGER));
    FetchResult execFetch = fetchBlocks("exec-1", new String[] { "rdd_1_0_0" });
    assertTrue(execFetch.successBlocks.isEmpty());
    assertEquals(Sets.newHashSet("rdd_1_0_0"), execFetch.failedBlocks);
  }

  @Test
  public void testFetchNonexistent() throws Exception {
    registerExecutor("exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
    FetchResult execFetch = fetchBlocks("exec-0",
      new String[] { "shuffle_2_0_0" });
    assertTrue(execFetch.successBlocks.isEmpty());
    assertEquals(Sets.newHashSet("shuffle_2_0_0"), execFetch.failedBlocks);
  }

  @Test
  public void testFetchWrongExecutor() throws Exception {
    registerExecutor("exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
    FetchResult execFetch = fetchBlocks("exec-0",
      new String[] { "shuffle_0_0_0" /* right */, "shuffle_1_0_0" /* wrong */ });
    // Both still fail, as we start by checking for all block.
    assertTrue(execFetch.successBlocks.isEmpty());
    assertEquals(Sets.newHashSet("shuffle_0_0_0", "shuffle_1_0_0"), execFetch.failedBlocks);
  }

  @Test
  public void testFetchUnregisteredExecutor() throws Exception {
    registerExecutor("exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
    FetchResult execFetch = fetchBlocks("exec-2",
      new String[] { "shuffle_0_0_0", "shuffle_1_0_0" });
    assertTrue(execFetch.successBlocks.isEmpty());
    assertEquals(Sets.newHashSet("shuffle_0_0_0", "shuffle_1_0_0"), execFetch.failedBlocks);
  }

  @Test
  public void testFetchNoServer() throws Exception {
    System.setProperty("spark.shuffle.io.maxRetries", "0");
    try {
      registerExecutor("exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
      FetchResult execFetch = fetchBlocks("exec-0",
        new String[]{"shuffle_1_0_0", "shuffle_1_0_1"}, 1 /* port */);
      assertTrue(execFetch.successBlocks.isEmpty());
      assertEquals(Sets.newHashSet("shuffle_1_0_0", "shuffle_1_0_1"), execFetch.failedBlocks);
    } finally {
      System.clearProperty("spark.shuffle.io.maxRetries");
    }
  }

  private void registerExecutor(String executorId, ExecutorShuffleInfo executorInfo)
      throws IOException {
    ExternalShuffleClient client = new ExternalShuffleClient(conf, null, false, false);
    client.init(APP_ID);
    client.registerWithShuffleServer(TestUtils.getLocalHost(), server.getPort(),
      executorId, executorInfo);
  }

  private void assertBufferListsEqual(List<ManagedBuffer> list0, List<byte[]> list1)
    throws Exception {
    assertEquals(list0.size(), list1.size());
    for (int i = 0; i < list0.size(); i ++) {
      assertBuffersEqual(list0.get(i), new NioManagedBuffer(ByteBuffer.wrap(list1.get(i))));
    }
  }

  private void assertBuffersEqual(ManagedBuffer buffer0, ManagedBuffer buffer1) throws Exception {
    ByteBuffer nio0 = buffer0.nioByteBuffer().toByteBuffer();
    ByteBuffer nio1 = buffer1.nioByteBuffer().toByteBuffer();

    int len = nio0.remaining();
    assertEquals(nio0.remaining(), nio1.remaining());
    for (int i = 0; i < len; i ++) {
      assertEquals(nio0.get(), nio1.get());
    }
  }
}
