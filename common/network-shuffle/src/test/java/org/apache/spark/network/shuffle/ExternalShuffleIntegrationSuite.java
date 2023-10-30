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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class ExternalShuffleIntegrationSuite {

  private static final String APP_ID = "app-id";
  private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";

  protected static final int RDD_ID = 1;
  protected static final int SPLIT_INDEX_VALID_BLOCK = 0;
  private static final int SPLIT_INDEX_MISSING_FILE = 1;
  protected static final int SPLIT_INDEX_CORRUPT_LENGTH = 2;
  protected static final int SPLIT_INDEX_VALID_BLOCK_TO_RM = 3;
  private static final int SPLIT_INDEX_MISSING_BLOCK_TO_RM = 4;

  // Executor 0 is sort-based
  static TestShuffleDataContext dataContext0;

  static ExternalBlockHandler handler;
  static TransportServer server;
  static TransportConf conf;
  static TransportContext transportContext;

  static byte[] exec0RddBlockValid = new byte[123];
  static byte[] exec0RddBlockToRemove = new byte[124];

  static byte[][] exec0Blocks = new byte[][] {
    new byte[123],
    new byte[12345],
    new byte[1234567],
  };

  static byte[][] exec1Blocks = new byte[][] {
    new byte[321],
    new byte[54321],
  };

  private static TransportConf createTransportConf(int maxRetries, boolean rddEnabled) {
    HashMap<String, String> config = new HashMap<>();
    config.put("spark.shuffle.io.maxRetries", String.valueOf(maxRetries));
    config.put(Constants.SHUFFLE_SERVICE_FETCH_RDD_ENABLED, String.valueOf(rddEnabled));
    return new TransportConf("shuffle", new MapConfigProvider(config));
  }

  // This is split out so it can be invoked in a subclass with a different config
  @BeforeAll
  public static void beforeAll() throws IOException {
    doBeforeAllWithConfig(createTransportConf(0, true));
  }

  public static void doBeforeAllWithConfig(TransportConf transportConf) throws IOException {
    Random rand = new Random();

    for (byte[] block : exec0Blocks) {
      rand.nextBytes(block);
    }
    for (byte[] block: exec1Blocks) {
      rand.nextBytes(block);
    }
    rand.nextBytes(exec0RddBlockValid);
    rand.nextBytes(exec0RddBlockToRemove);

    dataContext0 = new TestShuffleDataContext(2, 5);
    dataContext0.create();
    dataContext0.insertSortShuffleData(0, 0, exec0Blocks);
    dataContext0.insertCachedRddData(RDD_ID, SPLIT_INDEX_VALID_BLOCK, exec0RddBlockValid);
    dataContext0.insertCachedRddData(RDD_ID, SPLIT_INDEX_VALID_BLOCK_TO_RM, exec0RddBlockToRemove);

    conf = transportConf;
    handler = new ExternalBlockHandler(
      new OneForOneStreamManager(),
      new ExternalShuffleBlockResolver(conf, null) {
        @Override
        public ManagedBuffer getRddBlockData(String appId, String execId, int rddId, int splitIdx) {
          ManagedBuffer res;
          if (rddId == RDD_ID) {
            if (splitIdx == SPLIT_INDEX_CORRUPT_LENGTH) {
              res = new FileSegmentManagedBuffer(conf, new File("missing.file"), 0, 12);
            } else {
              res = super.getRddBlockData(appId, execId, rddId, splitIdx);
            }
          } else {
            res = super.getRddBlockData(appId, execId, rddId, splitIdx);
          }
          return res;
        }
      });
    transportContext = new TransportContext(conf, handler);
    server = transportContext.createServer();
  }

  @AfterAll
  public static void afterAll() {
    dataContext0.cleanup();
    server.close();
    transportContext.close();
  }

  @AfterEach
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
    return fetchBlocks(execId, blockIds, conf, server.getPort());
  }

  // Fetch a set of blocks from a pre-registered executor. Connects to the server on the given port,
  // to allow connecting to invalid servers.
  private FetchResult fetchBlocks(
      String execId,
      String[] blockIds,
      TransportConf clientConf,
      int port) throws Exception {
    final FetchResult res = new FetchResult();
    res.successBlocks = Collections.synchronizedSet(new HashSet<>());
    res.failedBlocks = Collections.synchronizedSet(new HashSet<>());
    res.buffers = Collections.synchronizedList(new LinkedList<>());

    final Semaphore requestsRemaining = new Semaphore(0);

    try (ExternalBlockStoreClient client = new ExternalBlockStoreClient(
        clientConf, null, false, 5000)) {
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
        }, null);

      if (!requestsRemaining.tryAcquire(blockIds.length, 5, TimeUnit.SECONDS)) {
        fail("Timeout getting response from the server");
      }
    }
    return res;
  }

  @Test
  public void testFetchOneSort() throws Exception {
    registerExecutor("exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
    FetchResult exec0Fetch = fetchBlocks("exec-0", new String[] { "shuffle_0_0_0" });
    assertEquals(Sets.newHashSet("shuffle_0_0_0"), exec0Fetch.successBlocks);
    assertTrue(exec0Fetch.failedBlocks.isEmpty());
    assertBufferListsEqual(exec0Fetch.buffers, Arrays.asList(exec0Blocks[0]));
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
    assertBufferListsEqual(exec0Fetch.buffers, Arrays.asList(exec0Blocks));
    exec0Fetch.releaseBuffers();
  }

  @Test
  public void testRegisterWithCustomShuffleManager() throws Exception {
    registerExecutor("exec-1", dataContext0.createExecutorInfo("custom shuffle manager"));
  }

  @Test
  public void testFetchWrongBlockId() throws Exception {
    registerExecutor("exec-1", dataContext0.createExecutorInfo(SORT_MANAGER));
    FetchResult execFetch = fetchBlocks("exec-1", new String[] { "broadcast_1" });
    assertTrue(execFetch.successBlocks.isEmpty());
    assertEquals(Sets.newHashSet("broadcast_1"), execFetch.failedBlocks);
  }

  @Test
  public void testFetchValidRddBlock() throws Exception {
    registerExecutor("exec-1", dataContext0.createExecutorInfo(SORT_MANAGER));
    String validBlockId = "rdd_" + RDD_ID +"_" + SPLIT_INDEX_VALID_BLOCK;
    FetchResult execFetch = fetchBlocks("exec-1", new String[] { validBlockId });
    assertTrue(execFetch.failedBlocks.isEmpty());
    assertEquals(Sets.newHashSet(validBlockId), execFetch.successBlocks);
    assertBuffersEqual(new NioManagedBuffer(ByteBuffer.wrap(exec0RddBlockValid)),
      execFetch.buffers.get(0));
  }

  @Test
  public void testFetchDeletedRddBlock() throws Exception {
    registerExecutor("exec-1", dataContext0.createExecutorInfo(SORT_MANAGER));
    String missingBlockId = "rdd_" + RDD_ID +"_" + SPLIT_INDEX_MISSING_FILE;
    FetchResult execFetch = fetchBlocks("exec-1", new String[] { missingBlockId });
    assertTrue(execFetch.successBlocks.isEmpty());
    assertEquals(Sets.newHashSet(missingBlockId), execFetch.failedBlocks);
  }

  @Test
  public void testRemoveRddBlocks() throws Exception {
    registerExecutor("exec-1", dataContext0.createExecutorInfo(SORT_MANAGER));
    String validBlockIdToRemove = "rdd_" + RDD_ID +"_" + SPLIT_INDEX_VALID_BLOCK_TO_RM;
    String missingBlockIdToRemove = "rdd_" + RDD_ID +"_" + SPLIT_INDEX_MISSING_BLOCK_TO_RM;

    try (ExternalBlockStoreClient client = new ExternalBlockStoreClient(conf, null, false, 5000)) {
      client.init(APP_ID);
      Future<Integer> numRemovedBlocks = client.removeBlocks(
        TestUtils.getLocalHost(),
        server.getPort(),
        "exec-1",
          new String[] { validBlockIdToRemove, missingBlockIdToRemove });
      assertEquals(1, numRemovedBlocks.get().intValue());
    }
  }

  @Test
  public void testFetchCorruptRddBlock() throws Exception {
    registerExecutor("exec-1", dataContext0.createExecutorInfo(SORT_MANAGER));
    String corruptBlockId = "rdd_" + RDD_ID +"_" + SPLIT_INDEX_CORRUPT_LENGTH;
    FetchResult execFetch = fetchBlocks("exec-1", new String[] { corruptBlockId });
    assertTrue(execFetch.successBlocks.isEmpty());
    assertEquals(Sets.newHashSet(corruptBlockId), execFetch.failedBlocks);
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
    FetchResult execFetch0 = fetchBlocks("exec-0", new String[] { "shuffle_0_0_0" /* right */});
    FetchResult execFetch1 = fetchBlocks("exec-0", new String[] { "shuffle_1_0_0" /* wrong */ });
    assertEquals(Sets.newHashSet("shuffle_0_0_0"), execFetch0.successBlocks);
    assertEquals(Sets.newHashSet("shuffle_1_0_0"), execFetch1.failedBlocks);
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
    TransportConf clientConf = createTransportConf(0, false);
    registerExecutor("exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
    FetchResult execFetch = fetchBlocks("exec-0",
      new String[]{"shuffle_1_0_0", "shuffle_1_0_1"}, clientConf, 1 /* port */);
    assertTrue(execFetch.successBlocks.isEmpty());
    assertEquals(Sets.newHashSet("shuffle_1_0_0", "shuffle_1_0_1"), execFetch.failedBlocks);
  }

  private static void registerExecutor(String executorId, ExecutorShuffleInfo executorInfo)
      throws IOException, InterruptedException {
    ExternalBlockStoreClient client = new ExternalBlockStoreClient(conf, null, false, 5000);
    client.init(APP_ID);
    client.registerWithShuffleServer(TestUtils.getLocalHost(), server.getPort(),
      executorId, executorInfo);
  }

  private static void assertBufferListsEqual(List<ManagedBuffer> list0, List<byte[]> list1)
    throws Exception {
    assertEquals(list0.size(), list1.size());
    for (int i = 0; i < list0.size(); i ++) {
      assertBuffersEqual(list0.get(i), new NioManagedBuffer(ByteBuffer.wrap(list1.get(i))));
    }
  }

  private static void assertBuffersEqual(ManagedBuffer buffer0, ManagedBuffer buffer1)
      throws Exception {
    ByteBuffer nio0 = buffer0.nioByteBuffer();
    ByteBuffer nio1 = buffer1.nioByteBuffer();

    int len = nio0.remaining();
    assertEquals(nio0.remaining(), nio1.remaining());
    for (int i = 0; i < len; i ++) {
      assertEquals(nio0.get(), nio1.get());
    }
  }
}
