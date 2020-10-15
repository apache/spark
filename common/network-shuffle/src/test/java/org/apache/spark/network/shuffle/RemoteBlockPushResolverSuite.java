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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.shuffle.protocol.FinalizeShuffleMerge;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

/**
 * Tests for {@link RemoteBlockPushResolver}.
 */
public class RemoteBlockPushResolverSuite {

  private static final Logger log = LoggerFactory.getLogger(RemoteBlockPushResolverSuite.class);
  private final String MERGE_DIR_RELATIVE_PATH = "usercache/%s/appcache/%s/";
  private final String TEST_USER = "testUser";
  private final String TEST_APP = "testApp";
  private final String BLOCK_MANAGER_DIR = "blockmgr-193d8401";

  private TransportConf conf;
  private RemoteBlockPushResolver pushResolver;
  private String[] localDirs;

  @Before
  public void before() throws IOException {
    localDirs = new String[]{Paths.get("target/l1").toAbsolutePath().toString(),
        Paths.get("target/l2").toAbsolutePath().toString()};
    cleanupLocalDirs();
    MapConfigProvider provider = new MapConfigProvider(
        ImmutableMap.of("spark.shuffle.server.minChunkSizeInMergedShuffleFile", "4"));
    conf = new TransportConf("shuffle", provider);
    pushResolver = new RemoteBlockPushResolver(conf, MERGE_DIR_RELATIVE_PATH);
  }

  @After
  public void after() {
    try {
      cleanupLocalDirs();
    } catch (IOException e) {
      // don't fail if clean up doesn't succeed.
      log.warn("Error deleting test local dirs", e);
    }
  }

  private void cleanupLocalDirs() throws IOException {
    for (String local : localDirs) {
      FileUtils.deleteDirectory(new File(local));
    }
  }

  @Test(expected = RuntimeException.class)
  public void testNoIndexFile() {
    try {
      registerApplication(TEST_APP, TEST_USER);
      registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));
      pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
      removeApplication(TEST_APP);
    } catch (Throwable t) {
      assertTrue(t.getMessage().startsWith("Application merged shuffle index file is not found"));
      Throwables.propagate(t);
    }
  }

  @Test
  public void testBasicBlockMerge() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));
    PushBlockStream[] pushBlocks = new PushBlockStream[] {
        new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0),
        new PushBlockStream(TEST_APP, "shuffle_0_1_0", 0),
    };
    ByteBuffer[] blocks = new ByteBuffer[]{
        ByteBuffer.wrap(new byte[4]),
        ByteBuffer.wrap(new byte[5])
    };
    pushBlockHelper(TEST_APP, pushBlocks, blocks);
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{4, 5}, new int[][]{{0}, {1}});
    removeApplication(TEST_APP);
  }

  @Test
  public void testDividingMergedBlocksIntoChunks() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));
    PushBlockStream[] pushBlocks = new PushBlockStream[] {
        new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0),
        new PushBlockStream(TEST_APP, "shuffle_0_1_0", 0),
        new PushBlockStream(TEST_APP, "shuffle_0_2_0", 0),
        new PushBlockStream(TEST_APP, "shuffle_0_3_0", 0),
    };
    ByteBuffer[] buffers = new ByteBuffer[]{
        ByteBuffer.wrap(new byte[2]),
        ByteBuffer.wrap(new byte[3]),
        ByteBuffer.wrap(new byte[5]),
        ByteBuffer.wrap(new byte[3])
    };
    pushBlockHelper(TEST_APP, pushBlocks, buffers);
    MergedBlockMeta meta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, meta, new int[]{5, 5, 3}, new int[][]{{0, 1}, {2}, {3}});
    removeApplication(TEST_APP);
  }

  @Test
  public void testDeferredBufsAreWrittenDuringOnData() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));

    PushBlockStream pbStream1 = new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0);
    StreamCallbackWithID stream1 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream1.blockId, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));

    PushBlockStream pbStream2 = new PushBlockStream(TEST_APP, "shuffle_0_1_0", 0);
    StreamCallbackWithID stream2 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream2.blockId, 0));
    // This should be deferred
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[3]));

    // stream 1 now completes
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    stream1.onComplete(stream1.getID());

    // stream 2 has more data and then completes
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[3]));
    stream2.onComplete(stream2.getID());

    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{4, 6}, new int[][]{{0}, {1}});
    removeApplication(TEST_APP);
  }

  @Test
  public void testDeferredBufsAreWrittenDuringOnComplete() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));

    PushBlockStream pbStream1 = new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0);
    StreamCallbackWithID stream1 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream1.blockId, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));

    PushBlockStream pbStream2 = new PushBlockStream(TEST_APP, "shuffle_0_1_0", 0);
    StreamCallbackWithID stream2 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream2.blockId, 0));
    // This should be deferred
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[3]));
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[3]));

    // stream 1 now completes
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    stream1.onComplete(stream1.getID());

    // stream 2 now completes completes
    stream2.onComplete(stream2.getID());

    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{4, 6}, new int[][]{{0}, {1}});
    removeApplication(TEST_APP);
  }

  @Test
  public void testDuplicateBlocksAreIgnoredWhenPrevStreamHasCompleted() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));

    PushBlockStream pbStream1 = new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0);
    StreamCallbackWithID stream1 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream1.blockId, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    stream1.onComplete(stream1.getID());

    PushBlockStream pbStream2 = new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0);
    StreamCallbackWithID stream2 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream2.blockId, 0));
    // This should be ignored
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[2]));
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[2]));
    stream2.onComplete(stream2.getID());

    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{4}, new int[][]{{0}});
    removeApplication(TEST_APP);
  }

  @Test
  public void testDuplicateBlocksAreIgnoredWhenPrevStreamIsInProgress() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));

    PushBlockStream pbStream1 = new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0);
    StreamCallbackWithID stream1 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream1.blockId, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));

    PushBlockStream pbStream2 = new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0);
    StreamCallbackWithID stream2 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream2.blockId, 0));
    // This should be ingored
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[2]));
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[2]));

    // stream 1 now completes
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    stream1.onComplete(stream1.getID());

    // stream 2 now completes completes
    stream2.onComplete(stream2.getID());

    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{4}, new int[][]{{0}});
    removeApplication(TEST_APP);
  }

  @Test
  public void testFailureAfterData() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));
    PushBlockStream pushBlock = new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0);

    StreamCallbackWithID stream =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pushBlock.blockId, 0));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[4]));
    stream.onFailure(stream.getID(), new RuntimeException("Forced Failure"));

    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    assertEquals("num-chunks", 0, blockMeta.getNumChunks());
    removeApplication(TEST_APP);
  }

  @Test
  public void testFailureAfterMultipleDataBlocks() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));
    PushBlockStream pushBlock = new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0);

    StreamCallbackWithID stream =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pushBlock.blockId, 0));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[2]));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[3]));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[4]));
    stream.onFailure(stream.getID(), new RuntimeException("Forced Failure"));

    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    assertEquals("num-chunks", 0, blockMeta.getNumChunks());
    removeApplication(TEST_APP);
  }

  @Test
  public void testFailureAfterComplete() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));
    PushBlockStream pushBlock = new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0);

    StreamCallbackWithID stream =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pushBlock.blockId, 0));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[2]));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[3]));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[4]));
    stream.onComplete(stream.getID());
    stream.onFailure(stream.getID(), new RuntimeException("Forced Failure"));

    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{9}, new int[][]{{0}});
    removeApplication(TEST_APP);
  }

  @Test (expected = RuntimeException.class)
  public void testTooLateArrival() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));
    PushBlockStream[] pushBlocks = new PushBlockStream[] {
        new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0),
        new PushBlockStream(TEST_APP, "shuffle_0_1_0", 0)};
    ByteBuffer[] blocks = new ByteBuffer[]{
        ByteBuffer.wrap(new byte[4]),
        ByteBuffer.wrap(new byte[5])
    };
    StreamCallbackWithID stream = pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, pushBlocks[0].blockId, 0));
    for (ByteBuffer block : blocks) {
      stream.onData(stream.getID(), block);
    }
    stream.onComplete(stream.getID());
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    StreamCallbackWithID stream1 = pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, pushBlocks[1].blockId, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[4]));
    try {
      stream1.onComplete(stream1.getID());
    } catch (RuntimeException re) {
      assertEquals(
          "Block shuffle_0_1_0 received after merged shuffle is finalized", re.getMessage());
      MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
      validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{9}, new int[][]{{0}});
      removeApplication(TEST_APP);
      throw re;
    }
  }

  @Test
  public void testIncompleteStreamsAreOverwritten() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));
    PushBlockStream pbStream1 = new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0);
    StreamCallbackWithID stream1 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream1.blockId, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[4]));
    // There is a failure
    stream1.onFailure(stream1.getID(), new RuntimeException("forced error"));

    PushBlockStream pbStream2 = new PushBlockStream(TEST_APP, "shuffle_0_1_0", 0);
    StreamCallbackWithID stream2 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream2.blockId, 0));
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[5]));
    stream2.onComplete(stream2.getID());
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{5}, new int[][]{{1}});
    removeApplication(TEST_APP);
  }

  @Test (expected = RuntimeException.class)
  public void testFailureWith3Streams() throws IOException {
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, localDirs));

    PushBlockStream pbStream1 = new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0);
    StreamCallbackWithID stream1 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream1.blockId, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));

    PushBlockStream pbStream2 = new PushBlockStream(TEST_APP, "shuffle_0_1_0", 0);
    StreamCallbackWithID stream2 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream2.blockId, 0));
    // There is a failure
    stream2.onFailure(stream2.getID(), new RuntimeException("forced error"));

    PushBlockStream pbStream3 = new PushBlockStream(TEST_APP, "shuffle_0_2_0", 0);
    StreamCallbackWithID stream3 =
        pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, pbStream3.blockId, 0));
    // This should be deferred
    stream3.onData(stream3.getID(), ByteBuffer.wrap(new byte[5]));
    // Since this stream didn't get any opportunity it will throw couldn't find opportunity error
    RuntimeException failedEx = null;
    try {
      stream3.onComplete(stream2.getID());
    } catch (RuntimeException re) {
      assertEquals(
          "Couldn't find an opportunity to write block shuffle_0_2_0 to merged shuffle",
          re.getMessage());
      failedEx = re;
    }
    // stream 1 now completes
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    stream1.onComplete(stream1.getID());

    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[] {4}, new int[][] {{0}});
    removeApplication(TEST_APP);
    if (failedEx != null) {
      throw failedEx;
    }
  }

  @Test
  public void testAppUsingFewerLocalDirs() throws IOException {
    String[] activeLocalDirs = Arrays.stream(localDirs).skip(1).toArray(String[]::new);
    registerApplication(TEST_APP, TEST_USER);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, activeLocalDirs));
    PushBlockStream[] pushBlocks = new PushBlockStream[] {
        new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0),
        new PushBlockStream(TEST_APP, "shuffle_0_1_0", 0),
        new PushBlockStream(TEST_APP, "shuffle_0_2_0", 0),
        new PushBlockStream(TEST_APP, "shuffle_0_3_0", 0),
    };
    ByteBuffer[] buffers = new ByteBuffer[]{
        ByteBuffer.wrap(new byte[2]),
        ByteBuffer.wrap(new byte[3]),
        ByteBuffer.wrap(new byte[5]),
        ByteBuffer.wrap(new byte[3])
    };
    pushBlockHelper(TEST_APP, pushBlocks, buffers);
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{5, 5, 3}, new int[][]{{0, 1}, {2}, {3}});
    removeApplication(TEST_APP);
  }

  @Test(expected = NullPointerException.class)
  public void testUpdateLocalDirsOnlyOnce() throws IOException {
    // First app init and executor register will store the active local dirs list
    registerApplication(TEST_APP, TEST_USER);
    String[] activeLocalDirs = Arrays.stream(localDirs).skip(1).toArray(String[]::new);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, activeLocalDirs));
    assertEquals(pushResolver.getMergedBlockDirs(TEST_APP).length, 1);
    assertTrue(pushResolver.getMergedBlockDirs(TEST_APP)[0].contains(
        "l2/usercache/" + TEST_USER + "/appcache/" + TEST_APP + "/merge_manager"));
    // Any later app init or executor register from the same application
    // won't change the active local dirs list
    registerApplication(TEST_APP, TEST_USER);
    assertEquals(pushResolver.getMergedBlockDirs(TEST_APP).length, 1);
    assertTrue(pushResolver.getMergedBlockDirs(TEST_APP)[0].contains(
        "l2/usercache/" + TEST_USER + "/appcache/" + TEST_APP + "/merge_manager"));
    activeLocalDirs = Arrays.stream(localDirs).toArray(String[]::new);
    registerExecutor(TEST_APP, prepareBlockManagerLocalDirs(TEST_APP, TEST_USER, activeLocalDirs));
    assertEquals(pushResolver.getMergedBlockDirs(TEST_APP).length, 1);
    assertTrue(pushResolver.getMergedBlockDirs(TEST_APP)[0].contains(
        "l2/usercache/" + TEST_USER + "/appcache/" + TEST_APP + "/merge_manager"));
    removeApplication(TEST_APP);
    try {
      pushResolver.getMergedBlockDirs(TEST_APP);
    } catch (Throwable e) {
      assertTrue(e.getMessage()
          .startsWith("application " + TEST_APP + " is not registered or NM was restarted."));
      Throwables.propagate(e);
    }
  }

  /**
   * Registers the app with RemoteBlockPushResolver.
   */
  private void registerApplication(String appId, String user) throws IOException {
    pushResolver.registerApplication(appId, user);
  }

  private void registerExecutor(String TEST_APP, String[] localDirs) throws IOException {
    pushResolver.registerExecutor(TEST_APP, localDirs);
    for (String localDir : pushResolver.getMergedBlockDirs(TEST_APP)) {
      Files.createDirectories(Paths.get(localDir));
    }
  }

  private String[] prepareBlockManagerLocalDirs(String TEST_APP, String user, String[] localDirs){
    return Arrays.stream(localDirs)
        .map(localDir ->
            localDir + "/" +
                String.format(MERGE_DIR_RELATIVE_PATH + BLOCK_MANAGER_DIR, user, TEST_APP))
        .toArray(String[]::new);
  }

  private void removeApplication(String TEST_APP) {
    // PushResolver cleans up the local dirs in a different thread which can conflict with the test
    // data of other tests, since they are using the same Application Id.
    pushResolver.applicationRemoved(TEST_APP,  false);
  }

  private void validateChunks(
      String TEST_APP, int shuffleId, int reduceId, MergedBlockMeta meta,
      int[] expectedSizes, int[][] expectedMapsPerChunk) throws IOException {
    assertEquals("num chunks", expectedSizes.length, meta.getNumChunks());
    RoaringBitmap[] bitmaps = meta.readChunkBitmaps();
    assertEquals("num of bitmaps", meta.getNumChunks(), bitmaps.length);
    for (int i = 0; i < meta.getNumChunks(); i++) {
      RoaringBitmap chunkBitmap = bitmaps[i];
      Arrays.stream(expectedMapsPerChunk[i]).forEach(x -> assertTrue(chunkBitmap.contains(x)));
    }
    for (int i = 0; i < meta.getNumChunks(); i++) {
      FileSegmentManagedBuffer mb =
          (FileSegmentManagedBuffer) pushResolver.getMergedBlockData(TEST_APP, shuffleId, reduceId,
              i);
      assertEquals(expectedSizes[i], mb.getLength());
    }
  }

  private void pushBlockHelper(String TEST_APP, PushBlockStream[] pushBlocks, ByteBuffer[] blocks)
      throws IOException {
    Preconditions.checkArgument(pushBlocks.length == blocks.length);
    for (int i = 0; i < pushBlocks.length; i++) {
      StreamCallbackWithID stream = pushResolver.receiveBlockDataAsStream(
          new PushBlockStream(TEST_APP, pushBlocks[i].blockId, 0));
      stream.onData(stream.getID(), blocks[i]);
      stream.onComplete(stream.getID());
    }
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
  }
}
