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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.shuffle.RemoteBlockPushResolver.MergeShuffleFile;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.FinalizeShuffleMerge;
import org.apache.spark.network.shuffle.protocol.MergeStatuses;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

/**
 * Tests for {@link RemoteBlockPushResolver}.
 */
public class RemoteBlockPushResolverSuite {

  private static final Logger log = LoggerFactory.getLogger(RemoteBlockPushResolverSuite.class);
  private final String TEST_APP = "testApp";
  private final String BLOCK_MANAGER_DIR = "blockmgr-193d8401";

  private TransportConf conf;
  private RemoteBlockPushResolver pushResolver;
  private Path[] localDirs;

  @Before
  public void before() throws IOException {
    localDirs = createLocalDirs(2);
    MapConfigProvider provider = new MapConfigProvider(
      ImmutableMap.of("spark.shuffle.server.minChunkSizeInMergedShuffleFile", "4"));
    conf = new TransportConf("shuffle", provider);
    pushResolver = new RemoteBlockPushResolver(conf);
    registerExecutor(TEST_APP, prepareLocalDirs(localDirs));
  }

  @After
  public void after() {
    try {
      for (Path local : localDirs) {
        FileUtils.deleteDirectory(local.toFile());
      }
      removeApplication(TEST_APP);
    } catch (Exception e) {
      // don't fail if clean up doesn't succeed.
      log.debug("Error while tearing down", e);
    }
  }

  @Test(expected = RuntimeException.class)
  public void testNoIndexFile() {
    try {
      pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    } catch (Throwable t) {
      assertTrue(t.getMessage().startsWith("Merged shuffle index file"));
      Throwables.propagate(t);
    }
  }

  @Test
  public void testBasicBlockMerge() throws IOException {
    PushBlock[] pushBlocks = new PushBlock[] {
      new PushBlock(0, 0, 0, ByteBuffer.wrap(new byte[4])),
      new PushBlock(0, 1, 0, ByteBuffer.wrap(new byte[5]))
    };
    pushBlockHelper(TEST_APP, pushBlocks);
    MergeStatuses statuses = pushResolver.finalizeShuffleMerge(
      new FinalizeShuffleMerge(TEST_APP, 0));
    validateMergeStatuses(statuses, new int[] {0}, new long[] {9});
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{4, 5}, new int[][]{{0}, {1}});
  }

  @Test
  public void testDividingMergedBlocksIntoChunks() throws IOException {
    PushBlock[] pushBlocks = new PushBlock[] {
      new PushBlock(0, 0, 0, ByteBuffer.wrap(new byte[2])),
      new PushBlock(0, 1, 0, ByteBuffer.wrap(new byte[3])),
      new PushBlock(0, 2, 0, ByteBuffer.wrap(new byte[5])),
      new PushBlock(0, 3, 0, ByteBuffer.wrap(new byte[3]))
    };
    pushBlockHelper(TEST_APP, pushBlocks);
    MergeStatuses statuses = pushResolver.finalizeShuffleMerge(
      new FinalizeShuffleMerge(TEST_APP, 0));
    validateMergeStatuses(statuses, new int[] {0}, new long[] {13});
    MergedBlockMeta meta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, meta, new int[]{5, 5, 3}, new int[][]{{0, 1}, {2}, {3}});
  }

  @Test
  public void testFinalizeWithMultipleReducePartitions() throws IOException {
    PushBlock[] pushBlocks = new PushBlock[] {
      new PushBlock(0, 0, 0, ByteBuffer.wrap(new byte[2])),
      new PushBlock(0, 1, 0, ByteBuffer.wrap(new byte[3])),
      new PushBlock(0, 0, 1, ByteBuffer.wrap(new byte[5])),
      new PushBlock(0, 1, 1, ByteBuffer.wrap(new byte[3]))
    };
    pushBlockHelper(TEST_APP, pushBlocks);
    MergeStatuses statuses = pushResolver.finalizeShuffleMerge(
      new FinalizeShuffleMerge(TEST_APP, 0));
    validateMergeStatuses(statuses, new int[] {0, 1}, new long[] {5, 8});
    MergedBlockMeta meta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, meta, new int[]{5}, new int[][]{{0, 1}});
  }

  @Test
  public void testDeferredBufsAreWrittenDuringOnData() throws IOException {
    StreamCallbackWithID stream1 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    StreamCallbackWithID stream2 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 1, 0, 0));
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
  }

  @Test
  public void testDeferredBufsAreWrittenDuringOnComplete() throws IOException {
    StreamCallbackWithID stream1 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    StreamCallbackWithID stream2 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 1, 0, 0));
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
  }

  @Test
  public void testDuplicateBlocksAreIgnoredWhenPrevStreamHasCompleted() throws IOException {
    StreamCallbackWithID stream1 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    stream1.onComplete(stream1.getID());
    StreamCallbackWithID stream2 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    // This should be ignored
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[2]));
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[2]));
    stream2.onComplete(stream2.getID());
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{4}, new int[][]{{0}});
  }

  @Test
  public void testDuplicateBlocksAreIgnoredWhenPrevStreamIsInProgress() throws IOException {
    StreamCallbackWithID stream1 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    StreamCallbackWithID stream2 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    // This should be ignored
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
  }

  @Test
  public void testFailureAfterData() throws IOException {
    StreamCallbackWithID stream =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[4]));
    stream.onFailure(stream.getID(), new RuntimeException("Forced Failure"));
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    assertEquals("num-chunks", 0, blockMeta.getNumChunks());
  }

  @Test
  public void testFailureAfterMultipleDataBlocks() throws IOException {
    StreamCallbackWithID stream =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[2]));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[3]));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[4]));
    stream.onFailure(stream.getID(), new RuntimeException("Forced Failure"));
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    assertEquals("num-chunks", 0, blockMeta.getNumChunks());
  }

  @Test
  public void testFailureAfterComplete() throws IOException {
    StreamCallbackWithID stream =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[2]));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[3]));
    stream.onData(stream.getID(), ByteBuffer.wrap(new byte[4]));
    stream.onComplete(stream.getID());
    stream.onFailure(stream.getID(), new RuntimeException("Forced Failure"));
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{9}, new int[][]{{0}});
  }

  @Test (expected = RuntimeException.class)
  public void testTooLateArrival() throws IOException {
    ByteBuffer[] blocks = new ByteBuffer[]{
      ByteBuffer.wrap(new byte[4]),
      ByteBuffer.wrap(new byte[5])
    };
    StreamCallbackWithID stream = pushResolver.receiveBlockDataAsStream(
      new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    for (ByteBuffer block : blocks) {
      stream.onData(stream.getID(), block);
    }
    stream.onComplete(stream.getID());
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    StreamCallbackWithID stream1 = pushResolver.receiveBlockDataAsStream(
      new PushBlockStream(TEST_APP, 0, 1, 0, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[4]));
    try {
      stream1.onComplete(stream1.getID());
    } catch (RuntimeException re) {
      assertEquals(
        "Block shufflePush_0_1_0 received after merged shuffle is finalized",
          re.getMessage());
      MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
      validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{9}, new int[][]{{0}});
      throw re;
    }
  }

  @Test
  public void testIncompleteStreamsAreOverwritten() throws IOException {
    registerExecutor(TEST_APP, prepareLocalDirs(localDirs));
    byte[] expectedBytes = new byte[4];
    ThreadLocalRandom.current().nextBytes(expectedBytes);

    StreamCallbackWithID stream1 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    byte[] data = new byte[10];
    ThreadLocalRandom.current().nextBytes(data);
    stream1.onData(stream1.getID(), ByteBuffer.wrap(data));
    // There is a failure
    stream1.onFailure(stream1.getID(), new RuntimeException("forced error"));
    StreamCallbackWithID stream2 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 1, 0, 0));
    ByteBuffer nextBuf= ByteBuffer.wrap(expectedBytes, 0, 2);
    stream2.onData(stream2.getID(), nextBuf);
    stream2.onComplete(stream2.getID());
    StreamCallbackWithID stream3 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 2, 0, 0));
    nextBuf =  ByteBuffer.wrap(expectedBytes, 2, 2);
    stream3.onData(stream3.getID(), nextBuf);
    stream3.onComplete(stream3.getID());
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[]{4}, new int[][]{{1, 2}});
    FileSegmentManagedBuffer mb =
      (FileSegmentManagedBuffer) pushResolver.getMergedBlockData(TEST_APP, 0, 0, 0);
    assertArrayEquals(expectedBytes, mb.nioByteBuffer().array());
  }

  @Test (expected = RuntimeException.class)
  public void testCollision() throws IOException {
    StreamCallbackWithID stream1 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    StreamCallbackWithID stream2 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 1, 0, 0));
    // This should be deferred
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[5]));
    // Since stream2 didn't get any opportunity it will throw couldn't find opportunity error
    try {
      stream2.onComplete(stream2.getID());
    } catch (RuntimeException re) {
      assertEquals(
        "Couldn't find an opportunity to write block shufflePush_0_1_0 to merged shuffle",
        re.getMessage());
      throw re;
    }
  }

  @Test (expected = RuntimeException.class)
  public void testFailureInAStreamDoesNotInterfereWithStreamWhichIsWriting() throws IOException {
    StreamCallbackWithID stream1 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    StreamCallbackWithID stream2 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 1, 0, 0));
    // There is a failure with stream2
    stream2.onFailure(stream2.getID(), new RuntimeException("forced error"));
    StreamCallbackWithID stream3 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 2, 0, 0));
    // This should be deferred
    stream3.onData(stream3.getID(), ByteBuffer.wrap(new byte[5]));
    // Since this stream didn't get any opportunity it will throw couldn't find opportunity error
    RuntimeException failedEx = null;
    try {
      stream3.onComplete(stream3.getID());
    } catch (RuntimeException re) {
      assertEquals(
        "Couldn't find an opportunity to write block shufflePush_0_2_0 to merged shuffle",
        re.getMessage());
      failedEx = re;
    }
    // stream 1 now completes
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    stream1.onComplete(stream1.getID());

    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[] {4}, new int[][] {{0}});
    if (failedEx != null) {
      throw failedEx;
    }
  }

  @Test(expected = NullPointerException.class)
  public void testUpdateLocalDirsOnlyOnce() throws IOException {
    String testApp = "updateLocalDirsOnlyOnceTest";
    Path[] activeLocalDirs = createLocalDirs(1);
    registerExecutor(testApp, prepareLocalDirs(activeLocalDirs));
    assertEquals(pushResolver.getMergedBlockDirs(testApp).length, 1);
    assertTrue(pushResolver.getMergedBlockDirs(testApp)[0].contains(
      activeLocalDirs[0].toFile().getPath()));
    // Any later executor register from the same application should not change the active local
    // dirs list
    Path[] updatedLocalDirs = localDirs;
    registerExecutor(testApp, prepareLocalDirs(updatedLocalDirs));
    assertEquals(pushResolver.getMergedBlockDirs(testApp).length, 1);
    assertTrue(pushResolver.getMergedBlockDirs(testApp)[0].contains(
      activeLocalDirs[0].toFile().getPath()));
    removeApplication(testApp);
    try {
      pushResolver.getMergedBlockDirs(testApp);
    } catch (Throwable e) {
      assertTrue(e.getMessage()
        .startsWith("application " + testApp + " is not registered or NM was restarted."));
      Throwables.propagate(e);
    }
  }

  @Test
  public void testCleanUpDirectory() throws IOException, InterruptedException {
    String testApp = "cleanUpDirectory";
    Semaphore deleted = new Semaphore(0);
    pushResolver = new RemoteBlockPushResolver(conf) {
      @Override
      void deleteExecutorDirs(Path[] dirs) {
        super.deleteExecutorDirs(dirs);
        deleted.release();
      }
    };
    Path[] activeDirs = createLocalDirs(1);
    registerExecutor(testApp, prepareLocalDirs(activeDirs));
    PushBlock[] pushBlocks = new PushBlock[] {
      new PushBlock(0, 0, 0, ByteBuffer.wrap(new byte[4]))};
    pushBlockHelper(testApp, pushBlocks);
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(testApp, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(testApp, 0, 0);
    validateChunks(testApp, 0, 0, blockMeta, new int[]{4}, new int[][]{{0}});
    String[] mergeDirs = pushResolver.getMergedBlockDirs(testApp);
    pushResolver.applicationRemoved(testApp,  true);
    // Since the cleanup happen in a different thread, check few times to see if the merge dirs gets
    // deleted.
    deleted.acquire();
    for (String mergeDir : mergeDirs) {
      Assert.assertFalse(Files.exists(Paths.get(mergeDir)));
    }
  }

  @Test
  public void testRecoverIndexFileAfterIOExceptions() throws IOException {
    useTestFiles(true, false);
    RemoteBlockPushResolver.PushBlockStreamCallback callback1 =
      (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    callback1.onData(callback1.getID(), ByteBuffer.wrap(new byte[4]));
    callback1.onComplete(callback1.getID());
    RemoteBlockPushResolver.AppShufflePartitionInfo partitionInfo = callback1.getPartitionInfo();
    // Close the index stream so it throws IOException
    TestMergeShuffleFile testIndexFile = (TestMergeShuffleFile) partitionInfo.getIndexFile();
    testIndexFile.close();
    StreamCallbackWithID callback2 = pushResolver.receiveBlockDataAsStream(
      new PushBlockStream(TEST_APP, 0, 1, 0, 0));
    callback2.onData(callback2.getID(), ByteBuffer.wrap(new byte[5]));
    // This will complete without any IOExceptions because number of IOExceptions are less than
    // the threshold but the update to index file will be unsuccessful.
    callback2.onComplete(callback2.getID());
    assertEquals("index position", 16, testIndexFile.getPos());
    // Restore the index stream so it can write successfully again.
    testIndexFile.restore();
    StreamCallbackWithID callback3 = pushResolver.receiveBlockDataAsStream(
      new PushBlockStream(TEST_APP, 0, 2, 0, 0));
    callback3.onData(callback3.getID(), ByteBuffer.wrap(new byte[2]));
    callback3.onComplete(callback3.getID());
    assertEquals("index position", 24, testIndexFile.getPos());
    MergeStatuses statuses = pushResolver.finalizeShuffleMerge(
      new FinalizeShuffleMerge(TEST_APP, 0));
    validateMergeStatuses(statuses, new int[] {0}, new long[] {11});
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[] {4, 7}, new int[][] {{0}, {1, 2}});
  }

  @Test
  public void testRecoverIndexFileAfterIOExceptionsInFinalize() throws IOException {
    useTestFiles(true, false);
    RemoteBlockPushResolver.PushBlockStreamCallback callback1 =
      (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    callback1.onData(callback1.getID(), ByteBuffer.wrap(new byte[4]));
    callback1.onComplete(callback1.getID());
    RemoteBlockPushResolver.AppShufflePartitionInfo partitionInfo = callback1.getPartitionInfo();
    // Close the index stream so it throws IOException
    TestMergeShuffleFile testIndexFile = (TestMergeShuffleFile) partitionInfo.getIndexFile();
    testIndexFile.close();
    StreamCallbackWithID callback2 = pushResolver.receiveBlockDataAsStream(
      new PushBlockStream(TEST_APP, 0, 1, 0, 0));
    callback2.onData(callback2.getID(), ByteBuffer.wrap(new byte[5]));
    // This will complete without any IOExceptions because number of IOExceptions are less than
    // the threshold but the update to index file will be unsuccessful.
    callback2.onComplete(callback2.getID());
    assertEquals("index position", 16, testIndexFile.getPos());
    // The last update to index was unsuccessful however any further updates will be successful.
    // Restore the index stream so it can write successfully again.
    testIndexFile.restore();
    MergeStatuses statuses = pushResolver.finalizeShuffleMerge(
      new FinalizeShuffleMerge(TEST_APP, 0));
    assertEquals("index position", 24, testIndexFile.getPos());
    validateMergeStatuses(statuses, new int[] {0}, new long[] {9});
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[] {4, 5}, new int[][] {{0}, {1}});
  }

  @Test
  public void testRecoverMetaFileAfterIOExceptions() throws IOException {
    useTestFiles(false, true);
    RemoteBlockPushResolver.PushBlockStreamCallback callback1 =
      (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    callback1.onData(callback1.getID(), ByteBuffer.wrap(new byte[4]));
    callback1.onComplete(callback1.getID());
    RemoteBlockPushResolver.AppShufflePartitionInfo partitionInfo = callback1.getPartitionInfo();
    // Close the meta stream so it throws IOException
    TestMergeShuffleFile testMetaFile = (TestMergeShuffleFile) partitionInfo.getMetaFile();
    long metaPosBeforeClose = testMetaFile.getPos();
    testMetaFile.close();
    StreamCallbackWithID callback2 = pushResolver.receiveBlockDataAsStream(
      new PushBlockStream(TEST_APP, 0, 1, 0, 0));
    callback2.onData(callback2.getID(), ByteBuffer.wrap(new byte[5]));
    // This will complete without any IOExceptions because number of IOExceptions are less than
    // the threshold but the update to index and meta file will be unsuccessful.
    callback2.onComplete(callback2.getID());
    assertEquals("index position", 16, partitionInfo.getIndexFile().getPos());
    assertEquals("meta position", metaPosBeforeClose, testMetaFile.getPos());
    // Restore the meta stream so it can write successfully again.
    testMetaFile.restore();
    StreamCallbackWithID callback3 = pushResolver.receiveBlockDataAsStream(
      new PushBlockStream(TEST_APP, 0, 2, 0, 0));
    callback3.onData(callback3.getID(), ByteBuffer.wrap(new byte[2]));
    callback3.onComplete(callback3.getID());
    assertEquals("index position", 24, partitionInfo.getIndexFile().getPos());
    assertTrue("meta position", testMetaFile.getPos() > metaPosBeforeClose);
    MergeStatuses statuses = pushResolver.finalizeShuffleMerge(
      new FinalizeShuffleMerge(TEST_APP, 0));
    validateMergeStatuses(statuses, new int[] {0}, new long[] {11});
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[] {4, 7}, new int[][] {{0}, {1, 2}});
  }

  @Test
  public void testRecoverMetaFileAfterIOExceptionsInFinalize() throws IOException {
    useTestFiles(false, true);
    RemoteBlockPushResolver.PushBlockStreamCallback callback1 =
      (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    callback1.onData(callback1.getID(), ByteBuffer.wrap(new byte[4]));
    callback1.onComplete(callback1.getID());
    RemoteBlockPushResolver.AppShufflePartitionInfo partitionInfo = callback1.getPartitionInfo();
    // Close the meta stream so it throws IOException
    TestMergeShuffleFile testMetaFile = (TestMergeShuffleFile) partitionInfo.getMetaFile();
    long metaPosBeforeClose = testMetaFile.getPos();
    testMetaFile.close();
    StreamCallbackWithID callback2 = pushResolver.receiveBlockDataAsStream(
      new PushBlockStream(TEST_APP, 0, 1, 0, 0));
    callback2.onData(callback2.getID(), ByteBuffer.wrap(new byte[5]));
    // This will complete without any IOExceptions because number of IOExceptions are less than
    // the threshold but the update to index and meta file will be unsuccessful.
    callback2.onComplete(callback2.getID());
    MergeShuffleFile indexFile = partitionInfo.getIndexFile();
    assertEquals("index position", 16, indexFile.getPos());
    assertEquals("meta position", metaPosBeforeClose, testMetaFile.getPos());
    // Restore the meta stream so it can write successfully again.
    testMetaFile.restore();
    MergeStatuses statuses = pushResolver.finalizeShuffleMerge(
      new FinalizeShuffleMerge(TEST_APP, 0));
    assertEquals("index position", 24, indexFile.getPos());
    assertTrue("meta position", testMetaFile.getPos() > metaPosBeforeClose);
    validateMergeStatuses(statuses, new int[] {0}, new long[] {9});
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[] {4, 5}, new int[][] {{0}, {1}});
  }

  @Test (expected = RuntimeException.class)
  public void testIOExceptionsExceededThreshold() throws IOException {
    RemoteBlockPushResolver.PushBlockStreamCallback callback =
      (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    RemoteBlockPushResolver.AppShufflePartitionInfo partitionInfo = callback.getPartitionInfo();
    callback.onData(callback.getID(), ByteBuffer.wrap(new byte[4]));
    callback.onComplete(callback.getID());
    // Close the data stream so it throws continuous IOException
    partitionInfo.getDataChannel().close();
    for (int i = 1; i < 5; i++) {
      RemoteBlockPushResolver.PushBlockStreamCallback callback1 =
        (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
          new PushBlockStream(TEST_APP, 0, i, 0, 0));
      try {
        callback1.onData(callback1.getID(), ByteBuffer.wrap(new byte[2]));
      } catch (IOException ioe) {
        // this will throw IOException so the client can retry.
        callback1.onFailure(callback1.getID(), ioe);
      }
    }
    assertEquals(4, partitionInfo.getNumIOExceptions());
    // After 4 IOException, the server will respond with IOExceptions exceeded threshold
    try {
      RemoteBlockPushResolver.PushBlockStreamCallback callback2 =
        (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
          new PushBlockStream(TEST_APP, 0, 5, 0, 0));
      callback2.onData(callback.getID(), ByteBuffer.wrap(new byte[1]));
    } catch (Throwable t) {
      assertEquals("IOExceptions exceeded the threshold when merging shufflePush_0_5_0",
        t.getMessage());
      throw t;
    }
  }

  @Test (expected = RuntimeException.class)
  public void testIOExceptionsDuringMetaUpdateIncreasesExceptionCount() throws IOException {
    useTestFiles(true, false);
    RemoteBlockPushResolver.PushBlockStreamCallback callback =
      (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    RemoteBlockPushResolver.AppShufflePartitionInfo partitionInfo = callback.getPartitionInfo();
    callback.onData(callback.getID(), ByteBuffer.wrap(new byte[4]));
    callback.onComplete(callback.getID());
    TestMergeShuffleFile testIndexFile = (TestMergeShuffleFile) partitionInfo.getIndexFile();
    testIndexFile.close();
    for (int i = 1; i < 5; i++) {
      RemoteBlockPushResolver.PushBlockStreamCallback callback1 =
        (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
          new PushBlockStream(TEST_APP, 0, i, 0, 0));
      callback1.onData(callback1.getID(), ByteBuffer.wrap(new byte[5]));
      // This will complete without any exceptions but the exception count is increased.
      callback1.onComplete(callback1.getID());
    }
    assertEquals(4, partitionInfo.getNumIOExceptions());
    // After 4 IOException, the server will respond with IOExceptions exceeded threshold for any
    // new request for this partition.
    try {
      RemoteBlockPushResolver.PushBlockStreamCallback callback2 =
      (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 5, 0, 0));
      callback2.onData(callback2.getID(), ByteBuffer.wrap(new byte[4]));
      callback2.onComplete(callback2.getID());
    } catch (Throwable t) {
      assertEquals("IOExceptions exceeded the threshold when merging shufflePush_0_5_0",
        t.getMessage());
      throw t;
    }
  }

  @Test (expected = RuntimeException.class)
  public void testRequestForAbortedShufflePartitionThrowsException() {
    try {
      testIOExceptionsDuringMetaUpdateIncreasesExceptionCount();
    } catch (Throwable t) {
      // No more blocks can be merged to this partition.
    }
    try {
      pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 10, 0, 0));
    } catch (Throwable t) {
      assertEquals("IOExceptions exceeded the threshold when merging shufflePush_0_10_0",
        t.getMessage());
      throw t;
    }
  }

  @Test (expected = RuntimeException.class)
  public void testPendingBlockIsAbortedImmediately() throws IOException {
    useTestFiles(true, false);
    RemoteBlockPushResolver.PushBlockStreamCallback callback =
      (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    RemoteBlockPushResolver.AppShufflePartitionInfo partitionInfo = callback.getPartitionInfo();
    TestMergeShuffleFile testIndexFile = (TestMergeShuffleFile) partitionInfo.getIndexFile();
    testIndexFile.close();
    for (int i = 1; i < 6; i++) {
      RemoteBlockPushResolver.PushBlockStreamCallback callback1 =
        (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
          new PushBlockStream(TEST_APP, 0, i, 0, 0));
      try {
        callback1.onData(callback1.getID(), ByteBuffer.wrap(new byte[5]));
        // This will complete without any exceptions but the exception count is increased.
        callback1.onComplete(callback1.getID());
      } catch (Throwable t) {
        callback1.onFailure(callback1.getID(), t);
      }
    }
    assertEquals(5, partitionInfo.getNumIOExceptions());
    // The server will respond with IOExceptions exceeded threshold for any additional attempts
    // to write.
    try {
      callback.onData(callback.getID(), ByteBuffer.wrap(new byte[4]));
    } catch (Throwable t) {
      assertEquals("IOExceptions exceeded the threshold when merging shufflePush_0_0_0",
        t.getMessage());
      throw t;
    }
  }

  @Test (expected = RuntimeException.class)
  public void testWritingPendingBufsIsAbortedImmediatelyDuringComplete() throws IOException {
    useTestFiles(true, false);
    RemoteBlockPushResolver.PushBlockStreamCallback callback =
      (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    RemoteBlockPushResolver.AppShufflePartitionInfo partitionInfo = callback.getPartitionInfo();
    TestMergeShuffleFile testIndexFile = (TestMergeShuffleFile) partitionInfo.getIndexFile();
    testIndexFile.close();
    for (int i = 1; i < 5; i++) {
      RemoteBlockPushResolver.PushBlockStreamCallback callback1 =
        (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
          new PushBlockStream(TEST_APP, 0, i, 0, 0));
      try {
        callback1.onData(callback1.getID(), ByteBuffer.wrap(new byte[5]));
        // This will complete without any exceptions but the exception count is increased.
        callback1.onComplete(callback1.getID());
      } catch (Throwable t) {
        callback1.onFailure(callback1.getID(), t);
      }
    }
    assertEquals(4, partitionInfo.getNumIOExceptions());
    RemoteBlockPushResolver.PushBlockStreamCallback callback2 =
      (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 5, 0, 0));
    callback2.onData(callback2.getID(), ByteBuffer.wrap(new byte[5]));
    // This is deferred
    callback.onData(callback.getID(), ByteBuffer.wrap(new byte[4]));
    // Callback2 completes which will throw another exception.
    try {
      callback2.onComplete(callback2.getID());
    } catch (Throwable t) {
      callback2.onFailure(callback2.getID(), t);
    }
    assertEquals(5, partitionInfo.getNumIOExceptions());
    // Restore index file so that any further writes to it are successful and any exceptions are
    // due to IOExceptions exceeding threshold.
    testIndexFile.restore();
    try {
      callback.onComplete(callback.getID());
    } catch (Throwable t) {
      assertEquals("IOExceptions exceeded the threshold when merging shufflePush_0_0_0",
        t.getMessage());
      throw t;
    }
  }

  @Test
  public void testFailureWhileTruncatingFiles() throws IOException {
    useTestFiles(true, false);
    PushBlock[] pushBlocks = new PushBlock[] {
      new PushBlock(0, 0, 0, ByteBuffer.wrap(new byte[2])),
      new PushBlock(0, 1, 0, ByteBuffer.wrap(new byte[3])),
      new PushBlock(0, 0, 1, ByteBuffer.wrap(new byte[5])),
      new PushBlock(0, 1, 1, ByteBuffer.wrap(new byte[3]))
    };
    pushBlockHelper(TEST_APP, pushBlocks);
    RemoteBlockPushResolver.PushBlockStreamCallback callback =
      (RemoteBlockPushResolver.PushBlockStreamCallback) pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(TEST_APP, 0, 2, 0, 0));
    callback.onData(callback.getID(), ByteBuffer.wrap(new byte[2]));
    callback.onComplete(callback.getID());
    RemoteBlockPushResolver.AppShufflePartitionInfo partitionInfo = callback.getPartitionInfo();
    TestMergeShuffleFile testIndexFile = (TestMergeShuffleFile) partitionInfo.getIndexFile();
    // Close the index file so truncate throws IOException
    testIndexFile.close();
    MergeStatuses statuses = pushResolver.finalizeShuffleMerge(
      new FinalizeShuffleMerge(TEST_APP, 0));
    validateMergeStatuses(statuses, new int[] {1}, new long[] {8});
    MergedBlockMeta meta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 1);
    validateChunks(TEST_APP, 0, 1, meta, new int[]{5, 3}, new int[][]{{0},{1}});
  }

  @Test
  public void testOnFailureInvokedMoreThanOncePerBlock() throws IOException {
    StreamCallbackWithID stream1 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    stream1.onFailure(stream1.getID(), new RuntimeException("forced error"));
    StreamCallbackWithID stream2 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 1, 0, 0));
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[5]));
    // On failure on stream1 gets invoked again and should cause no interference
    stream1.onFailure(stream1.getID(), new RuntimeException("2nd forced error"));
    StreamCallbackWithID stream3 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 3, 0, 0));
    // This should be deferred as stream 2 is still the active stream
    stream3.onData(stream3.getID(), ByteBuffer.wrap(new byte[2]));
    // Stream 2 writes more and completes
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[4]));
    stream2.onComplete(stream2.getID());
    stream3.onComplete(stream3.getID());
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[] {9, 2}, new int[][] {{1},{3}});
    removeApplication(TEST_APP);
  }

  @Test (expected = RuntimeException.class)
  public void testFailureAfterDuplicateBlockDoesNotInterfereActiveStream() throws IOException {
    StreamCallbackWithID stream1 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    StreamCallbackWithID stream1Duplicate =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 0, 0, 0));
    stream1.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));
    stream1.onComplete(stream1.getID());
    stream1Duplicate.onData(stream1.getID(), ByteBuffer.wrap(new byte[2]));

    StreamCallbackWithID stream2 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 1, 0, 0));
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[5]));
    // Should not change the current map id of the reduce partition
    stream1Duplicate.onFailure(stream2.getID(), new RuntimeException("forced error"));

    StreamCallbackWithID stream3 =
      pushResolver.receiveBlockDataAsStream(new PushBlockStream(TEST_APP, 0, 2, 0, 0));
    // This should be deferred as stream 2 is still the active stream
    stream3.onData(stream3.getID(), ByteBuffer.wrap(new byte[2]));
    RuntimeException failedEx = null;
    try {
      stream3.onComplete(stream3.getID());
    } catch (RuntimeException re) {
      assertEquals(
        "Couldn't find an opportunity to write block shufflePush_0_2_0 to merged shuffle",
        re.getMessage());
      failedEx = re;
    }
    // Stream 2 writes more and completes
    stream2.onData(stream2.getID(), ByteBuffer.wrap(new byte[4]));
    stream2.onComplete(stream2.getID());
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(TEST_APP, 0));
    MergedBlockMeta blockMeta = pushResolver.getMergedBlockMeta(TEST_APP, 0, 0);
    validateChunks(TEST_APP, 0, 0, blockMeta, new int[] {11}, new int[][] {{0, 1}});
    removeApplication(TEST_APP);
    if (failedEx != null) {
      throw failedEx;
    }
  }

  private void useTestFiles(boolean useTestIndexFile, boolean useTestMetaFile) throws IOException {
    pushResolver = new RemoteBlockPushResolver(conf) {
      @Override
      AppShufflePartitionInfo newAppShufflePartitionInfo(AppShuffleId appShuffleId, int reduceId,
        File dataFile, File indexFile, File metaFile) throws IOException {
        MergeShuffleFile mergedIndexFile = useTestIndexFile ? new TestMergeShuffleFile(indexFile)
          : new MergeShuffleFile(indexFile);
        MergeShuffleFile mergedMetaFile = useTestMetaFile ? new TestMergeShuffleFile(metaFile) :
          new MergeShuffleFile(metaFile);
        return new AppShufflePartitionInfo(appShuffleId, reduceId, dataFile, mergedIndexFile,
          mergedMetaFile);
      }
    };
    registerExecutor(TEST_APP, prepareLocalDirs(localDirs));
  }

  private Path[] createLocalDirs(int numLocalDirs) throws IOException {
    Path[] localDirs = new Path[numLocalDirs];
    for (int i = 0; i < localDirs.length; i++) {
      localDirs[i] = Files.createTempDirectory("shuffleMerge");
      localDirs[i].toFile().deleteOnExit();
    }
    return localDirs;
  }

  private void registerExecutor(String appId, String[] localDirs) throws IOException {
    ExecutorShuffleInfo shuffleInfo = new ExecutorShuffleInfo(localDirs, 1, "mergedShuffle");
    pushResolver.registerExecutor(appId, shuffleInfo);
  }

  private String[] prepareLocalDirs(Path[] localDirs) throws IOException {
    String[] blockMgrDirs = new String[localDirs.length];
    for (int i = 0; i< localDirs.length; i++) {
      Files.createDirectories(localDirs[i].resolve(
        RemoteBlockPushResolver.MERGE_MANAGER_DIR + File.separator + "00"));
      blockMgrDirs[i] = localDirs[i].toFile().getPath() + File.separator + BLOCK_MANAGER_DIR;
    }
    return blockMgrDirs;
  }

  private void removeApplication(String appId) {
    // PushResolver cleans up the local dirs in a different thread which can conflict with the test
    // data of other tests, since they are using the same Application Id.
    pushResolver.applicationRemoved(appId,  false);
  }

  private void validateMergeStatuses(
      MergeStatuses mergeStatuses,
      int[] expectedReduceIds,
      long[] expectedSizes) {
    assertArrayEquals(expectedReduceIds, mergeStatuses.reduceIds);
    assertArrayEquals(expectedSizes, mergeStatuses.sizes);
  }

  private void validateChunks(
      String appId,
      int shuffleId,
      int reduceId,
      MergedBlockMeta meta,
      int[] expectedSizes,
      int[][] expectedMapsPerChunk) throws IOException {
    assertEquals("num chunks", expectedSizes.length, meta.getNumChunks());
    RoaringBitmap[] bitmaps = meta.readChunkBitmaps();
    assertEquals("num of bitmaps", meta.getNumChunks(), bitmaps.length);
    for (int i = 0; i < meta.getNumChunks(); i++) {
      RoaringBitmap chunkBitmap = bitmaps[i];
      Arrays.stream(expectedMapsPerChunk[i]).forEach(x -> assertTrue(chunkBitmap.contains(x)));
    }
    for (int i = 0; i < meta.getNumChunks(); i++) {
      FileSegmentManagedBuffer mb =
        (FileSegmentManagedBuffer) pushResolver.getMergedBlockData(appId, shuffleId, reduceId, i);
      assertEquals(expectedSizes[i], mb.getLength());
    }
  }

  private void pushBlockHelper(
      String appId,
      PushBlock[] blocks) throws IOException {
    for (int i = 0; i < blocks.length; i++) {
      StreamCallbackWithID stream = pushResolver.receiveBlockDataAsStream(
        new PushBlockStream(appId, blocks[i].shuffleId, blocks[i].mapIndex, blocks[i].reduceId, 0));
      stream.onData(stream.getID(), blocks[i].buffer);
      stream.onComplete(stream.getID());
    }
  }

  private static class PushBlock {
    private final int shuffleId;
    private final int mapIndex;
    private final int reduceId;
    private final ByteBuffer buffer;
    PushBlock(int shuffleId, int mapIndex, int reduceId, ByteBuffer buffer) {
      this.shuffleId = shuffleId;
      this.mapIndex = mapIndex;
      this.reduceId = reduceId;
      this.buffer = buffer;
    }
  }

  private static class TestMergeShuffleFile extends MergeShuffleFile {
    private DataOutputStream activeDos;
    private File file;
    private FileChannel channel;

    private TestMergeShuffleFile(File file) throws IOException {
      super(null, null);
      this.file = file;
      FileOutputStream fos = new FileOutputStream(file);
      channel = fos.getChannel();
      activeDos = new DataOutputStream(fos);
    }

    @Override
    DataOutputStream getDos() {
      return activeDos;
    }

    @Override
    FileChannel getChannel() {
      return channel;
    }

    @Override
    void close() throws IOException {
      activeDos.close();
    }

    void restore() throws IOException {
      FileOutputStream fos = new FileOutputStream(file, true);
      channel = fos.getChannel();
      activeDos = new DataOutputStream(fos);
    }
  }
}
