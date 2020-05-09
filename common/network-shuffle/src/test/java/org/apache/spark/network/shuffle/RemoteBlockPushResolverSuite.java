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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.shuffle.protocol.FinalizeShuffleMerge;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class RemoteBlockPushResolverSuite {

  private static final Logger log = LoggerFactory.getLogger(RemoteBlockPushResolverSuite.class);
  private static final String[] LOCAL_DIRS = new String[]{"target/l1", "target/l2"};
  private static final String TEST_APP = "testApp";

  private TransportConf conf;
  private RemoteBlockPushResolver pushResolver;

  @Before
  public void before() throws IOException {
    cleanupLocalDirs();
    for (String localDir : LOCAL_DIRS) {
      Files.createDirectories(Paths.get(localDir).resolve(TEST_APP));
    }
    MapConfigProvider provider = new MapConfigProvider(
        ImmutableMap.of("spark.shuffle.server.minChunkSizeInMergedShuffleFile", "4"));
    conf = new TransportConf("shuffle", provider);
    pushResolver = new RemoteBlockPushResolver(conf, LOCAL_DIRS);
    pushResolver.registerApplication(TEST_APP, TEST_APP);
  }

  @After
  public void after() {
    try {
      pushResolver.applicationRemoved(TEST_APP, true);
      cleanupLocalDirs();
    } catch (IOException e) {
      // don't fail if clean up doesn't succeed.
      log.warn("Error deleting test local dirs", e);
    }
  }

  private static void cleanupLocalDirs() throws IOException {
    for (String local : LOCAL_DIRS) {
      FileUtils.deleteDirectory(new File(local));
    }
  }

  @Test(expected = RuntimeException.class)
  public void testNoIndexFile() {
    try {
      pushResolver.getChunkCount(TEST_APP, 0, 0);
    } catch (Throwable t) {
      assertTrue(t.getMessage().startsWith("Application merged shuffle index file is not found"));
      Throwables.propagate(t);
    }
  }

  @Test
  public void testChunkCountsAndBlockData() throws IOException {
    PushBlockStream[] pushBlocks = new PushBlockStream[] {
        new PushBlockStream(TEST_APP, "shuffle_0_0_0", 0),
        new PushBlockStream(TEST_APP, "shuffle_0_1_0", 0),
    };
    ByteBuffer[] blocks = new ByteBuffer[]{
        ByteBuffer.wrap(new byte[4]),
        ByteBuffer.wrap(new byte[5])
    };
    pushBlockHelper(pushBlocks, blocks);
    int numChunks = pushResolver.getChunkCount(TEST_APP, 0, 0);
    assertEquals(2, numChunks);
    validateChunks(0, 0, numChunks, new int[]{4, 5});
  }

  @Test
  public void testMultipleBlocksInAChunk() throws IOException {
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
    pushBlockHelper(pushBlocks, buffers);
    int numChunks = pushResolver.getChunkCount(TEST_APP, 0, 0);
    assertEquals(3, numChunks);
    validateChunks(0, 0, numChunks, new int[]{5, 5, 3});
  }

  private void validateChunks(
      int shuffleId, int reduceId, int numChunks, int[] expectedSizes) {
    for (int i = 0; i < numChunks; i++) {
      FileSegmentManagedBuffer mb =
          (FileSegmentManagedBuffer) pushResolver.getMergedBlockData(TEST_APP, shuffleId, reduceId,
              i);
      assertEquals(expectedSizes[i], mb.getLength());
    }
  }

  private void pushBlockHelper(PushBlockStream[] pushBlocks, ByteBuffer[] blocks)
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
