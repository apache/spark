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
    pushResolver = new RemoteBlockPushResolver(conf, localDirs);
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
      String appId = "app_NoIndexFile";
      registerApplication(appId);
      pushResolver.getChunkCount(appId, 0, 0);
      removeApplication(appId);
    } catch (Throwable t) {
      assertTrue(t.getMessage().startsWith("Application merged shuffle index file is not found"));
      Throwables.propagate(t);
    }
  }

  @Test
  public void testChunkCountsAndBlockData() throws IOException {
    String appId = "app_ChunkCountsAndBlockData";
    registerApplication(appId);
    PushBlockStream[] pushBlocks = new PushBlockStream[] {
        new PushBlockStream(appId, "shuffle_0_0_0", 0),
        new PushBlockStream(appId, "shuffle_0_1_0", 0),
    };
    ByteBuffer[] blocks = new ByteBuffer[]{
        ByteBuffer.wrap(new byte[4]),
        ByteBuffer.wrap(new byte[5])
    };
    pushBlockHelper(appId, pushBlocks, blocks);
    int numChunks = pushResolver.getChunkCount(appId, 0, 0);
    assertEquals(2, numChunks);
    validateChunks(appId,0, 0, numChunks, new int[]{4, 5});
    removeApplication(appId);
  }

  @Test
  public void testMultipleBlocksInAChunk() throws IOException {
    String appId = "app_MultipleBlocksInAChunk";
    registerApplication(appId);
    PushBlockStream[] pushBlocks = new PushBlockStream[] {
        new PushBlockStream(appId, "shuffle_0_0_0", 0),
        new PushBlockStream(appId, "shuffle_0_1_0", 0),
        new PushBlockStream(appId, "shuffle_0_2_0", 0),
        new PushBlockStream(appId, "shuffle_0_3_0", 0),
    };
    ByteBuffer[] buffers = new ByteBuffer[]{
        ByteBuffer.wrap(new byte[2]),
        ByteBuffer.wrap(new byte[3]),
        ByteBuffer.wrap(new byte[5]),
        ByteBuffer.wrap(new byte[3])
    };
    pushBlockHelper(appId, pushBlocks, buffers);
    int numChunks = pushResolver.getChunkCount(appId, 0, 0);
    assertEquals(3, numChunks);
    validateChunks(appId,0, 0, numChunks, new int[]{5, 5, 3});
    removeApplication(appId);
  }

  /**
   * Registers the app with RemoteBlockPushResolver. Use a different appId for different tests.
   * This is because when the application gets removed, the directory cleaner removes the merged
   * data and file in a different thread which can delete the relevant data of a different test.
   */
  private void registerApplication(String appId) throws IOException {
    for (String localDir : localDirs) {
      Files.createDirectories(Paths.get(localDir).resolve(appId + "/merge_manager"));
    }
    pushResolver.registerApplication(appId, appId + "/merge_manager");
  }

  private void removeApplication(String appId) {
    pushResolver.applicationRemoved(appId,  true);
  }

  private void validateChunks(
      String appId, int shuffleId, int reduceId, int numChunks, int[] expectedSizes) {
    for (int i = 0; i < numChunks; i++) {
      FileSegmentManagedBuffer mb =
          (FileSegmentManagedBuffer) pushResolver.getMergedBlockData(appId, shuffleId, reduceId,
              i);
      assertEquals(expectedSizes[i], mb.getLength());
    }
  }

  private void pushBlockHelper(String appId, PushBlockStream[] pushBlocks, ByteBuffer[] blocks)
      throws IOException {
    Preconditions.checkArgument(pushBlocks.length == blocks.length);
    for (int i = 0; i < pushBlocks.length; i++) {
      StreamCallbackWithID stream = pushResolver.receiveBlockDataAsStream(
          new PushBlockStream(appId, pushBlocks[i].blockId, 0));
      stream.onData(stream.getID(), blocks[i]);
      stream.onComplete(stream.getID());
    }
    pushResolver.finalizeShuffleMerge(new FinalizeShuffleMerge(appId, 0));
  }
}
