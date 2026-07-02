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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExternalShuffleBlockResolverSuite {
  private static final String sortBlock0 = "Hello!";
  private static final String sortBlock1 = "World!";
  private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";

  private static TestShuffleDataContext dataContext;

  private static final TransportConf conf =
      new TransportConf("shuffle", MapConfigProvider.EMPTY);

  @BeforeClass
  public static void beforeAll() throws IOException {
    dataContext = new TestShuffleDataContext(2, 5);

    dataContext.create();
    // Write some sort data.
    dataContext.insertSortShuffleData(0, 0, new byte[][] {
        sortBlock0.getBytes(StandardCharsets.UTF_8),
        sortBlock1.getBytes(StandardCharsets.UTF_8)});
  }

  @AfterClass
  public static void afterAll() {
    dataContext.cleanup();
  }

  @Test
  public void testBadRequests() throws IOException {
    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
    // Unregistered executor
    RuntimeException e = assertThrows(RuntimeException.class,
      () -> resolver.getBlockData("app0", "exec1", 1, 1, 0));
    assertTrue("Bad error message: " + e, e.getMessage().contains("not registered"));

    // Nonexistent shuffle block
    resolver.registerExecutor("app0", "exec3",
      dataContext.createExecutorInfo(SORT_MANAGER));
    assertThrows(Exception.class,
      () -> resolver.getBlockData("app0", "exec3", 1, 1, 0));
  }

  @Test
  public void testSortShuffleBlocks() throws IOException {
    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
    resolver.registerExecutor("app0", "exec0",
      dataContext.createExecutorInfo(SORT_MANAGER));

    try (InputStream block0Stream = resolver.getBlockData(
        "app0", "exec0", 0, 0, 0).createInputStream()) {
      String block0 =
        CharStreams.toString(new InputStreamReader(block0Stream, StandardCharsets.UTF_8));
      assertEquals(sortBlock0, block0);
    }

    try (InputStream block1Stream = resolver.getBlockData(
        "app0", "exec0", 0, 0, 1).createInputStream()) {
      String block1 =
        CharStreams.toString(new InputStreamReader(block1Stream, StandardCharsets.UTF_8));
      assertEquals(sortBlock1, block1);
    }

    try (InputStream blocksStream = resolver.getContinuousBlocksData(
        "app0", "exec0", 0, 0, 0, 2).createInputStream()) {
      String blocks =
        CharStreams.toString(new InputStreamReader(blocksStream, StandardCharsets.UTF_8));
      assertEquals(sortBlock0 + sortBlock1, blocks);
    }
  }

  @Test
  public void jsonSerializationOfExecutorRegistration() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    AppExecId appId = new AppExecId("foo", "bar");
    String appIdJson = mapper.writeValueAsString(appId);
    AppExecId parsedAppId = mapper.readValue(appIdJson, AppExecId.class);
    assertEquals(parsedAppId, appId);

    ExecutorShuffleInfo shuffleInfo =
      new ExecutorShuffleInfo(new String[]{"/bippy", "/flippy"}, 7, SORT_MANAGER);
    String shuffleJson = mapper.writeValueAsString(shuffleInfo);
    ExecutorShuffleInfo parsedShuffleInfo =
      mapper.readValue(shuffleJson, ExecutorShuffleInfo.class);
    assertEquals(parsedShuffleInfo, shuffleInfo);

    // Intentionally keep these hard-coded strings in here, to check backwards-compatibility.
    // its not legacy yet, but keeping this here in case anybody changes it
    String legacyAppIdJson = "{\"appId\":\"foo\", \"execId\":\"bar\"}";
    assertEquals(appId, mapper.readValue(legacyAppIdJson, AppExecId.class));
    String legacyShuffleJson = "{\"localDirs\": [\"/bippy\", \"/flippy\"], " +
      "\"subDirsPerLocalDir\": 7, \"shuffleManager\": " + "\"" + SORT_MANAGER + "\"}";
    assertEquals(shuffleInfo, mapper.readValue(legacyShuffleJson, ExecutorShuffleInfo.class));
  }

  /**
   * removeBlocks only operates on plain block-id file names. A block id containing parent-directory
   * segments is skipped: it is not removed, and a file located outside the executor's local
   * directory is left untouched.
   */
  @Test
  public void removeBlocksIgnoresBlockIdWithParentDirSegments() throws IOException {
    TestShuffleDataContext context = new TestShuffleDataContext(1, 5);
    context.create();
    File outsideFile = null;
    try {
      File localDir = new File(context.localDirs[0]);
      File parentOfLocalDir = localDir.getParentFile();
      outsideFile = new File(parentOfLocalDir, "outside-" + UUID.randomUUID() + ".txt");
      Files.write(outsideFile.toPath(), "should not be deleted".getBytes(StandardCharsets.UTF_8));
      assertTrue("precondition: file was created", outsideFile.exists());

      ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
      resolver.registerExecutor("app0", "0", context.createExecutorInfo(SORT_MANAGER));

      String blockId = ".." + File.separator + ".." + File.separator + outsideFile.getName();
      int removed = resolver.removeBlocks("app0", "0", new String[] { blockId });

      assertEquals("A block id with parent-dir segments must not remove a block", 0, removed);
      assertTrue(
        "A file outside the local directory must not be removed (" +
          outsideFile.getAbsolutePath() + ")",
        outsideFile.exists());
    } finally {
      if (outsideFile != null && outsideFile.exists()) {
        assertTrue(outsideFile.delete() || !outsideFile.exists());
      }
      context.cleanup();
    }
  }

  /**
   * A block id containing a path separator is not a plain file name and is skipped, so removeBlocks
   * reports zero removed blocks.
   */
  @Test
  public void removeBlocksIgnoresBlockIdWithSeparator() throws IOException {
    TestShuffleDataContext context = new TestShuffleDataContext(1, 5);
    context.create();
    try {
      ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
      resolver.registerExecutor("app0", "0", context.createExecutorInfo(SORT_MANAGER));

      String blockId = "sub" + File.separator + "child";
      int removed = resolver.removeBlocks("app0", "0", new String[] { blockId });

      assertEquals("A block id containing a path separator must be skipped", 0, removed);
    } finally {
      context.cleanup();
    }
  }

  /**
   * In a batched call, an invalid block id does not abort the whole operation: a valid block id in
   * the same batch is still removed while the invalid one is skipped.
   */
  @Test
  public void removeBlocksContinuesAfterIgnoringInvalidBlockId() throws IOException {
    TestShuffleDataContext context = new TestShuffleDataContext(1, 5);
    context.create();
    File outsideFile = null;
    try {
      File localDir = new File(context.localDirs[0]);
      File parentOfLocalDir = localDir.getParentFile();
      outsideFile = new File(parentOfLocalDir, "outside-" + UUID.randomUUID() + ".txt");
      Files.write(outsideFile.toPath(), "should not be deleted".getBytes(StandardCharsets.UTF_8));

      // Plant a real, legitimately-named block in the localDir so the valid removal has something
      // to actually remove.
      context.insertCachedRddData(7, 0, new byte[] { 1, 2, 3 });
      String validBlockId = "rdd_7_0";
      File validFile = new File(ExecutorDiskUtils.getFilePath(
        context.localDirs, context.subDirsPerLocalDir, validBlockId));
      assertTrue("precondition: block file was created", validFile.exists());

      ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
      resolver.registerExecutor("app0", "0", context.createExecutorInfo(SORT_MANAGER));

      String invalidBlockId =
        ".." + File.separator + ".." + File.separator + outsideFile.getName();
      int removed = resolver.removeBlocks(
        "app0", "0", new String[] { invalidBlockId, validBlockId });

      assertEquals("The valid block id in the batch should still be removed", 1, removed);
      assertTrue(
        "The invalid entry must not remove a file outside the local directory",
        outsideFile.exists());
      assertFalse("The valid block file should have been removed by the batch", validFile.exists());
    } finally {
      if (outsideFile != null && outsideFile.exists()) {
        assertTrue(outsideFile.delete() || !outsideFile.exists());
      }
      context.cleanup();
    }
  }

}
