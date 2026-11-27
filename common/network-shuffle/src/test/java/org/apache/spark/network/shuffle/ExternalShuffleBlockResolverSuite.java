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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ExternalShuffleBlockResolverSuite {
  private static final String sortBlock0 = "Hello!";
  private static final String sortBlock1 = "World!";
  private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";

  private static TestShuffleDataContext dataContext;

  private static final TransportConf conf =
      new TransportConf("shuffle", MapConfigProvider.EMPTY);

  @BeforeAll
  public static void beforeAll() throws IOException {
    dataContext = new TestShuffleDataContext(2, 5);

    dataContext.create();
    // Write some sort data.
    dataContext.insertSortShuffleData(0, 0, new byte[][] {
        sortBlock0.getBytes(StandardCharsets.UTF_8),
        sortBlock1.getBytes(StandardCharsets.UTF_8)});
  }

  @AfterAll
  public static void afterAll() {
    dataContext.cleanup();
  }

  @Test
  public void testBadRequests() throws IOException {
    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
    // Unregistered executor
    RuntimeException e = assertThrows(RuntimeException.class,
      () -> resolver.getBlockData("app0", "exec1", 1, 1, 0));
    assertTrue(e.getMessage().contains("not registered"), "Bad error message: " + e);

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
      assertEquals(sortBlock0, JavaUtils.toString(block0Stream));
    }

    try (InputStream block1Stream = resolver.getBlockData(
        "app0", "exec0", 0, 0, 1).createInputStream()) {
      assertEquals(sortBlock1, JavaUtils.toString(block1Stream));
    }

    try (InputStream blocksStream = resolver.getContinuousBlocksData(
        "app0", "exec0", 0, 0, 0, 2).createInputStream()) {
      assertEquals(sortBlock0 + sortBlock1, JavaUtils.toString(blocksStream));
    }
  }

  @Test
  public void testBlockFileResolvingOutsideLocalDirsRejected() throws IOException {
    // A registered executor's directories can be modified after registration. The resolver
    // must refuse to serve a block whose resolved path escapes the registered local dirs.
    File outsideFile = File.createTempFile("outside", ".txt");
    TestShuffleDataContext context = new TestShuffleDataContext(2, 5);
    context.create();
    try {
      context.insertSortShuffleData(1, 0, new byte[][] {
        "AAAA".getBytes(StandardCharsets.UTF_8),
        "BBBB".getBytes(StandardCharsets.UTF_8)});
      ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
      resolver.registerExecutor("app1", "exec0", context.createExecutorInfo(SORT_MANAGER));

      // Sanity check: the block is served normally before the file is replaced.
      try (InputStream in = resolver.getBlockData("app1", "exec0", 1, 0, 0)
          .createInputStream()) {
        assertEquals("AAAA", JavaUtils.toString(in));
      }

      // Point the data file at a path outside the registered local dirs.
      File dataFile = new File(ExecutorDiskUtils.getFilePath(
        context.localDirs, context.subDirsPerLocalDir, "shuffle_1_0_0.data"));
      assertTrue(dataFile.delete());
      try {
        Files.createSymbolicLink(dataFile.toPath(), outsideFile.toPath());
      } catch (UnsupportedOperationException | IOException e) {
        // Filesystems without symlink support cannot exercise this case.
        Assumptions.assumeTrue(false, "symlinks unsupported on this filesystem");
      }

      RuntimeException e = assertThrows(RuntimeException.class,
        () -> resolver.getBlockData("app1", "exec0", 1, 0, 0));
      assertTrue(e.getMessage().contains("resolves outside"), "Bad error message: " + e);
    } finally {
      context.cleanup();
      outsideFile.delete();
    }
  }

  @Test
  public void testIndexFileResolvingOutsideLocalDirsRejected() throws IOException {
    // The index file is verified when it is loaded into the index cache.
    File outsideFile = File.createTempFile("outside", ".index");
    TestShuffleDataContext context = new TestShuffleDataContext(2, 5);
    context.create();
    try {
      context.insertSortShuffleData(1, 0, new byte[][] {
        "AAAA".getBytes(StandardCharsets.UTF_8),
        "BBBB".getBytes(StandardCharsets.UTF_8)});
      ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
      resolver.registerExecutor("app1", "exec0", context.createExecutorInfo(SORT_MANAGER));

      // Replace the index file with a link to a valid copy outside the registered local dirs.
      File indexFile = new File(ExecutorDiskUtils.getFilePath(
        context.localDirs, context.subDirsPerLocalDir, "shuffle_1_0_0.index"));
      Files.copy(indexFile.toPath(), outsideFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
      assertTrue(indexFile.delete());
      try {
        Files.createSymbolicLink(indexFile.toPath(), outsideFile.toPath());
      } catch (UnsupportedOperationException | IOException e) {
        // Filesystems without symlink support cannot exercise this case.
        Assumptions.assumeTrue(false, "symlinks unsupported on this filesystem");
      }

      RuntimeException e = assertThrows(RuntimeException.class,
        () -> resolver.getBlockData("app1", "exec0", 1, 0, 0));
      assertTrue(e.getMessage().contains("resolves outside"), "Bad error message: " + e);
    } finally {
      context.cleanup();
      outsideFile.delete();
    }
  }

  @Test
  public void canonicalLocalDirsEvictedOnApplicationRemoved() throws IOException {
    ExternalShuffleBlockResolver resolver =
      new ExternalShuffleBlockResolver(conf, null, Runnable::run);
    String root = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString())
      .getAbsolutePath();
    String sharedDir = root + "/shared";
    int numExecutors = 100;
    for (String appId : new String[] {"app1", "app2"}) {
      for (int i = 0; i < numExecutors; i++) {
        String[] localDirs = {root + "/" + appId + "/exec" + i, sharedDir};
        resolver.registerExecutor(appId, "exec" + i,
          new ExecutorShuffleInfo(localDirs, 64, SORT_MANAGER));
      }
    }
    assertEquals(2 * numExecutors + 1, resolver.canonicalLocalDirs.size());

    // Executor removal keeps the executor registered, so its dirs stay cached.
    for (int i = 0; i < numExecutors; i++) {
      resolver.executorRemoved("exec" + i, "app1");
    }
    assertEquals(2 * numExecutors + 1, resolver.canonicalLocalDirs.size());

    // A dir still referenced by another application's executor is kept.
    resolver.applicationRemoved("app1", false);
    assertEquals(numExecutors + 1, resolver.canonicalLocalDirs.size());
    assertTrue(resolver.canonicalLocalDirs.containsKey(sharedDir));

    resolver.applicationRemoved("app2", false);
    assertTrue(resolver.canonicalLocalDirs.isEmpty());
    assertEquals(0, resolver.getRegisteredExecutorsSize());
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
      Files.writeString(outsideFile.toPath(), "should not be deleted", StandardCharsets.UTF_8);
      assertTrue(outsideFile.exists(), "precondition: file was created");

      ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
      resolver.registerExecutor("app0", "0", context.createExecutorInfo(SORT_MANAGER));

      String blockId = ".." + File.separator + ".." + File.separator + outsideFile.getName();
      int removed = resolver.removeBlocks("app0", "0", new String[] { blockId });

      assertEquals(0, removed, "A block id with parent-dir segments must not remove a block");
      assertTrue(outsideFile.exists(),
        "A file outside the local directory must not be removed (" +
          outsideFile.getAbsolutePath() + ")");
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

      assertEquals(0, removed, "A block id containing a path separator must be skipped");
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
      Files.writeString(outsideFile.toPath(), "should not be deleted", StandardCharsets.UTF_8);

      // Plant a real, legitimately-named block in the localDir so the valid removal has something
      // to actually remove.
      context.insertCachedRddData(7, 0, new byte[] { 1, 2, 3 });
      String validBlockId = "rdd_7_0";
      File validFile = new File(ExecutorDiskUtils.getFilePath(
        context.localDirs, context.subDirsPerLocalDir, validBlockId));
      assertTrue(validFile.exists(), "precondition: block file was created");

      ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
      resolver.registerExecutor("app0", "0", context.createExecutorInfo(SORT_MANAGER));

      String invalidBlockId =
        ".." + File.separator + ".." + File.separator + outsideFile.getName();
      int removed = resolver.removeBlocks(
        "app0", "0", new String[] { invalidBlockId, validBlockId });

      assertEquals(1, removed, "The valid block id in the batch should still be removed");
      assertTrue(outsideFile.exists(),
        "The invalid entry must not remove a file outside the local directory");
      assertFalse(validFile.exists(), "The valid block file should have been removed by the batch");
    } finally {
      if (outsideFile != null && outsideFile.exists()) {
        assertTrue(outsideFile.delete() || !outsideFile.exists());
      }
      context.cleanup();
    }
  }

  private static void createSymbolicLinkOrSkip(File link, File target) {
    try {
      Files.createSymbolicLink(link.toPath(), target.toPath());
    } catch (UnsupportedOperationException | IOException e) {
      // Filesystems without symlink support cannot exercise this case.
      Assumptions.assumeTrue(false, "symlinks unsupported on this filesystem");
    }
  }

  /**
   * removeBlocks checks the resolved path of a block like a block fetch does, so a block whose
   * hashed sub-directory was replaced by a link to another directory is not removed there. The
   * rest of the batch is still removed.
   */
  @Test
  public void removeBlocksSkipsBlockResolvingOutsideLocalDirs() throws Exception {
    TestShuffleDataContext context = new TestShuffleDataContext(1, 5);
    context.create();
    File outsideDir = Files.createTempDirectory("outside").toFile();
    try {
      String blockId = "rdd_7_0";
      File subDir = new File(ExecutorDiskUtils.getFilePath(
        context.localDirs, context.subDirsPerLocalDir, blockId)).getParentFile();
      File realFile = null;
      for (int split = 1; realFile == null; split++) {
        File candidate = new File(ExecutorDiskUtils.getFilePath(
          context.localDirs, context.subDirsPerLocalDir, "rdd_7_" + split));
        if (!candidate.getParentFile().equals(subDir)) {
          realFile = candidate;
        }
      }
      Files.writeString(realFile.toPath(), "block", StandardCharsets.UTF_8);
      File outsideFile = new File(outsideDir, blockId);
      Files.writeString(outsideFile.toPath(), "should not be deleted", StandardCharsets.UTF_8);
      JavaUtils.deleteRecursively(subDir);
      createSymbolicLinkOrSkip(subDir, outsideDir);

      ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
      resolver.registerExecutor("app0", "0", context.createExecutorInfo(SORT_MANAGER));

      assertEquals(1,
        resolver.removeBlocks("app0", "0", new String[] { blockId, realFile.getName() }));
      assertTrue(outsideFile.exists(), "A file outside the local directory must not be removed");
      assertFalse(realFile.exists(), "The block after the skipped one should be removed");
    } finally {
      context.cleanup();
      JavaUtils.deleteRecursively(outsideDir);
    }
  }

  /**
   * Executors reloaded from the state store are validated like a registration, and the ones the
   * validator rejects are dropped, also from the state store.
   */
  @Test
  public void reloadDropsExecutorsWithInvalidLocalDirs() throws Exception {
    File root = Files.createTempDirectory("ess-local-root").toFile();
    File dbDir = Files.createTempDirectory("ess-db").toFile();
    try {
      File dbFile = new File(dbDir, "registeredExecutors.rdb");
      File ownDir = new File(root, "spark-x/executor-x/blockmgr-x");
      assertTrue(ownDir.mkdirs(), "precondition: directory created");

      ExternalShuffleBlockResolver writer = new ExternalShuffleBlockResolver(conf, dbFile);
      try {
        writer.registerExecutor("app0", "exec0",
          new ExecutorShuffleInfo(new String[] { ownDir.getPath() }, 64, SORT_MANAGER));
        writer.registerExecutor("app0", "exec1",
          new ExecutorShuffleInfo(new String[] { root.getPath() }, 64, SORT_MANAGER));
      } finally {
        writer.close();
      }

      LocalDirValidator validator = new LocalDirValidator(new String[] { root.getPath() }, false);
      ExternalShuffleBlockResolver reader =
        new ExternalShuffleBlockResolver(conf, dbFile, Runnable::run, validator);
      try {
        AppExecId kept = new AppExecId("app0", "exec0");
        assertEquals(Set.of(kept), reader.executors.keySet());
        assertEquals(Set.of(kept),
          ExternalShuffleBlockResolver.reloadRegisteredExecutors(reader.db).keySet());
      } finally {
        reader.close();
      }
    } finally {
      JavaUtils.deleteRecursively(root);
      JavaUtils.deleteRecursively(dbDir);
    }
  }

  /**
   * With app scoping, the reload checks each executor against its own application, so an executor
   * whose local dir is in another application's directory is dropped, also from the state store.
   */
  @Test
  public void reloadDropsExecutorsWithLocalDirsOfAnotherApplication() throws Exception {
    File root = Files.createTempDirectory("ess-nm-local").toFile();
    File dbDir = Files.createTempDirectory("ess-db").toFile();
    try {
      File dbFile = new File(dbDir, "registeredExecutors.rdb");
      File ownDir = new File(root, "usercache/u/appcache/app1/blockmgr-a");
      File otherAppDir = new File(root, "usercache/u/appcache/app2/blockmgr-b");

      ExternalShuffleBlockResolver writer = new ExternalShuffleBlockResolver(conf, dbFile);
      try {
        writer.registerExecutor("app1", "exec1",
          new ExecutorShuffleInfo(new String[] { ownDir.getPath() }, 64, SORT_MANAGER));
        writer.registerExecutor("app1", "exec2",
          new ExecutorShuffleInfo(new String[] { otherAppDir.getPath() }, 64, SORT_MANAGER));
      } finally {
        writer.close();
      }

      LocalDirValidator validator = new LocalDirValidator(new String[] { root.getPath() }, true);
      ExternalShuffleBlockResolver reader =
        new ExternalShuffleBlockResolver(conf, dbFile, Runnable::run, validator);
      try {
        AppExecId kept = new AppExecId("app1", "exec1");
        assertEquals(Set.of(kept), reader.executors.keySet());
        assertEquals(Set.of(kept),
          ExternalShuffleBlockResolver.reloadRegisteredExecutors(reader.db).keySet());
      } finally {
        reader.close();
      }
    } finally {
      JavaUtils.deleteRecursively(root);
      JavaUtils.deleteRecursively(dbDir);
    }
  }

  /**
   * Creates the file of {@code blockId} in {@code dir}, under the hashed sub-directory name that
   * the block has in {@code localDir}.
   */
  private static File createBlockFileIn(
      File dir, String localDir, int subDirsPerLocalDir, String blockId) throws IOException {
    File blockFile = new File(
      ExecutorDiskUtils.getFilePath(new String[] { localDir }, subDirsPerLocalDir, blockId));
    File file = new File(new File(dir, blockFile.getParentFile().getName()), blockId);
    assertTrue(file.getParentFile().mkdirs(), "precondition: directory created");
    Files.writeString(file.toPath(), "should not be deleted", StandardCharsets.UTF_8);
    return file;
  }

  /**
   * removeBlocks checks a block against the local dir as it was at registration, also when the
   * read-time check is disabled, so a registered dir replaced by a link later is not followed.
   */
  @Test
  public void removeBlocksSkipsReplacedLocalDirWithoutReadVerification() throws Exception {
    TransportConf noReadVerification = new TransportConf("shuffle", new MapConfigProvider(
      Map.of(Constants.SHUFFLE_SERVICE_VERIFY_BLOCK_PATHS_AT_READ, "false")));
    TestShuffleDataContext context = new TestShuffleDataContext(1, 5);
    context.create();
    File outsideDir = Files.createTempDirectory("outside").toFile();
    try {
      String blockId = "rdd_7_0";
      File outsideFile =
        createBlockFileIn(outsideDir, context.localDirs[0], context.subDirsPerLocalDir, blockId);
      ExternalShuffleBlockResolver resolver =
        new ExternalShuffleBlockResolver(noReadVerification, null);
      resolver.registerExecutor("app0", "0", context.createExecutorInfo(SORT_MANAGER));

      // Replace the registered dir itself with a link to another directory.
      File localDir = new File(context.localDirs[0]);
      JavaUtils.deleteRecursively(localDir);
      createSymbolicLinkOrSkip(localDir, outsideDir);

      assertEquals(0, resolver.removeBlocks("app0", "0", new String[] { blockId }));
      assertTrue(outsideFile.exists(), "A file outside the local directory must not be removed");
    } finally {
      context.cleanup();
      JavaUtils.deleteRecursively(outsideDir);
    }
  }

  /**
   * An executor reloaded from the state store gets its local dirs pinned after the validation, so a
   * registered dir replaced by a link after the restart is not followed when a block is removed.
   */
  @Test
  public void removeBlocksSkipsReplacedLocalDirOfReloadedExecutor() throws Exception {
    File root = Files.createTempDirectory("ess-local-root").toFile();
    File outsideDir = Files.createTempDirectory("outside").toFile();
    File dbDir = Files.createTempDirectory("ess-db").toFile();
    try {
      File dbFile = new File(dbDir, "registeredExecutors.rdb");
      File localDir = new File(root, "spark-x/blockmgr-x");
      assertTrue(localDir.mkdirs(), "precondition: directory created");
      String blockId = "rdd_7_0";
      File outsideFile = createBlockFileIn(outsideDir, localDir.getPath(), 64, blockId);

      ExternalShuffleBlockResolver writer = new ExternalShuffleBlockResolver(conf, dbFile);
      try {
        writer.registerExecutor("app0", "exec0",
          new ExecutorShuffleInfo(new String[] { localDir.getPath() }, 64, SORT_MANAGER));
      } finally {
        writer.close();
      }

      LocalDirValidator validator = new LocalDirValidator(new String[] { root.getPath() }, false);
      ExternalShuffleBlockResolver reader =
        new ExternalShuffleBlockResolver(conf, dbFile, Runnable::run, validator);
      try {
        // Replace the registered dir itself with a link to another directory after the restart.
        JavaUtils.deleteRecursively(localDir);
        createSymbolicLinkOrSkip(localDir, outsideDir);

        assertEquals(0, reader.removeBlocks("app0", "exec0", new String[] { blockId }));
        assertTrue(outsideFile.exists(), "A file outside the local directory must not be removed");
      } finally {
        reader.close();
      }
    } finally {
      JavaUtils.deleteRecursively(root);
      JavaUtils.deleteRecursively(outsideDir);
      JavaUtils.deleteRecursively(dbDir);
    }
  }

  /**
   * Only registration and reload pin a local dir. A block removed after its executor's pins were
   * evicted resolves the dir for that check only, and does not pin it again.
   */
  @Test
  public void removeBlocksDoesNotPinEvictedLocalDirs() throws Exception {
    TestShuffleDataContext context = new TestShuffleDataContext(1, 5);
    context.create();
    try {
      String blockId = "rdd_7_0";
      File blockFile = new File(ExecutorDiskUtils.getFilePath(
        context.localDirs, context.subDirsPerLocalDir, blockId));
      Files.writeString(blockFile.toPath(), "block", StandardCharsets.UTF_8);
      ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
      resolver.registerExecutor("app0", "0", context.createExecutorInfo(SORT_MANAGER));
      // As when applicationRemoved evicts them while a removal for the executor is still running.
      resolver.canonicalLocalDirs.clear();

      assertEquals(1, resolver.removeBlocks("app0", "0", new String[] { blockId }));
      assertTrue(resolver.canonicalLocalDirs.isEmpty(), "A block removal must not pin a local dir");
    } finally {
      context.cleanup();
    }
  }

  /**
   * An ExternalBlockHandler built with local-dir roots passes its validator to the block resolver
   * it creates, so the resolver validates the executors it reloads.
   */
  @Test
  public void blockHandlerPassesItsValidatorToTheResolver() throws Exception {
    File root = Files.createTempDirectory("ess-local-root").toFile();
    File dbDir = Files.createTempDirectory("ess-db").toFile();
    try {
      File dbFile = new File(dbDir, "registeredExecutors.rdb");
      File ownDir = new File(root, "spark-x/executor-x/blockmgr-x");
      assertTrue(ownDir.mkdirs(), "precondition: directory created");
      ExternalShuffleBlockResolver writer = new ExternalShuffleBlockResolver(conf, dbFile);
      try {
        writer.registerExecutor("app0", "exec0",
          new ExecutorShuffleInfo(new String[] { ownDir.getPath() }, 64, SORT_MANAGER));
        writer.registerExecutor("app0", "exec1",
          new ExecutorShuffleInfo(new String[] { root.getPath() }, 64, SORT_MANAGER));
      } finally {
        writer.close();
      }

      ExternalBlockHandler handler =
        new ExternalBlockHandler(conf, dbFile, new String[] { root.getPath() }, false);
      try {
        assertEquals(Set.of(new AppExecId("app0", "exec0")),
          handler.getBlockResolver().executors.keySet());
      } finally {
        handler.close();
      }
    } finally {
      JavaUtils.deleteRecursively(root);
      JavaUtils.deleteRecursively(dbDir);
    }
  }

}
