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
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class CleanupNonShuffleServiceServedFilesSuite {

  // Same-thread Executor used to ensure cleanup happens synchronously in test thread.
  private Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();
  private TransportConf conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
  private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";

  @Test
  public void cleanupOnRemovedExecutorWithFilesToKeep() throws IOException {
    cleanupOnRemovedExecutor(true);
  }

  @Test
  public void cleanupOnRemovedExecutorWithoutFilesToKeep() throws IOException {
    cleanupOnRemovedExecutor(false);
  }

  private void cleanupOnRemovedExecutor(boolean withFilesToKeep) throws IOException {
    TestShuffleDataContext dataContext = initDataContext(withFilesToKeep);

    ExternalShuffleBlockResolver resolver =
      new ExternalShuffleBlockResolver(conf, null, sameThreadExecutor);
    resolver.registerExecutor("app", "exec0", dataContext.createExecutorInfo(SORT_MANAGER));
    resolver.executorRemoved("exec0", "app");

    assertCleanedUp(dataContext);
  }

  @Test
  public void cleanupUsesExecutorWithFilesToKeep() throws IOException {
    cleanupUsesExecutor(true);
  }

  @Test
  public void cleanupUsesExecutorWithoutFilesToKeep() throws IOException {
    cleanupUsesExecutor(false);
  }

  private void cleanupUsesExecutor(boolean withFilesToKeep) throws IOException {
    TestShuffleDataContext dataContext = initDataContext(withFilesToKeep);

    AtomicBoolean cleanupCalled = new AtomicBoolean(false);

    // Executor which does nothing to ensure we're actually using it.
    Executor noThreadExecutor = runnable -> cleanupCalled.set(true);

    ExternalShuffleBlockResolver manager =
      new ExternalShuffleBlockResolver(conf, null, noThreadExecutor);

    manager.registerExecutor("app", "exec0", dataContext.createExecutorInfo(SORT_MANAGER));
    manager.executorRemoved("exec0", "app");

    assertTrue(cleanupCalled.get());
    assertStillThere(dataContext);
  }

  @Test
  public void cleanupOnlyRemovedExecutorWithFilesToKeep() throws IOException {
    cleanupOnlyRemovedExecutor(true);
  }

  @Test
  public void cleanupOnlyRemovedExecutorWithoutFilesToKeep() throws IOException {
    cleanupOnlyRemovedExecutor(false);
  }

  private void cleanupOnlyRemovedExecutor(boolean withFilesToKeep) throws IOException {
    TestShuffleDataContext dataContext0 = initDataContext(withFilesToKeep);
    TestShuffleDataContext dataContext1 = initDataContext(withFilesToKeep);

    ExternalShuffleBlockResolver resolver =
      new ExternalShuffleBlockResolver(conf, null, sameThreadExecutor);
    resolver.registerExecutor("app", "exec0", dataContext0.createExecutorInfo(SORT_MANAGER));
    resolver.registerExecutor("app", "exec1", dataContext1.createExecutorInfo(SORT_MANAGER));


    resolver.executorRemoved("exec-nonexistent", "app");
    assertStillThere(dataContext0);
    assertStillThere(dataContext1);

    resolver.executorRemoved("exec0", "app");
    assertCleanedUp(dataContext0);
    assertStillThere(dataContext1);

    resolver.executorRemoved("exec1", "app");
    assertCleanedUp(dataContext0);
    assertCleanedUp(dataContext1);

    // Make sure it's not an error to cleanup multiple times
    resolver.executorRemoved("exec1", "app");
    assertCleanedUp(dataContext0);
    assertCleanedUp(dataContext1);
  }

  @Test
  public void cleanupOnlyRegisteredExecutorWithFilesToKeep() throws IOException {
    cleanupOnlyRegisteredExecutor(true);
  }

  @Test
  public void cleanupOnlyRegisteredExecutorWithoutFilesToKeep() throws IOException {
    cleanupOnlyRegisteredExecutor(false);
  }

  private void cleanupOnlyRegisteredExecutor(boolean withFilesToKeep) throws IOException {
    TestShuffleDataContext dataContext = initDataContext(withFilesToKeep);

    ExternalShuffleBlockResolver resolver =
      new ExternalShuffleBlockResolver(conf, null, sameThreadExecutor);
    resolver.registerExecutor("app", "exec0", dataContext.createExecutorInfo(SORT_MANAGER));

    resolver.executorRemoved("exec1", "app");
    assertStillThere(dataContext);

    resolver.executorRemoved("exec0", "app");
    assertCleanedUp(dataContext);
  }

  private static void assertStillThere(TestShuffleDataContext dataContext) {
    for (String localDir : dataContext.localDirs) {
      assertTrue(localDir + " was cleaned up prematurely", new File(localDir).exists());
    }
  }

  private static FilenameFilter filter = (dir, name) -> {
    // Don't delete shuffle data or shuffle index files.
    return !name.endsWith(".index") && !name.endsWith(".data") && !name.startsWith("rdd_");
  };

  private static boolean assertOnlyFilesToKeepInDir(File[] dirs) {
    for (File dir : dirs) {
      assertTrue(dir.getName() + " wasn't cleaned up", !dir.exists() ||
        dir.listFiles(filter).length == 0 || assertOnlyFilesToKeepInDir(dir.listFiles()));
    }
    return true;
  }

  private static void assertCleanedUp(TestShuffleDataContext dataContext) {
    for (String localDir : dataContext.localDirs) {
      File[] dirs = new File[] {new File(localDir)};
      assertOnlyFilesToKeepInDir(dirs);
    }
  }

  private static TestShuffleDataContext initDataContext(boolean withFilesToKeep)
    throws IOException {
    TestShuffleDataContext dataContext = new TestShuffleDataContext(10, 5);
    dataContext.create();
    if (withFilesToKeep) {
      createFilesToKeep(dataContext);
    } else {
      createRemovableTestFiles(dataContext);
    }
    return dataContext;
  }

  private static void createFilesToKeep(TestShuffleDataContext dataContext) throws IOException {
    Random rand = new Random(123);
    dataContext.insertSortShuffleData(rand.nextInt(1000), rand.nextInt(1000), new byte[][] {
        "ABC".getBytes(StandardCharsets.UTF_8),
        "DEF".getBytes(StandardCharsets.UTF_8)});
    dataContext.insertCachedRddData();
  }

  private static void createRemovableTestFiles(TestShuffleDataContext dataContext) throws IOException {
    dataContext.insertSpillData();
    dataContext.insertBroadcastData();
    dataContext.insertTempShuffleData();
  }
}
