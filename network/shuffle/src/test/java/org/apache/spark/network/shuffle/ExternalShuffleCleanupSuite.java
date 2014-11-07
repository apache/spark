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
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExternalShuffleCleanupSuite {

  // Same-thread Executor used to ensure cleanup happens synchronously in test thread.
  Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();

  @Test
  public void noCleanupAndCleanup() throws IOException {
    TestShuffleDataContext dataContext = createSomeData();

    ExternalShuffleBlockManager manager = new ExternalShuffleBlockManager(sameThreadExecutor);
    manager.registerExecutor("app", "exec0", dataContext.createExecutorInfo("shuffleMgr"));
    manager.applicationRemoved("app", false /* cleanup */);

    assertStillThere(dataContext);

    manager.registerExecutor("app", "exec1", dataContext.createExecutorInfo("shuffleMgr"));
    manager.applicationRemoved("app", true /* cleanup */);

    assertCleanedUp(dataContext);
  }

  @Test
  public void cleanupUsesExecutor() throws IOException {
    TestShuffleDataContext dataContext = createSomeData();

    final AtomicBoolean cleanupCalled = new AtomicBoolean(false);

    // Executor which does nothing to ensure we're actually using it.
    Executor noThreadExecutor = new Executor() {
      @Override public void execute(Runnable runnable) { cleanupCalled.set(true); }
    };

    ExternalShuffleBlockManager manager = new ExternalShuffleBlockManager(noThreadExecutor);

    manager.registerExecutor("app", "exec0", dataContext.createExecutorInfo("shuffleMgr"));
    manager.applicationRemoved("app", true);

    assertTrue(cleanupCalled.get());
    assertStillThere(dataContext);

    dataContext.cleanup();
    assertCleanedUp(dataContext);
  }

  @Test
  public void cleanupMultipleExecutors() throws IOException {
    TestShuffleDataContext dataContext0 = createSomeData();
    TestShuffleDataContext dataContext1 = createSomeData();

    ExternalShuffleBlockManager manager = new ExternalShuffleBlockManager(sameThreadExecutor);

    manager.registerExecutor("app", "exec0", dataContext0.createExecutorInfo("shuffleMgr"));
    manager.registerExecutor("app", "exec1", dataContext1.createExecutorInfo("shuffleMgr"));
    manager.applicationRemoved("app", true);

    assertCleanedUp(dataContext0);
    assertCleanedUp(dataContext1);
  }

  @Test
  public void cleanupOnlyRemovedApp() throws IOException {
    TestShuffleDataContext dataContext0 = createSomeData();
    TestShuffleDataContext dataContext1 = createSomeData();

    ExternalShuffleBlockManager manager = new ExternalShuffleBlockManager(sameThreadExecutor);

    manager.registerExecutor("app-0", "exec0", dataContext0.createExecutorInfo("shuffleMgr"));
    manager.registerExecutor("app-1", "exec0", dataContext1.createExecutorInfo("shuffleMgr"));

    manager.applicationRemoved("app-nonexistent", true);
    assertStillThere(dataContext0);
    assertStillThere(dataContext1);

    manager.applicationRemoved("app-0", true);
    assertCleanedUp(dataContext0);
    assertStillThere(dataContext1);

    manager.applicationRemoved("app-1", true);
    assertCleanedUp(dataContext0);
    assertCleanedUp(dataContext1);

    // Make sure it's not an error to cleanup multiple times
    manager.applicationRemoved("app-1", true);
    assertCleanedUp(dataContext0);
    assertCleanedUp(dataContext1);
  }

  private void assertStillThere(TestShuffleDataContext dataContext) {
    for (String localDir : dataContext.localDirs) {
      assertTrue(localDir + " was cleaned up prematurely", new File(localDir).exists());
    }
  }

  private void assertCleanedUp(TestShuffleDataContext dataContext) {
    for (String localDir : dataContext.localDirs) {
      assertFalse(localDir + " wasn't cleaned up", new File(localDir).exists());
    }
  }

  private TestShuffleDataContext createSomeData() throws IOException {
    Random rand = new Random(123);
    TestShuffleDataContext dataContext = new TestShuffleDataContext(10, 5);

    dataContext.create();
    dataContext.insertSortShuffleData(rand.nextInt(1000), rand.nextInt(1000),
      new byte[][] { "ABC".getBytes(), "DEF".getBytes() } );
    dataContext.insertHashShuffleData(rand.nextInt(1000), rand.nextInt(1000) + 1000,
      new byte[][] { "GHI".getBytes(), "JKLMNOPQRSTUVWXYZ".getBytes() } );
    return dataContext;
  }
}
