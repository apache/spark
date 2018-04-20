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

package org.apache.spark.launcher;

import java.time.Duration;

import org.junit.After;
import org.slf4j.bridge.SLF4JBridgeHandler;
import static org.junit.Assert.*;

/**
 * Handles configuring the JUL -> SLF4J bridge, and provides some utility methods for tests.
 */
class BaseSuite {

  static {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  @After
  public void postChecks() {
    LauncherServer server = LauncherServer.getServer();
    if (server != null) {
      // Shut down the server to clean things up for the next test.
      try {
        server.close();
      } catch (Exception e) {
        // Ignore.
      }
    }
    assertNull(server);
  }

  protected void waitFor(final SparkAppHandle handle) throws Exception {
    try {
      eventually(Duration.ofSeconds(10), Duration.ofMillis(10), () -> {
        assertTrue("Handle is not in final state.", handle.getState().isFinal());
      });
    } finally {
      if (!handle.getState().isFinal()) {
        handle.kill();
      }
    }

    // Wait until the handle has been marked as disposed, to make sure all cleanup tasks
    // have been performed.
    AbstractAppHandle ahandle = (AbstractAppHandle) handle;
    eventually(Duration.ofSeconds(10), Duration.ofMillis(10), () -> {
      assertTrue("Handle is still not marked as disposed.", ahandle.isDisposed());
    });
  }

  /**
   * Call a closure that performs a check every "period" until it succeeds, or the timeout
   * elapses.
   */
  protected void eventually(Duration timeout, Duration period, Runnable check) throws Exception {
    assertTrue("Timeout needs to be larger than period.", timeout.compareTo(period) > 0);
    long deadline = System.nanoTime() + timeout.toNanos();
    int count = 0;
    while (true) {
      try {
        count++;
        check.run();
        return;
      } catch (Throwable t) {
        if (System.nanoTime() >= deadline) {
          String msg = String.format("Failed check after %d tries: %s.", count, t.getMessage());
          throw new IllegalStateException(msg, t);
        }
        Thread.sleep(period.toMillis());
      }
    }
  }

}
