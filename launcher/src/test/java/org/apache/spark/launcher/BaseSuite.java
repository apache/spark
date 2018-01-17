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

import java.util.concurrent.TimeUnit;

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

  protected void waitFor(SparkAppHandle handle) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    try {
      while (!handle.getState().isFinal()) {
        assertTrue("Timed out waiting for handle to transition to final state.",
          System.nanoTime() < deadline);
        TimeUnit.MILLISECONDS.sleep(10);
      }
    } finally {
      if (!handle.getState().isFinal()) {
        handle.kill();
      }
    }
  }

}
