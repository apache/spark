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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class InProcessLauncherSuite extends BaseSuite {

  // Arguments passed to the test class to identify the test being run.
  private static final String TEST_SUCCESS = "success";
  private static final String TEST_FAILURE = "failure";
  private static final String TEST_KILL = "kill";

  private static final String TEST_FAILURE_MESSAGE = "d'oh";

  private static Throwable lastError;

  @Before
  public void testSetup() {
    lastError = null;
  }

  @Test
  public void testLauncher() throws Exception {
    SparkAppHandle app = startTest(TEST_SUCCESS);
    waitFor(app);
    assertNull(lastError);

    // Because the test doesn't implement the launcher protocol, the final state here will be
    // LOST instead of FINISHED.
    assertEquals(SparkAppHandle.State.LOST, app.getState());
  }

  @Test
  public void testKill() throws Exception {
    SparkAppHandle app = startTest(TEST_KILL);
    app.kill();
    waitFor(app);
    assertNull(lastError);
    assertEquals(SparkAppHandle.State.KILLED, app.getState());
  }

  @Test
  public void testErrorPropagation() throws Exception {
    SparkAppHandle app = startTest(TEST_FAILURE);
    waitFor(app);
    assertEquals(SparkAppHandle.State.FAILED, app.getState());

    assertNotNull(lastError);
    assertEquals(TEST_FAILURE_MESSAGE, lastError.getMessage());
  }

  private SparkAppHandle startTest(String test) throws Exception {
    return new TestInProcessLauncher()
      .addAppArgs(test)
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .startApplication();
  }

  public static void runTest(String[] args) {
    try {
      assertTrue(args.length != 0);

      // Make sure at least the launcher-provided config options are in the args array.
      final AtomicReference<String> port = new AtomicReference<>();
      final AtomicReference<String> secret = new AtomicReference<>();
      SparkSubmitOptionParser parser = new SparkSubmitOptionParser() {

        @Override
        protected boolean handle(String opt, String value) {
          if (opt == CONF) {
            String[] conf = value.split("=");
            switch(conf[0]) {
              case LauncherProtocol.CONF_LAUNCHER_PORT:
                port.set(conf[1]);
                break;

              case LauncherProtocol.CONF_LAUNCHER_SECRET:
                secret.set(conf[1]);
                break;

              default:
                // no op
            }
          }

          return true;
        }

        @Override
        protected boolean handleUnknown(String opt) {
          return true;
        }

        @Override
        protected void handleExtraArgs(List<String> extra) {
          // no op.
        }

      };

      parser.parse(Arrays.asList(args));
      assertNotNull("Launcher port not found.", port.get());
      assertNotNull("Launcher secret not found.", secret.get());

      String test = args[args.length - 1];
      switch (test) {
      case TEST_SUCCESS:
        break;

      case TEST_FAILURE:
        throw new IllegalStateException(TEST_FAILURE_MESSAGE);

      case TEST_KILL:
        try {
          // Wait for a reasonable amount of time to avoid the test hanging forever on failure,
          // but still allowing for time outs to hopefully not occur on busy machines.
          Thread.sleep(10000);
          fail("Did not get expected interrupt after 10s.");
        } catch (InterruptedException ie) {
          // Expected.
        }
        break;

      default:
        fail("Unknown test " + test);
      }
    } catch (Throwable t) {
      lastError = t;
      throw new RuntimeException(t);
    }
  }

  private static class TestInProcessLauncher extends InProcessLauncher {

    @Override
    Method findSparkSubmit() throws IOException {
      try {
        return InProcessLauncherSuite.class.getMethod("runTest", String[].class);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

  }

}
