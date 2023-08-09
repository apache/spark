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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.internal.config.package$;
import org.apache.spark.util.Utils;

/**
 * These tests require the Spark assembly to be built before they can be run.
 */
public class SparkLauncherSuite extends BaseSuite {

  private static final NamedThreadFactory TF = new NamedThreadFactory("SparkLauncherSuite-%d");
  private static final String EXCEPTION_MESSAGE = "dummy-exception";
  private static final RuntimeException DUMMY_EXCEPTION = new RuntimeException(EXCEPTION_MESSAGE);

  private final SparkLauncher launcher = new SparkLauncher();

  @Test
  public void testSparkArgumentHandling() throws Exception {
    SparkSubmitOptionParser opts = new SparkSubmitOptionParser();

    launcher.addSparkArg(opts.HELP);
    assertThrows(IllegalArgumentException.class, () -> launcher.addSparkArg(opts.PROXY_USER));

    launcher.addSparkArg(opts.PROXY_USER, "someUser");
    assertThrows(IllegalArgumentException.class,
      () -> launcher.addSparkArg(opts.HELP, "someValue"));

    launcher.addSparkArg("--future-argument");
    launcher.addSparkArg("--future-argument", "someValue");

    launcher.addSparkArg(opts.MASTER, "myMaster");
    assertEquals("myMaster", launcher.builder.master);

    launcher.addJar("foo");
    launcher.addSparkArg(opts.JARS, "bar");
    assertEquals(Arrays.asList("bar"), launcher.builder.jars);

    launcher.addFile("foo");
    launcher.addSparkArg(opts.FILES, "bar");
    assertEquals(Arrays.asList("bar"), launcher.builder.files);

    launcher.addPyFile("foo");
    launcher.addSparkArg(opts.PY_FILES, "bar");
    assertEquals(Arrays.asList("bar"), launcher.builder.pyFiles);

    launcher.setConf("spark.foo", "foo");
    launcher.addSparkArg(opts.CONF, "spark.foo=bar");
    assertEquals("bar", launcher.builder.conf.get("spark.foo"));

    launcher.setConf(SparkLauncher.PYSPARK_DRIVER_PYTHON, "python3.4");
    launcher.setConf(SparkLauncher.PYSPARK_PYTHON, "python3.5");
    assertEquals("python3.4", launcher.builder.conf.get(
      package$.MODULE$.PYSPARK_DRIVER_PYTHON().key()));
    assertEquals("python3.5", launcher.builder.conf.get(package$.MODULE$.PYSPARK_PYTHON().key()));
  }

  @Test
  public void testChildProcLauncher() throws Exception {
    // This test is failed on Windows due to the failure of initiating executors
    // by the path length limitation. See SPARK-18718.
    assumeTrue(!Utils.isWindows());

    SparkSubmitOptionParser opts = new SparkSubmitOptionParser();
    Map<String, String> env = new HashMap<>();
    env.put("SPARK_PRINT_LAUNCH_COMMAND", "1");

    launcher
      .setMaster("local")
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .addSparkArg(opts.CONF,
        String.format("%s=-Dfoo=ShouldBeOverriddenBelow", SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS))
      .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS,
        "-Dfoo=bar -Dtest.appender=console")
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))
      .addSparkArg(opts.CLASS, "ShouldBeOverriddenBelow")
      .setMainClass(SparkLauncherTestApp.class.getName())
      .redirectError()
      .addAppArgs("proc");
    final Process app = launcher.launch();

    new OutputRedirector(app.getInputStream(), getClass().getName() + ".child", TF);
    assertEquals(0, app.waitFor());
  }

  @Test
  public void testInProcessLauncher() throws Exception {
    // Because this test runs SparkLauncher in process and in client mode, it pollutes the system
    // properties, and that can cause test failures down the test pipeline. So restore the original
    // system properties after this test runs.
    Map<Object, Object> properties = new HashMap<>(System.getProperties());
    try {
      inProcessLauncherTestImpl();
    } finally {
      restoreSystemProperties(properties);
      waitForSparkContextShutdown();
    }
  }

  private void inProcessLauncherTestImpl() throws Exception {
    final List<SparkAppHandle.State> transitions = new ArrayList<>();
    SparkAppHandle.Listener listener = mock(SparkAppHandle.Listener.class);
    doAnswer(invocation -> {
      SparkAppHandle h = (SparkAppHandle) invocation.getArguments()[0];
      synchronized (transitions) {
        transitions.add(h.getState());
      }
      return null;
    }).when(listener).stateChanged(any(SparkAppHandle.class));

    SparkAppHandle handle = null;
    try {
      synchronized (InProcessTestApp.LOCK) {
        handle = new InProcessLauncher()
          .setMaster("local")
          .setAppResource(SparkLauncher.NO_RESOURCE)
          .setMainClass(InProcessTestApp.class.getName())
          .addAppArgs("hello")
          .startApplication(listener);

        // SPARK-23020: see doc for InProcessTestApp.LOCK for a description of the race. Here
        // we wait until we know that the connection between the app and the launcher has been
        // established before allowing the app to finish.
        final SparkAppHandle _handle = handle;
        eventually(Duration.ofSeconds(5), Duration.ofMillis(10), () -> {
          assertNotEquals(SparkAppHandle.State.UNKNOWN, _handle.getState());
        });

        InProcessTestApp.LOCK.wait(5000);
      }

      waitFor(handle);
      assertEquals(SparkAppHandle.State.FINISHED, handle.getState());

      // Matches the behavior of LocalSchedulerBackend.
      List<SparkAppHandle.State> expected = Arrays.asList(
        SparkAppHandle.State.CONNECTED,
        SparkAppHandle.State.RUNNING,
        SparkAppHandle.State.FINISHED);
      assertEquals(expected, transitions);
    } finally {
      if (handle != null) {
        handle.kill();
      }
    }
  }

  @Test
  public void testInProcessLauncherDoesNotKillJvm() throws Exception {
    SparkSubmitOptionParser opts = new SparkSubmitOptionParser();
    List<String[]> wrongArgs = Arrays.asList(
      new String[] { "--unknown" },
      new String[] { opts.DEPLOY_MODE, "invalid" });

    for (String[] args : wrongArgs) {
      InProcessLauncher launcher = new InProcessLauncher()
        .setAppResource(SparkLauncher.NO_RESOURCE);
      switch (args.length) {
        case 2:
          launcher.addSparkArg(args[0], args[1]);
          break;

        case 1:
          launcher.addSparkArg(args[0]);
          break;

        default:
          fail("FIXME: invalid test.");
      }

      SparkAppHandle handle = launcher.startApplication();
      waitFor(handle);
      assertEquals(SparkAppHandle.State.FAILED, handle.getState());
    }

    // Run --version, which is useless as a use case, but should succeed and not exit the JVM.
    // The expected state is "LOST" since "--version" doesn't report state back to the handle.
    SparkAppHandle handle = new InProcessLauncher().addSparkArg(opts.VERSION).startApplication();
    waitFor(handle);
    assertEquals(SparkAppHandle.State.LOST, handle.getState());
  }

  @Test
  public void testInProcessLauncherGetError() throws Exception {
    // Because this test runs SparkLauncher in process and in client mode, it pollutes the system
    // properties, and that can cause test failures down the test pipeline. So restore the original
    // system properties after this test runs.
    Map<Object, Object> properties = new HashMap<>(System.getProperties());

    SparkAppHandle handle = null;
    try {
      handle = new InProcessLauncher()
        .setMaster("local")
        .setAppResource(SparkLauncher.NO_RESOURCE)
        .setMainClass(ErrorInProcessTestApp.class.getName())
        .addAppArgs("hello")
        .startApplication();

      final SparkAppHandle _handle = handle;
      eventually(Duration.ofSeconds(60), Duration.ofMillis(1000), () -> {
        assertEquals(SparkAppHandle.State.FAILED, _handle.getState());
      });

      assertNotNull(handle.getError());
      assertTrue(handle.getError().isPresent());
      assertSame(handle.getError().get(), DUMMY_EXCEPTION);
    } finally {
      if (handle != null) {
        handle.kill();
      }
      restoreSystemProperties(properties);
      waitForSparkContextShutdown();
    }
  }

  @Test
  public void testSparkLauncherGetError() throws Exception {
    SparkAppHandle handle = null;
    try {
      handle = new SparkLauncher()
        .setMaster("local")
        .setAppResource(SparkLauncher.NO_RESOURCE)
        .setMainClass(ErrorInProcessTestApp.class.getName())
        .addAppArgs("hello")
        .startApplication();

      final SparkAppHandle _handle = handle;
      eventually(Duration.ofSeconds(60), Duration.ofMillis(1000), () -> {
        assertEquals(SparkAppHandle.State.FAILED, _handle.getState());
      });

      assertNotNull(handle.getError());
      assertTrue(handle.getError().isPresent());
      assertTrue(handle.getError().get().getMessage().contains(EXCEPTION_MESSAGE));
    } finally {
      if (handle != null) {
        handle.kill();
      }
    }
  }

  private void restoreSystemProperties(Map<Object, Object> properties) {
    Properties p = new Properties();
    p.putAll(properties);
    System.setProperties(p);
  }

  private void waitForSparkContextShutdown() throws Exception {
    // Here DAGScheduler is stopped, while SparkContext.clearActiveContext may not be called yet.
    // Wait for a reasonable amount of time to avoid creating two active SparkContext in JVM.
    // See SPARK-23019 and SparkContext.stop() for details.
    eventually(Duration.ofSeconds(5), Duration.ofMillis(10), () -> {
      assertTrue("SparkContext is still alive.", SparkContext$.MODULE$.getActive().isEmpty());
    });
  }

  public static class SparkLauncherTestApp {

    public static void main(String[] args) throws Exception {
      assertEquals(1, args.length);
      assertEquals("proc", args[0]);
      assertEquals("bar", System.getProperty("foo"));
      assertEquals("local", System.getProperty(SparkLauncher.SPARK_MASTER));
    }

  }

  public static class InProcessTestApp {

    /**
     * SPARK-23020: there's a race caused by a child app finishing too quickly. This would cause
     * the InProcessAppHandle to dispose of itself even before the child connection was properly
     * established, so no state changes would be detected for the application and its final
     * state would be LOST.
     *
     * It's not really possible to fix that race safely in the handle code itself without changing
     * the way in-process apps talk to the launcher library, so we work around that in the test by
     * synchronizing on this object.
     */
    public static final Object LOCK = new Object();

    public static void main(String[] args) throws Exception {
      assertNotEquals(0, args.length);
      assertEquals("hello", args[0]);
      new SparkContext().stop();

      synchronized (LOCK) {
        LOCK.notifyAll();
      }
    }

  }

  /**
   * Similar to {@link InProcessTestApp} except it throws an exception
   */
  public static class ErrorInProcessTestApp {

    public static void main(String[] args) {
      assertNotEquals(0, args.length);
      assertEquals("hello", args[0]);
      throw DUMMY_EXCEPTION;
    }
  }
}
