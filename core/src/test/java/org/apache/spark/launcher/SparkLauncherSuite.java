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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

import org.apache.spark.SparkContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.util.Utils;

/**
 * These tests require the Spark assembly to be built before they can be run.
 */
public class SparkLauncherSuite extends BaseSuite {

  private static final NamedThreadFactory TF = new NamedThreadFactory("SparkLauncherSuite-%d");

  private final SparkLauncher launcher = new SparkLauncher();

  @Test
  public void testSparkArgumentHandling() throws Exception {
    SparkSubmitOptionParser opts = new SparkSubmitOptionParser();

    launcher.addSparkArg(opts.HELP);
    try {
      launcher.addSparkArg(opts.PROXY_USER);
      fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // Expected.
    }

    launcher.addSparkArg(opts.PROXY_USER, "someUser");
    try {
      launcher.addSparkArg(opts.HELP, "someValue");
      fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // Expected.
    }

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
        "-Dfoo=bar -Dtest.appender=childproc")
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))
      .addSparkArg(opts.CLASS, "ShouldBeOverriddenBelow")
      .setMainClass(SparkLauncherTestApp.class.getName())
      .redirectError()
      .addAppArgs("proc");
    final Process app = launcher.launch();

    new OutputRedirector(app.getInputStream(), getClass().getName() + ".child", TF);
    assertEquals(0, app.waitFor());
  }

  // TODO: [SPARK-23020] Re-enable this
  @Ignore
  public void testInProcessLauncher() throws Exception {
    // Because this test runs SparkLauncher in process and in client mode, it pollutes the system
    // properties, and that can cause test failures down the test pipeline. So restore the original
    // system properties after this test runs.
    Map<Object, Object> properties = new HashMap<>(System.getProperties());
    try {
      inProcessLauncherTestImpl();
    } finally {
      Properties p = new Properties();
      for (Map.Entry<Object, Object> e : properties.entrySet()) {
        p.put(e.getKey(), e.getValue());
      }
      System.setProperties(p);
      // Here DAGScheduler is stopped, while SparkContext.clearActiveContext may not be called yet.
      // Wait for a reasonable amount of time to avoid creating two active SparkContext in JVM.
      // See SPARK-23019 and SparkContext.stop() for details.
      TimeUnit.MILLISECONDS.sleep(500);
    }
  }

  private void inProcessLauncherTestImpl() throws Exception {
    final List<SparkAppHandle.State> transitions = new ArrayList<>();
    SparkAppHandle.Listener listener = mock(SparkAppHandle.Listener.class);
    doAnswer(invocation -> {
      SparkAppHandle h = (SparkAppHandle) invocation.getArguments()[0];
      transitions.add(h.getState());
      return null;
    }).when(listener).stateChanged(any(SparkAppHandle.class));

    SparkAppHandle handle = new InProcessLauncher()
      .setMaster("local")
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .setMainClass(InProcessTestApp.class.getName())
      .addAppArgs("hello")
      .startApplication(listener);

    waitFor(handle);
    assertEquals(SparkAppHandle.State.FINISHED, handle.getState());

    // Matches the behavior of LocalSchedulerBackend.
    List<SparkAppHandle.State> expected = Arrays.asList(
      SparkAppHandle.State.CONNECTED,
      SparkAppHandle.State.RUNNING,
      SparkAppHandle.State.FINISHED);
    assertEquals(expected, transitions);
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

    public static void main(String[] args) throws Exception {
      assertNotEquals(0, args.length);
      assertEquals(args[0], "hello");
      new SparkContext().stop();
    }

  }

}
