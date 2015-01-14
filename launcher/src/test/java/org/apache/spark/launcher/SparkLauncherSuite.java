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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

/**
 * These tests require the Spark assembly to be built before they can be run.
 *
 * TODO: these tests do not work on Win32.
 */
public class SparkLauncherSuite {

  private static final Logger LOG = LoggerFactory.getLogger(SparkLauncherSuite.class);

  @Test
  public void testDriverCmdBuilder() throws Exception {
    testCmdBuilder(true);
  }

  @Test
  public void testClusterCmdBuilder() throws Exception {
    testCmdBuilder(false);
  }

  @Test
  public void testChildProcLauncher() throws Exception {
    SparkLauncher launcher = new SparkLauncher()
      .setSparkHome(System.getProperty("spark.test.home"))
      .setMaster("local")
      .setAppResource("spark-internal")
      .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, "-Dfoo=bar -Dtest.name=-testChildProcLauncher")
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))
      .setMainClass(SparkLauncherTestApp.class.getName())
      .addAppArgs("proc");
    printArgs(launcher.buildLauncherCommand());
    final Process app = launcher.launch();
    new Redirector("stdout", app.getInputStream()).start();
    new Redirector("stderr", app.getErrorStream()).start();
    assertEquals(0, app.waitFor());
  }

  @Test
  public void testThreadAppLauncher() throws Exception {
    // Do this to avoid overwriting the main test log file.
    System.setProperty("test.name", "-testThreadAppLauncher");

    SparkLauncher launcher = new SparkLauncher()
      .setSparkHome(System.getProperty("spark.test.home"))
      .setMaster("local")
      .setAppResource("spark-internal")
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))
      .setMainClass(SparkLauncherTestApp.class.getName())
      .addAppArgs("thread");

    printArgs(launcher.buildLauncherCommand());

    Thread app = launcher.start(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        String msg = "Uncaught exception in app.";
        LOG.error(msg, e);
        fail(msg);
      }
    }, true);
    app.join();
  }

  @Test
  public void testInProcessDriverArgValidator() throws Exception {
    testInvalidDriverConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS);
    testInvalidDriverConf(SparkLauncher.DRIVER_MEMORY);
    testInvalidDriverConf(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH);
  }

  private void testCmdBuilder(boolean isDriver) throws Exception {
    String deployMode = isDriver ? "client" : "cluster";

    SparkLauncher launcher = new SparkLauncher()
      .setSparkHome(System.getProperty("spark.test.home"))
      .setMaster("yarn")
      .setDeployMode(deployMode)
      .setAppResource("/foo")
      .setAppName("MyApp")
      .setMainClass("my.Class")
      .addAppArgs("foo", "bar")
      .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, "/driver")
      .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, "-Ddriver")
      .setConf(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH, "/native")
      .setConf("spark.foo", "foo");

    List<String> cmd = launcher.buildLauncherCommand();

    // Checks below are different for driver and non-driver mode.

    if (isDriver) {
      assertTrue("Driver -Xms should be configured.", contains("-Xms1g", cmd));
      assertTrue("Driver -Xmx should be configured.", contains("-Xmx1g", cmd));
    } else {
      boolean found = false;
      for (String arg : cmd) {
        if (arg.startsWith("-Xms") || arg.startsWith("-Xmx")) {
          found = true;
          break;
        }
      }
      assertFalse("Memory arguments should not be set.", found);
    }

    String[] cp = findArgValue(cmd, "-cp").split(Pattern.quote(File.pathSeparator));
    if (isDriver) {
      assertTrue("Driver classpath should contain provided entry.", contains("/driver", cp));
    } else {
      assertFalse("Driver classpath should not be in command.", contains("/driver", cp));
    }

    String[] libPath = null;
    String envName = launcher.getLibPathEnvName() + "=";
    for (String arg : cmd) {
      if (arg.startsWith(envName)) {
        libPath = arg.substring(envName.length()).split(Pattern.quote(File.pathSeparator));
        break;
      }
    }
    if (isDriver) {
      assertNotNull("Native library path should be set.", libPath);
      assertTrue("Native library path should contain provided entry.", contains("/native", libPath));
    } else {
      assertNull("Native library should not be set.", libPath);
    }

    // Checks below are the same for both driver and non-driver mode.

    assertEquals("yarn", findArgValue(cmd, "--master"));
    assertEquals(deployMode, findArgValue(cmd, "--deploy-mode"));
    assertEquals("my.Class", findArgValue(cmd, "--class"));
    assertEquals("MyApp", findArgValue(cmd, "--name"));

    boolean appArgsOk = false;
    for (int i = 0; i < cmd.size(); i++) {
      if (cmd.get(i).equals("/foo")) {
        assertEquals("foo", cmd.get(i + 1));
        assertEquals("bar", cmd.get(i + 2));
        assertEquals(cmd.size(), i + 3);
        appArgsOk = true;
        break;
      }
    }
    assertTrue("App resource and args should be added to command.", appArgsOk);

    Map<String, String> conf = parseConf(cmd);
    assertEquals("foo", conf.get("spark.foo"));
  }

  private void testInvalidDriverConf(String key) throws Exception {
    try {
      new SparkLauncher()
        .setSparkHome(System.getProperty("spark.test.home"))
        .setAppResource("spark-internal")
        .setMainClass(SparkLauncherTestApp.class.getName())
        .addAppArgs("thread")
        .setConf(key, "foo")
        .start(null, true);
      fail("Should have failed to start app.");
    } catch (IllegalStateException e) {
      assertTrue("Correct exception should be thrown.",
        e.getMessage().indexOf("running in-process") > 0);
    }
  }

  private String findArgValue(List<String> cmd, String name) {
    for (int i = 0; i < cmd.size(); i++) {
      if (cmd.get(i).equals(name)) {
        return cmd.get(i + 1);
      }
    }
    fail(String.format("arg '%s' not found", name));
    return null;
  }

  private boolean contains(String needle, String[] haystack) {
    for (String entry : haystack) {
      if (entry.equals(needle)) {
        return true;
      }
    }
    return false;
  }

  private boolean contains(String needle, Iterable<String> haystack) {
    for (String entry : haystack) {
      if (entry.equals(needle)) {
        return true;
      }
    }
    return false;
  }

  private Map<String, String> parseConf(List<String> cmd) {
    Map<String, String> conf = new HashMap<String, String>();
    for (int i = 0; i < cmd.size(); i++) {
      if (cmd.get(i).equals("--conf")) {
        String[] val = cmd.get(i + 1).split("=", 2);
        conf.put(val[0], val[1]);
        i += 1;
      }
    }
    return conf;
  }

  private void printArgs(List<String> cmd) {
    StringBuilder cmdLine = new StringBuilder();
    for (String arg : cmd) {
      if (cmdLine.length() > 0) {
        cmdLine.append(" ");
      }
      cmdLine.append("[").append(arg).append("]");
    }
    LOG.info("Launching SparkSubmit with args: {}", cmdLine.toString());
  }

  public static class SparkLauncherTestApp {

    public static void main(String[] args) throws Exception {
      if (args[0].equals("proc")) {
        assertEquals("bar", System.getProperty("foo"));
      } else if (args[0].equals("arg")) {
        assertEquals("newline=", args[1]);
      } else {
        assertEquals("thread", args[0]);
      }
      assertEquals("local", System.getProperty(SparkLauncher.SPARK_MASTER));
    }

  }

  private static class Redirector extends Thread {

    private final InputStream in;

    Redirector(String name, InputStream in) {
      this.in = in;
      setName(name);
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        String line;
        while ((line = reader.readLine()) != null) {
          LOG.warn(line);
        }
      } catch (Exception e) {
        LOG.error("Error reading process output.", e);
      }
    }

  }

}
