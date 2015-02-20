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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

/**
 * These tests require the Spark assembly to be built before they can be run.
 */
public class SparkLauncherSuite {

  private static final Logger LOG = LoggerFactory.getLogger(SparkLauncherSuite.class);

  private static File dummyPropsFile;

  @BeforeClass
  public static void setUp() throws Exception {
    dummyPropsFile = File.createTempFile("spark", "properties");
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    dummyPropsFile.delete();
  }

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
      .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS,
        "-Dfoo=bar -Dtest.name=-testChildProcLauncher")
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))
      .setMainClass(SparkLauncherTestApp.class.getName())
      .addAppArgs("proc");
    final Process app = launcher.launch();
    new Redirector("stdout", app.getInputStream()).start();
    new Redirector("stderr", app.getErrorStream()).start();
    assertEquals(0, app.waitFor());
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
      .setPropertiesFile(dummyPropsFile.getAbsolutePath())
      .addAppArgs("foo", "bar")
      .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, "/driver")
      .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, "-Ddriver -XX:MaxPermSize=256m")
      .setConf(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH, "/native")
      .setConf("spark.foo", "foo");

    Map<String, String> env = new HashMap<String, String>();
    List<String> cmd = launcher.buildSparkSubmitCommand(env);

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

    for (String arg : cmd) {
      if (arg.startsWith("-XX:MaxPermSize=")) {
        if (isDriver) {
          assertEquals("-XX:MaxPermSize=256m", arg);
        } else {
          assertEquals("-XX:MaxPermSize=128m", arg);
        }
      }
    }

    String[] cp = findArgValue(cmd, "-cp").split(Pattern.quote(File.pathSeparator));
    if (isDriver) {
      assertTrue("Driver classpath should contain provided entry.", contains("/driver", cp));
    } else {
      assertFalse("Driver classpath should not be in command.", contains("/driver", cp));
    }

    String libPath = env.get(CommandBuilderUtils.getLibPathEnvName());
    if (isDriver) {
      assertNotNull("Native library path should be set.", libPath);
      assertTrue("Native library path should contain provided entry.",
        contains("/native", libPath.split(Pattern.quote(File.pathSeparator))));
    } else {
      assertNull("Native library should not be set.", libPath);
    }

    // Checks below are the same for both driver and non-driver mode.
    SparkSubmitOptionParser parser = new SparkSubmitOptionParser();

    assertEquals(dummyPropsFile.getAbsolutePath(), findArgValue(cmd, parser.PROPERTIES_FILE));
    assertEquals("yarn", findArgValue(cmd, parser.MASTER));
    assertEquals(deployMode, findArgValue(cmd, parser.DEPLOY_MODE));
    assertEquals("my.Class", findArgValue(cmd, parser.CLASS));
    assertEquals("MyApp", findArgValue(cmd, parser.NAME));

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

    Map<String, String> conf = parseConf(cmd, parser);
    assertEquals("foo", conf.get("spark.foo"));
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

  private Map<String, String> parseConf(List<String> cmd, SparkSubmitOptionParser parser) {
    Map<String, String> conf = new HashMap<String, String>();
    for (int i = 0; i < cmd.size(); i++) {
      if (cmd.get(i).equals(parser.CONF)) {
        String[] val = cmd.get(i + 1).split("=", 2);
        conf.put(val[0], val[1]);
        i += 1;
      }
    }
    return conf;
  }

  public static class SparkLauncherTestApp {

    public static void main(String[] args) throws Exception {
      assertEquals(1, args.length);
      assertEquals("proc", args[0]);
      assertEquals("bar", System.getProperty("foo"));
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
