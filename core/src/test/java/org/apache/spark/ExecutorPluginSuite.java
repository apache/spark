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

package org.apache.spark;

import org.apache.spark.api.java.JavaSparkContext;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExecutorPluginSuite {
  private static final String EXECUTOR_PLUGIN_CONF_NAME = "spark.executor.plugins";
  private static final String testBadPluginName = TestBadShutdownPlugin.class.getName();
  private static final String testPluginName = TestExecutorPlugin.class.getName();
  private static final String testSecondPluginName = TestSecondPlugin.class.getName();

  // Static value modified by testing plugins to ensure plugins loaded correctly.
  public static int numSuccessfulPlugins = 0;

  // Static value modified by testing plugins to verify plugins shut down properly.
  public static int numSuccessfulTerminations = 0;

  private JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = null;
    numSuccessfulPlugins = 0;
    numSuccessfulTerminations = 0;
  }

  @After
  public void tearDown() {
    if (sc != null) {
      sc.stop();
      sc = null;
    }
  }

  private SparkConf initializeSparkConf(String pluginNames) {
    return new SparkConf()
        .setMaster("local")
        .setAppName("test")
        .set(EXECUTOR_PLUGIN_CONF_NAME, pluginNames);
  }

  @Test
  public void testPluginClassDoesNotExist() {
    SparkConf conf = initializeSparkConf("nonexistent.plugin");
    try {
      sc = new JavaSparkContext(conf);
      fail("No exception thrown for nonexistent plugin");
    } catch (Exception e) {
      // We cannot catch ClassNotFoundException directly because Java doesn't think it'll be thrown
      assertTrue(e.toString().startsWith("java.lang.ClassNotFoundException"));
    }
  }

  @Test
  public void testAddPlugin() throws InterruptedException {
    // Load the sample TestExecutorPlugin, which will change the value of numSuccessfulPlugins
    SparkConf conf = initializeSparkConf(testPluginName);
    sc = new JavaSparkContext(conf);
    assertEquals(1, numSuccessfulPlugins);
    sc.stop();
    sc = null;
    assertEquals(1, numSuccessfulTerminations);
  }

  @Test
  public void testAddMultiplePlugins() throws InterruptedException {
    // Load two plugins and verify they both execute.
    SparkConf conf = initializeSparkConf(testPluginName + "," + testSecondPluginName);
    sc = new JavaSparkContext(conf);
    assertEquals(2, numSuccessfulPlugins);
    sc.stop();
    sc = null;
    assertEquals(2, numSuccessfulTerminations);
  }

  @Test
  public void testPluginShutdownWithException() {
    // Verify an exception in one plugin shutdown does not affect the others
    String pluginNames = testPluginName + "," + testBadPluginName + "," + testPluginName;
    SparkConf conf = initializeSparkConf(pluginNames);
    sc = new JavaSparkContext(conf);
    assertEquals(3, numSuccessfulPlugins);
    sc.stop();
    sc = null;
    assertEquals(2, numSuccessfulTerminations);
  }

  public static class TestExecutorPlugin implements ExecutorPlugin {
    public void init() {
      ExecutorPluginSuite.numSuccessfulPlugins++;
    }

    public void shutdown() {
      ExecutorPluginSuite.numSuccessfulTerminations++;
    }
  }

  public static class TestSecondPlugin implements ExecutorPlugin {
    public void init() {
      ExecutorPluginSuite.numSuccessfulPlugins++;
    }

    public void shutdown() {
      ExecutorPluginSuite.numSuccessfulTerminations++;
    }
  }

  public static class TestBadShutdownPlugin implements ExecutorPlugin {
    public void init() {
      ExecutorPluginSuite.numSuccessfulPlugins++;
    }

    public void shutdown() {
      throw new RuntimeException("This plugin will fail to cleanly shut down");
    }
  }
}
