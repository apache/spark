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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

/**
 * These tests require the Spark assembly to be built before they can be run.
 */
public class SparkLauncherSuite extends BaseSuite {

  private static final Logger LOG = LoggerFactory.getLogger(SparkLauncherSuite.class);
  private static final NamedThreadFactory TF = new NamedThreadFactory("SparkLauncherSuite-%d");

  @Test
  public void testChildProcLauncher() throws Exception {
    Map<String, String> env = new HashMap<String, String>();
    env.put("SPARK_PRINT_LAUNCH_COMMAND", "1");

    SparkLauncher launcher = new SparkLauncher(env)
      .setSparkHome(System.getProperty("spark.test.home"))
      .setMaster("local")
      .setAppResource("spark-internal")
      .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS,
        "-Dfoo=bar -Dtest.appender=childproc")
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))
      .setMainClass(SparkLauncherTestApp.class.getName())
      .addAppArgs("proc");
    final Process app = launcher.launch();

    new OutputRedirector(app.getInputStream(), TF);
    new OutputRedirector(app.getErrorStream(), TF);
    assertEquals(0, app.waitFor());
  }

  public static class SparkLauncherTestApp {

    public static void main(String[] args) throws Exception {
      assertEquals(1, args.length);
      assertEquals("proc", args[0]);
      assertEquals("bar", System.getProperty("foo"));
      assertEquals("local", System.getProperty(SparkLauncher.SPARK_MASTER));
    }

  }

}
