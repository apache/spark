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
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.logging.Logger;

/**
 * In-process launcher for Spark applications.
 * <p>
 * Use this class to start Spark applications programmatically. Applications launched using this
 * class will run in the same process as the caller.
 * <p>
 * Because Spark only supports a single active instance of <code>SparkContext</code> per JVM, code
 * that uses this class should be careful about which applications are launched. It's recommended
 * that this launcher only be used to launch applications in cluster mode.
 * <p>
 * Also note that, when running applications in client mode, JVM-related configurations (like
 * driver memory or configs which modify the driver's class path) do not take effect. Logging
 * configuration is also inherited from the parent application.
 *
 * @since Spark 2.3.0
 */
public class InProcessLauncher extends AbstractLauncher<InProcessLauncher> {

  private static final Logger LOG = Logger.getLogger(InProcessLauncher.class.getName());

  /**
   * Starts a Spark application.
   *
   * @see AbstractLauncher#startApplication(SparkAppHandle.Listener...)
   * @param listeners Listeners to add to the handle before the app is launched.
   * @return A handle for the launched application.
   */
  @Override
  public SparkAppHandle startApplication(SparkAppHandle.Listener... listeners) throws IOException {
    if (builder.isClientMode(builder.getEffectiveConfig())) {
      LOG.warning("It's not recommended to run client-mode applications using InProcessLauncher.");
    }

    Method main = findSparkSubmit();
    LauncherServer server = LauncherServer.getOrCreateServer();
    InProcessAppHandle handle = new InProcessAppHandle(server);
    for (SparkAppHandle.Listener l : listeners) {
      handle.addListener(l);
    }

    String secret = server.registerHandle(handle);
    setConf(LauncherProtocol.CONF_LAUNCHER_PORT, String.valueOf(server.getPort()));
    setConf(LauncherProtocol.CONF_LAUNCHER_SECRET, secret);

    List<String> sparkArgs = builder.buildSparkSubmitArgs();
    String[] argv = sparkArgs.toArray(new String[sparkArgs.size()]);

    String appName = CommandBuilderUtils.firstNonEmpty(builder.appName, builder.mainClass,
      "<unknown>");
    handle.start(appName, main, argv);
    return handle;
  }

  @Override
  InProcessLauncher self() {
    return this;
  }

  // Visible for testing.
  Method findSparkSubmit() throws IOException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = getClass().getClassLoader();
    }

    Class<?> sparkSubmit;
    // SPARK-22941: first try the new SparkSubmit interface that has better error handling,
    // but fall back to the old interface in case someone is mixing & matching launcher and
    // Spark versions.
    try {
      sparkSubmit = cl.loadClass("org.apache.spark.deploy.InProcessSparkSubmit");
    } catch (Exception e1) {
      try {
        sparkSubmit = cl.loadClass("org.apache.spark.deploy.SparkSubmit");
      } catch (Exception e2) {
        throw new IOException("Cannot find SparkSubmit; make sure necessary jars are available.",
          e2);
      }
    }

    Method main;
    try {
      main = sparkSubmit.getMethod("main", String[].class);
    } catch (Exception e) {
      throw new IOException("Cannot find SparkSubmit main method.", e);
    }

    CommandBuilderUtils.checkState(Modifier.isStatic(main.getModifiers()),
      "main method is not static.");
    return main;
  }

}
