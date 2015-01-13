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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Launcher for Spark applications.
 * <p/>
 * Use this class to start Spark applications programmatically. The class uses a builder pattern
 * to allow clients to configure the Spark application and launch it as a child process.
 * <p/>
 * There's also support for running the application on a separate thread, although that is to
 * be considered experimental and avoided in production environments.
 * <p/>
 * Note that launching Spark applications using this class will not automatically load environment
 * variables from the "spark-env.sh" or "spark-env.cmd" scripts in the configuration directory.
 */
public class SparkLauncher extends AbstractLauncher<SparkLauncher> {

  private static final AtomicLong THREAD_ID = new AtomicLong();

  protected boolean verbose;
  protected String appName;
  protected String master;
  protected String deployMode;
  protected String mainClass;
  protected String appResource;
  protected final List<String> sparkArgs;
  protected final List<String> appArgs;
  protected final List<String> jars;
  protected final List<String> files;
  protected final List<String> pyFiles;

  public SparkLauncher() {
    this.sparkArgs = new ArrayList<String>();
    this.appArgs = new ArrayList<String>();
    this.jars = new ArrayList<String>();
    this.files = new ArrayList<String>();
    this.pyFiles = new ArrayList<String>();
  }

  /** Set the application name. */
  public SparkLauncher setAppName(String appName) {
    checkNotNull(appName, "appName");
    this.appName = appName;
    return this;
  }

  /** Set the Spark master for the application. */
  public SparkLauncher setMaster(String master) {
    checkNotNull(master, "master");
    this.master = master;
    return this;
  }

  /** Set the deploy mode for the application. */
  public SparkLauncher setDeployMode(String mode) {
    checkNotNull(mode, "mode");
    this.deployMode = mode;
    return this;
  }

  /**
   * Set the main application resource. This should be the location of a jar file for Scala/Java
   * applications, or a python script for PySpark applications.
   */
  public SparkLauncher setAppResource(String resource) {
    checkNotNull(resource, "resource");
    this.appResource = resource;
    return this;
  }

  /** Sets the application class name for Java/Scala applications. */
  public SparkLauncher setMainClass(String mainClass) {
    checkNotNull(mainClass, "mainClass");
    this.mainClass = mainClass;
    return this;
  }

  /** Adds command line arguments for the application. */
  public SparkLauncher addAppArgs(String... args) {
    for (String arg : args) {
      checkNotNull(arg, "arg");
      appArgs.add(arg);
    }
    return this;
  }

  /** Adds a jar file to be submitted with the application. */
  public SparkLauncher addJar(String jar) {
    checkNotNull(jar, "jar");
    jars.add(jar);
    return this;
  }

  /** Adds a file to be submitted with the application. */
  public SparkLauncher addFile(String file) {
    checkNotNull(file, "file");
    files.add(file);
    return this;
  }

  /** Adds a python file / zip / egg to be submitted with the application. */
  public SparkLauncher addPyFile(String file) {
    checkNotNull(file, "file");
    pyFiles.add(file);
    return this;
  }

  /** Enables verbose reporting for SparkSubmit. */
  public SparkLauncher setVerbose(boolean verbose) {
    this.verbose = verbose;
    return this;
  }

  /**
   * Starts a new thread that will run the Spark application.
   * <p/>
   * The application will run on a separate thread and use a separate, isolated class loader.
   * No classes or resources from the current thread's class loader will be visible to the app.
   * <p/>
   * This mode does not support certain configuration parameters, like configuring the amount of
   * driver memory or custom driver command line options. If such configuration is detected, an
   * exception will be thrown.
   * <p/>
   * This is extremely experimental and should not be used in production environments.
   * <p/>
   * NOTE: SparkSubmit uses system properties to propagate some configuration value to the app
   * are run concurrently, they may affect each other's configurations.
   * <p/>
   * NOTE: for users running JDK versions older than 8, this option can add a lot of overhead
   * to the VM's perm gen.
   *
   * @param exceptionHandler Optional handler for handling exceptions in the app thread.
   * @param daemon Whether to start a daemon thread.
   * @return A non-daemon thread that will run the application using SparkSubmit. The thread will
   *         already be started.
   */
  public Thread start(Thread.UncaughtExceptionHandler handler, boolean daemon) throws IOException {
    // Do some sanity checking that incompatible driver options are not used, because they
    // cannot be set in this mode.
    Properties props = loadPropertiesFile();
    String extraClassPath = null;
    if (isClientMode(props)) {
      checkState(
        find(DRIVER_EXTRA_JAVA_OPTIONS, conf, props) == null,
        "Cannot set driver VM options when running in-process.");
      checkState(
        find(DRIVER_EXTRA_LIBRARY_PATH, conf, props) == null,
        "Cannot set native library path when running in-process.");
      checkState(
        find(DRIVER_MEMORY, conf, props) == null,
        "Cannot set driver memory when running in-process.");
      extraClassPath = find(DRIVER_EXTRA_CLASSPATH, conf, props);
    }

    List<String> cp = buildClassPath(extraClassPath);
    URL[] cpUrls = new URL[cp.size()];
    int idx = 0;
    for (String entry : cp) {
      cpUrls[idx++] = new File(entry).toURI().toURL();
    }

    URLClassLoader cl = new URLClassLoader(cpUrls, null);

    Thread appThread;
    try {
      Class<?> sparkSubmit = cl.loadClass("org.apache.spark.deploy.SparkSubmit");
      Method main = sparkSubmit.getDeclaredMethod("main", String[].class);
      List<String> args = buildSparkSubmitArgs();
      appThread = new Thread(new SparkSubmitRunner(main, args));
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } catch (NoSuchMethodException nsme) {
      throw new IOException(nsme);
    }

    appThread.setName("SparkLauncher-Submit-" + THREAD_ID.incrementAndGet());
    appThread.setContextClassLoader(cl);
    if (handler != null) {
      appThread.setUncaughtExceptionHandler(handler);
    }
    appThread.setDaemon(daemon);
    appThread.start();
    return appThread;
  }

  /**
   * Launches a sub-process that will start the configured Spark application.
   *
   * @return A process handle for the Spark app.
   */
  public Process launch() throws IOException {
    List<String> cmd = buildLauncherCommand();
    return Runtime.getRuntime().exec(cmd.toArray(new String[cmd.size()]));
  }

  SparkLauncher addSparkArgs(String... args) {
    for (String arg : args) {
      sparkArgs.add(arg);
    }
    return this;
  }

  // Visible for testing.
  List<String> buildSparkSubmitArgs() {
    List<String> args = new ArrayList<String>();

    if (verbose) {
      args.add("--verbose");
    }

    if (master != null) {
      args.add("--master");
      args.add(master);
    }

    if (deployMode != null) {
      args.add("--deploy-mode");
      args.add(deployMode);
    }

    if (appName != null) {
      args.add("--name");
      args.add(appName);
    }

    for (Map.Entry<String, String> e : conf.entrySet()) {
      args.add("--conf");
      args.add(String.format("%s=%s", e.getKey(), e.getValue()));
    }

    if (propertiesFile != null) {
      args.add("--properties-file");
      args.add(propertiesFile);
    }

    if (!jars.isEmpty()) {
      args.add("--jars");
      args.add(join(",", jars));
    }

    if (!files.isEmpty()) {
      args.add("--files");
      args.add(join(",", files));
    }

    if (!pyFiles.isEmpty()) {
      args.add("--py-files");
      args.add(join(",", pyFiles));
    }

    if (mainClass != null) {
      args.add("--class");
      args.add(mainClass);
    }

    args.addAll(sparkArgs);
    if (appResource != null) {
      args.add(appResource);
    }
    args.addAll(appArgs);

    return args;
  }

  @Override
  protected List<String> buildLauncherCommand() throws IOException {
    List<String> cmd = buildJavaCommand();
    addOptionString(cmd, System.getenv("SPARK_SUBMIT_OPTS"));
    addOptionString(cmd, System.getenv("SPARK_JAVA_OPTS"));

    // Load the properties file and check whether spark-submit will be running the app's driver
    // or just launching a cluster app. When running the driver, the JVM's argument will be
    // modified to cover the driver's configuration.
    Properties props = loadPropertiesFile();
    boolean isClientMode = isClientMode(props);

    String extraClassPath = isClientMode ? find(DRIVER_EXTRA_CLASSPATH, conf, props) : null;
    cmd.add("-cp");
    cmd.add(join(File.pathSeparator, buildClassPath(extraClassPath)));

    String libPath = null;
    if (isClientMode) {
      // Figuring out where the memory value come from is a little tricky due to precedence.
      // Precedence is observed in the following order:
      // - explicit configuration (setConf()), which also covers --driver-memory cli argument.
      // - properties file.
      // - SPARK_DRIVER_MEMORY env variable
      // - SPARK_MEM env variable
      // - default value (512m)
      String memory = firstNonEmpty(find(DRIVER_MEMORY, conf, props),
        System.getenv("SPARK_DRIVER_MEMORY"), System.getenv("SPARK_MEM"), DEFAULT_MEM);
      cmd.add("-Xms" + memory);
      cmd.add("-Xmx" + memory);
      addOptionString(cmd, find(DRIVER_EXTRA_JAVA_OPTIONS, conf, props));
      libPath = find(DRIVER_EXTRA_LIBRARY_PATH, conf, props);
    }

    cmd.add("org.apache.spark.deploy.SparkSubmit");
    cmd.addAll(buildSparkSubmitArgs());
    return prepareForOs(cmd, libPath);
  }

  private boolean isClientMode(Properties userProps) {
    String userMaster = firstNonEmpty(master, (String) userProps.get(SPARK_MASTER));
    return userMaster == null ||
      "client".equals(deployMode) ||
      "yarn-client".equals(userMaster) ||
      (deployMode == null && !userMaster.startsWith("yarn-"));
  }

  private static class SparkSubmitRunner implements Runnable {

    private final Method main;
    private final Object args;

    SparkSubmitRunner(Method main, List<String> args) {
      this.main = main;
      this.args = args.toArray(new String[args.size()]);
    }

    @Override
    public void run() {
      try {
        main.invoke(null, args);
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

}
