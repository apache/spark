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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Launcher for Spark applications.
 * <p>
 * Use this class to start Spark applications programmatically. The class uses a builder pattern
 * to allow clients to configure the Spark application and launch it as a child process.
 * </p>
 */
public class SparkLauncher extends AbstractLauncher<SparkLauncher> {

  private static final Logger LOG = Logger.getLogger(SparkLauncher.class.getName());

  /** The Spark master. */
  public static final String SPARK_MASTER = "spark.master";

  /** The Spark remote. */
  public static final String SPARK_REMOTE = "spark.remote";
  public static final String SPARK_LOCAL_REMOTE = "spark.local.connect";

  /** The Spark deploy mode. */
  public static final String DEPLOY_MODE = "spark.submit.deployMode";

  /** Configuration key for the driver memory. */
  public static final String DRIVER_MEMORY = "spark.driver.memory";
  /** Configuration key for the driver default extra class path. */
  public static final String DRIVER_DEFAULT_EXTRA_CLASS_PATH =
    "spark.driver.defaultExtraClassPath";
  public static final String DRIVER_DEFAULT_EXTRA_CLASS_PATH_VALUE = "hive-jackson/*";
  /** Configuration key for the driver class path. */
  public static final String DRIVER_EXTRA_CLASSPATH = "spark.driver.extraClassPath";
  /** Configuration key for the default driver VM options. */
  public static final String DRIVER_DEFAULT_JAVA_OPTIONS = "spark.driver.defaultJavaOptions";
  /** Configuration key for the driver VM options. */
  public static final String DRIVER_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions";
  /** Configuration key for the driver native library path. */
  public static final String DRIVER_EXTRA_LIBRARY_PATH = "spark.driver.extraLibraryPath";

  /** Configuration key for the executor memory. */
  public static final String EXECUTOR_MEMORY = "spark.executor.memory";
  /** Configuration key for the executor default extra class path. */
  public static final String EXECUTOR_DEFAULT_EXTRA_CLASS_PATH =
    "spark.executor.defaultExtraClassPath";
  public static final String EXECUTOR_DEFAULT_EXTRA_CLASS_PATH_VALUE = "hive-jackson/*";
  /** Configuration key for the executor class path. */
  public static final String EXECUTOR_EXTRA_CLASSPATH = "spark.executor.extraClassPath";
  /** Configuration key for the default executor VM options. */
  public static final String EXECUTOR_DEFAULT_JAVA_OPTIONS = "spark.executor.defaultJavaOptions";
  /** Configuration key for the executor VM options. */
  public static final String EXECUTOR_EXTRA_JAVA_OPTIONS = "spark.executor.extraJavaOptions";
  /** Configuration key for the executor native library path. */
  public static final String EXECUTOR_EXTRA_LIBRARY_PATH = "spark.executor.extraLibraryPath";
  /** Configuration key for the number of executor CPU cores. */
  public static final String EXECUTOR_CORES = "spark.executor.cores";

  static final String PYSPARK_DRIVER_PYTHON = "spark.pyspark.driver.python";

  static final String PYSPARK_PYTHON = "spark.pyspark.python";

  static final String SPARKR_R_SHELL = "spark.r.shell.command";

  /** Logger name to use when launching a child process. */
  public static final String CHILD_PROCESS_LOGGER_NAME = "spark.launcher.childProcLoggerName";

  /**
   * A special value for the resource that tells Spark to not try to process the app resource as a
   * file. This is useful when the class being executed is added to the application using other
   * means - for example, by adding jars using the package download feature.
   */
  public static final String NO_RESOURCE = "spark-internal";

  /**
   * Maximum time (in ms) to wait for a child process to connect back to the launcher server
   * when using @link{#start()}.
   *
   * @deprecated use `CHILD_CONNECTION_TIMEOUT`
   * @since 1.6.0
   */
  @Deprecated(since = "3.2.0")
  public static final String DEPRECATED_CHILD_CONNECTION_TIMEOUT =
    "spark.launcher.childConectionTimeout";

  /**
   * Maximum time (in ms) to wait for a child process to connect back to the launcher server
   * when using @link{#start()}.
   */
  public static final String CHILD_CONNECTION_TIMEOUT = "spark.launcher.childConnectionTimeout";

  /** Used internally to create unique logger names. */
  private static final AtomicInteger COUNTER = new AtomicInteger();

  /** Factory for creating OutputRedirector threads. **/
  static final ThreadFactory REDIRECTOR_FACTORY = new NamedThreadFactory("launcher-proc-%d");

  static final Map<String, String> launcherConfig = new HashMap<>();

  /**
   * Set a configuration value for the launcher library. These config values do not affect the
   * launched application, but rather the behavior of the launcher library itself when managing
   * applications.
   *
   * @since 1.6.0
   * @param name Config name.
   * @param value Config value.
   */
  public static void setConfig(String name, String value) {
    launcherConfig.put(name, value);
  }

  // Visible for testing.
  File workingDir;
  boolean redirectErrorStream;
  ProcessBuilder.Redirect errorStream;
  ProcessBuilder.Redirect outputStream;

  public SparkLauncher() {
    this(null);
  }

  /**
   * Creates a launcher that will set the given environment variables in the child.
   *
   * @param env Environment variables to set.
   */
  public SparkLauncher(Map<String, String> env) {
    if (env != null) {
      this.builder.childEnv.putAll(env);
    }
  }

  /**
   * Set a custom JAVA_HOME for launching the Spark application.
   *
   * @param javaHome Path to the JAVA_HOME to use.
   * @return This launcher.
   */
  public SparkLauncher setJavaHome(String javaHome) {
    checkNotNull(javaHome, "javaHome");
    builder.javaHome = javaHome;
    return this;
  }

  /**
   * Set a custom Spark installation location for the application.
   *
   * @param sparkHome Path to the Spark installation to use.
   * @return This launcher.
   */
  public SparkLauncher setSparkHome(String sparkHome) {
    checkNotNull(sparkHome, "sparkHome");
    builder.childEnv.put(ENV_SPARK_HOME, sparkHome);
    return this;
  }

  /**
   * Sets the working directory of spark-submit.
   *
   * @param dir The directory to set as spark-submit's working directory.
   * @return This launcher.
   */
  public SparkLauncher directory(File dir) {
    workingDir = dir;
    return this;
  }

  /**
   * Specifies that stderr in spark-submit should be redirected to stdout.
   *
   * @return This launcher.
   */
  public SparkLauncher redirectError() {
    redirectErrorStream = true;
    return this;
  }

  /**
   * Redirects error output to the specified Redirect.
   *
   * @param to The method of redirection.
   * @return This launcher.
   */
  public SparkLauncher redirectError(ProcessBuilder.Redirect to) {
    errorStream = to;
    return this;
  }

  /**
   * Redirects standard output to the specified Redirect.
   *
   * @param to The method of redirection.
   * @return This launcher.
   */
  public SparkLauncher redirectOutput(ProcessBuilder.Redirect to) {
    outputStream = to;
    return this;
  }

  /**
   * Redirects error output to the specified File.
   *
   * @param errFile The file to which stderr is written.
   * @return This launcher.
   */
  public SparkLauncher redirectError(File errFile) {
    errorStream = ProcessBuilder.Redirect.to(errFile);
    return this;
  }

  /**
   * Redirects error output to the specified File.
   *
   * @param outFile The file to which stdout is written.
   * @return This launcher.
   */
  public SparkLauncher redirectOutput(File outFile) {
    outputStream = ProcessBuilder.Redirect.to(outFile);
    return this;
  }

  /**
   * Sets all output to be logged and redirected to a logger with the specified name.
   *
   * @param loggerName The name of the logger to log stdout and stderr.
   * @return This launcher.
   */
  public SparkLauncher redirectToLog(String loggerName) {
    setConf(CHILD_PROCESS_LOGGER_NAME, loggerName);
    return this;
  }

  // The following methods just delegate to the parent class, but they are needed to keep
  // binary compatibility with previous versions of this class.

  @Override
  public SparkLauncher setPropertiesFile(String path) {
    return super.setPropertiesFile(path);
  }

  @Override
  public SparkLauncher setConf(String key, String value) {
    return super.setConf(key, value);
  }

  @Override
  public SparkLauncher setAppName(String appName) {
    return super.setAppName(appName);
  }

  @Override
  public SparkLauncher setMaster(String master) {
    return super.setMaster(master);
  }

  @Override
  public SparkLauncher setDeployMode(String mode) {
    return super.setDeployMode(mode);
  }

  @Override
  public SparkLauncher setAppResource(String resource) {
    return super.setAppResource(resource);
  }

  @Override
  public SparkLauncher setMainClass(String mainClass) {
    return super.setMainClass(mainClass);
  }

  @Override
  public SparkLauncher addSparkArg(String arg) {
    return super.addSparkArg(arg);
  }

  @Override
  public SparkLauncher addSparkArg(String name, String value) {
    return super.addSparkArg(name, value);
  }

  @Override
  public SparkLauncher addAppArgs(String... args) {
    return super.addAppArgs(args);
  }

  @Override
  public SparkLauncher addJar(String jar) {
    return super.addJar(jar);
  }

  @Override
  public SparkLauncher addFile(String file) {
    return super.addFile(file);
  }

  @Override
  public SparkLauncher addPyFile(String file) {
    return super.addPyFile(file);
  }

  @Override
  public SparkLauncher setVerbose(boolean verbose) {
    return super.setVerbose(verbose);
  }

  /**
   * Launches a sub-process that will start the configured Spark application.
   * <p>
   * The {@link #startApplication(SparkAppHandle.Listener...)} method is preferred when launching
   * Spark, since it provides better control of the child application.
   *
   * @return A process handle for the Spark app.
   */
  public Process launch() throws IOException {
    ProcessBuilder pb = createBuilder();

    boolean outputToLog = outputStream == null;
    boolean errorToLog = !redirectErrorStream && errorStream == null;

    String loggerName = getLoggerName();
    if (loggerName != null && outputToLog && errorToLog) {
      pb.redirectErrorStream(true);
    }

    Process childProc = pb.start();
    if (loggerName != null) {
      InputStream logStream = outputToLog ? childProc.getInputStream() : childProc.getErrorStream();
      new OutputRedirector(logStream, loggerName, REDIRECTOR_FACTORY);
    }

    return childProc;
  }

  /**
   * Starts a Spark application.
   *
   * <p>
   * Applications launched by this launcher run as child processes. The child's stdout and stderr
   * are merged and written to a logger (see <code>java.util.logging</code>) only if redirection
   * has not otherwise been configured on this <code>SparkLauncher</code>. The logger's name can be
   * defined by setting {@link #CHILD_PROCESS_LOGGER_NAME} in the app's configuration. If that
   * option is not set, the code will try to derive a name from the application's name or main
   * class / script file. If those cannot be determined, an internal, unique name will be used.
   * In all cases, the logger name will start with "org.apache.spark.launcher.app", to fit more
   * easily into the configuration of commonly-used logging systems.
   *
   * @since 1.6.0
   * @see AbstractLauncher#startApplication(SparkAppHandle.Listener...)
   * @param listeners Listeners to add to the handle before the app is launched.
   * @return A handle for the launched application.
   */
  @Override
  public SparkAppHandle startApplication(SparkAppHandle.Listener... listeners) throws IOException {
    LauncherServer server = LauncherServer.getOrCreateServer();
    ChildProcAppHandle handle = new ChildProcAppHandle(server);
    for (SparkAppHandle.Listener l : listeners) {
      handle.addListener(l);
    }

    String secret = server.registerHandle(handle);

    String loggerName = getLoggerName();
    ProcessBuilder pb = createBuilder();
    if (LOG.isLoggable(Level.FINE)) {
      LOG.fine(String.format("Launching Spark application:%n%s", join(" ", pb.command())));
    }

    boolean outputToLog = outputStream == null;
    boolean errorToLog = !redirectErrorStream && errorStream == null;

    // Only setup stderr + stdout to logger redirection if user has not otherwise configured output
    // redirection.
    if (loggerName == null && (outputToLog || errorToLog)) {
      String appName;
      if (builder.appName != null) {
        appName = builder.appName;
      } else if (builder.mainClass != null) {
        int dot = builder.mainClass.lastIndexOf(".");
        if (dot >= 0 && dot < builder.mainClass.length() - 1) {
          appName = builder.mainClass.substring(dot + 1, builder.mainClass.length());
        } else {
          appName = builder.mainClass;
        }
      } else if (builder.appResource != null) {
        appName = new File(builder.appResource).getName();
      } else {
        appName = String.valueOf(COUNTER.incrementAndGet());
      }
      String loggerPrefix = getClass().getPackage().getName();
      loggerName = String.format("%s.app.%s", loggerPrefix, appName);
    }

    if (outputToLog && errorToLog) {
      pb.redirectErrorStream(true);
    }

    pb.environment().put(LauncherProtocol.ENV_LAUNCHER_PORT, String.valueOf(server.getPort()));
    pb.environment().put(LauncherProtocol.ENV_LAUNCHER_SECRET, secret);
    try {
      Process child = pb.start();
      InputStream logStream = null;
      if (loggerName != null) {
        logStream = outputToLog ? child.getInputStream() : child.getErrorStream();
      }
      handle.setChildProc(child, loggerName, logStream);
    } catch (IOException ioe) {
      handle.kill();
      throw ioe;
    }

    return handle;
  }

  private ProcessBuilder createBuilder() throws IOException {
    List<String> cmd = new ArrayList<>();
    cmd.add(findSparkSubmit());
    cmd.addAll(builder.buildSparkSubmitArgs());

    // Since the child process is a batch script, let's quote things so that special characters are
    // preserved, otherwise the batch interpreter will mess up the arguments. Batch scripts are
    // weird.
    if (isWindows()) {
      List<String> winCmd = new ArrayList<>();
      for (String arg : cmd) {
        winCmd.add(quoteForBatchScript(arg));
      }
      cmd = winCmd;
    }

    ProcessBuilder pb = new ProcessBuilder(cmd.toArray(new String[cmd.size()]));
    for (Map.Entry<String, String> e : builder.childEnv.entrySet()) {
      pb.environment().put(e.getKey(), e.getValue());
    }

    if (workingDir != null) {
      pb.directory(workingDir);
    }

    // Only one of redirectError and redirectError(...) can be specified.
    // Similarly, if redirectToLog is specified, no other redirections should be specified.
    checkState(!redirectErrorStream || errorStream == null,
      "Cannot specify both redirectError() and redirectError(...) ");
    checkState(getLoggerName() == null ||
      ((!redirectErrorStream && errorStream == null) || outputStream == null),
      "Cannot used redirectToLog() in conjunction with other redirection methods.");

    if (redirectErrorStream) {
      pb.redirectErrorStream(true);
    }
    if (errorStream != null) {
      pb.redirectError(errorStream);
    }
    if (outputStream != null) {
      pb.redirectOutput(outputStream);
    }

    return pb;
  }

  @Override
  SparkLauncher self() {
    return this;
  }

  // Visible for testing.
  String findSparkSubmit() {
    String script = isWindows() ? "spark-submit.cmd" : "spark-submit";
    return join(File.separator, builder.getSparkHome(), "bin", script);
  }

  private String getLoggerName() throws IOException {
    return builder.getEffectiveConfig().get(CHILD_PROCESS_LOGGER_NAME);
  }

}
