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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Launcher for Spark applications.
 * <p>
 * Use this class to start Spark applications programmatically. The class uses a builder pattern
 * to allow clients to configure the Spark application and launch it as a child process.
 * </p>
 */
public class SparkLauncher {

  /** The Spark master. */
  public static final String SPARK_MASTER = "spark.master";

  /** Configuration key for the driver memory. */
  public static final String DRIVER_MEMORY = "spark.driver.memory";
  /** Configuration key for the driver class path. */
  public static final String DRIVER_EXTRA_CLASSPATH = "spark.driver.extraClassPath";
  /** Configuration key for the driver VM options. */
  public static final String DRIVER_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions";
  /** Configuration key for the driver native library path. */
  public static final String DRIVER_EXTRA_LIBRARY_PATH = "spark.driver.extraLibraryPath";

  /** Configuration key for the executor memory. */
  public static final String EXECUTOR_MEMORY = "spark.executor.memory";
  /** Configuration key for the executor class path. */
  public static final String EXECUTOR_EXTRA_CLASSPATH = "spark.executor.extraClassPath";
  /** Configuration key for the executor VM options. */
  public static final String EXECUTOR_EXTRA_JAVA_OPTIONS = "spark.executor.extraJavaOptions";
  /** Configuration key for the executor native library path. */
  public static final String EXECUTOR_EXTRA_LIBRARY_PATH = "spark.executor.extraLibraryPath";
  /** Configuration key for the number of executor CPU cores. */
  public static final String EXECUTOR_CORES = "spark.executor.cores";

  // Visible for testing.
  final SparkSubmitCommandBuilder builder;

  public SparkLauncher() {
    this(null);
  }

  /**
   * Creates a launcher that will set the given environment variables in the child.
   *
   * @param env Environment variables to set.
   */
  public SparkLauncher(Map<String, String> env) {
    this.builder = new SparkSubmitCommandBuilder();
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
   * Set a custom properties file with Spark configuration for the application.
   *
   * @param path Path to custom properties file to use.
   * @return This launcher.
   */
  public SparkLauncher setPropertiesFile(String path) {
    checkNotNull(path, "path");
    builder.propertiesFile = path;
    return this;
  }

  /**
   * Set a single configuration value for the application.
   *
   * @param key Configuration key.
   * @param value The value to use.
   * @return This launcher.
   */
  public SparkLauncher setConf(String key, String value) {
    checkNotNull(key, "key");
    checkNotNull(value, "value");
    checkArgument(key.startsWith("spark."), "'key' must start with 'spark.'");
    builder.conf.put(key, value);
    return this;
  }

  /**
   * Set the application name.
   *
   * @param appName Application name.
   * @return This launcher.
   */
  public SparkLauncher setAppName(String appName) {
    checkNotNull(appName, "appName");
    builder.appName = appName;
    return this;
  }

  /**
   * Set the Spark master for the application.
   *
   * @param master Spark master.
   * @return This launcher.
   */
  public SparkLauncher setMaster(String master) {
    checkNotNull(master, "master");
    builder.master = master;
    return this;
  }

  /**
   * Set the deploy mode for the application.
   *
   * @param mode Deploy mode.
   * @return This launcher.
   */
  public SparkLauncher setDeployMode(String mode) {
    checkNotNull(mode, "mode");
    builder.deployMode = mode;
    return this;
  }

  /**
   * Set the main application resource. This should be the location of a jar file for Scala/Java
   * applications, or a python script for PySpark applications.
   *
   * @param resource Path to the main application resource.
   * @return This launcher.
   */
  public SparkLauncher setAppResource(String resource) {
    checkNotNull(resource, "resource");
    builder.appResource = resource;
    return this;
  }

  /**
   * Sets the application class name for Java/Scala applications.
   *
   * @param mainClass Application's main class.
   * @return This launcher.
   */
  public SparkLauncher setMainClass(String mainClass) {
    checkNotNull(mainClass, "mainClass");
    builder.mainClass = mainClass;
    return this;
  }

  /**
   * Adds a no-value argument to the Spark invocation. If the argument is known, this method
   * validates whether the argument is indeed a no-value argument, and throws an exception
   * otherwise.
   * <p>
   * Use this method with caution. It is possible to create an invalid Spark command by passing
   * unknown arguments to this method, since those are allowed for forward compatibility.
   *
   * @param arg Argument to add.
   * @return This launcher.
   */
  public SparkLauncher addSparkArg(String arg) {
    SparkSubmitOptionParser validator = new ArgumentValidator(false);
    validator.parse(Arrays.asList(arg));
    builder.sparkArgs.add(arg);
    return this;
  }

  /**
   * Adds an argument with a value to the Spark invocation. If the argument name corresponds to
   * a known argument, the code validates that the argument actually expects a value, and throws
   * an exception otherwise.
   * <p>
   * It is safe to add arguments modified by other methods in this class (such as
   * {@link #setMaster(String)} - the last invocation will be the one to take effect.
   * <p>
   * Use this method with caution. It is possible to create an invalid Spark command by passing
   * unknown arguments to this method, since those are allowed for forward compatibility.
   *
   * @param name Name of argument to add.
   * @param value Value of the argument.
   * @return This launcher.
   */
  public SparkLauncher addSparkArg(String name, String value) {
    SparkSubmitOptionParser validator = new ArgumentValidator(true);
    if (validator.MASTER.equals(name)) {
      setMaster(value);
    } else if (validator.PROPERTIES_FILE.equals(name)) {
      setPropertiesFile(value);
    } else if (validator.CONF.equals(name)) {
      String[] vals = value.split("=", 2);
      setConf(vals[0], vals[1]);
    } else if (validator.CLASS.equals(name)) {
      setMainClass(value);
    } else if (validator.JARS.equals(name)) {
      builder.jars.clear();
      for (String jar : value.split(",")) {
        addJar(jar);
      }
    } else if (validator.FILES.equals(name)) {
      builder.files.clear();
      for (String file : value.split(",")) {
        addFile(file);
      }
    } else if (validator.PY_FILES.equals(name)) {
      builder.pyFiles.clear();
      for (String file : value.split(",")) {
        addPyFile(file);
      }
    } else {
      validator.parse(Arrays.asList(name, value));
      builder.sparkArgs.add(name);
      builder.sparkArgs.add(value);
    }
    return this;
  }

  /**
   * Adds command line arguments for the application.
   *
   * @param args Arguments to pass to the application's main class.
   * @return This launcher.
   */
  public SparkLauncher addAppArgs(String... args) {
    for (String arg : args) {
      checkNotNull(arg, "arg");
      builder.appArgs.add(arg);
    }
    return this;
  }

  /**
   * Adds a jar file to be submitted with the application.
   *
   * @param jar Path to the jar file.
   * @return This launcher.
   */
  public SparkLauncher addJar(String jar) {
    checkNotNull(jar, "jar");
    builder.jars.add(jar);
    return this;
  }

  /**
   * Adds a file to be submitted with the application.
   *
   * @param file Path to the file.
   * @return This launcher.
   */
  public SparkLauncher addFile(String file) {
    checkNotNull(file, "file");
    builder.files.add(file);
    return this;
  }

  /**
   * Adds a python file / zip / egg to be submitted with the application.
   *
   * @param file Path to the file.
   * @return This launcher.
   */
  public SparkLauncher addPyFile(String file) {
    checkNotNull(file, "file");
    builder.pyFiles.add(file);
    return this;
  }

  /**
   * Enables verbose reporting for SparkSubmit.
   *
   * @param verbose Whether to enable verbose output.
   * @return This launcher.
   */
  public SparkLauncher setVerbose(boolean verbose) {
    builder.verbose = verbose;
    return this;
  }

  /**
   * Launches a sub-process that will start the configured Spark application.
   *
   * @return A process handle for the Spark app.
   */
  public Process launch() throws IOException {
    List<String> cmd = new ArrayList<String>();
    String script = isWindows() ? "spark-submit.cmd" : "spark-submit";
    cmd.add(join(File.separator, builder.getSparkHome(), "bin", script));
    cmd.addAll(builder.buildSparkSubmitArgs());

    // Since the child process is a batch script, let's quote things so that special characters are
    // preserved, otherwise the batch interpreter will mess up the arguments. Batch scripts are
    // weird.
    if (isWindows()) {
      List<String> winCmd = new ArrayList<String>();
      for (String arg : cmd) {
        winCmd.add(quoteForBatchScript(arg));
      }
      cmd = winCmd;
    }

    ProcessBuilder pb = new ProcessBuilder(cmd.toArray(new String[cmd.size()]));
    for (Map.Entry<String, String> e : builder.childEnv.entrySet()) {
      pb.environment().put(e.getKey(), e.getValue());
    }
    return pb.start();
  }

  private static class ArgumentValidator extends SparkSubmitOptionParser {

    private final boolean hasValue;

    ArgumentValidator(boolean hasValue) {
      this.hasValue = hasValue;
    }

    @Override
    protected boolean handle(String opt, String value) {
      if (value == null && hasValue) {
        throw new IllegalArgumentException(String.format("'%s' does not expect a value.", opt));
      }
      return true;
    }

    @Override
    protected boolean handleUnknown(String opt) {
      // Do not fail on unknown arguments, to support future arguments added to SparkSubmit.
      return true;
    }

    protected void handleExtraArgs(List<String> extra) {
      // No op.
    }

  };

}
