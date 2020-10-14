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
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Base class for launcher implementations.
 *
 * @since Spark 2.3.0
 */
public abstract class AbstractLauncher<T extends AbstractLauncher<T>> {

  final SparkSubmitCommandBuilder builder;

  AbstractLauncher() {
    this.builder = new SparkSubmitCommandBuilder();
  }

  /**
   * Set a custom properties file with Spark configuration for the application.
   *
   * @param path Path to custom properties file to use.
   * @return This launcher.
   */
  public T setPropertiesFile(String path) {
    checkNotNull(path, "path");
    builder.setPropertiesFile(path);
    return self();
  }

  /**
   * Set a single configuration value for the application.
   *
   * @param key Configuration key.
   * @param value The value to use.
   * @return This launcher.
   */
  public T setConf(String key, String value) {
    checkNotNull(key, "key");
    checkNotNull(value, "value");
    checkArgument(key.startsWith("spark."), "'key' must start with 'spark.'");
    builder.conf.put(key, value);
    return self();
  }

  /**
   * Set the application name.
   *
   * @param appName Application name.
   * @return This launcher.
   */
  public T setAppName(String appName) {
    checkNotNull(appName, "appName");
    builder.appName = appName;
    return self();
  }

  /**
   * Set the Spark master for the application.
   *
   * @param master Spark master.
   * @return This launcher.
   */
  public T setMaster(String master) {
    checkNotNull(master, "master");
    builder.master = master;
    return self();
  }

  /**
   * Set the deploy mode for the application.
   *
   * @param mode Deploy mode.
   * @return This launcher.
   */
  public T setDeployMode(String mode) {
    checkNotNull(mode, "mode");
    builder.deployMode = mode;
    return self();
  }

  /**
   * Set the main application resource. This should be the location of a jar file for Scala/Java
   * applications, or a python script for PySpark applications.
   *
   * @param resource Path to the main application resource.
   * @return This launcher.
   */
  public T setAppResource(String resource) {
    checkNotNull(resource, "resource");
    builder.appResource = resource;
    return self();
  }

  /**
   * Sets the application class name for Java/Scala applications.
   *
   * @param mainClass Application's main class.
   * @return This launcher.
   */
  public T setMainClass(String mainClass) {
    checkNotNull(mainClass, "mainClass");
    builder.mainClass = mainClass;
    return self();
  }

  /**
   * Adds a no-value argument to the Spark invocation. If the argument is known, this method
   * validates whether the argument is indeed a no-value argument, and throws an exception
   * otherwise.
   * <p>
   * Use this method with caution. It is possible to create an invalid Spark command by passing
   * unknown arguments to this method, since those are allowed for forward compatibility.
   *
   * @since 1.5.0
   * @param arg Argument to add.
   * @return This launcher.
   */
  public T addSparkArg(String arg) {
    SparkSubmitOptionParser validator = new ArgumentValidator(false);
    validator.parse(Arrays.asList(arg));
    builder.userArgs.add(arg);
    return self();
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
   * @since 1.5.0
   * @param name Name of argument to add.
   * @param value Value of the argument.
   * @return This launcher.
   */
  public T addSparkArg(String name, String value) {
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
      builder.userArgs.add(name);
      builder.userArgs.add(value);
    }
    return self();
  }

  /**
   * Adds command line arguments for the application.
   *
   * @param args Arguments to pass to the application's main class.
   * @return This launcher.
   */
  public T addAppArgs(String... args) {
    for (String arg : args) {
      checkNotNull(arg, "arg");
      builder.appArgs.add(arg);
    }
    return self();
  }

  /**
   * Adds a jar file to be submitted with the application.
   *
   * @param jar Path to the jar file.
   * @return This launcher.
   */
  public T addJar(String jar) {
    checkNotNull(jar, "jar");
    builder.jars.add(jar);
    return self();
  }

  /**
   * Adds a file to be submitted with the application.
   *
   * @param file Path to the file.
   * @return This launcher.
   */
  public T addFile(String file) {
    checkNotNull(file, "file");
    builder.files.add(file);
    return self();
  }

  /**
   * Adds a python file / zip / egg to be submitted with the application.
   *
   * @param file Path to the file.
   * @return This launcher.
   */
  public T addPyFile(String file) {
    checkNotNull(file, "file");
    builder.pyFiles.add(file);
    return self();
  }

  /**
   * Enables verbose reporting for SparkSubmit.
   *
   * @param verbose Whether to enable verbose output.
   * @return This launcher.
   */
  public T setVerbose(boolean verbose) {
    builder.verbose = verbose;
    return self();
  }

  /**
   * Starts a Spark application.
   *
   * <p>
   * This method returns a handle that provides information about the running application and can
   * be used to do basic interaction with it.
   * <p>
   * The returned handle assumes that the application will instantiate a single SparkContext
   * during its lifetime. Once that context reports a final state (one that indicates the
   * SparkContext has stopped), the handle will not perform new state transitions, so anything
   * that happens after that cannot be monitored. If the underlying application is launched as
   * a child process, {@link SparkAppHandle#kill()} can still be used to kill the child process.
   *
   * @since 1.6.0
   * @param listeners Listeners to add to the handle before the app is launched.
   * @return A handle for the launched application.
   */
  public abstract SparkAppHandle startApplication(SparkAppHandle.Listener... listeners)
    throws IOException;

  abstract T self();

  private static class ArgumentValidator extends SparkSubmitOptionParser {

    private final boolean hasValue;

    ArgumentValidator(boolean hasValue) {
      this.hasValue = hasValue;
    }

    @Override
    protected boolean handle(String opt, String value) {
      if (value == null && hasValue) {
        throw new IllegalArgumentException(String.format("'%s' expects a value.", opt));
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

  }

}
