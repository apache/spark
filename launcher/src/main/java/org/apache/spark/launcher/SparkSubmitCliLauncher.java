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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Special launcher for handling a CLI invocation of SparkSubmit.
 * <p/>
 * This launcher extends SparkLauncher to add command line parsing compatible with
 * SparkSubmit. It handles setting driver-side options and special parsing needed
 * for the different shells.
 * <p/>
 * This class has also some special features to aid PySparkLauncher.
 */
public class SparkSubmitCliLauncher extends SparkLauncher {

  /** List of spark-submit arguments that take an argument. */
  public static final List<String> SPARK_SUBMIT_OPTS = Arrays.asList(
    "--archives",
    "--class",
    "--conf",
    "--deploy-mode",
    "--driver-class-path",
    "--driver-cores",
    "--driver-java-options",
    "--driver-library-path",
    "--driver-memory",
    "--executor-cores",
    "--executor-memory",
    "--files",
    "--jars",
    "--master",
    "--name",
    "--num-executors",
    "--properties-file",
    "--py-files",
    "--queue",
    "--total-executor-cores");

  /** List of spark-submit arguments that do not take an argument. */
  public static final List<String> SPARK_SUBMIT_SWITCHES = Arrays.asList(
    "--supervise",
    "--verbose",
    "-v");

  /**
   * This map must match the class names for available shells, since this modifies the way
   * command line parsing works. This maps the shell class name to the resource to use when
   * calling spark-submit.
   */
  private static final Map<String, String> shells = new HashMap<String, String>();
  static {
    shells.put("org.apache.spark.repl.Main", "spark-shell");
    shells.put("org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver", "spark-internal");
  }

  private final List<String> driverArgs;
  private boolean isShell;

  SparkSubmitCliLauncher(List<String> args) {
    this(false, args);
  }

  SparkSubmitCliLauncher(boolean isShell, List<String> args) {
    boolean sparkSubmitOptionsEnded = false;
    this.driverArgs = new ArrayList<String>();
    this.isShell = isShell;

    Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");

    for (Iterator<String> it = args.iterator(); it.hasNext(); ) {
      String arg = it.next();

      Matcher m = eqSeparatedOpt.matcher(arg);
      if (m.matches()) {
        parseOpt(m.group(1), m.group(2), it);
      } else {
        parseOpt(arg, it);
      }
    }
  }

  private void parseOpt(String arg, Iterator<String> tail) {
    if (SPARK_SUBMIT_SWITCHES.contains(arg)) {
      addSparkArgs(arg);
    } if (!SPARK_SUBMIT_OPTS.contains(arg)) {
      parseOpt(arg, null, tail);
    } else {
      parseOpt(arg, getArgValue(tail, arg), tail);
    }
  }

  private void parseOpt(String arg, String value, Iterator<String> tail) {
    if (!SPARK_SUBMIT_OPTS.contains(arg)) {
      // When running a shell, add unrecognized parameters directly to the user arguments list.
      // In normal mode, any unrecognized parameter triggers the end of command line parsing.
      // The remaining params will be appended to the list of SparkSubmit arguments.
      if (isShell) {
        addArgs(arg);
      } else {
        addSparkArgs(arg);
        while (tail.hasNext()) {
          addSparkArgs(tail.next());
        }
      }
    } else if (arg.equals("--master")) {
      setMaster(value);
      driverArgs.add(arg);
      driverArgs.add(value);
    } else if (arg.equals("--deploy-mode")) {
      setDeployMode(value);
      driverArgs.add(arg);
      driverArgs.add(value);
    } else if (arg.equals("--properties-file")) {
      setPropertiesFile(value);
      driverArgs.add(arg);
      driverArgs.add(value);
    } else if (arg.equals("--driver-memory")) {
      setConf(DRIVER_MEMORY, value);
      driverArgs.add(arg);
      driverArgs.add(value);
    } else if (arg.equals("--driver-java-options")) {
      setConf(DRIVER_JAVA_OPTIONS, value);
      driverArgs.add(arg);
      driverArgs.add(value);
    } else if (arg.equals("--driver-library-path")) {
      setConf(DRIVER_LIBRARY_PATH, value);
      driverArgs.add(arg);
      driverArgs.add(value);
    } else if (arg.equals("--driver-class-path")) {
      setConf(DRIVER_CLASSPATH, value);
      driverArgs.add(arg);
      driverArgs.add(value);
    } else if (arg.equals("--class")) {
      // The shell launchers require some special command line handling, since they allow
      // mixing spark-submit arguments with arguments that should be propagated to the shell
      // itself. Note that for this to work, the "--class" argument must come before any
      // non-spark-submit arguments.
      setClass(value);
      if (shells.containsKey(value)) {
        isShell = true;
        setAppResource(shells.get(value));
      }
    } else {
      addSparkArgs(arg, value);
    }
  }
  /** Visible for PySparkLauncher. */
  String getAppResource() {
    return userResource;
  }

  /** Visible for PySparkLauncher. */
  List<String> getArgs() {
    return userArgs;
  }

  /** Visible for PySparkLauncher. */
  List<String> getSparkArgs() {
    return sparkArgs;
  }

  /** Visible for PySparkLauncher. */
  List<String> getDriverArgs() {
    return driverArgs;
  }

  private String getArgValue(Iterator<String> it, String name) {
    checkArgument(it.hasNext(), "Missing argument for '%s'.", name);
    return it.next();
  }

}
