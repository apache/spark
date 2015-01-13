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
 * for the different specialClasses.
 * <p/>
 * This class has also some special features to aid PySparkLauncher.
 */
public class SparkSubmitCliLauncher extends SparkLauncher {

  /**
   * This map must match the class names for available special classes, since this modifies the way
   * command line parsing works. This maps the class name to the resource to use when calling
   * spark-submit.
   */
  private static final Map<String, String> specialClasses = new HashMap<String, String>();
  static {
    specialClasses.put("org.apache.spark.repl.Main", "spark-shell");
    specialClasses.put("org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver",
      "spark-internal");
  }

  private final List<String> driverArgs;
  private boolean hasMixedArguments;

  SparkSubmitCliLauncher(List<String> args) {
    this(false, args);
  }

  SparkSubmitCliLauncher(boolean hasMixedArguments, List<String> args) {
    this.driverArgs = new ArrayList<String>();
    this.hasMixedArguments = hasMixedArguments;
    new OptionParser().parse(args);
  }

  // Visible for PySparkLauncher.
  String getAppResource() {
    return appResource;
  }

  // Visible for PySparkLauncher.
  List<String> getAppArgs() {
    return appArgs;
  }

  // Visible for PySparkLauncher.
  List<String> getSparkArgs() {
    return sparkArgs;
  }

  // Visible for PySparkLauncher.
  List<String> getDriverArgs() {
    return driverArgs;
  }

  private String getArgValue(Iterator<String> it, String name) {
    checkArgument(it.hasNext(), "Missing argument for '%s'.", name);
    return it.next();
  }

  private class OptionParser extends SparkSubmitOptionParser {

    @Override
    protected boolean handle(String opt, String value) {
      if (opt.equals(MASTER)) {
        setMaster(value);
        driverArgs.add(opt);
        driverArgs.add(value);
      } else if (opt.equals(DEPLOY_MODE)) {
        setDeployMode(value);
        driverArgs.add(opt);
        driverArgs.add(value);
      } else if (opt.equals(PROPERTIES_FILE)) {
        setPropertiesFile(value);
        driverArgs.add(opt);
        driverArgs.add(value);
      } else if (opt.equals(DRIVER_MEMORY)) {
        setConf(LauncherCommon.DRIVER_MEMORY, value);
        driverArgs.add(opt);
        driverArgs.add(value);
      } else if (opt.equals(DRIVER_JAVA_OPTIONS)) {
        setConf(LauncherCommon.DRIVER_EXTRA_JAVA_OPTIONS, value);
        driverArgs.add(opt);
        driverArgs.add(value);
      } else if (opt.equals(DRIVER_LIBRARY_PATH)) {
        setConf(LauncherCommon.DRIVER_EXTRA_LIBRARY_PATH, value);
        driverArgs.add(opt);
        driverArgs.add(value);
      } else if (opt.equals(DRIVER_CLASS_PATH)) {
        setConf(LauncherCommon.DRIVER_EXTRA_CLASSPATH, value);
        driverArgs.add(opt);
        driverArgs.add(value);
      } else if (opt.equals(CLASS)) {
        // The special classes require some special command line handling, since they allow
        // mixing spark-submit arguments with arguments that should be propagated to the shell
        // itself. Note that for this to work, the "--class" argument must come before any
        // non-spark-submit arguments.
        setMainClass(value);
        if (specialClasses.containsKey(value)) {
          hasMixedArguments = true;
          setAppResource(specialClasses.get(value));
        }
      } else {
        addSparkArgs(opt, value);
      }
      return true;
    }

    @Override
    protected boolean handleUnknown(String opt) {
      // When mixing arguments, add unrecognized parameters directly to the user arguments list.
      // In normal mode, any unrecognized parameter triggers the end of command line parsing.
      // The remaining params will be appended to the list of SparkSubmit arguments.
      if (hasMixedArguments) {
        addAppArgs(opt);
        return true;
      } else {
        addSparkArgs(opt);
        return false;
      }
    }

    @Override
    protected void handleExtraArgs(List<String> extra) {
      for (String arg : extra) {
        addSparkArgs(arg);
      }
    }

  }

}
