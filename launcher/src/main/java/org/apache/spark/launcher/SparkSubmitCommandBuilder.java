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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Special command builder for handling a CLI invocation of SparkSubmit.
 * <p/>
 * This builder adds command line parsing compatible with SparkSubmit. It handles setting
 * driver-side options and special parsing behavior needed for the special-casing certain internal
 * Spark applications.
 * <p/>
 * This class has also some special features to aid launching pyspark.
 */
class SparkSubmitCommandBuilder extends AbstractCommandBuilder {

  /**
   * Name of the app resource used to identify the PySpark shell. The command line parser expects
   * the resource name to be the very first argument to spark-submit in this case.
   *
   * NOTE: this cannot be "pyspark-shell" since that identifies the PySpark shell to SparkSubmit
   * (see java_gateway.py), and can cause this code to enter into an infinite loop.
   */
  static final String PYSPARK_SHELL = "pyspark-shell-main";

  /**
   * This is the actual resource name that identifies the PySpark shell to SparkSubmit.
   */
  static final String PYSPARK_SHELL_RESOURCE = "pyspark-shell";

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
    specialClasses.put("org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
      "spark-internal");
  }

  private final List<String> sparkArgs;

  /**
   * Controls whether mixing spark-submit arguments with app arguments is allowed. This is needed
   * to parse the command lines for things like bin/spark-shell, which allows users to mix and
   * match arguments (e.g. "bin/spark-shell SparkShellArg --master foo").
   */
  private boolean allowsMixedArguments;

  SparkSubmitCommandBuilder() {
    this.sparkArgs = new ArrayList<String>();
  }

  SparkSubmitCommandBuilder(List<String> args) {
    this();
    List<String> submitArgs = args;
    if (args.size() > 0 && args.get(0).equals(PYSPARK_SHELL)) {
      this.allowsMixedArguments = true;
      appResource = PYSPARK_SHELL_RESOURCE;
      submitArgs = args.subList(1, args.size());
    } else {
      this.allowsMixedArguments = false;
    }

    new OptionParser().parse(submitArgs);
  }

  @Override
  public List<String> buildCommand(Map<String, String> env) throws IOException {
    if (PYSPARK_SHELL_RESOURCE.equals(appResource)) {
      return buildPySparkShellCommand(env);
    } else {
      return buildSparkSubmitCommand(env);
    }
  }

  List<String> buildSparkSubmitArgs() {
    List<String> args = new ArrayList<String>();
    SparkSubmitOptionParser parser = new SparkSubmitOptionParser();

    if (verbose) {
      args.add(parser.VERBOSE);
    }

    if (master != null) {
      args.add(parser.MASTER);
      args.add(master);
    }

    if (deployMode != null) {
      args.add(parser.DEPLOY_MODE);
      args.add(deployMode);
    }

    if (appName != null) {
      args.add(parser.NAME);
      args.add(appName);
    }

    for (Map.Entry<String, String> e : conf.entrySet()) {
      args.add(parser.CONF);
      args.add(String.format("%s=%s", e.getKey(), e.getValue()));
    }

    if (propertiesFile != null) {
      args.add(parser.PROPERTIES_FILE);
      args.add(propertiesFile);
    }

    if (!jars.isEmpty()) {
      args.add(parser.JARS);
      args.add(join(",", jars));
    }

    if (!files.isEmpty()) {
      args.add(parser.FILES);
      args.add(join(",", files));
    }

    if (!pyFiles.isEmpty()) {
      args.add(parser.PY_FILES);
      args.add(join(",", pyFiles));
    }

    if (mainClass != null) {
      args.add(parser.CLASS);
      args.add(mainClass);
    }

    args.addAll(sparkArgs);
    if (appResource != null) {
      args.add(appResource);
    }
    args.addAll(appArgs);

    return args;
  }

  private List<String> buildSparkSubmitCommand(Map<String, String> env) throws IOException {
    // Load the properties file and check whether spark-submit will be running the app's driver
    // or just launching a cluster app. When running the driver, the JVM's argument will be
    // modified to cover the driver's configuration.
    Properties props = loadPropertiesFile();
    boolean isClientMode = isClientMode(props);
    String extraClassPath = isClientMode ?
      firstNonEmptyValue(SparkLauncher.DRIVER_EXTRA_CLASSPATH, conf, props) : null;

    List<String> cmd = buildJavaCommand(extraClassPath);
    addOptionString(cmd, System.getenv("SPARK_SUBMIT_OPTS"));
    addOptionString(cmd, System.getenv("SPARK_JAVA_OPTS"));

    if (isClientMode) {
      // Figuring out where the memory value come from is a little tricky due to precedence.
      // Precedence is observed in the following order:
      // - explicit configuration (setConf()), which also covers --driver-memory cli argument.
      // - properties file.
      // - SPARK_DRIVER_MEMORY env variable
      // - SPARK_MEM env variable
      // - default value (512m)
      String memory = firstNonEmpty(firstNonEmptyValue(SparkLauncher.DRIVER_MEMORY, conf, props),
        System.getenv("SPARK_DRIVER_MEMORY"), System.getenv("SPARK_MEM"), DEFAULT_MEM);
      cmd.add("-Xms" + memory);
      cmd.add("-Xmx" + memory);
      addOptionString(cmd, firstNonEmptyValue(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, conf, props));
      mergeEnvPathList(env, getLibPathEnvName(),
        firstNonEmptyValue(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH, conf, props));
    }

    addPermGenSizeOpt(cmd);
    cmd.add("org.apache.spark.deploy.SparkSubmit");
    cmd.addAll(buildSparkSubmitArgs());
    return cmd;
  }

  private List<String> buildPySparkShellCommand(Map<String, String> env) throws IOException {
    // For backwards compatibility, if a script is specified in
    // the pyspark command line, then run it using spark-submit.
    if (!appArgs.isEmpty() && appArgs.get(0).endsWith(".py")) {
      System.err.println(
        "WARNING: Running python applications through 'pyspark' is deprecated as of Spark 1.0.\n" +
        "Use ./bin/spark-submit <python file>");
      appResource = appArgs.get(0);
      appArgs.remove(0);
      return buildCommand(env);
    }

    // When launching the pyspark shell, the spark-submit arguments should be stored in the
    // PYSPARK_SUBMIT_ARGS env variable. The executable is the PYSPARK_DRIVER_PYTHON env variable
    // set by the pyspark script, followed by PYSPARK_DRIVER_PYTHON_OPTS.
    checkArgument(appArgs.isEmpty(), "pyspark does not support any application options.");

    Properties props = loadPropertiesFile();
    mergeEnvPathList(env, getLibPathEnvName(),
      firstNonEmptyValue(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH, conf, props));

    // Store spark-submit arguments in an environment variable, since there's no way to pass
    // them to shell.py on the comand line.
    StringBuilder submitArgs = new StringBuilder();
    for (String arg : buildSparkSubmitArgs()) {
      if (submitArgs.length() > 0) {
        submitArgs.append(" ");
      }
      submitArgs.append(quoteForPython(arg));
    }
    env.put("PYSPARK_SUBMIT_ARGS", submitArgs.toString());

    List<String> pyargs = new ArrayList<String>();
    pyargs.add(firstNonEmpty(System.getenv("PYSPARK_DRIVER_PYTHON"), "python"));
    String pyOpts = System.getenv("PYSPARK_DRIVER_PYTHON_OPTS");
    if (!isEmpty(pyOpts)) {
      pyargs.addAll(parseOptionString(pyOpts));
    }

    return pyargs;
  }

  private boolean isClientMode(Properties userProps) {
    String userMaster = firstNonEmpty(master, (String) userProps.get(SparkLauncher.SPARK_MASTER));
    // Default master is "local[*]", so assume client mode in that case.
    return userMaster == null ||
      "client".equals(deployMode) ||
      (!userMaster.equals("yarn-cluster") && deployMode == null);
  }

  private class OptionParser extends SparkSubmitOptionParser {

    @Override
    protected boolean handle(String opt, String value) {
      if (opt.equals(MASTER)) {
        master = value;
      } else if (opt.equals(DEPLOY_MODE)) {
        deployMode = value;
      } else if (opt.equals(PROPERTIES_FILE)) {
        propertiesFile = value;
      } else if (opt.equals(DRIVER_MEMORY)) {
        conf.put(SparkLauncher.DRIVER_MEMORY, value);
      } else if (opt.equals(DRIVER_JAVA_OPTIONS)) {
        conf.put(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, value);
      } else if (opt.equals(DRIVER_LIBRARY_PATH)) {
        conf.put(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH, value);
      } else if (opt.equals(DRIVER_CLASS_PATH)) {
        conf.put(SparkLauncher.DRIVER_EXTRA_CLASSPATH, value);
      } else if (opt.equals(CONF)) {
        String[] setConf = value.split("=", 2);
        checkArgument(setConf.length == 2, "Invalid argument to %s: %s", CONF, value);
        conf.put(setConf[0], setConf[1]);
      } else if (opt.equals(CLASS)) {
        // The special classes require some special command line handling, since they allow
        // mixing spark-submit arguments with arguments that should be propagated to the shell
        // itself. Note that for this to work, the "--class" argument must come before any
        // non-spark-submit arguments.
        mainClass = value;
        if (specialClasses.containsKey(value)) {
          allowsMixedArguments = true;
          appResource = specialClasses.get(value);
        }
      } else {
        sparkArgs.add(opt);
        if (value != null) {
          sparkArgs.add(value);
        }
      }
      return true;
    }

    @Override
    protected boolean handleUnknown(String opt) {
      // When mixing arguments, add unrecognized parameters directly to the user arguments list. In
      // normal mode, any unrecognized parameter triggers the end of command line parsing, and the
      // parameter itself will be interpreted by SparkSubmit as the application resource. The
      // remaining params will be appended to the list of SparkSubmit arguments.
      if (allowsMixedArguments) {
        appArgs.add(opt);
        return true;
      } else {
        sparkArgs.add(opt);
        return false;
      }
    }

    @Override
    protected void handleExtraArgs(List<String> extra) {
      for (String arg : extra) {
        sparkArgs.add(arg);
      }
    }

  }

}
