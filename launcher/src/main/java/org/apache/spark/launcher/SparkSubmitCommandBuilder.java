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
import java.util.*;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Special command builder for handling a CLI invocation of SparkSubmit.
 * <p>
 * This builder adds command line parsing compatible with SparkSubmit. It handles setting
 * driver-side options and special parsing behavior needed for the special-casing certain internal
 * Spark applications.
 * <p>
 * This class has also some special features to aid launching shells (pyspark and sparkR) and also
 * examples.
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
   * Name of the app resource used to identify the SparkR shell. The command line parser expects
   * the resource name to be the very first argument to spark-submit in this case.
   *
   * NOTE: this cannot be "sparkr-shell" since that identifies the SparkR shell to SparkSubmit
   * (see sparkR.R), and can cause this code to enter into an infinite loop.
   */
  static final String SPARKR_SHELL = "sparkr-shell-main";

  /**
   * This is the actual resource name that identifies the SparkR shell to SparkSubmit.
   */
  static final String SPARKR_SHELL_RESOURCE = "sparkr-shell";

  /**
   * Name of app resource used to identify examples. When running examples, args[0] should be
   * this name. The app resource will identify the example class to run.
   */
  static final String RUN_EXAMPLE = "run-example";

  /**
   * Prefix for example class names.
   */
  static final String EXAMPLE_CLASS_PREFIX = "org.apache.spark.examples.";

  /**
   * This map must match the class names for available special classes, since this modifies the way
   * command line parsing works. This maps the class name to the resource to use when calling
   * spark-submit.
   */
  private static final Map<String, String> specialClasses = new HashMap<>();
  static {
    specialClasses.put("org.apache.spark.repl.Main", "spark-shell");
    specialClasses.put("org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver",
      SparkLauncher.NO_RESOURCE);
    specialClasses.put("org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
      SparkLauncher.NO_RESOURCE);
  }

  final List<String> userArgs;
  private final List<String> parsedArgs;
  // Special command means no appResource and no mainClass required
  private final boolean isSpecialCommand;
  private final boolean isExample;

  /**
   * Controls whether mixing spark-submit arguments with app arguments is allowed. This is needed
   * to parse the command lines for things like bin/spark-shell, which allows users to mix and
   * match arguments (e.g. "bin/spark-shell SparkShellArg --master foo").
   */
  private boolean allowsMixedArguments;

  /**
   * This constructor is used when creating a user-configurable launcher. It allows the
   * spark-submit argument list to be modified after creation.
   */
  SparkSubmitCommandBuilder() {
    this.isSpecialCommand = false;
    this.isExample = false;
    this.parsedArgs = new ArrayList<>();
    this.userArgs = new ArrayList<>();
  }

  /**
   * This constructor is used when invoking spark-submit; it parses and validates arguments
   * provided by the user on the command line.
   */
  SparkSubmitCommandBuilder(List<String> args) {
    this.allowsMixedArguments = false;
    this.parsedArgs = new ArrayList<>();
    boolean isExample = false;
    List<String> submitArgs = args;
    this.userArgs = Collections.emptyList();

    if (args.size() > 0) {
      switch (args.get(0)) {
        case PYSPARK_SHELL:
          this.allowsMixedArguments = true;
          appResource = PYSPARK_SHELL;
          submitArgs = args.subList(1, args.size());
          break;

        case SPARKR_SHELL:
          this.allowsMixedArguments = true;
          appResource = SPARKR_SHELL;
          submitArgs = args.subList(1, args.size());
          break;

        case RUN_EXAMPLE:
          isExample = true;
          appResource = findExamplesAppJar();
          submitArgs = args.subList(1, args.size());
      }

      this.isExample = isExample;
      OptionParser parser = new OptionParser(true);
      parser.parse(submitArgs);
      this.isSpecialCommand = parser.isSpecialCommand;
    } else {
      this.isExample = isExample;
      this.isSpecialCommand = true;
    }
  }

  @Override
  public List<String> buildCommand(Map<String, String> env)
      throws IOException, IllegalArgumentException {
    if (PYSPARK_SHELL.equals(appResource) && !isSpecialCommand) {
      return buildPySparkShellCommand(env);
    } else if (SPARKR_SHELL.equals(appResource) && !isSpecialCommand) {
      return buildSparkRCommand(env);
    } else {
      return buildSparkSubmitCommand(env);
    }
  }

  List<String> buildSparkSubmitArgs() {
    List<String> args = new ArrayList<>();
    OptionParser parser = new OptionParser(false);
    final boolean isSpecialCommand;

    // If the user args array is not empty, we need to parse it to detect exactly what
    // the user is trying to run, so that checks below are correct.
    if (!userArgs.isEmpty()) {
      parser.parse(userArgs);
      isSpecialCommand = parser.isSpecialCommand;
    } else {
      isSpecialCommand = this.isSpecialCommand;
    }

    if (!allowsMixedArguments && !isSpecialCommand) {
      checkArgument(appResource != null, "Missing application resource.");
    }

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

    if (isExample) {
      jars.addAll(findExamplesJars());
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

    if (isExample && !isSpecialCommand) {
      checkArgument(mainClass != null, "Missing example class name.");
    }

    if (mainClass != null) {
      args.add(parser.CLASS);
      args.add(mainClass);
    }

    args.addAll(parsedArgs);

    if (appResource != null) {
      args.add(appResource);
    }

    args.addAll(appArgs);

    return args;
  }

  private List<String> buildSparkSubmitCommand(Map<String, String> env)
      throws IOException, IllegalArgumentException {
    // Load the properties file and check whether spark-submit will be running the app's driver
    // or just launching a cluster app. When running the driver, the JVM's argument will be
    // modified to cover the driver's configuration.
    Map<String, String> config = getEffectiveConfig();
    boolean isClientMode = isClientMode(config);
    String extraClassPath = isClientMode ? config.get(SparkLauncher.DRIVER_EXTRA_CLASSPATH) : null;

    List<String> cmd = buildJavaCommand(extraClassPath);
    // Take Thrift Server as daemon
    if (isThriftServer(mainClass)) {
      addOptionString(cmd, System.getenv("SPARK_DAEMON_JAVA_OPTS"));
    }
    addOptionString(cmd, System.getenv("SPARK_SUBMIT_OPTS"));

    // We don't want the client to specify Xmx. These have to be set by their corresponding
    // memory flag --driver-memory or configuration entry spark.driver.memory
    String driverDefaultJavaOptions = config.get(SparkLauncher.DRIVER_DEFAULT_JAVA_OPTIONS);
    checkJavaOptions(driverDefaultJavaOptions);
    String driverExtraJavaOptions = config.get(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS);
    checkJavaOptions(driverExtraJavaOptions);

    if (isClientMode) {
      // Figuring out where the memory value come from is a little tricky due to precedence.
      // Precedence is observed in the following order:
      // - explicit configuration (setConf()), which also covers --driver-memory cli argument.
      // - properties file.
      // - SPARK_DRIVER_MEMORY env variable
      // - SPARK_MEM env variable
      // - default value (1g)
      // Take Thrift Server as daemon
      String tsMemory =
        isThriftServer(mainClass) ? System.getenv("SPARK_DAEMON_MEMORY") : null;
      String memory = firstNonEmpty(tsMemory, config.get(SparkLauncher.DRIVER_MEMORY),
        System.getenv("SPARK_DRIVER_MEMORY"), System.getenv("SPARK_MEM"), DEFAULT_MEM);
      cmd.add("-Xmx" + memory);
      addOptionString(cmd, driverDefaultJavaOptions);
      addOptionString(cmd, driverExtraJavaOptions);
      mergeEnvPathList(env, getLibPathEnvName(),
        config.get(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH));
    }

    // SPARK-36796: Always add default `--add-opens` to submit command
    addOptionString(cmd, JavaModuleOptions.defaultModuleOptions());
    cmd.add("org.apache.spark.deploy.SparkSubmit");
    cmd.addAll(buildSparkSubmitArgs());
    return cmd;
  }

  private void checkJavaOptions(String javaOptions) {
    if (!isEmpty(javaOptions) && javaOptions.contains("Xmx")) {
      String msg = String.format("Not allowed to specify max heap(Xmx) memory settings through " +
        "java options (was %s). Use the corresponding --driver-memory or " +
        "spark.driver.memory configuration instead.", javaOptions);
      throw new IllegalArgumentException(msg);
    }
  }

  private List<String> buildPySparkShellCommand(Map<String, String> env) throws IOException {
    // For backwards compatibility, if a script is specified in
    // the pyspark command line, then run it using spark-submit.
    if (!appArgs.isEmpty() && appArgs.get(0).endsWith(".py")) {
      System.err.println(
        "Running python applications through 'pyspark' is not supported as of Spark 2.0.\n" +
        "Use ./bin/spark-submit <python file>");
      System.exit(-1);
    }

    checkArgument(appArgs.isEmpty(), "pyspark does not support any application options.");

    // When launching the pyspark shell, the spark-submit arguments should be stored in the
    // PYSPARK_SUBMIT_ARGS env variable.
    appResource = PYSPARK_SHELL_RESOURCE;
    constructEnvVarArgs(env, "PYSPARK_SUBMIT_ARGS");

    // Will pick up the binary executable in the following order
    // 1. conf spark.pyspark.driver.python
    // 2. conf spark.pyspark.python
    // 3. environment variable PYSPARK_DRIVER_PYTHON
    // 4. environment variable PYSPARK_PYTHON
    // 5. python
    List<String> pyargs = new ArrayList<>();
    pyargs.add(firstNonEmpty(conf.get(SparkLauncher.PYSPARK_DRIVER_PYTHON),
      conf.get(SparkLauncher.PYSPARK_PYTHON),
      System.getenv("PYSPARK_DRIVER_PYTHON"),
      System.getenv("PYSPARK_PYTHON"),
      "python3"));
    String pyOpts = System.getenv("PYSPARK_DRIVER_PYTHON_OPTS");
    if (conf.containsKey(SparkLauncher.PYSPARK_PYTHON)) {
      // pass conf spark.pyspark.python to python by environment variable.
      env.put("PYSPARK_PYTHON", conf.get(SparkLauncher.PYSPARK_PYTHON));
    }
    if (!isEmpty(pyOpts)) {
      pyargs.addAll(parseOptionString(pyOpts));
    }

    return pyargs;
  }

  private List<String> buildSparkRCommand(Map<String, String> env) throws IOException {
    if (!appArgs.isEmpty() && (appArgs.get(0).endsWith(".R") || appArgs.get(0).endsWith(".r"))) {
      System.err.println(
        "Running R applications through 'sparkR' is not supported as of Spark 2.0.\n" +
        "Use ./bin/spark-submit <R file>");
      System.exit(-1);
    }
    // When launching the SparkR shell, store the spark-submit arguments in the SPARKR_SUBMIT_ARGS
    // env variable.
    appResource = SPARKR_SHELL_RESOURCE;
    constructEnvVarArgs(env, "SPARKR_SUBMIT_ARGS");

    // Set shell.R as R_PROFILE_USER to load the SparkR package when the shell comes up.
    String sparkHome = System.getenv("SPARK_HOME");
    env.put("R_PROFILE_USER",
            join(File.separator, sparkHome, "R", "lib", "SparkR", "profile", "shell.R"));

    List<String> args = new ArrayList<>();
    args.add(firstNonEmpty(conf.get(SparkLauncher.SPARKR_R_SHELL),
      System.getenv("SPARKR_DRIVER_R"), "R"));
    return args;
  }

  private void constructEnvVarArgs(
      Map<String, String> env,
      String submitArgsEnvVariable) throws IOException {
    mergeEnvPathList(env, getLibPathEnvName(),
      getEffectiveConfig().get(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH));

    StringBuilder submitArgs = new StringBuilder();
    for (String arg : buildSparkSubmitArgs()) {
      if (submitArgs.length() > 0) {
        submitArgs.append(" ");
      }
      submitArgs.append(quoteForCommandString(arg));
    }
    env.put(submitArgsEnvVariable, submitArgs.toString());
  }

  boolean isClientMode(Map<String, String> userProps) {
    String userMaster = firstNonEmpty(master, userProps.get(SparkLauncher.SPARK_MASTER));
    String userDeployMode = firstNonEmpty(deployMode, userProps.get(SparkLauncher.DEPLOY_MODE));
    // Default master is "local[*]", so assume client mode in that case
    return userMaster == null || userDeployMode == null || "client".equals(userDeployMode);
  }

  /**
   * Return whether the given main class represents a thrift server.
   */
  private boolean isThriftServer(String mainClass) {
    return (mainClass != null &&
      mainClass.equals("org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"));
  }

  private String findExamplesAppJar() {
    boolean isTesting = "1".equals(getenv("SPARK_TESTING"));
    if (isTesting) {
      return SparkLauncher.NO_RESOURCE;
    } else {
      for (String exampleJar : findExamplesJars()) {
        if (new File(exampleJar).getName().startsWith("spark-examples")) {
          return exampleJar;
        }
      }
      throw new IllegalStateException("Failed to find examples' main app jar.");
    }
  }

  private List<String> findExamplesJars() {
    boolean isTesting = "1".equals(getenv("SPARK_TESTING"));
    List<String> examplesJars = new ArrayList<>();
    String sparkHome = getSparkHome();

    File jarsDir;
    if (new File(sparkHome, "RELEASE").isFile()) {
      jarsDir = new File(sparkHome, "examples/jars");
    } else {
      jarsDir = new File(sparkHome,
        String.format("examples/target/scala-%s/jars", getScalaVersion()));
    }

    boolean foundDir = jarsDir.isDirectory();
    checkState(isTesting || foundDir, "Examples jars directory '%s' does not exist.",
        jarsDir.getAbsolutePath());

    if (foundDir) {
      for (File f: jarsDir.listFiles()) {
        examplesJars.add(f.getAbsolutePath());
      }
    }
    return examplesJars;
  }

  private class OptionParser extends SparkSubmitOptionParser {

    boolean isSpecialCommand = false;
    private final boolean errorOnUnknownArgs;

    OptionParser(boolean errorOnUnknownArgs) {
      this.errorOnUnknownArgs = errorOnUnknownArgs;
    }

    @Override
    protected boolean handle(String opt, String value) {
      switch (opt) {
        case MASTER:
          master = value;
          break;
        case DEPLOY_MODE:
          deployMode = value;
          break;
        case PROPERTIES_FILE:
          propertiesFile = value;
          break;
        case DRIVER_MEMORY:
          conf.put(SparkLauncher.DRIVER_MEMORY, value);
          break;
        case DRIVER_JAVA_OPTIONS:
          conf.put(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, value);
          break;
        case DRIVER_LIBRARY_PATH:
          conf.put(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH, value);
          break;
        case DRIVER_CLASS_PATH:
          conf.put(SparkLauncher.DRIVER_EXTRA_CLASSPATH, value);
          break;
        case CONF:
          checkArgument(value != null, "Missing argument to %s", CONF);
          String[] setConf = value.split("=", 2);
          checkArgument(setConf.length == 2, "Invalid argument to %s: %s", CONF, value);
          conf.put(setConf[0], setConf[1]);
          break;
        case CLASS:
          // The special classes require some special command line handling, since they allow
          // mixing spark-submit arguments with arguments that should be propagated to the shell
          // itself. Note that for this to work, the "--class" argument must come before any
          // non-spark-submit arguments.
          mainClass = value;
          if (specialClasses.containsKey(value)) {
            allowsMixedArguments = true;
            appResource = specialClasses.get(value);
          }
          break;
        case KILL_SUBMISSION:
        case STATUS:
          isSpecialCommand = true;
          parsedArgs.add(opt);
          parsedArgs.add(value);
          break;
        case HELP:
        case USAGE_ERROR:
        case VERSION:
          isSpecialCommand = true;
          parsedArgs.add(opt);
          break;
        default:
          parsedArgs.add(opt);
          if (value != null) {
            parsedArgs.add(value);
          }
          break;
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
      } else if (isExample) {
        String className = opt;
        if (!className.startsWith(EXAMPLE_CLASS_PREFIX)) {
          className = EXAMPLE_CLASS_PREFIX + className;
        }
        mainClass = className;
        appResource = findExamplesAppJar();
        return false;
      } else if (errorOnUnknownArgs) {
        checkArgument(!opt.startsWith("-"), "Unrecognized option: %s", opt);
        checkState(appResource == null, "Found unrecognized argument but resource is already set.");
        appResource = opt;
        return false;
      }
      return true;
    }

    @Override
    protected void handleExtraArgs(List<String> extra) {
      appArgs.addAll(extra);
    }

  }

}
