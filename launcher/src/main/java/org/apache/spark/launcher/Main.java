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
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Command line interface for the Spark launcher. Used internally by Spark scripts.
 */
class Main {

  /**
   * Usage: Main [class] [class args]
   * <p>
   * This CLI works in two different modes:
   * <ul>
   *   <li>"spark-submit": if <i>class</i> is "org.apache.spark.deploy.SparkSubmit", the
   *   {@link SparkLauncher} class is used to launch a Spark application.</li>
   *   <li>"spark-class": if another class is provided, an internal Spark class is run.</li>
   * </ul>
   *
   * This class works in tandem with the "bin/spark-class" script on Unix-like systems, and
   * "bin/spark-class2.cmd" batch script on Windows to execute the final command.
   * <p>
   * On Unix-like systems, the output is a list of command arguments, separated by the NULL
   * character. On Windows, the output is a command line suitable for direct execution from the
   * script.
   */
  public static void main(String[] argsArray) throws Exception {
    checkArgument(argsArray.length > 0, "Not enough arguments: missing class name.");

    List<String> args = new ArrayList<>(Arrays.asList(argsArray));
    String className = args.remove(0);

    boolean printLaunchCommand = !isEmpty(System.getenv("SPARK_PRINT_LAUNCH_COMMAND"));
    AbstractCommandBuilder builder;
    if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
      try {
        builder = new SparkSubmitCommandBuilder(args);
      } catch (IllegalArgumentException e) {
        printLaunchCommand = false;
        System.err.println("Error: " + e.getMessage());
        System.err.println();

        MainClassOptionParser parser = new MainClassOptionParser();
        try {
          parser.parse(args);
        } catch (Exception ignored) {
          // Ignore parsing exceptions.
        }

        List<String> help = new ArrayList<>();
        if (parser.className != null) {
          help.add(parser.CLASS);
          help.add(parser.className);
        }
        help.add(parser.USAGE_ERROR);
        builder = new SparkSubmitCommandBuilder(help);
      }
    } else {
      builder = new SparkClassCommandBuilder(className, args);
    }

    Map<String, String> env = new HashMap<>();
    List<String> cmd = new ArrayList<>();
    try {
      cmd = builder.buildCommand(env);
    } catch (IllegalArgumentException e) {
      if (argsArray.length == 1) {
        printUsage();
      } else {
        System.err.println(e.getMessage());
      }
    }
    if (printLaunchCommand) {
      System.err.println("Spark Command: " + join(" ", cmd));
      System.err.println("========================================");
    }

    if (isWindows()) {
      System.out.println(prepareWindowsCommand(cmd, env));
    } else {
      // In bash, use NULL as the arg separator since it cannot be used in an argument.
      List<String> bashCmd = prepareBashCommand(cmd, env);
      for (String c : bashCmd) {
        System.out.print(c);
        System.out.print('\0');
      }
    }
  }

  private static void printUsage() {
    String usage = "Usage: spark-submit [options] <app jar | python file> [app arguments]\n" +
    "Usage: spark-submit --kill [submission ID] --master [spark://...]\n" +
    "Usage: spark-submit --status [submission ID] --master [spark://...]\n" +
    "Usage: spark-submit run-example [options] example-class [example args]\n";
    System.err.println(usage);
    String mem = CommandBuilderUtils.DEFAULT_MEM;
    String options =
    "Options:\n"+
    "  --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.\n" +
    "  --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally (\"client\") or\n" +
    "                              on one of the worker machines inside the cluster (\"cluster\")\n" +
    "                              (Default: client).\n" +
    "  --class CLASS_NAME          Your application's main class (for Java / Scala apps).\n" +
    "  --name NAME                 A name of your application.\n" +
    "  --jars JARS                 Comma-separated list of local jars to include on the driver\n" +
    "                              and executor classpaths.\n" +
    "  --packages                  Comma-separated list of maven coordinates of jars to include\n" +
    "                              on the driver and executor classpaths. Will search the local\n" +
    "                              maven repo, then maven central and any additional remote\n" +
    "                              repositories given by --repositories. The format for the\n" +
    "                              coordinates should be groupId:artifactId:version.\n" +
    "  --exclude-packages          Comma-separated list of groupId:artifactId, to exclude while\n" +
    "                              resolving the dependencies provided in --packages to avoid\n" +
    "                              dependency conflicts.\n" +
    "  --repositories              Comma-separated list of additional remote repositories to\n" +
    "                              search for the maven coordinates given with --packages.\n" +
    "  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place\n" +
    "                              on the PYTHONPATH for Python apps.\n" +
    "  --files FILES               Comma-separated list of files to be placed in the working\n" +
    "                              directory of each executor.\n\n" +
    "  --conf PROP=VALUE           Arbitrary Spark configuration property.\n" +
    "  --properties-file FILE      Path to a file from which to load extra properties. If not\n" +
    "                              specified, this will look for conf/spark-defaults.conf.\n\n" +
    "  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: " + mem + ").\n" +
    "  --driver-java-options       Extra Java options to pass to the driver.\n" +
    "  --driver-library-path       Extra library path entries to pass to the driver.\n" +
    "  --driver-class-path         Extra class path entries to pass to the driver. Note that\n" +
    "                              jars added with --jars are automatically included in the\n" +
    "                              classpath.\n\n" +
    "  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).\n\n" +
    "  --proxy-user NAME           User to impersonate when submitting the application.\n" +
    "                              This argument does not work with --principal / --keytab.\n\n" +
    "  --help, -h                  Show this help message and exit.\n" +
    "  --verbose, -v               Print additional debug output.\n" +
    "  --version,                  Print the version of current Spark.\n\n" +
    " Spark standalone with cluster deploy mode only:\n" +
    "  --driver-cores NUM          Cores for driver (Default: 1).\n\n" +
    " Spark standalone or Mesos with cluster deploy mode only:\n" +
    "  --supervise                 If given, restarts the driver on failure.\n" +
    "  --kill SUBMISSION_ID        If given, kills the driver specified.\n" +
    "  --status SUBMISSION_ID      If given, requests the status of the driver specified.\n\n"+
    " Spark standalone and Mesos only:\n" +
    "  --total-executor-cores      NUM Total cores for all executors.\n\n" +
    " Spark standalone and YARN only:\n" +
    "  --executor-cores NUM        Number of cores per executor. (Default: 1 in YARN mode,\n" +
    "                              or all available cores on the worker in standalone mode)\n\n" +
    " YARN-only:\n" +
    "  --driver-cores NUM          Number of cores used by the driver, only in cluster mode\n" +
    "                              (Default: 1).\n" +
    "  --queue QUEUE_NAME          The YARN queue to submit to (Default: \"default\").\n" +
    "  --num-executors NUM         Number of executors to launch (Default: 2).\n" +
    "  --archives ARCHIVES         Comma separated list of archives to be extracted into the\n" +
    "                              working directory of each executor.\n" +
    "  --principal PRINCIPAL       Principal to be used to login to KDC, while running on\n" +
    "                              secure HDFS.\n" +
    "  --keytab KEYTAB             The full path to the file that contains the keytab for the\n" +
    "                              principal specified above. This keytab will be copied to\n" +
    "                              the node running the Application Master via the Secure\n" +
    "                              Distributed Cache, for renewing the login tickets and the\n" +
    "                              delegation tokens periodically.\n";
    System.err.println(options);
  }

  /**
   * Prepare a command line for execution from a Windows batch script.
   *
   * The method quotes all arguments so that spaces are handled as expected. Quotes within arguments
   * are "double quoted" (which is batch for escaping a quote). This page has more details about
   * quoting and other batch script fun stuff: http://ss64.com/nt/syntax-esc.html
   */
  private static String prepareWindowsCommand(List<String> cmd, Map<String, String> childEnv) {
    StringBuilder cmdline = new StringBuilder();
    for (Map.Entry<String, String> e : childEnv.entrySet()) {
      cmdline.append(String.format("set %s=%s", e.getKey(), e.getValue()));
      cmdline.append(" && ");
    }
    for (String arg : cmd) {
      cmdline.append(quoteForBatchScript(arg));
      cmdline.append(" ");
    }
    return cmdline.toString();
  }

  /**
   * Prepare the command for execution from a bash script. The final command will have commands to
   * set up any needed environment variables needed by the child process.
   */
  private static List<String> prepareBashCommand(List<String> cmd, Map<String, String> childEnv) {
    if (childEnv.isEmpty()) {
      return cmd;
    }

    List<String> newCmd = new ArrayList<>();
    newCmd.add("env");

    for (Map.Entry<String, String> e : childEnv.entrySet()) {
      newCmd.add(String.format("%s=%s", e.getKey(), e.getValue()));
    }
    newCmd.addAll(cmd);
    return newCmd;
  }

  /**
   * A parser used when command line parsing fails for spark-submit. It's used as a best-effort
   * at trying to identify the class the user wanted to invoke, since that may require special
   * usage strings (handled by SparkSubmitArguments).
   */
  private static class MainClassOptionParser extends SparkSubmitOptionParser {

    String className;

    @Override
    protected boolean handle(String opt, String value) {
      if (CLASS.equals(opt)) {
        className = value;
      }
      return false;
    }

    @Override
    protected boolean handleUnknown(String opt) {
      return false;
    }

    @Override
    protected void handleExtraArgs(List<String> extra) {

    }

  }

}
