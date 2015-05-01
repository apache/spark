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
   * <p/>
   * This CLI works in two different modes:
   * <ul>
   *   <li>"spark-submit": if <i>class</i> is "org.apache.spark.deploy.SparkSubmit", the
   *   {@link SparkLauncher} class is used to launch a Spark application.</li>
   *   <li>"spark-class": if another class is provided, an internal Spark class is run.</li>
   * </ul>
   *
   * This class works in tandem with the "bin/spark-class" script on Unix-like systems, and
   * "bin/spark-class2.cmd" batch script on Windows to execute the final command.
   * <p/>
   * On Unix-like systems, the output is a list of command arguments, separated by the NULL
   * character. On Windows, the output is a command line suitable for direct execution from the
   * script.
   */
  public static void main(String[] argsArray) throws Exception {
    checkArgument(argsArray.length > 0, "Not enough arguments: missing class name.");

    List<String> args = new ArrayList<String>(Arrays.asList(argsArray));
    String className = args.remove(0);

    boolean printLaunchCommand;
    boolean printUsage;
    AbstractCommandBuilder builder;
    try {
      if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
        builder = new SparkSubmitCommandBuilder(args);
      } else {
        builder = new SparkClassCommandBuilder(className, args);
      }
      printLaunchCommand = !isEmpty(System.getenv("SPARK_PRINT_LAUNCH_COMMAND"));
      printUsage = false;
    } catch (IllegalArgumentException e) {
      builder = new UsageCommandBuilder(e.getMessage());
      printLaunchCommand = false;
      printUsage = true;
    }

    Map<String, String> env = new HashMap<String, String>();
    List<String> cmd = builder.buildCommand(env);
    if (printLaunchCommand) {
      System.err.println("Spark Command: " + join(" ", cmd));
      System.err.println("========================================");
    }

    if (isWindows()) {
      // When printing the usage message, we can't use "cmd /v" since that prevents the env
      // variable from being seen in the caller script. So do not call prepareWindowsCommand().
      if (printUsage) {
        System.out.println(join(" ", cmd));
      } else {
        System.out.println(prepareWindowsCommand(cmd, env));
      }
    } else {
      // In bash, use NULL as the arg separator since it cannot be used in an argument.
      List<String> bashCmd = prepareBashCommand(cmd, env);
      for (String c : bashCmd) {
        System.out.print(c);
        System.out.print('\0');
      }
    }
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

    List<String> newCmd = new ArrayList<String>();
    newCmd.add("env");

    for (Map.Entry<String, String> e : childEnv.entrySet()) {
      newCmd.add(String.format("%s=%s", e.getKey(), e.getValue()));
    }
    newCmd.addAll(cmd);
    return newCmd;
  }

  /**
   * Internal builder used when command line parsing fails. This will behave differently depending
   * on the platform:
   *
   * - On Unix-like systems, it will print a call to the "usage" function with two arguments: the
   *   the error string, and the exit code to use. The function is expected to print the command's
   *   usage and exit with the provided exit code. The script should use "export -f usage" after
   *   declaring a function called "usage", so that the function is available to downstream scripts.
   *
   * - On Windows it will set the variable "SPARK_LAUNCHER_USAGE_ERROR" to the usage error message.
   *   The batch script should check for this variable and print its usage, since batch scripts
   *   don't really support the "export -f" functionality used in bash.
   */
  private static class UsageCommandBuilder extends AbstractCommandBuilder {

    private final String message;

    UsageCommandBuilder(String message) {
      this.message = message;
    }

    @Override
    public List<String> buildCommand(Map<String, String> env) {
      if (isWindows()) {
        return Arrays.asList("set", "SPARK_LAUNCHER_USAGE_ERROR=" + message);
      } else {
        return Arrays.asList("usage", message, "1");
      }
    }

  }

}
