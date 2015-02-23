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
   * character. On Windows, the output is single command line suitable for direct execution
   * form the script.
   */
  public static void main(String[] argsArray) throws Exception {
    checkArgument(argsArray.length > 0, "Not enough arguments: missing class name.");

    List<String> args = new ArrayList<String>(Arrays.asList(argsArray));
    String className = args.remove(0);

    boolean printLaunchCommand;
    CommandBuilder builder;
    try {
      if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
        builder = new SparkSubmitCommandBuilder(args);
      } else {
        builder = new SparkClassCommandBuilder(className, args);
      }
      printLaunchCommand = !isEmpty(System.getenv("SPARK_PRINT_LAUNCH_COMMAND"));
    } catch (IllegalArgumentException e) {
      builder = new UsageLauncher();
      printLaunchCommand = false;
    }

    Map<String, String> env = new HashMap<String, String>();
    List<String> cmd = builder.buildCommand(env);
    if (printLaunchCommand) {
      System.err.println("Spark Command: " + join(" ", cmd));
      System.err.println("========================================");
    }

    if (isWindows()) {
      System.out.println(prepareForWindows(cmd, env));
    } else {
      List<String> bashCmd = prepareForBash(cmd, env);
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
   *
   * The command is executed using "cmd /c" and formatted in single line, since that's the
   * easiest way to consume this from a batch script (see spark-class2.cmd).
   */
  private static String prepareForWindows(List<String> cmd, Map<String, String> childEnv) {
    StringBuilder cmdline = new StringBuilder("cmd /c \"");
    for (Map.Entry<String, String> e : childEnv.entrySet()) {
      cmdline.append(String.format("set %s=%s", e.getKey(), e.getValue()));
      cmdline.append(" && ");
    }
    for (String arg : cmd) {
      cmdline.append(quoteForBatchScript(arg));
      cmdline.append(" ");
    }
    cmdline.append("\"");
    return cmdline.toString();
  }

  /**
   * Prepare the command for execution from a bash script. The final command will have commands to
   * set up any needed environment variables needed by the child process.
   */
  private static List<String> prepareForBash(List<String> cmd, Map<String, String> childEnv) {
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
   * Quote a command argument for a command to be run by a Windows batch script, if the argument
   * needs quoting. Arguments only seem to need quotes in batch scripts if they have whitespace.
   */
  private static String quoteForBatchScript(String arg) {
    boolean needsQuotes = false;
    for (int i = 0; i < arg.length(); i++) {
      if (Character.isWhitespace(arg.codePointAt(i))) {
        needsQuotes = true;
        break;
      }
    }
    if (!needsQuotes) {
      return arg;
    }
    StringBuilder quoted = new StringBuilder();
    quoted.append("\"");
    for (int i = 0; i < arg.length(); i++) {
      int cp = arg.codePointAt(i);
      if (cp == '\"') {
        quoted.append("\"");
      }
      quoted.appendCodePoint(cp);
    }
    quoted.append("\"");
    return quoted.toString();
  }

  /**
   * Internal launcher used when command line parsing fails. This will behave differently depending
   * on the platform:
   *
   * - On Unix-like systems, it will print a call to the "usage" function with argument "1". The
   *   function is expected to print the command's usage and exit with the provided exit code.
   *   The script should use "export -f usage" after declaring a function called "usage", so that
   *   the function is available to downstream scripts.
   *
   * - On Windows it will set the variable "SPARK_LAUNCHER_USAGE_ERROR" to "1". The batch script
   *   should check for this variable and print its usage, since batch scripts don't really support
   *   the "export -f" functionality used in bash.
   */
  private static class UsageLauncher implements CommandBuilder {

    @Override
    public List<String> buildCommand(Map<String, String> env) {
      if (isWindows()) {
        return Arrays.asList("set SPARK_LAUNCHER_USAGE_ERROR=1");
      } else {
        return Arrays.asList("usage 1");
      }
    }

  }

}
