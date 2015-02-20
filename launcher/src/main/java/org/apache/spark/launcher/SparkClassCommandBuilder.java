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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Command builder for internal Spark classes.
 * <p/>
 * This class handles building the command to launch all internal Spark classes except for
 * SparkSubmit (which is handled by {@link SparkSubmitCommandBuilder} class.
 */
class SparkClassCommandBuilder extends SparkLauncher implements CommandBuilder {

  private final String className;
  private final List<String> classArgs;

  SparkClassCommandBuilder(String className, List<String> classArgs) {
    this.className = className;
    this.classArgs = classArgs;
  }

  @Override
  public List<String> buildCommand(Map<String, String> env) throws IOException {
    List<String> javaOptsKeys = new ArrayList<String>();
    String memKey = null;
    String extraClassPath = null;

    // Master, Worker, and HistoryServer use SPARK_DAEMON_JAVA_OPTS (and specific opts) +
    // SPARK_DAEMON_MEMORY.
    if (className.equals("org.apache.spark.deploy.master.Master")) {
      javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
      javaOptsKeys.add("SPARK_MASTER_OPTS");
      memKey = "SPARK_DAEMON_MEMORY";
    } else if (className.equals("org.apache.spark.deploy.worker.Worker")) {
      javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
      javaOptsKeys.add("SPARK_WORKER_OPTS");
      memKey = "SPARK_DAEMON_MEMORY";
    } else if (className.equals("org.apache.spark.deploy.history.HistoryServer")) {
      javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
      javaOptsKeys.add("SPARK_HISTORY_OPTS");
      memKey = "SPARK_DAEMON_MEMORY";
    } else if (className.equals("org.apache.spark.executor.CoarseGrainedExecutorBackend")) {
      javaOptsKeys.add("SPARK_JAVA_OPTS");
      javaOptsKeys.add("SPARK_EXECUTOR_OPTS");
      memKey = "SPARK_EXECUTOR_MEMORY";
    } else if (className.equals("org.apache.spark.executor.MesosExecutorBackend")) {
      javaOptsKeys.add("SPARK_EXECUTOR_OPTS");
      memKey = "SPARK_EXECUTOR_MEMORY";
    } else if (className.startsWith("org.apache.spark.tools.")) {
      String sparkHome = getSparkHome();
      File toolsDir = new File(join(File.separator, sparkHome, "tools", "target",
        "scala-" + getScalaVersion()));
      checkState(toolsDir.isDirectory(), "Cannot find tools build directory.");

      Pattern re = Pattern.compile("spark-tools_.*\\.jar");
      for (File f : toolsDir.listFiles()) {
        if (re.matcher(f.getName()).matches()) {
          extraClassPath = f.getAbsolutePath();
          break;
        }
      }

      checkState(extraClassPath != null,
        "Failed to find Spark Tools Jar in %s.\n" +
        "You need to run \"build/sbt tools/package\" before running %s.",
        toolsDir.getAbsolutePath(), className);

      javaOptsKeys.add("SPARK_JAVA_OPTS");
    } else {
      // Any classes not explicitly listed above are submitted using SparkSubmit.
      return createSparkSubmitCommand(env);
    }

    List<String> cmd = buildJavaCommand(extraClassPath);
    for (String key : javaOptsKeys) {
      addOptionString(cmd, System.getenv(key));
    }

    String mem = firstNonEmpty(memKey != null ? System.getenv(memKey) : null, DEFAULT_MEM);
    cmd.add("-Xms" + mem);
    cmd.add("-Xmx" + mem);
    addPermGenSizeOpt(cmd);
    cmd.add(className);
    cmd.addAll(classArgs);
    return cmd;
  }

  private List<String> createSparkSubmitCommand(Map<String, String> env) throws IOException {
    final List<String> sparkSubmitArgs = new ArrayList<String>();
    final List<String> appArgs = new ArrayList<String>();

    // Parse the command line and special-case the HELP command line argument, allowing it to be
    // propagated to the app being launched.
    SparkSubmitOptionParser parser = new SparkSubmitOptionParser() {

      @Override
      protected boolean handle(String opt, String value) {
        if (opt.equals(HELP)) {
          appArgs.add(opt);
        } else {
          sparkSubmitArgs.add(opt);
          sparkSubmitArgs.add(value);
        }
        return true;
      }

      @Override
      protected boolean handleUnknown(String opt) {
        appArgs.add(opt);
        return true;
      }

      @Override
      protected void handleExtraArgs(List<String> extra) {
        appArgs.addAll(extra);
      }

    };

    parser.parse(classArgs);
    sparkSubmitArgs.add(parser.CLASS);
    sparkSubmitArgs.add(className);

    SparkSubmitCommandBuilder builder = new SparkSubmitCommandBuilder(true, sparkSubmitArgs);
    builder.setAppResource("spark-internal");
    for (String arg: appArgs) {
      builder.addAppArgs(arg);
    }
    return builder.buildCommand(env);
  }

}
