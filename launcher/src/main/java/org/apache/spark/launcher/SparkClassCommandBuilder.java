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
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Command builder for internal Spark classes.
 * <p>
 * This class handles building the command to launch all internal Spark classes except for
 * SparkSubmit (which is handled by {@link SparkSubmitCommandBuilder} class.
 */
class SparkClassCommandBuilder extends AbstractCommandBuilder {

  private final String className;
  private final List<String> classArgs;

  SparkClassCommandBuilder(String className, List<String> classArgs) {
    this.className = className;
    this.classArgs = classArgs;
  }

  @Override
  public List<String> buildCommand(Map<String, String> env)
      throws IOException, IllegalArgumentException {
    List<String> javaOptsKeys = new ArrayList<>();
    String memKey = null;
    String extraClassPath = null;

    // Master, Worker, HistoryServer, ExternalShuffleService, MesosClusterDispatcher use
    // SPARK_DAEMON_JAVA_OPTS (and specific opts) + SPARK_DAEMON_MEMORY.
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
      javaOptsKeys.add("SPARK_EXECUTOR_OPTS");
      memKey = "SPARK_EXECUTOR_MEMORY";
    } else if (className.equals("org.apache.spark.executor.MesosExecutorBackend")) {
      javaOptsKeys.add("SPARK_EXECUTOR_OPTS");
      memKey = "SPARK_EXECUTOR_MEMORY";
    } else if (className.equals("org.apache.spark.deploy.mesos.MesosClusterDispatcher")) {
      javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
    } else if (className.equals("org.apache.spark.deploy.ExternalShuffleService") ||
        className.equals("org.apache.spark.deploy.mesos.MesosExternalShuffleService")) {
      javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
      javaOptsKeys.add("SPARK_SHUFFLE_OPTS");
      memKey = "SPARK_DAEMON_MEMORY";
    } else {
      memKey = "SPARK_DRIVER_MEMORY";
    }

    List<String> cmd = buildJavaCommand(extraClassPath);

    for (String key : javaOptsKeys) {
      String envValue = System.getenv(key);
      if (!isEmpty(envValue) && envValue.contains("Xmx")) {
        String msg = String.format("%s is not allowed to specify max heap(Xmx) memory settings " +
                "(was %s). Use the corresponding configuration instead.", key, envValue);
        throw new IllegalArgumentException(msg);
      }
      addOptionString(cmd, envValue);
    }

    String mem = firstNonEmpty(memKey != null ? System.getenv(memKey) : null, DEFAULT_MEM);
    cmd.add("-Xmx" + mem);
    addPermGenSizeOpt(cmd);
    cmd.add(className);
    cmd.addAll(classArgs);
    return cmd;
  }

}
