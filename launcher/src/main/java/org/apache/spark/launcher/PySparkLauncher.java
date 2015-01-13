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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Launcher for PySpark.
 * <p/>
 * Handles parsing command line options passed to the pyspark script. This allows
 * sharing that logic with other launchers, to keep them in sync.
 */
class PySparkLauncher extends AbstractLauncher<PySparkLauncher> {

  private final List<String> args;

  PySparkLauncher(List<String> args) {
    this.args = args;
  }

  @Override
  protected List<String> buildLauncherCommand() throws IOException {
    SparkSubmitCliLauncher launcher = new SparkSubmitCliLauncher(true, args);

    // For backwards compatibility, if a script is specified in
    // the pyspark command line, then run it using spark-submit.
    if (!launcher.getAppArgs().isEmpty() && launcher.getAppArgs().get(0).endsWith(".py")) {
      System.err.println(
        "WARNING: Running python applications through 'pyspark' is deprecated as of Spark 1.0.\n" +
        "Use ./bin/spark-submit <python file>");
      return launcher.buildLauncherCommand();
    }

    // When launching the pyspark shell, the spark-submit arguments should be stored in the
    // PYSPARK_SUBMIT_ARGS env variable. The executable is the PYSPARK_DRIVER_PYTHON env variable
    // set by the pyspark script, followed by PYSPARK_DRIVER_PYTHON_OPTS.
    checkArgument(launcher.getAppArgs().isEmpty(),
      "pyspark does not support any application options.");

    Properties props = loadPropertiesFile();
    String libPath = find(DRIVER_EXTRA_LIBRARY_PATH, conf, props);

    StringBuilder submitArgs = new StringBuilder();
    for (String arg : launcher.getSparkArgs()) {
      if (submitArgs.length() > 0) {
        submitArgs.append(" ");
      }
      submitArgs.append(quote(arg));
    }
    for (String arg : launcher.getDriverArgs()) {
      if (submitArgs.length() > 0) {
        submitArgs.append(" ");
      }
      submitArgs.append(quote(arg));
    }

    Map<String, String> env = new HashMap<String, String>();
    env.put("PYSPARK_SUBMIT_ARGS", submitArgs.toString());

    List<String> pyargs = new ArrayList<String>();
    pyargs.add(System.getenv("PYSPARK_DRIVER_PYTHON"));
    String pyOpts = System.getenv("PYSPARK_DRIVER_PYTHON_OPTS");
    if (!isEmpty(pyOpts)) {
      pyargs.addAll(parseOptionString(pyOpts));
    }

    return prepareForOs(pyargs, libPath, env);
  }

  /**
   * Quotes a string so that it can be used in a command string and be parsed back into a single
   * argument by python's "shlex.split()" function.
   */
  private String quote(String s) {
    StringBuilder quoted = new StringBuilder().append('"');
    for (int i = 0; i < s.length(); i++) {
      int cp = s.codePointAt(i);
      if (cp == '"' || cp == '\\') {
        quoted.appendCodePoint('\\');
      }
      quoted.appendCodePoint(cp);
    }
    return quoted.append('"').toString();
  }

}
