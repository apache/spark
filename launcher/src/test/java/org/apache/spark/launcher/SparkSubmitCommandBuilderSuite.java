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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

public class SparkSubmitCommandBuilderSuite {

  @Test
  public void testShellCliParser() throws Exception {
    List<String> sparkSubmitArgs = Arrays.asList(
      "--class",
      "org.apache.spark.repl.Main",
      "--master",
      "foo",
      "--app-arg",
      "bar",
      "--app-switch",
      "--files",
      "baz",
      "--name",
      "appName");

    List<String> args = new SparkSubmitCommandBuilder(sparkSubmitArgs).buildSparkSubmitArgs();
    List<String> expected = Arrays.asList("spark-shell", "--app-arg", "bar", "--app-switch");
    assertEquals(expected, args.subList(args.size() - expected.size(), args.size()));
  }

  @Test
  public void testAlternateSyntaxParsing() throws Exception {
    List<String> sparkSubmitArgs = Arrays.asList(
      "--class=org.my.Class",
      "--master=foo",
      "--deploy-mode=bar");

    List<String> cmd = new SparkSubmitCommandBuilder(sparkSubmitArgs).buildSparkSubmitArgs();
    assertEquals("org.my.Class", findArgValue(cmd, "--class"));
    assertEquals("foo", findArgValue(cmd, "--master"));
    assertEquals("bar", findArgValue(cmd, "--deploy-mode"));
  }

  @Test
  public void testPySparkLauncher() throws Exception {
    List<String> sparkSubmitArgs = Arrays.asList(
      SparkSubmitCommandBuilder.PYSPARK_SHELL,
      "--master=foo",
      "--deploy-mode=bar");

    Map<String, String> env = new HashMap<String, String>();
    List<String> cmd = buildCommand(sparkSubmitArgs, env);
    assertEquals("python", cmd.get(cmd.size() - 1));
    assertEquals("\"--master\" \"foo\" \"--deploy-mode\" \"bar\"", env.get("PYSPARK_SUBMIT_ARGS"));
  }

  @Test
  public void testPySparkFallback() throws Exception {
    List<String> sparkSubmitArgs = Arrays.asList(
      "--master=foo",
      "--deploy-mode=bar",
      "script.py",
      "arg1");

    Map<String, String> env = new HashMap<String, String>();
    List<String> cmd = buildCommand(sparkSubmitArgs, env);

    assertEquals("foo", findArgValue(cmd, "--master"));
    assertEquals("bar", findArgValue(cmd, "--deploy-mode"));
    assertEquals("script.py", cmd.get(cmd.size() - 2));
    assertEquals("arg1", cmd.get(cmd.size() - 1));
  }

  private String findArgValue(List<String> cmd, String name) {
    for (int i = 0; i < cmd.size(); i++) {
      if (cmd.get(i).equals(name)) {
        return cmd.get(i + 1);
      }
    }
    fail(String.format("arg '%s' not found", name));
    return null;
  }

  private List<String> buildCommand(List<String> args, Map<String, String> env) throws Exception {
    SparkSubmitCommandBuilder builder = new SparkSubmitCommandBuilder(args);
    builder.setSparkHome(System.getProperty("spark.test.home"));
    return builder.buildCommand(env);
  }

}
