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
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

public class CommandBuilderUtilsSuite {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void testValidOptionStrings() {
    testOpt("a b c d e", Arrays.asList("a", "b", "c", "d", "e"));
    testOpt("a 'b c' \"d\" e", Arrays.asList("a", "b c", "d", "e"));
    testOpt("a 'b\\\"c' \"'d'\" e", Arrays.asList("a", "b\\\"c", "'d'", "e"));
    testOpt("a 'b\"c' \"\\\"d\\\"\" e", Arrays.asList("a", "b\"c", "\"d\"", "e"));
    testOpt(" a b c \\\\ ", Arrays.asList("a", "b", "c", "\\"));

    // Following tests ported from UtilsSuite.scala.
    testOpt("", new ArrayList<>());
    testOpt("a", Arrays.asList("a"));
    testOpt("aaa", Arrays.asList("aaa"));
    testOpt("a b c", Arrays.asList("a", "b", "c"));
    testOpt("  a   b\t c ", Arrays.asList("a", "b", "c"));
    testOpt("a 'b c'", Arrays.asList("a", "b c"));
    testOpt("a 'b c' d", Arrays.asList("a", "b c", "d"));
    testOpt("'b c'", Arrays.asList("b c"));
    testOpt("a \"b c\"", Arrays.asList("a", "b c"));
    testOpt("a \"b c\" d", Arrays.asList("a", "b c", "d"));
    testOpt("\"b c\"", Arrays.asList("b c"));
    testOpt("a 'b\" c' \"d' e\"", Arrays.asList("a", "b\" c", "d' e"));
    testOpt("a\t'b\nc'\nd", Arrays.asList("a", "b\nc", "d"));
    testOpt("a \"b\\\\c\"", Arrays.asList("a", "b\\c"));
    testOpt("a \"b\\\"c\"", Arrays.asList("a", "b\"c"));
    testOpt("a 'b\\\"c'", Arrays.asList("a", "b\\\"c"));
    testOpt("'a'b", Arrays.asList("ab"));
    testOpt("'a''b'", Arrays.asList("ab"));
    testOpt("\"a\"b", Arrays.asList("ab"));
    testOpt("\"a\"\"b\"", Arrays.asList("ab"));
    testOpt("''", Arrays.asList(""));
    testOpt("\"\"", Arrays.asList(""));
  }

  @Test
  public void testInvalidOptionStrings() {
    testInvalidOpt("\\");
    testInvalidOpt("\"abcde");
    testInvalidOpt("'abcde");
  }

  @Test
  public void testWindowsBatchQuoting() {
    assertEquals("abc", quoteForBatchScript("abc"));
    assertEquals("\"a b c\"", quoteForBatchScript("a b c"));
    assertEquals("\"a \"\"b\"\" c\"", quoteForBatchScript("a \"b\" c"));
    assertEquals("\"a\"\"b\"\"c\"", quoteForBatchScript("a\"b\"c"));
    assertEquals("\"ab=\"\"cd\"\"\"", quoteForBatchScript("ab=\"cd\""));
    assertEquals("\"a,b,c\"", quoteForBatchScript("a,b,c"));
    assertEquals("\"a;b;c\"", quoteForBatchScript("a;b;c"));
    assertEquals("\"a,b,c\\\\\"", quoteForBatchScript("a,b,c\\"));
  }

  @Test
  public void testPythonArgQuoting() {
    assertEquals("\"abc\"", quoteForCommandString("abc"));
    assertEquals("\"a b c\"", quoteForCommandString("a b c"));
    assertEquals("\"a \\\"b\\\" c\"", quoteForCommandString("a \"b\" c"));
  }

  @Test
  public void testJavaMajorVersion() {
    assertEquals(6, javaMajorVersion("1.6.0_50"));
    assertEquals(7, javaMajorVersion("1.7.0_79"));
    assertEquals(8, javaMajorVersion("1.8.0_66"));
    assertEquals(9, javaMajorVersion("9-ea"));
    assertEquals(9, javaMajorVersion("9+100"));
    assertEquals(9, javaMajorVersion("9"));
    assertEquals(9, javaMajorVersion("9.1.0"));
    assertEquals(10, javaMajorVersion("10"));
  }

  @Test
  public void testFindJarsDir() throws IOException {
    String tmpRoot = tempFolder.getRoot().toString();
    String sparkHome = tempFolder.newFolder("spark", "home").getAbsolutePath();

    // if SPARK_HOME/jars is missing and there is no dir to fallback, throws IllegalStateException
    try {
      findJarsDir(sparkHome, "2.13", true);
    } catch (IllegalStateException e) {
      assertEquals(
        "Library directory '" + sparkHome +
        "/assembly/target/scala-2.13/jars' does not exist; make sure Spark is built.",
        e.getMessage());
    }

    // but if failIfNotFound is false, return null without exception
    assertEquals(null, findJarsDir(sparkHome, "2.13", false));

    // if we have built jars dir, use it as jars dir
    tempFolder.newFolder("spark", "home", "assembly", "target", "scala-2.13", "jars");
    assertEquals(join(File.separator, tmpRoot, "spark", "home", "assembly", "target",
      "scala-2.13", "jars"), findJarsDir(sparkHome, "2.13", true));

    // if we have SPARK_HOME/jars, use it as jars dir
    tempFolder.newFolder("spark", "home", "jars");
    assertEquals(
      join(File.separator, tmpRoot, "spark", "home", "jars"),
      findJarsDir(sparkHome, "2.13", true));

    // if we have SPARK_JARS_DIR, use it as jars dir
    File spefifiedJarsDir = tempFolder.newFolder("jars-dir");
    environmentVariables.set(ENV_SPARK_JARS_DIR, spefifiedJarsDir.getAbsolutePath());
    assertEquals(join(File.separator, tmpRoot, "jars-dir"),
      findJarsDir(sparkHome, "2.13", true));

    // if SPARK_JARS_DIR is specified but not exists, throws IllegalStateException
    environmentVariables.set(ENV_SPARK_JARS_DIR,
      join(File.separator, tmpRoot, "jars-dir-not-exists"));
    try {
      findJarsDir(sparkHome, "2.13", true);
    } catch (IllegalStateException e) {
      assertEquals(
          "The specified SPARK_JARS_DIR '" + tmpRoot + File.separator +
          "jars-dir-not-exists' does not exist; make sure SPARK_JARS_DIR is set appropriately.",
          e.getMessage());
    }

    // but if failIfNotFound is false, warn and continue the old lookup behavior
    assertEquals(join(File.separator, tmpRoot, "spark", "home", "jars"),
      findJarsDir(sparkHome, "2.13", false));
  }

  private static void testOpt(String opts, List<String> expected) {
    assertEquals(String.format("test string failed to parse: [[ %s ]]", opts),
        expected, parseOptionString(opts));
  }

  private static void testInvalidOpt(String opts) {
    try {
      parseOptionString(opts);
      fail("Expected exception for invalid option string.");
    } catch (IllegalArgumentException e) {
      // pass.
    }
  }

}
