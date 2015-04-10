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
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

public class CommandBuilderUtilsSuite {

  @Test
  public void testValidOptionStrings() {
    testOpt("a b c d e", Arrays.asList("a", "b", "c", "d", "e"));
    testOpt("a 'b c' \"d\" e", Arrays.asList("a", "b c", "d", "e"));
    testOpt("a 'b\\\"c' \"'d'\" e", Arrays.asList("a", "b\\\"c", "'d'", "e"));
    testOpt("a 'b\"c' \"\\\"d\\\"\" e", Arrays.asList("a", "b\"c", "\"d\"", "e"));
    testOpt(" a b c \\\\ ", Arrays.asList("a", "b", "c", "\\"));

    // Following tests ported from UtilsSuite.scala.
    testOpt("", new ArrayList<String>());
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
    assertEquals("\"ab^=\"\"cd\"\"\"", quoteForBatchScript("ab=\"cd\""));
  }

  @Test
  public void testPythonArgQuoting() {
    assertEquals("\"abc\"", quoteForCommandString("abc"));
    assertEquals("\"a b c\"", quoteForCommandString("a b c"));
    assertEquals("\"a \\\"b\\\" c\"", quoteForCommandString("a \"b\" c"));
  }

  private void testOpt(String opts, List<String> expected) {
    assertEquals(String.format("test string failed to parse: [[ %s ]]", opts),
        expected, parseOptionString(opts));
  }

  private void testInvalidOpt(String opts) {
    try {
      parseOptionString(opts);
      fail("Expected exception for invalid option string.");
    } catch (IllegalArgumentException e) {
      // pass.
    }
  }

}
