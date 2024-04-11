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
package org.apache.spark.unsafe.types;

import org.apache.spark.SparkException;
import org.apache.spark.sql.catalyst.util.CollationFactory;
import org.apache.spark.sql.catalyst.util.CollationSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class CollationSupportSuite {

  /**
   * Collation-aware string expressions.
   */

  private void assertContains(String pattern, String target, String collationName, boolean value)
          throws SparkException {
    UTF8String l = UTF8String.fromString(pattern);
    UTF8String r = UTF8String.fromString(target);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(CollationSupport.Contains.contains(l, r, collationId), value);
  }

  @Test
  public void testContains() throws SparkException {
    assertContains("", "", "UTF8_BINARY", true);
    assertContains("c", "", "UTF8_BINARY", true);
    assertContains("", "c", "UTF8_BINARY", false);
    assertContains("abcde", "c", "UTF8_BINARY", true);
    assertContains("abcde", "C", "UTF8_BINARY", false);
    assertContains("abcde", "bcd", "UTF8_BINARY", true);
    assertContains("abcde", "BCD", "UTF8_BINARY", false);
    assertContains("abcde", "fgh", "UTF8_BINARY", false);
    assertContains("abcde", "FGH", "UTF8_BINARY", false);
    assertContains("", "", "UNICODE", true);
    assertContains("c", "", "UNICODE", true);
    assertContains("", "c", "UNICODE", false);
    assertContains("abcde", "c", "UNICODE", true);
    assertContains("abcde", "C", "UNICODE", false);
    assertContains("abcde", "bcd", "UNICODE", true);
    assertContains("abcde", "BCD", "UNICODE", false);
    assertContains("abcde", "fgh", "UNICODE", false);
    assertContains("abcde", "FGH", "UNICODE", false);
    assertContains("", "", "UTF8_BINARY_LCASE", true);
    assertContains("c", "", "UTF8_BINARY_LCASE", true);
    assertContains("", "c", "UTF8_BINARY_LCASE", false);
    assertContains("abcde", "c", "UTF8_BINARY_LCASE", true);
    assertContains("abcde", "C", "UTF8_BINARY_LCASE", true);
    assertContains("abcde", "bcd", "UTF8_BINARY_LCASE", true);
    assertContains("abcde", "BCD", "UTF8_BINARY_LCASE", true);
    assertContains("abcde", "fgh", "UTF8_BINARY_LCASE", false);
    assertContains("abcde", "FGH", "UTF8_BINARY_LCASE", false);
    assertContains("", "", "UNICODE_CI", true);
    assertContains("c", "", "UNICODE_CI", true);
    assertContains("", "c", "UNICODE_CI", false);
    assertContains("abcde", "c", "UNICODE_CI", true);
    assertContains("abcde", "C", "UNICODE_CI", true);
    assertContains("abcde", "bcd", "UNICODE_CI", true);
    assertContains("abcde", "BCD", "UNICODE_CI", true);
    assertContains("abcde", "fgh", "UNICODE_CI", false);
    assertContains("abcde", "FGH", "UNICODE_CI", false);
  }

  private void assertStartsWith(String pattern, String prefix, String collationName, boolean value)
          throws SparkException {
    UTF8String l = UTF8String.fromString(pattern);
    UTF8String r = UTF8String.fromString(prefix);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(CollationSupport.StartsWith.startsWith(l, r, collationId), value);
  }

  @Test
  public void testStartsWith() throws SparkException {
    assertStartsWith("", "", "UTF8_BINARY", true);
    assertStartsWith("c", "", "UTF8_BINARY", true);
    assertStartsWith("", "c", "UTF8_BINARY", false);
    assertStartsWith("abcde", "a", "UTF8_BINARY", true);
    assertStartsWith("abcde", "A", "UTF8_BINARY", false);
    assertStartsWith("abcde", "bcd", "UTF8_BINARY", false);
    assertStartsWith("abcde", "BCD", "UTF8_BINARY", false);
    assertStartsWith("abcde", "fgh", "UTF8_BINARY", false);
    assertStartsWith("abcde", "FGH", "UTF8_BINARY", false);
    assertStartsWith("", "", "UNICODE", true);
    assertStartsWith("c", "", "UNICODE", true);
    assertStartsWith("", "c", "UNICODE", false);
    assertStartsWith("abcde", "a", "UNICODE", true);
    assertStartsWith("abcde", "A", "UNICODE", false);
    assertStartsWith("abcde", "bcd", "UNICODE", false);
    assertStartsWith("abcde", "BCD", "UNICODE", false);
    assertStartsWith("abcde", "fgh", "UNICODE", false);
    assertStartsWith("abcde", "FGH", "UNICODE", false);
    assertStartsWith("", "", "UTF8_BINARY_LCASE", true);
    assertStartsWith("c", "", "UTF8_BINARY_LCASE", true);
    assertStartsWith("", "c", "UTF8_BINARY_LCASE", false);
    assertStartsWith("abcde", "a", "UTF8_BINARY_LCASE", true);
    assertStartsWith("abcde", "A", "UTF8_BINARY_LCASE", true);
    assertStartsWith("abcde", "abc", "UTF8_BINARY_LCASE", true);
    assertStartsWith("abcde", "BCD", "UTF8_BINARY_LCASE", false);
    assertStartsWith("abcde", "fgh", "UTF8_BINARY_LCASE", false);
    assertStartsWith("abcde", "FGH", "UTF8_BINARY_LCASE", false);
    assertStartsWith("", "", "UNICODE_CI", true);
    assertStartsWith("c", "", "UNICODE_CI", true);
    assertStartsWith("", "c", "UNICODE_CI", false);
    assertStartsWith("abcde", "a", "UNICODE_CI", true);
    assertStartsWith("abcde", "A", "UNICODE_CI", true);
    assertStartsWith("abcde", "abc", "UNICODE_CI", true);
    assertStartsWith("abcde", "BCD", "UNICODE_CI", false);
    assertStartsWith("abcde", "fgh", "UNICODE_CI", false);
    assertStartsWith("abcde", "FGH", "UNICODE_CI", false);
  }

  private void assertEndsWith(String pattern, String suffix, String collationName, boolean value)
          throws SparkException {
    UTF8String l = UTF8String.fromString(pattern);
    UTF8String r = UTF8String.fromString(suffix);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(CollationSupport.EndsWith.endsWith(l, r, collationId), value);
  }

  @Test
  public void testEndsWith() throws SparkException {
    assertEndsWith("", "", "UTF8_BINARY", true);
    assertEndsWith("c", "", "UTF8_BINARY", true);
    assertEndsWith("", "c", "UTF8_BINARY", false);
    assertEndsWith("abcde", "e", "UTF8_BINARY", true);
    assertEndsWith("abcde", "E", "UTF8_BINARY", false);
    assertEndsWith("abcde", "bcd", "UTF8_BINARY", false);
    assertEndsWith("abcde", "BCD", "UTF8_BINARY", false);
    assertEndsWith("abcde", "fgh", "UTF8_BINARY", false);
    assertEndsWith("abcde", "FGH", "UTF8_BINARY", false);
    assertEndsWith("", "", "UNICODE", true);
    assertEndsWith("c", "", "UNICODE", true);
    assertEndsWith("", "c", "UNICODE", false);
    assertEndsWith("abcde", "e", "UNICODE", true);
    assertEndsWith("abcde", "E", "UNICODE", false);
    assertEndsWith("abcde", "bcd", "UNICODE", false);
    assertEndsWith("abcde", "BCD", "UNICODE", false);
    assertEndsWith("abcde", "fgh", "UNICODE", false);
    assertEndsWith("abcde", "FGH", "UNICODE", false);
    assertEndsWith("", "", "UTF8_BINARY_LCASE", true);
    assertEndsWith("c", "", "UTF8_BINARY_LCASE", true);
    assertEndsWith("", "c", "UTF8_BINARY_LCASE", false);
    assertEndsWith("abcde", "e", "UTF8_BINARY_LCASE", true);
    assertEndsWith("abcde", "E", "UTF8_BINARY_LCASE", true);
    assertEndsWith("abcde", "cde", "UTF8_BINARY_LCASE", true);
    assertEndsWith("abcde", "BCD", "UTF8_BINARY_LCASE", false);
    assertEndsWith("abcde", "fgh", "UTF8_BINARY_LCASE", false);
    assertEndsWith("abcde", "FGH", "UTF8_BINARY_LCASE", false);
    assertEndsWith("", "", "UNICODE_CI", true);
    assertEndsWith("c", "", "UNICODE_CI", true);
    assertEndsWith("", "c", "UNICODE_CI", false);
    assertEndsWith("abcde", "e", "UNICODE_CI", true);
    assertEndsWith("abcde", "E", "UNICODE_CI", true);
    assertEndsWith("abcde", "cde", "UNICODE_CI", true);
    assertEndsWith("abcde", "BCD", "UNICODE_CI", false);
    assertEndsWith("abcde", "fgh", "UNICODE_CI", false);
    assertEndsWith("abcde", "FGH", "UNICODE_CI", false);
  }

  // TODO: Test more collation-aware string expressions.

  /**
   * Collation-aware regexp expressions.
   */

  // TODO: Test more collation-aware regexp expressions.

  /**
   * Other collation-aware expressions.
   */

  // TODO: Test other collation-aware expressions.

}
