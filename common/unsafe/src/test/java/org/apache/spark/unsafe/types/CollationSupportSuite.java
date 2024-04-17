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

  private void assertContains(String pattern, String target, String collationName, boolean expected)
          throws SparkException {
    UTF8String l = UTF8String.fromString(pattern);
    UTF8String r = UTF8String.fromString(target);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(expected, CollationSupport.Contains.exec(l, r, collationId));
  }

  @Test
  public void testContains() throws SparkException {
    // Edge cases
    assertContains("", "", "UTF8_BINARY", true);
    assertContains("c", "", "UTF8_BINARY", true);
    assertContains("", "c", "UTF8_BINARY", false);
    assertContains("", "", "UNICODE", true);
    assertContains("c", "", "UNICODE", true);
    assertContains("", "c", "UNICODE", false);
    assertContains("", "", "UTF8_BINARY_LCASE", true);
    assertContains("c", "", "UTF8_BINARY_LCASE", true);
    assertContains("", "c", "UTF8_BINARY_LCASE", false);
    assertContains("", "", "UNICODE_CI", true);
    assertContains("c", "", "UNICODE_CI", true);
    assertContains("", "c", "UNICODE_CI", false);
    // Basic tests
    assertContains("abcde", "bcd", "UTF8_BINARY", true);
    assertContains("abcde", "bde", "UTF8_BINARY", false);
    assertContains("abcde", "fgh", "UTF8_BINARY", false);
    assertContains("abcde", "abcde", "UNICODE", true);
    assertContains("abcde", "aBcDe", "UNICODE", false);
    assertContains("abcde", "fghij", "UNICODE", false);
    assertContains("abcde", "C", "UTF8_BINARY_LCASE", true);
    assertContains("abcde", "AbCdE", "UTF8_BINARY_LCASE", true);
    assertContains("abcde", "X", "UTF8_BINARY_LCASE", false);
    assertContains("abcde", "c", "UNICODE_CI", true);
    assertContains("abcde", "bCD", "UNICODE_CI", true);
    assertContains("abcde", "123", "UNICODE_CI", false);
    // Case variation
    assertContains("aBcDe", "bcd", "UTF8_BINARY", false);
    assertContains("aBcDe", "BcD", "UTF8_BINARY", true);
    assertContains("aBcDe", "abcde", "UNICODE", false);
    assertContains("aBcDe", "aBcDe", "UNICODE", true);
    assertContains("aBcDe", "bcd", "UTF8_BINARY_LCASE", true);
    assertContains("aBcDe", "BCD", "UTF8_BINARY_LCASE", true);
    assertContains("aBcDe", "abcde", "UNICODE_CI", true);
    assertContains("aBcDe", "AbCdE", "UNICODE_CI", true);
    // Accent variation
    assertContains("aBcDe", "bćd", "UTF8_BINARY", false);
    assertContains("aBcDe", "BćD", "UTF8_BINARY", false);
    assertContains("aBcDe", "abćde", "UNICODE", false);
    assertContains("aBcDe", "aBćDe", "UNICODE", false);
    assertContains("aBcDe", "bćd", "UTF8_BINARY_LCASE", false);
    assertContains("aBcDe", "BĆD", "UTF8_BINARY_LCASE", false);
    assertContains("aBcDe", "abćde", "UNICODE_CI", false);
    assertContains("aBcDe", "AbĆdE", "UNICODE_CI", false);
    // Variable byte length characters
    assertContains("ab世De", "b世D", "UTF8_BINARY", true);
    assertContains("ab世De", "B世d", "UTF8_BINARY", false);
    assertContains("äbćδe", "bćδ", "UTF8_BINARY", true);
    assertContains("äbćδe", "BcΔ", "UTF8_BINARY", false);
    assertContains("ab世De", "ab世De", "UNICODE", true);
    assertContains("ab世De", "AB世dE", "UNICODE", false);
    assertContains("äbćδe", "äbćδe", "UNICODE", true);
    assertContains("äbćδe", "ÄBcΔÉ", "UNICODE", false);
    assertContains("ab世De", "b世D", "UTF8_BINARY_LCASE", true);
    assertContains("ab世De", "B世d", "UTF8_BINARY_LCASE", true);
    assertContains("äbćδe", "bćδ", "UTF8_BINARY_LCASE", true);
    assertContains("äbćδe", "BcΔ", "UTF8_BINARY_LCASE", false);
    assertContains("ab世De", "ab世De", "UNICODE_CI", true);
    assertContains("ab世De", "AB世dE", "UNICODE_CI", true);
    assertContains("äbćδe", "ÄbćδE", "UNICODE_CI", true);
    assertContains("äbćδe", "ÄBcΔÉ", "UNICODE_CI", false);
    // Case-variable character length
    assertContains("abİo12", "i̇o", "UNICODE_CI", true);
    assertContains("abi̇o12", "İo", "UNICODE_CI", true);
  }

  private void assertStartsWith(
          String pattern, String prefix, String collationName, boolean expected)
          throws SparkException {
    UTF8String l = UTF8String.fromString(pattern);
    UTF8String r = UTF8String.fromString(prefix);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(expected, CollationSupport.StartsWith.exec(l, r, collationId));
  }

  @Test
  public void testStartsWith() throws SparkException {
    // Edge cases
    assertStartsWith("", "", "UTF8_BINARY", true);
    assertStartsWith("c", "", "UTF8_BINARY", true);
    assertStartsWith("", "c", "UTF8_BINARY", false);
    assertStartsWith("", "", "UNICODE", true);
    assertStartsWith("c", "", "UNICODE", true);
    assertStartsWith("", "c", "UNICODE", false);
    assertStartsWith("", "", "UTF8_BINARY_LCASE", true);
    assertStartsWith("c", "", "UTF8_BINARY_LCASE", true);
    assertStartsWith("", "c", "UTF8_BINARY_LCASE", false);
    assertStartsWith("", "", "UNICODE_CI", true);
    assertStartsWith("c", "", "UNICODE_CI", true);
    assertStartsWith("", "c", "UNICODE_CI", false);
    // Basic tests
    assertStartsWith("abcde", "abc", "UTF8_BINARY", true);
    assertStartsWith("abcde", "abd", "UTF8_BINARY", false);
    assertStartsWith("abcde", "fgh", "UTF8_BINARY", false);
    assertStartsWith("abcde", "abcde", "UNICODE", true);
    assertStartsWith("abcde", "aBcDe", "UNICODE", false);
    assertStartsWith("abcde", "fghij", "UNICODE", false);
    assertStartsWith("abcde", "A", "UTF8_BINARY_LCASE", true);
    assertStartsWith("abcde", "AbCdE", "UTF8_BINARY_LCASE", true);
    assertStartsWith("abcde", "X", "UTF8_BINARY_LCASE", false);
    assertStartsWith("abcde", "a", "UNICODE_CI", true);
    assertStartsWith("abcde", "aBC", "UNICODE_CI", true);
    assertStartsWith("abcde", "bcd", "UNICODE_CI", false);
    assertStartsWith("abcde", "123", "UNICODE_CI", false);
    // Case variation
    assertStartsWith("aBcDe", "abc", "UTF8_BINARY", false);
    assertStartsWith("aBcDe", "aBc", "UTF8_BINARY", true);
    assertStartsWith("aBcDe", "abcde", "UNICODE", false);
    assertStartsWith("aBcDe", "aBcDe", "UNICODE", true);
    assertStartsWith("aBcDe", "abc", "UTF8_BINARY_LCASE", true);
    assertStartsWith("aBcDe", "ABC", "UTF8_BINARY_LCASE", true);
    assertStartsWith("aBcDe", "abcde", "UNICODE_CI", true);
    assertStartsWith("aBcDe", "AbCdE", "UNICODE_CI", true);
    // Accent variation
    assertStartsWith("aBcDe", "abć", "UTF8_BINARY", false);
    assertStartsWith("aBcDe", "aBć", "UTF8_BINARY", false);
    assertStartsWith("aBcDe", "abćde", "UNICODE", false);
    assertStartsWith("aBcDe", "aBćDe", "UNICODE", false);
    assertStartsWith("aBcDe", "abć", "UTF8_BINARY_LCASE", false);
    assertStartsWith("aBcDe", "ABĆ", "UTF8_BINARY_LCASE", false);
    assertStartsWith("aBcDe", "abćde", "UNICODE_CI", false);
    assertStartsWith("aBcDe", "AbĆdE", "UNICODE_CI", false);
    // Variable byte length characters
    assertStartsWith("ab世De", "ab世", "UTF8_BINARY", true);
    assertStartsWith("ab世De", "aB世", "UTF8_BINARY", false);
    assertStartsWith("äbćδe", "äbć", "UTF8_BINARY", true);
    assertStartsWith("äbćδe", "äBc", "UTF8_BINARY", false);
    assertStartsWith("ab世De", "ab世De", "UNICODE", true);
    assertStartsWith("ab世De", "AB世dE", "UNICODE", false);
    assertStartsWith("äbćδe", "äbćδe", "UNICODE", true);
    assertStartsWith("äbćδe", "ÄBcΔÉ", "UNICODE", false);
    assertStartsWith("ab世De", "ab世", "UTF8_BINARY_LCASE", true);
    assertStartsWith("ab世De", "aB世", "UTF8_BINARY_LCASE", true);
    assertStartsWith("äbćδe", "äbć", "UTF8_BINARY_LCASE", true);
    assertStartsWith("äbćδe", "äBc", "UTF8_BINARY_LCASE", false);
    assertStartsWith("ab世De", "ab世De", "UNICODE_CI", true);
    assertStartsWith("ab世De", "AB世dE", "UNICODE_CI", true);
    assertStartsWith("äbćδe", "ÄbćδE", "UNICODE_CI", true);
    assertStartsWith("äbćδe", "ÄBcΔÉ", "UNICODE_CI", false);
    // Case-variable character length
    assertStartsWith("İonic", "i̇o", "UNICODE_CI", true);
    assertStartsWith("i̇onic", "İo", "UNICODE_CI", true);
  }

  private void assertEndsWith(String pattern, String suffix, String collationName, boolean expected)
          throws SparkException {
    UTF8String l = UTF8String.fromString(pattern);
    UTF8String r = UTF8String.fromString(suffix);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(expected, CollationSupport.EndsWith.exec(l, r, collationId));
  }

  @Test
  public void testEndsWith() throws SparkException {
    // Edge cases
    assertEndsWith("", "", "UTF8_BINARY", true);
    assertEndsWith("c", "", "UTF8_BINARY", true);
    assertEndsWith("", "c", "UTF8_BINARY", false);
    assertEndsWith("", "", "UNICODE", true);
    assertEndsWith("c", "", "UNICODE", true);
    assertEndsWith("", "c", "UNICODE", false);
    assertEndsWith("", "", "UTF8_BINARY_LCASE", true);
    assertEndsWith("c", "", "UTF8_BINARY_LCASE", true);
    assertEndsWith("", "c", "UTF8_BINARY_LCASE", false);
    assertEndsWith("", "", "UNICODE_CI", true);
    assertEndsWith("c", "", "UNICODE_CI", true);
    assertEndsWith("", "c", "UNICODE_CI", false);
    // Basic tests
    assertEndsWith("abcde", "cde", "UTF8_BINARY", true);
    assertEndsWith("abcde", "bde", "UTF8_BINARY", false);
    assertEndsWith("abcde", "fgh", "UTF8_BINARY", false);
    assertEndsWith("abcde", "abcde", "UNICODE", true);
    assertEndsWith("abcde", "aBcDe", "UNICODE", false);
    assertEndsWith("abcde", "fghij", "UNICODE", false);
    assertEndsWith("abcde", "E", "UTF8_BINARY_LCASE", true);
    assertEndsWith("abcde", "AbCdE", "UTF8_BINARY_LCASE", true);
    assertEndsWith("abcde", "X", "UTF8_BINARY_LCASE", false);
    assertEndsWith("abcde", "e", "UNICODE_CI", true);
    assertEndsWith("abcde", "CDe", "UNICODE_CI", true);
    assertEndsWith("abcde", "bcd", "UNICODE_CI", false);
    assertEndsWith("abcde", "123", "UNICODE_CI", false);
    // Case variation
    assertEndsWith("aBcDe", "cde", "UTF8_BINARY", false);
    assertEndsWith("aBcDe", "cDe", "UTF8_BINARY", true);
    assertEndsWith("aBcDe", "abcde", "UNICODE", false);
    assertEndsWith("aBcDe", "aBcDe", "UNICODE", true);
    assertEndsWith("aBcDe", "cde", "UTF8_BINARY_LCASE", true);
    assertEndsWith("aBcDe", "CDE", "UTF8_BINARY_LCASE", true);
    assertEndsWith("aBcDe", "abcde", "UNICODE_CI", true);
    assertEndsWith("aBcDe", "AbCdE", "UNICODE_CI", true);
    // Accent variation
    assertEndsWith("aBcDe", "ćde", "UTF8_BINARY", false);
    assertEndsWith("aBcDe", "ćDe", "UTF8_BINARY", false);
    assertEndsWith("aBcDe", "abćde", "UNICODE", false);
    assertEndsWith("aBcDe", "aBćDe", "UNICODE", false);
    assertEndsWith("aBcDe", "ćde", "UTF8_BINARY_LCASE", false);
    assertEndsWith("aBcDe", "ĆDE", "UTF8_BINARY_LCASE", false);
    assertEndsWith("aBcDe", "abćde", "UNICODE_CI", false);
    assertEndsWith("aBcDe", "AbĆdE", "UNICODE_CI", false);
    // Variable byte length characters
    assertEndsWith("ab世De", "世De", "UTF8_BINARY", true);
    assertEndsWith("ab世De", "世dE", "UTF8_BINARY", false);
    assertEndsWith("äbćδe", "ćδe", "UTF8_BINARY", true);
    assertEndsWith("äbćδe", "cΔé", "UTF8_BINARY", false);
    assertEndsWith("ab世De", "ab世De", "UNICODE", true);
    assertEndsWith("ab世De", "AB世dE", "UNICODE", false);
    assertEndsWith("äbćδe", "äbćδe", "UNICODE", true);
    assertEndsWith("äbćδe", "ÄBcΔÉ", "UNICODE", false);
    assertEndsWith("ab世De", "世De", "UTF8_BINARY_LCASE", true);
    assertEndsWith("ab世De", "世dE", "UTF8_BINARY_LCASE", true);
    assertEndsWith("äbćδe", "ćδe", "UTF8_BINARY_LCASE", true);
    assertEndsWith("äbćδe", "cδE", "UTF8_BINARY_LCASE", false);
    assertEndsWith("ab世De", "ab世De", "UNICODE_CI", true);
    assertEndsWith("ab世De", "AB世dE", "UNICODE_CI", true);
    assertEndsWith("äbćδe", "ÄbćδE", "UNICODE_CI", true);
    assertEndsWith("äbćδe", "ÄBcΔÉ", "UNICODE_CI", false);
    // Case-variable character length
    assertEndsWith("The İo", "i̇o", "UNICODE_CI", true);
    assertEndsWith("The i̇o", "İo", "UNICODE_CI", true);
  }

  private void assertSubstringIndex(String string, String delimiter, Integer count,
        String collationName, String expected) throws SparkException {
    UTF8String str = UTF8String.fromString(string);
    UTF8String delim = UTF8String.fromString(delimiter);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(expected,
      CollationSupport.SubstringIndex.exec(str, delim, count, collationId).toString());
  }

  @Test
  public void testSubstringIndex() throws SparkException {
    assertSubstringIndex("wwwgapachegorg", "g", -3, "UTF8_BINARY", "apachegorg");
    assertSubstringIndex("www||apache||org", "||", 2, "UTF8_BINARY", "www||apache");
    assertSubstringIndex("aaaaaaaaaa", "aa", 2, "UTF8_BINARY", "a");
    assertSubstringIndex("AaAaAaAaAa", "aa", 2, "UTF8_BINARY_LCASE", "A");
    assertSubstringIndex("www.apache.org", ".", 3, "UTF8_BINARY_LCASE", "www.apache.org");
    assertSubstringIndex("wwwXapacheXorg", "x", 2, "UTF8_BINARY_LCASE", "wwwXapache");
    assertSubstringIndex("wwwxapachexorg", "X", 1, "UTF8_BINARY_LCASE", "www");
    assertSubstringIndex("www.apache.org", ".", 0, "UTF8_BINARY_LCASE", "");
    assertSubstringIndex("www.apache.ORG", ".", -3, "UTF8_BINARY_LCASE", "www.apache.ORG");
    assertSubstringIndex("wwwGapacheGorg", "g", 1, "UTF8_BINARY_LCASE", "www");
    assertSubstringIndex("wwwGapacheGorg", "g", 3, "UTF8_BINARY_LCASE", "wwwGapacheGor");
    assertSubstringIndex("gwwwGapacheGorg", "g", 3, "UTF8_BINARY_LCASE", "gwwwGapache");
    assertSubstringIndex("wwwGapacheGorg", "g", -3, "UTF8_BINARY_LCASE", "apacheGorg");
    assertSubstringIndex("wwwmapacheMorg", "M", -2, "UTF8_BINARY_LCASE", "apacheMorg");
    assertSubstringIndex("www.apache.org", ".", -1, "UTF8_BINARY_LCASE", "org");
    assertSubstringIndex("www.apache.org.", ".", -1, "UTF8_BINARY_LCASE", "");
    assertSubstringIndex("", ".", -2, "UTF8_BINARY_LCASE", "");
    assertSubstringIndex("test大千世界X大千世界", "x", -1, "UTF8_BINARY_LCASE", "大千世界");
    assertSubstringIndex("test大千世界X大千世界", "X", 1, "UTF8_BINARY_LCASE", "test大千世界");
    assertSubstringIndex("test大千世界大千世界", "千", 2, "UTF8_BINARY_LCASE", "test大千世界大");
    assertSubstringIndex("www||APACHE||org", "||", 2, "UTF8_BINARY_LCASE", "www||APACHE");
    assertSubstringIndex("www||APACHE||org", "||", -1, "UTF8_BINARY_LCASE", "org");
    assertSubstringIndex("AaAaAaAaAa", "Aa", 2, "UNICODE", "Aa");
    assertSubstringIndex("wwwYapacheyorg", "y", 3, "UNICODE", "wwwYapacheyorg");
    assertSubstringIndex("www.apache.org", ".", 2, "UNICODE", "www.apache");
    assertSubstringIndex("wwwYapacheYorg", "Y", 1, "UNICODE", "www");
    assertSubstringIndex("wwwYapacheYorg", "y", 1, "UNICODE", "wwwYapacheYorg");
    assertSubstringIndex("wwwGapacheGorg", "g", 1, "UNICODE", "wwwGapacheGor");
    assertSubstringIndex("GwwwGapacheGorG", "G", 3, "UNICODE", "GwwwGapache");
    assertSubstringIndex("wwwGapacheGorG", "G", -3, "UNICODE", "apacheGorG");
    assertSubstringIndex("www.apache.org", ".", 0, "UNICODE", "");
    assertSubstringIndex("www.apache.org", ".", -3, "UNICODE", "www.apache.org");
    assertSubstringIndex("www.apache.org", ".", -2, "UNICODE", "apache.org");
    assertSubstringIndex("www.apache.org", ".", -1, "UNICODE", "org");
    assertSubstringIndex("", ".", -2, "UNICODE", "");
    assertSubstringIndex("test大千世界X大千世界", "X", -1, "UNICODE", "大千世界");
    assertSubstringIndex("test大千世界X大千世界", "X", 1, "UNICODE", "test大千世界");
    assertSubstringIndex("大x千世界大千世x界", "x", 1, "UNICODE", "大");
    assertSubstringIndex("大x千世界大千世x界", "x", -1, "UNICODE", "界");
    assertSubstringIndex("大x千世界大千世x界", "x", -2, "UNICODE", "千世界大千世x界");
    assertSubstringIndex("大千世界大千世界", "千", 2, "UNICODE", "大千世界大");
    assertSubstringIndex("www||apache||org", "||", 2, "UNICODE", "www||apache");
    assertSubstringIndex("AaAaAaAaAa", "aa", 2, "UNICODE_CI", "A");
    assertSubstringIndex("www.apache.org", ".", 3, "UNICODE_CI", "www.apache.org");
    assertSubstringIndex("wwwXapacheXorg", "x", 2, "UNICODE_CI", "wwwXapache");
    assertSubstringIndex("wwwxapacheXorg", "X", 1, "UNICODE_CI", "www");
    assertSubstringIndex("www.apache.org", ".", 0, "UNICODE_CI", "");
    assertSubstringIndex("wwwGapacheGorg", "G", 3, "UNICODE_CI", "wwwGapacheGor");
    assertSubstringIndex("gwwwGapacheGorg", "g", 3, "UNICODE_CI", "gwwwGapache");
    assertSubstringIndex("gwwwGapacheGorg", "g", -3, "UNICODE_CI", "apacheGorg");
    assertSubstringIndex("www.apache.ORG", ".", -3, "UNICODE_CI", "www.apache.ORG");
    assertSubstringIndex("wwwmapacheMorg", "M", -2, "UNICODE_CI", "apacheMorg");
    assertSubstringIndex("www.apache.org", ".", -1, "UNICODE_CI", "org");
    assertSubstringIndex("", ".", -2, "UNICODE_CI", "");
    assertSubstringIndex("test大千世界X大千世界", "X", -1, "UNICODE_CI", "大千世界");
    assertSubstringIndex("test大千世界X大千世界", "X", 1, "UNICODE_CI", "test大千世界");
    assertSubstringIndex("test大千世界大千世界", "千", 2, "UNICODE_CI", "test大千世界大");
    assertSubstringIndex("www||APACHE||org", "||", 2, "UNICODE_CI", "www||APACHE");
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
