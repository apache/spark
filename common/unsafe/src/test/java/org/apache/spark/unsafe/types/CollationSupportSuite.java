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

// checkstyle.off: AvoidEscapedUnicodeCharacters
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
    // Characters with the same binary lowercase representation
    assertContains("The Kelvin.", "Kelvin", "UTF8_BINARY_LCASE", true);
    assertContains("The Kelvin.", "Kelvin", "UTF8_BINARY_LCASE", true);
    assertContains("The KKelvin.", "KKelvin", "UTF8_BINARY_LCASE", true);
    assertContains("2 Kelvin.", "2 Kelvin", "UTF8_BINARY_LCASE", true);
    assertContains("2 Kelvin.", "2 Kelvin", "UTF8_BINARY_LCASE", true);
    assertContains("The KKelvin.", "KKelvin,", "UTF8_BINARY_LCASE", false);
    // Case-variable character length
    assertContains("i̇", "i", "UNICODE_CI", false);
    assertContains("i̇", "\u0307", "UNICODE_CI", false);
    assertContains("i̇", "İ", "UNICODE_CI", true);
    assertContains("İ", "i", "UNICODE_CI", false);
    assertContains("adi̇os", "io", "UNICODE_CI", false);
    assertContains("adi̇os", "Io", "UNICODE_CI", false);
    assertContains("adi̇os", "i̇o", "UNICODE_CI", true);
    assertContains("adi̇os", "İo", "UNICODE_CI", true);
    assertContains("adİos", "io", "UNICODE_CI", false);
    assertContains("adİos", "Io", "UNICODE_CI", false);
    assertContains("adİos", "i̇o", "UNICODE_CI", true);
    assertContains("adİos", "İo", "UNICODE_CI", true);
    assertContains("i̇", "i", "UTF8_BINARY_LCASE", true); // != UNICODE_CI
    assertContains("İ", "\u0307", "UTF8_BINARY_LCASE", false);
    assertContains("İ", "i", "UTF8_BINARY_LCASE", false);
    assertContains("i̇", "\u0307", "UTF8_BINARY_LCASE", true); // != UNICODE_CI
    assertContains("i̇", "İ", "UTF8_BINARY_LCASE", true);
    assertContains("İ", "i", "UTF8_BINARY_LCASE", false);
    assertContains("adi̇os", "io", "UTF8_BINARY_LCASE", false);
    assertContains("adi̇os", "Io", "UTF8_BINARY_LCASE", false);
    assertContains("adi̇os", "i̇o", "UTF8_BINARY_LCASE", true);
    assertContains("adi̇os", "İo", "UTF8_BINARY_LCASE", true);
    assertContains("adİos", "io", "UTF8_BINARY_LCASE", false);
    assertContains("adİos", "Io", "UTF8_BINARY_LCASE", false);
    assertContains("adİos", "i̇o", "UTF8_BINARY_LCASE", true);
    assertContains("adİos", "İo", "UTF8_BINARY_LCASE", true);
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
    // Characters with the same binary lowercase representation
    assertStartsWith("Kelvin.", "Kelvin", "UTF8_BINARY_LCASE", true);
    assertStartsWith("Kelvin.", "Kelvin", "UTF8_BINARY_LCASE", true);
    assertStartsWith("KKelvin.", "KKelvin", "UTF8_BINARY_LCASE", true);
    assertStartsWith("2 Kelvin.", "2 Kelvin", "UTF8_BINARY_LCASE", true);
    assertStartsWith("2 Kelvin.", "2 Kelvin", "UTF8_BINARY_LCASE", true);
    assertStartsWith("KKelvin.", "KKelvin,", "UTF8_BINARY_LCASE", false);
    // Case-variable character length
    assertStartsWith("i̇", "i", "UNICODE_CI", false);
    assertStartsWith("i̇", "İ", "UNICODE_CI", true);
    assertStartsWith("İ", "i", "UNICODE_CI", false);
    assertStartsWith("İİİ", "i̇i̇", "UNICODE_CI", true);
    assertStartsWith("İİİ", "i̇i", "UNICODE_CI", false);
    assertStartsWith("İi̇İ", "i̇İ", "UNICODE_CI", true);
    assertStartsWith("i̇İi̇i̇", "İi̇İi", "UNICODE_CI", false);
    assertStartsWith("i̇onic", "io", "UNICODE_CI", false);
    assertStartsWith("i̇onic", "Io", "UNICODE_CI", false);
    assertStartsWith("i̇onic", "i̇o", "UNICODE_CI", true);
    assertStartsWith("i̇onic", "İo", "UNICODE_CI", true);
    assertStartsWith("İonic", "io", "UNICODE_CI", false);
    assertStartsWith("İonic", "Io", "UNICODE_CI", false);
    assertStartsWith("İonic", "i̇o", "UNICODE_CI", true);
    assertStartsWith("İonic", "İo", "UNICODE_CI", true);
    assertStartsWith("i̇", "i", "UTF8_BINARY_LCASE", true); // != UNICODE_CI
    assertStartsWith("i̇", "İ", "UTF8_BINARY_LCASE", true);
    assertStartsWith("İ", "i", "UTF8_BINARY_LCASE", false);
    assertStartsWith("İİİ", "i̇i̇", "UTF8_BINARY_LCASE", true);
    assertStartsWith("İİİ", "i̇i", "UTF8_BINARY_LCASE", false);
    assertStartsWith("İi̇İ", "i̇İ", "UTF8_BINARY_LCASE", true);
    assertStartsWith("i̇İi̇i̇", "İi̇İi", "UTF8_BINARY_LCASE", true); // != UNICODE_CI
    assertStartsWith("i̇onic", "io", "UTF8_BINARY_LCASE", false);
    assertStartsWith("i̇onic", "Io", "UTF8_BINARY_LCASE", false);
    assertStartsWith("i̇onic", "i̇o", "UTF8_BINARY_LCASE", true);
    assertStartsWith("i̇onic", "İo", "UTF8_BINARY_LCASE", true);
    assertStartsWith("İonic", "io", "UTF8_BINARY_LCASE", false);
    assertStartsWith("İonic", "Io", "UTF8_BINARY_LCASE", false);
    assertStartsWith("İonic", "i̇o", "UTF8_BINARY_LCASE", true);
    assertStartsWith("İonic", "İo", "UTF8_BINARY_LCASE", true);
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
    // Characters with the same binary lowercase representation
    assertEndsWith("The Kelvin", "Kelvin", "UTF8_BINARY_LCASE", true);
    assertEndsWith("The Kelvin", "Kelvin", "UTF8_BINARY_LCASE", true);
    assertEndsWith("The KKelvin", "KKelvin", "UTF8_BINARY_LCASE", true);
    assertEndsWith("The 2 Kelvin", "2 Kelvin", "UTF8_BINARY_LCASE", true);
    assertEndsWith("The 2 Kelvin", "2 Kelvin", "UTF8_BINARY_LCASE", true);
    assertEndsWith("The KKelvin", "KKelvin,", "UTF8_BINARY_LCASE", false);
    // Case-variable character length
    assertEndsWith("i̇", "\u0307", "UNICODE_CI", false);
    assertEndsWith("i̇", "İ", "UNICODE_CI", true);
    assertEndsWith("İ", "i", "UNICODE_CI", false);
    assertEndsWith("İİİ", "i̇i̇", "UNICODE_CI", true);
    assertEndsWith("İİİ", "ii̇", "UNICODE_CI", false);
    assertEndsWith("İi̇İ", "İi̇", "UNICODE_CI", true);
    assertEndsWith("i̇İi̇i̇", "\u0307İi̇İ", "UNICODE_CI", false);
    assertEndsWith("the i̇o", "io", "UNICODE_CI", false);
    assertEndsWith("the i̇o", "Io", "UNICODE_CI", false);
    assertEndsWith("the i̇o", "i̇o", "UNICODE_CI", true);
    assertEndsWith("the i̇o", "İo", "UNICODE_CI", true);
    assertEndsWith("the İo", "io", "UNICODE_CI", false);
    assertEndsWith("the İo", "Io", "UNICODE_CI", false);
    assertEndsWith("the İo", "i̇o", "UNICODE_CI", true);
    assertEndsWith("the İo", "İo", "UNICODE_CI", true);
    assertEndsWith("i̇", "\u0307", "UTF8_BINARY_LCASE", true); // != UNICODE_CI
    assertEndsWith("i̇", "İ", "UTF8_BINARY_LCASE", true);
    assertEndsWith("İ", "\u0307", "UTF8_BINARY_LCASE", false);
    assertEndsWith("İİİ", "i̇i̇", "UTF8_BINARY_LCASE", true);
    assertEndsWith("İİİ", "ii̇", "UTF8_BINARY_LCASE", false);
    assertEndsWith("İi̇İ", "İi̇", "UTF8_BINARY_LCASE", true);
    assertEndsWith("i̇İi̇i̇", "\u0307İi̇İ", "UTF8_BINARY_LCASE", true); // != UNICODE_CI
    assertEndsWith("i̇İi̇i̇", "\u0307İİ", "UTF8_BINARY_LCASE", false);
    assertEndsWith("the i̇o", "io", "UTF8_BINARY_LCASE", false);
    assertEndsWith("the i̇o", "Io", "UTF8_BINARY_LCASE", false);
    assertEndsWith("the i̇o", "i̇o", "UTF8_BINARY_LCASE", true);
    assertEndsWith("the i̇o", "İo", "UTF8_BINARY_LCASE", true);
    assertEndsWith("the İo", "io", "UTF8_BINARY_LCASE", false);
    assertEndsWith("the İo", "Io", "UTF8_BINARY_LCASE", false);
    assertEndsWith("the İo", "i̇o", "UTF8_BINARY_LCASE", true);
    assertEndsWith("the İo", "İo", "UTF8_BINARY_LCASE", true);
  }

  private void assertStringSplitSQL(String str, String delimiter, String collationName,
      UTF8String[] expected) throws SparkException {
    UTF8String s = UTF8String.fromString(str);
    UTF8String d = UTF8String.fromString(delimiter);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertArrayEquals(expected, CollationSupport.StringSplitSQL.exec(s, d, collationId));
  }

  @Test
  public void testStringSplitSQL() throws SparkException {
    // Possible splits
    var empty_match = new UTF8String[] { UTF8String.fromString("") };
    var array_abc = new UTF8String[] { UTF8String.fromString("abc") };
    var array_1a2 = new UTF8String[] { UTF8String.fromString("1a2") };
    var array_AaXbB = new UTF8String[] { UTF8String.fromString("AaXbB") };
    var array_aBcDe = new UTF8String[] { UTF8String.fromString("aBcDe") };
    var array_special = new UTF8String[] { UTF8String.fromString("äb世De") };
    var array_abcde = new UTF8String[] { UTF8String.fromString("äbćδe") };
    var full_match = new UTF8String[] { UTF8String.fromString(""), UTF8String.fromString("") };
    var array_1_2 = new UTF8String[] { UTF8String.fromString("1"), UTF8String.fromString("2") };
    var array_A_B = new UTF8String[] { UTF8String.fromString("A"), UTF8String.fromString("B") };
    var array_a_e = new UTF8String[] { UTF8String.fromString("ä"), UTF8String.fromString("e") };
    var array_Aa_bB = new UTF8String[] { UTF8String.fromString("Aa"), UTF8String.fromString("bB") };
    // Edge cases
    assertStringSplitSQL("", "", "UTF8_BINARY", empty_match);
    assertStringSplitSQL("abc", "", "UTF8_BINARY", array_abc);
    assertStringSplitSQL("", "abc", "UTF8_BINARY", empty_match);
    assertStringSplitSQL("", "", "UNICODE", empty_match);
    assertStringSplitSQL("abc", "", "UNICODE", array_abc);
    assertStringSplitSQL("", "abc", "UNICODE", empty_match);
    assertStringSplitSQL("", "", "UTF8_BINARY_LCASE", empty_match);
    assertStringSplitSQL("abc", "", "UTF8_BINARY_LCASE", array_abc);
    assertStringSplitSQL("", "abc", "UTF8_BINARY_LCASE", empty_match);
    assertStringSplitSQL("", "", "UNICODE_CI", empty_match);
    assertStringSplitSQL("abc", "", "UNICODE_CI", array_abc);
    assertStringSplitSQL("", "abc", "UNICODE_CI", empty_match);
    // Basic tests
    assertStringSplitSQL("1a2", "a", "UTF8_BINARY", array_1_2);
    assertStringSplitSQL("1a2", "A", "UTF8_BINARY", array_1a2);
    assertStringSplitSQL("1a2", "b", "UTF8_BINARY", array_1a2);
    assertStringSplitSQL("1a2", "1a2", "UNICODE", full_match);
    assertStringSplitSQL("1a2", "1A2", "UNICODE", array_1a2);
    assertStringSplitSQL("1a2", "3b4", "UNICODE", array_1a2);
    assertStringSplitSQL("1a2", "A", "UTF8_BINARY_LCASE", array_1_2);
    assertStringSplitSQL("1a2", "1A2", "UTF8_BINARY_LCASE", full_match);
    assertStringSplitSQL("1a2", "X", "UTF8_BINARY_LCASE", array_1a2);
    assertStringSplitSQL("1a2", "a", "UNICODE_CI", array_1_2);
    assertStringSplitSQL("1a2", "A", "UNICODE_CI", array_1_2);
    assertStringSplitSQL("1a2", "1A2", "UNICODE_CI", full_match);
    assertStringSplitSQL("1a2", "123", "UNICODE_CI", array_1a2);
    // Case variation
    assertStringSplitSQL("AaXbB", "x", "UTF8_BINARY", array_AaXbB);
    assertStringSplitSQL("AaXbB", "X", "UTF8_BINARY", array_Aa_bB);
    assertStringSplitSQL("AaXbB", "axb", "UNICODE", array_AaXbB);
    assertStringSplitSQL("AaXbB", "aXb", "UNICODE", array_A_B);
    assertStringSplitSQL("AaXbB", "axb", "UTF8_BINARY_LCASE", array_A_B);
    assertStringSplitSQL("AaXbB", "AXB", "UTF8_BINARY_LCASE", array_A_B);
    assertStringSplitSQL("AaXbB", "axb", "UNICODE_CI", array_A_B);
    assertStringSplitSQL("AaXbB", "AxB", "UNICODE_CI", array_A_B);
    // Accent variation
    assertStringSplitSQL("aBcDe", "bćd", "UTF8_BINARY", array_aBcDe);
    assertStringSplitSQL("aBcDe", "BćD", "UTF8_BINARY", array_aBcDe);
    assertStringSplitSQL("aBcDe", "abćde", "UNICODE", array_aBcDe);
    assertStringSplitSQL("aBcDe", "aBćDe", "UNICODE", array_aBcDe);
    assertStringSplitSQL("aBcDe", "bćd", "UTF8_BINARY_LCASE", array_aBcDe);
    assertStringSplitSQL("aBcDe", "BĆD", "UTF8_BINARY_LCASE", array_aBcDe);
    assertStringSplitSQL("aBcDe", "abćde", "UNICODE_CI", array_aBcDe);
    assertStringSplitSQL("aBcDe", "AbĆdE", "UNICODE_CI", array_aBcDe);
    // Variable byte length characters
    assertStringSplitSQL("äb世De", "b世D", "UTF8_BINARY", array_a_e);
    assertStringSplitSQL("äb世De", "B世d", "UTF8_BINARY", array_special);
    assertStringSplitSQL("äbćδe", "bćδ", "UTF8_BINARY", array_a_e);
    assertStringSplitSQL("äbćδe", "BcΔ", "UTF8_BINARY", array_abcde);
    assertStringSplitSQL("äb世De", "äb世De", "UNICODE", full_match);
    assertStringSplitSQL("äb世De", "äB世de", "UNICODE", array_special);
    assertStringSplitSQL("äbćδe", "äbćδe", "UNICODE", full_match);
    assertStringSplitSQL("äbćδe", "ÄBcΔÉ", "UNICODE", array_abcde);
    assertStringSplitSQL("äb世De", "b世D", "UTF8_BINARY_LCASE", array_a_e);
    assertStringSplitSQL("äb世De", "B世d", "UTF8_BINARY_LCASE", array_a_e);
    assertStringSplitSQL("äbćδe", "bćδ", "UTF8_BINARY_LCASE", array_a_e);
    assertStringSplitSQL("äbćδe", "BcΔ", "UTF8_BINARY_LCASE", array_abcde);
    assertStringSplitSQL("äb世De", "ab世De", "UNICODE_CI", array_special);
    assertStringSplitSQL("äb世De", "AB世dE", "UNICODE_CI", array_special);
    assertStringSplitSQL("äbćδe", "ÄbćδE", "UNICODE_CI", full_match);
    assertStringSplitSQL("äbćδe", "ÄBcΔÉ", "UNICODE_CI", array_abcde);
  }

  private void assertUpper(String target, String collationName, String expected)
          throws SparkException {
    UTF8String target_utf8 = UTF8String.fromString(target);
    UTF8String expected_utf8 = UTF8String.fromString(expected);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(expected_utf8, CollationSupport.Upper.exec(target_utf8, collationId));
  }

  @Test
  public void testUpper() throws SparkException {
    // Edge cases
    assertUpper("", "UTF8_BINARY", "");
    assertUpper("", "UTF8_BINARY_LCASE", "");
    assertUpper("", "UNICODE", "");
    assertUpper("", "UNICODE_CI", "");
    // Basic tests
    assertUpper("abcde", "UTF8_BINARY", "ABCDE");
    assertUpper("abcde", "UTF8_BINARY_LCASE", "ABCDE");
    assertUpper("abcde", "UNICODE", "ABCDE");
    assertUpper("abcde", "UNICODE_CI", "ABCDE");
    // Uppercase present
    assertUpper("AbCdE", "UTF8_BINARY", "ABCDE");
    assertUpper("aBcDe", "UTF8_BINARY", "ABCDE");
    assertUpper("AbCdE", "UTF8_BINARY_LCASE", "ABCDE");
    assertUpper("aBcDe", "UTF8_BINARY_LCASE", "ABCDE");
    assertUpper("AbCdE", "UNICODE", "ABCDE");
    assertUpper("aBcDe", "UNICODE", "ABCDE");
    assertUpper("AbCdE", "UNICODE_CI", "ABCDE");
    assertUpper("aBcDe", "UNICODE_CI", "ABCDE");
    // Accent letters
    assertUpper("aBćDe","UTF8_BINARY", "ABĆDE");
    assertUpper("aBćDe","UTF8_BINARY_LCASE", "ABĆDE");
    assertUpper("aBćDe","UNICODE", "ABĆDE");
    assertUpper("aBćDe","UNICODE_CI", "ABĆDE");
    // Variable byte length characters
    assertUpper("ab世De", "UTF8_BINARY", "AB世DE");
    assertUpper("äbćδe", "UTF8_BINARY", "ÄBĆΔE");
    assertUpper("ab世De", "UTF8_BINARY_LCASE", "AB世DE");
    assertUpper("äbćδe", "UTF8_BINARY_LCASE", "ÄBĆΔE");
    assertUpper("ab世De", "UNICODE", "AB世DE");
    assertUpper("äbćδe", "UNICODE", "ÄBĆΔE");
    assertUpper("ab世De", "UNICODE_CI", "AB世DE");
    assertUpper("äbćδe", "UNICODE_CI", "ÄBĆΔE");
    // Case-variable character length
    assertUpper("i̇o", "UTF8_BINARY","İO");
    assertUpper("i̇o", "UTF8_BINARY_LCASE","İO");
    assertUpper("i̇o", "UNICODE","İO");
    assertUpper("i̇o", "UNICODE_CI","İO");
  }

  private void assertLower(String target, String collationName, String expected)
          throws SparkException {
    UTF8String target_utf8 = UTF8String.fromString(target);
    UTF8String expected_utf8 = UTF8String.fromString(expected);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(expected_utf8, CollationSupport.Lower.exec(target_utf8, collationId));
  }

  @Test
  public void testLower() throws SparkException {
    // Edge cases
    assertLower("", "UTF8_BINARY", "");
    assertLower("", "UTF8_BINARY_LCASE", "");
    assertLower("", "UNICODE", "");
    assertLower("", "UNICODE_CI", "");
    // Basic tests
    assertLower("ABCDE", "UTF8_BINARY", "abcde");
    assertLower("ABCDE", "UTF8_BINARY_LCASE", "abcde");
    assertLower("ABCDE", "UNICODE", "abcde");
    assertLower("ABCDE", "UNICODE_CI", "abcde");
    // Uppercase present
    assertLower("AbCdE", "UTF8_BINARY", "abcde");
    assertLower("aBcDe", "UTF8_BINARY", "abcde");
    assertLower("AbCdE", "UTF8_BINARY_LCASE", "abcde");
    assertLower("aBcDe", "UTF8_BINARY_LCASE", "abcde");
    assertLower("AbCdE", "UNICODE", "abcde");
    assertLower("aBcDe", "UNICODE", "abcde");
    assertLower("AbCdE", "UNICODE_CI", "abcde");
    assertLower("aBcDe", "UNICODE_CI", "abcde");
    // Accent letters
    assertLower("AbĆdE","UTF8_BINARY", "abćde");
    assertLower("AbĆdE","UTF8_BINARY_LCASE", "abćde");
    assertLower("AbĆdE","UNICODE", "abćde");
    assertLower("AbĆdE","UNICODE_CI", "abćde");
    // Variable byte length characters
    assertLower("aB世De", "UTF8_BINARY", "ab世de");
    assertLower("ÄBĆΔE", "UTF8_BINARY", "äbćδe");
    assertLower("aB世De", "UTF8_BINARY_LCASE", "ab世de");
    assertLower("ÄBĆΔE", "UTF8_BINARY_LCASE", "äbćδe");
    assertLower("aB世De", "UNICODE", "ab世de");
    assertLower("ÄBĆΔE", "UNICODE", "äbćδe");
    assertLower("aB世De", "UNICODE_CI", "ab世de");
    assertLower("ÄBĆΔE", "UNICODE_CI", "äbćδe");
    // Case-variable character length
    assertLower("İo", "UTF8_BINARY","i̇o");
    assertLower("İo", "UTF8_BINARY_LCASE","i̇o");
    assertLower("İo", "UNICODE","i̇o");
    assertLower("İo", "UNICODE_CI","i̇o");
  }

  private void assertInitCap(String target, String collationName, String expected)
          throws SparkException {
    UTF8String target_utf8 = UTF8String.fromString(target);
    UTF8String expected_utf8 = UTF8String.fromString(expected);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(expected_utf8, CollationSupport.InitCap.exec(target_utf8, collationId));
  }

  @Test
  public void testInitCap() throws SparkException {
    // Edge cases
    assertInitCap("", "UTF8_BINARY", "");
    assertInitCap("", "UTF8_BINARY_LCASE", "");
    assertInitCap("", "UNICODE", "");
    assertInitCap("", "UNICODE_CI", "");
    // Basic tests
    assertInitCap("ABCDE", "UTF8_BINARY", "Abcde");
    assertInitCap("ABCDE", "UTF8_BINARY_LCASE", "Abcde");
    assertInitCap("ABCDE", "UNICODE", "Abcde");
    assertInitCap("ABCDE", "UNICODE_CI", "Abcde");
    // Uppercase present
    assertInitCap("AbCdE", "UTF8_BINARY", "Abcde");
    assertInitCap("aBcDe", "UTF8_BINARY", "Abcde");
    assertInitCap("AbCdE", "UTF8_BINARY_LCASE", "Abcde");
    assertInitCap("aBcDe", "UTF8_BINARY_LCASE", "Abcde");
    assertInitCap("AbCdE", "UNICODE", "Abcde");
    assertInitCap("aBcDe", "UNICODE", "Abcde");
    assertInitCap("AbCdE", "UNICODE_CI", "Abcde");
    assertInitCap("aBcDe", "UNICODE_CI", "Abcde");
    // Accent letters
    assertInitCap("AbĆdE", "UTF8_BINARY", "Abćde");
    assertInitCap("AbĆdE", "UTF8_BINARY_LCASE", "Abćde");
    assertInitCap("AbĆdE", "UNICODE", "Abćde");
    assertInitCap("AbĆdE", "UNICODE_CI", "Abćde");
    // Variable byte length characters
    assertInitCap("aB 世 De", "UTF8_BINARY", "Ab 世 De");
    assertInitCap("ÄBĆΔE", "UTF8_BINARY", "Äbćδe");
    assertInitCap("aB 世 De", "UTF8_BINARY_LCASE", "Ab 世 De");
    assertInitCap("ÄBĆΔE", "UTF8_BINARY_LCASE", "Äbćδe");
    assertInitCap("aB 世 De", "UNICODE", "Ab 世 De");
    assertInitCap("ÄBĆΔE", "UNICODE", "Äbćδe");
    assertInitCap("aB 世 de", "UNICODE_CI", "Ab 世 De");
    assertInitCap("ÄBĆΔE", "UNICODE_CI", "Äbćδe");
    // Case-variable character length
    assertInitCap("İo", "UTF8_BINARY", "İo");
    assertInitCap("İo", "UTF8_BINARY_LCASE", "İo");
    assertInitCap("İo", "UNICODE", "İo");
    assertInitCap("İo", "UNICODE_CI", "İo");
  }

  private void assertStringInstr(String string, String substring, String collationName,
          Integer expected) throws SparkException {
    UTF8String str = UTF8String.fromString(string);
    UTF8String substr = UTF8String.fromString(substring);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(expected, CollationSupport.StringInstr.exec(str, substr, collationId) + 1);
  }

  @Test
  public void testStringInstr() throws SparkException {
    assertStringInstr("aaads", "Aa", "UTF8_BINARY", 0);
    assertStringInstr("aaaDs", "de", "UTF8_BINARY", 0);
    assertStringInstr("aaads", "ds", "UTF8_BINARY", 4);
    assertStringInstr("xxxx", "", "UTF8_BINARY", 1);
    assertStringInstr("", "xxxx", "UTF8_BINARY", 0);
    assertStringInstr("test大千世界X大千世界", "大千", "UTF8_BINARY", 5);
    assertStringInstr("test大千世界X大千世界", "界X", "UTF8_BINARY", 8);
    assertStringInstr("aaads", "Aa", "UTF8_BINARY_LCASE", 1);
    assertStringInstr("aaaDs", "de", "UTF8_BINARY_LCASE", 0);
    assertStringInstr("aaaDs", "ds", "UTF8_BINARY_LCASE", 4);
    assertStringInstr("xxxx", "", "UTF8_BINARY_LCASE", 1);
    assertStringInstr("", "xxxx", "UTF8_BINARY_LCASE", 0);
    assertStringInstr("test大千世界X大千世界", "大千", "UTF8_BINARY_LCASE", 5);
    assertStringInstr("test大千世界X大千世界", "界x", "UTF8_BINARY_LCASE", 8);
    assertStringInstr("aaads", "Aa", "UNICODE", 0);
    assertStringInstr("aaads", "aa", "UNICODE", 1);
    assertStringInstr("aaads", "de", "UNICODE", 0);
    assertStringInstr("xxxx", "", "UNICODE", 1);
    assertStringInstr("", "xxxx", "UNICODE", 0);
    assertStringInstr("test大千世界X大千世界", "界x", "UNICODE", 0);
    assertStringInstr("test大千世界X大千世界", "界X", "UNICODE", 8);
    assertStringInstr("aaads", "AD", "UNICODE_CI", 3);
    assertStringInstr("aaads", "dS", "UNICODE_CI", 4);
    assertStringInstr("test大千世界X大千世界", "界y", "UNICODE_CI", 0);
    assertStringInstr("test大千世界X大千世界", "界x", "UNICODE_CI", 8);
    assertStringInstr("i̇", "i", "UNICODE_CI", 0);
    assertStringInstr("i̇", "\u0307", "UNICODE_CI", 0);
    assertStringInstr("i̇", "İ", "UNICODE_CI", 1);
    assertStringInstr("İ", "i", "UNICODE_CI", 0);
    assertStringInstr("İoi̇o12", "i̇o", "UNICODE_CI", 1);
    assertStringInstr("i̇oİo12", "İo", "UNICODE_CI", 1);
    assertStringInstr("abİoi̇o", "i̇o", "UNICODE_CI", 3);
    assertStringInstr("abi̇oİo", "İo", "UNICODE_CI", 3);
    assertStringInstr("ai̇oxXİo", "Xx", "UNICODE_CI", 5);
    assertStringInstr("aİoi̇oxx", "XX", "UNICODE_CI", 7);
    assertStringInstr("i̇", "i", "UTF8_BINARY_LCASE", 1); // != UNICODE_CI
    assertStringInstr("i̇", "\u0307", "UTF8_BINARY_LCASE", 2); // != UNICODE_CI
    assertStringInstr("i̇", "İ", "UTF8_BINARY_LCASE", 1);
    assertStringInstr("İ", "i", "UTF8_BINARY_LCASE", 0);
    assertStringInstr("İoi̇o12", "i̇o", "UTF8_BINARY_LCASE", 1);
    assertStringInstr("i̇oİo12", "İo", "UTF8_BINARY_LCASE", 1);
    assertStringInstr("abİoi̇o", "i̇o", "UTF8_BINARY_LCASE", 3);
    assertStringInstr("abi̇oİo", "İo", "UTF8_BINARY_LCASE", 3);
    assertStringInstr("abI\u0307oi̇o", "İo", "UTF8_BINARY_LCASE", 3);
    assertStringInstr("ai̇oxXİo", "Xx", "UTF8_BINARY_LCASE", 5);
    assertStringInstr("abİoi̇o", "\u0307o", "UTF8_BINARY_LCASE", 6);
    assertStringInstr("aİoi̇oxx", "XX", "UTF8_BINARY_LCASE", 7);
  }

  private void assertFindInSet(String word, String set, String collationName,
        Integer expected) throws SparkException {
    UTF8String w = UTF8String.fromString(word);
    UTF8String s = UTF8String.fromString(set);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(expected, CollationSupport.FindInSet.exec(w, s, collationId));
  }

  @Test
  public void testFindInSet() throws SparkException {
    assertFindInSet("AB", "abc,b,ab,c,def", "UTF8_BINARY", 0);
    assertFindInSet("abc", "abc,b,ab,c,def", "UTF8_BINARY", 1);
    assertFindInSet("def", "abc,b,ab,c,def", "UTF8_BINARY", 5);
    assertFindInSet("d,ef", "abc,b,ab,c,def", "UTF8_BINARY", 0);
    assertFindInSet("", "abc,b,ab,c,def", "UTF8_BINARY", 0);
    assertFindInSet("a", "abc,b,ab,c,def", "UTF8_BINARY_LCASE", 0);
    assertFindInSet("c", "abc,b,ab,c,def", "UTF8_BINARY_LCASE", 4);
    assertFindInSet("AB", "abc,b,ab,c,def", "UTF8_BINARY_LCASE", 3);
    assertFindInSet("AbC", "abc,b,ab,c,def", "UTF8_BINARY_LCASE", 1);
    assertFindInSet("abcd", "abc,b,ab,c,def", "UTF8_BINARY_LCASE", 0);
    assertFindInSet("d,ef", "abc,b,ab,c,def", "UTF8_BINARY_LCASE", 0);
    assertFindInSet("XX", "xx", "UTF8_BINARY_LCASE", 1);
    assertFindInSet("", "abc,b,ab,c,def", "UTF8_BINARY_LCASE", 0);
    assertFindInSet("界x", "test,大千,世,界X,大,千,世界", "UTF8_BINARY_LCASE", 4);
    assertFindInSet("a", "abc,b,ab,c,def", "UNICODE", 0);
    assertFindInSet("ab", "abc,b,ab,c,def", "UNICODE", 3);
    assertFindInSet("Ab", "abc,b,ab,c,def", "UNICODE", 0);
    assertFindInSet("d,ef", "abc,b,ab,c,def", "UNICODE", 0);
    assertFindInSet("xx", "xx", "UNICODE", 1);
    assertFindInSet("界x", "test,大千,世,界X,大,千,世界", "UNICODE", 0);
    assertFindInSet("大", "test,大千,世,界X,大,千,世界", "UNICODE", 5);
    assertFindInSet("a", "abc,b,ab,c,def", "UNICODE_CI", 0);
    assertFindInSet("C", "abc,b,ab,c,def", "UNICODE_CI", 4);
    assertFindInSet("DeF", "abc,b,ab,c,dEf", "UNICODE_CI", 5);
    assertFindInSet("DEFG", "abc,b,ab,c,def", "UNICODE_CI", 0);
    assertFindInSet("XX", "xx", "UNICODE_CI", 1);
    assertFindInSet("界x", "test,大千,世,界X,大,千,世界", "UNICODE_CI", 4);
    assertFindInSet("界x", "test,大千,界Xx,世,界X,大,千,世界", "UNICODE_CI", 5);
    assertFindInSet("大", "test,大千,世,界X,大,千,世界", "UNICODE_CI", 5);
    assertFindInSet("i̇o", "ab,İo,12", "UNICODE_CI", 2);
    assertFindInSet("İo", "ab,i̇o,12", "UNICODE_CI", 2);
  }

  private void assertReplace(String source, String search, String replace, String collationName,
        String expected) throws SparkException {
    UTF8String src = UTF8String.fromString(source);
    UTF8String sear = UTF8String.fromString(search);
    UTF8String repl = UTF8String.fromString(replace);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(expected, CollationSupport.StringReplace
      .exec(src, sear, repl, collationId).toString());
  }

  @Test
  public void testReplace() throws SparkException {
    assertReplace("r世eplace", "pl", "123", "UTF8_BINARY", "r世e123ace");
    assertReplace("replace", "pl", "", "UTF8_BINARY", "reace");
    assertReplace("repl世ace", "Pl", "", "UTF8_BINARY", "repl世ace");
    assertReplace("replace", "", "123", "UTF8_BINARY", "replace");
    assertReplace("abcabc", "b", "12", "UTF8_BINARY", "a12ca12c");
    assertReplace("abcdabcd", "bc", "", "UTF8_BINARY", "adad");
    assertReplace("r世eplace", "pl", "xx", "UTF8_BINARY_LCASE", "r世exxace");
    assertReplace("repl世ace", "PL", "AB", "UTF8_BINARY_LCASE", "reAB世ace");
    assertReplace("Replace", "", "123", "UTF8_BINARY_LCASE", "Replace");
    assertReplace("re世place", "世", "x", "UTF8_BINARY_LCASE", "rexplace");
    assertReplace("abcaBc", "B", "12", "UTF8_BINARY_LCASE", "a12ca12c");
    assertReplace("AbcdabCd", "Bc", "", "UTF8_BINARY_LCASE", "Adad");
    assertReplace("re世place", "plx", "123", "UNICODE", "re世place");
    assertReplace("世Replace", "re", "", "UNICODE", "世Replace");
    assertReplace("replace世", "", "123", "UNICODE", "replace世");
    assertReplace("aBc世abc", "b", "12", "UNICODE", "aBc世a12c");
    assertReplace("abcdabcd", "bc", "", "UNICODE", "adad");
    assertReplace("replace", "plx", "123", "UNICODE_CI", "replace");
    assertReplace("Replace", "re", "", "UNICODE_CI", "place");
    assertReplace("replace", "", "123", "UNICODE_CI", "replace");
    assertReplace("aBc世abc", "b", "12", "UNICODE_CI", "a12c世a12c");
    assertReplace("a世Bcdabcd", "bC", "", "UNICODE_CI", "a世dad");
    assertReplace("abİo12i̇o", "i̇o", "xx", "UNICODE_CI", "abxx12xx");
    assertReplace("abi̇o12i̇o", "İo", "yy", "UNICODE_CI", "abyy12yy");
  }

  private void assertLocate(String substring, String string, Integer start, String collationName,
        Integer expected) throws SparkException {
    UTF8String substr = UTF8String.fromString(substring);
    UTF8String str = UTF8String.fromString(string);
    int collationId = CollationFactory.collationNameToId(collationName);
    assertEquals(expected, CollationSupport.StringLocate.exec(str, substr,
      start - 1, collationId) + 1);
  }

  @Test
  public void testLocate() throws SparkException {
    // If you add tests with start < 1 be careful to understand the behavior of the indexOf method
    // and usage of indexOf in the StringLocate class.
    assertLocate("aa", "aaads", 1, "UTF8_BINARY", 1);
    assertLocate("aa", "aaads", 2, "UTF8_BINARY", 2);
    assertLocate("aa", "aaads", 3, "UTF8_BINARY", 0);
    assertLocate("Aa", "aaads", 1, "UTF8_BINARY", 0);
    assertLocate("Aa", "aAads", 1, "UTF8_BINARY", 2);
    assertLocate("界x", "test大千世界X大千世界", 1, "UTF8_BINARY", 0);
    assertLocate("界X", "test大千世界X大千世界", 1, "UTF8_BINARY", 8);
    assertLocate("界", "test大千世界X大千世界", 13, "UTF8_BINARY", 13);
    assertLocate("AA", "aaads", 1, "UTF8_BINARY_LCASE", 1);
    assertLocate("aa", "aAads", 2, "UTF8_BINARY_LCASE", 2);
    assertLocate("aa", "aaAds", 3, "UTF8_BINARY_LCASE", 0);
    assertLocate("abC", "abcabc", 1, "UTF8_BINARY_LCASE", 1);
    assertLocate("abC", "abCabc", 2, "UTF8_BINARY_LCASE", 4);
    assertLocate("abc", "abcabc", 4, "UTF8_BINARY_LCASE", 4);
    assertLocate("界x", "test大千世界X大千世界", 1, "UTF8_BINARY_LCASE", 8);
    assertLocate("界X", "test大千世界Xtest大千世界", 1, "UTF8_BINARY_LCASE", 8);
    assertLocate("界", "test大千世界X大千世界", 13, "UTF8_BINARY_LCASE", 13);
    assertLocate("大千", "test大千世界大千世界", 1, "UTF8_BINARY_LCASE", 5);
    assertLocate("大千", "test大千世界大千世界", 9, "UTF8_BINARY_LCASE", 9);
    assertLocate("大千", "大千世界大千世界", 1, "UTF8_BINARY_LCASE", 1);
    assertLocate("aa", "Aaads", 1, "UNICODE", 2);
    assertLocate("AA", "aaads", 1, "UNICODE", 0);
    assertLocate("aa", "aAads", 2, "UNICODE", 0);
    assertLocate("aa", "aaAds", 3, "UNICODE", 0);
    assertLocate("abC", "abcabc", 1, "UNICODE", 0);
    assertLocate("abC", "abCabc", 2, "UNICODE", 0);
    assertLocate("abC", "abCabC", 2, "UNICODE", 4);
    assertLocate("abc", "abcabc", 1, "UNICODE", 1);
    assertLocate("abc", "abcabc", 3, "UNICODE", 4);
    assertLocate("界x", "test大千世界X大千世界", 1, "UNICODE", 0);
    assertLocate("界X", "test大千世界X大千世界", 1, "UNICODE", 8);
    assertLocate("界", "test大千世界X大千世界", 13, "UNICODE", 13);
    assertLocate("AA", "aaads", 1, "UNICODE_CI", 1);
    assertLocate("aa", "aAads", 2, "UNICODE_CI", 2);
    assertLocate("aa", "aaAds", 3, "UNICODE_CI", 0);
    assertLocate("abC", "abcabc", 1, "UNICODE_CI", 1);
    assertLocate("abC", "abCabc", 2, "UNICODE_CI", 4);
    assertLocate("abc", "abcabc", 4, "UNICODE_CI", 4);
    assertLocate("界x", "test大千世界X大千世界", 1, "UNICODE_CI", 8);
    assertLocate("界", "test大千世界X大千世界", 13, "UNICODE_CI", 13);
    assertLocate("大千", "test大千世界大千世界", 1, "UNICODE_CI", 5);
    assertLocate("大千", "test大千世界大千世界", 9, "UNICODE_CI", 9);
    assertLocate("大千", "大千世界大千世界", 1, "UNICODE_CI", 1);
    // Case-variable character length
    assertLocate("\u0307", "i̇", 1, "UTF8_BINARY", 2);
    assertLocate("\u0307", "İ", 1, "UTF8_BINARY_LCASE", 0); // != UTF8_BINARY
    assertLocate("i", "i̇", 1, "UNICODE_CI", 0);
    assertLocate("\u0307", "i̇", 1, "UNICODE_CI", 0);
    assertLocate("i̇", "i", 1, "UNICODE_CI", 0);
    assertLocate("İ", "i̇", 1, "UNICODE_CI", 1);
    assertLocate("İ", "i", 1, "UNICODE_CI", 0);
    assertLocate("i", "i̇", 1, "UTF8_BINARY_LCASE", 1); // != UNICODE_CI
    assertLocate("\u0307", "i̇", 1, "UTF8_BINARY_LCASE", 2); // != UNICODE_CI
    assertLocate("i̇", "i", 1, "UTF8_BINARY_LCASE", 0);
    assertLocate("İ", "i̇", 1, "UTF8_BINARY_LCASE", 1);
    assertLocate("İ", "i", 1, "UTF8_BINARY_LCASE", 0);
    assertLocate("i̇o", "İo世界大千世界", 1, "UNICODE_CI", 1);
    assertLocate("i̇o", "大千İo世界大千世界", 1, "UNICODE_CI", 3);
    assertLocate("i̇o", "世界İo大千世界大千İo", 4, "UNICODE_CI", 11);
    assertLocate("İo", "i̇o世界大千世界", 1, "UNICODE_CI", 1);
    assertLocate("İo", "大千i̇o世界大千世界", 1, "UNICODE_CI", 3);
    assertLocate("İo", "世界i̇o大千世界大千i̇o", 4, "UNICODE_CI", 12);
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
    assertSubstringIndex("abİo12", "i̇o", 1, "UNICODE_CI", "ab");
    assertSubstringIndex("abİo12", "i̇o", -1, "UNICODE_CI", "12");
    assertSubstringIndex("abi̇o12", "İo", 1, "UNICODE_CI", "ab");
    assertSubstringIndex("abi̇o12", "İo", -1, "UNICODE_CI", "12");
    assertSubstringIndex("ai̇bi̇o12", "İo", 1, "UNICODE_CI", "ai̇b");
    assertSubstringIndex("ai̇bi̇o12i̇o", "İo", 2, "UNICODE_CI", "ai̇bi̇o12");
    assertSubstringIndex("ai̇bi̇o12i̇o", "İo", -1, "UNICODE_CI", "");
    assertSubstringIndex("ai̇bi̇o12i̇o", "İo", -2, "UNICODE_CI", "12i̇o");
    assertSubstringIndex("ai̇bi̇oİo12İoi̇o", "İo", -4, "UNICODE_CI", "İo12İoi̇o");
    assertSubstringIndex("ai̇bi̇oİo12İoi̇o", "i̇o", -4, "UNICODE_CI", "İo12İoi̇o");
    assertSubstringIndex("ai̇bİoi̇o12i̇oİo", "İo", -4, "UNICODE_CI", "i̇o12i̇oİo");
    assertSubstringIndex("ai̇bİoi̇o12i̇oİo", "i̇o", -4, "UNICODE_CI", "i̇o12i̇oİo");
    assertSubstringIndex("abi̇12", "i", 1, "UNICODE_CI", "abi̇12");
    assertSubstringIndex("abi̇12", "\u0307", 1, "UNICODE_CI", "abi̇12");
    assertSubstringIndex("abi̇12", "İ", 1, "UNICODE_CI", "ab");
    assertSubstringIndex("abİ12", "i", 1, "UNICODE_CI", "abİ12");
    assertSubstringIndex("ai̇bi̇oİo12İoi̇o", "İo", -4, "UNICODE_CI", "İo12İoi̇o");
    assertSubstringIndex("ai̇bi̇oİo12İoi̇o", "i̇o", -4, "UNICODE_CI", "İo12İoi̇o");
    assertSubstringIndex("ai̇bİoi̇o12i̇oİo", "İo", -4, "UNICODE_CI", "i̇o12i̇oİo");
    assertSubstringIndex("ai̇bİoi̇o12i̇oİo", "i̇o", -4, "UNICODE_CI", "i̇o12i̇oİo");
    assertSubstringIndex("ai̇bi̇oİo12İoi̇o", "İo", 3, "UNICODE_CI", "ai̇bi̇oİo12");
    assertSubstringIndex("ai̇bi̇oİo12İoi̇o", "i̇o", 3, "UNICODE_CI", "ai̇bi̇oİo12");
    assertSubstringIndex("ai̇bİoi̇o12i̇oİo", "İo", 3, "UNICODE_CI", "ai̇bİoi̇o12");
    assertSubstringIndex("ai̇bİoi̇o12i̇oİo", "i̇o", 3, "UNICODE_CI", "ai̇bİoi̇o12");
    assertSubstringIndex("abi̇12", "i", 1, "UTF8_BINARY_LCASE", "ab"); // != UNICODE_CI
    assertSubstringIndex("abi̇12", "\u0307", 1, "UTF8_BINARY_LCASE", "abi"); // != UNICODE_CI
    assertSubstringIndex("abi̇12", "İ", 1, "UTF8_BINARY_LCASE", "ab");
    assertSubstringIndex("abİ12", "i", 1, "UTF8_BINARY_LCASE", "abİ12");
    assertSubstringIndex("ai̇bi̇oİo12İoi̇o", "İo", -4, "UTF8_BINARY_LCASE", "İo12İoi̇o");
    assertSubstringIndex("ai̇bi̇oİo12İoi̇o", "i̇o", -4, "UTF8_BINARY_LCASE", "İo12İoi̇o");
    assertSubstringIndex("ai̇bİoi̇o12i̇oİo", "İo", -4, "UTF8_BINARY_LCASE", "i̇o12i̇oİo");
    assertSubstringIndex("ai̇bİoi̇o12i̇oİo", "i̇o", -4, "UTF8_BINARY_LCASE", "i̇o12i̇oİo");
    assertSubstringIndex("bİoi̇o12i̇o", "\u0307oi", 1, "UTF8_BINARY_LCASE", "bİoi̇o12i̇o");
    assertSubstringIndex("ai̇bi̇oİo12İoi̇o", "İo", 3, "UTF8_BINARY_LCASE", "ai̇bi̇oİo12");
    assertSubstringIndex("ai̇bi̇oİo12İoi̇o", "i̇o", 3, "UTF8_BINARY_LCASE", "ai̇bi̇oİo12");
    assertSubstringIndex("ai̇bİoi̇o12i̇oİo", "İo", 3, "UTF8_BINARY_LCASE", "ai̇bİoi̇o12");
    assertSubstringIndex("ai̇bİoi̇o12i̇oİo", "i̇o", 3, "UTF8_BINARY_LCASE", "ai̇bİoi̇o12");
    assertSubstringIndex("bİoi̇o12i̇o", "\u0307oi", 1, "UTF8_BINARY_LCASE", "bİoi̇o12i̇o");
  }

  private void assertStringTrim(
      String collation,
      String sourceString,
      String trimString,
      String expectedResultString) throws SparkException {
    int collationId = CollationFactory.collationNameToId(collation);
    String result;

    if (trimString == null) {
      result = CollationSupport.StringTrim.exec(
        UTF8String.fromString(sourceString), collationId).toString();
    } else {
      result = CollationSupport.StringTrim.exec(
        UTF8String
          .fromString(sourceString), UTF8String.fromString(trimString), collationId)
          .toString();
    }

    assertEquals(expectedResultString, result);
  }

  private void assertStringTrimLeft(
      String collation,
      String sourceString,
      String trimString,
      String expectedResultString) throws SparkException {
    int collationId = CollationFactory.collationNameToId(collation);
    String result;

    if (trimString == null) {
      result = CollationSupport.StringTrimLeft.exec(
        UTF8String.fromString(sourceString), collationId).toString();
    } else {
      result = CollationSupport.StringTrimLeft.exec(
        UTF8String
          .fromString(sourceString), UTF8String.fromString(trimString), collationId)
          .toString();
    }

    assertEquals(expectedResultString, result);
  }

  private void assertStringTrimRight(
      String collation,
      String sourceString,
      String trimString,
      String expectedResultString) throws SparkException {
    int collationId = CollationFactory.collationNameToId(collation);
    String result;

    if (trimString == null) {
      result = CollationSupport.StringTrimRight.exec(
        UTF8String.fromString(sourceString), collationId).toString();
    } else {
      result = CollationSupport.StringTrimRight.exec(
        UTF8String
          .fromString(sourceString), UTF8String.fromString(trimString), collationId)
          .toString();
    }

    assertEquals(expectedResultString, result);
  }

  @Test
  public void testStringTrim() throws SparkException {
    assertStringTrim("UTF8_BINARY", "asd", null, "asd");
    assertStringTrim("UTF8_BINARY", "  asd  ", null, "asd");
    assertStringTrim("UTF8_BINARY", " a世a ", null, "a世a");
    assertStringTrim("UTF8_BINARY", "asd", "x", "asd");
    assertStringTrim("UTF8_BINARY", "xxasdxx", "x", "asd");
    assertStringTrim("UTF8_BINARY", "xa世ax", "x", "a世a");

    assertStringTrimLeft("UTF8_BINARY", "asd", null, "asd");
    assertStringTrimLeft("UTF8_BINARY", "  asd  ", null, "asd  ");
    assertStringTrimLeft("UTF8_BINARY", " a世a ", null, "a世a ");
    assertStringTrimLeft("UTF8_BINARY", "asd", "x", "asd");
    assertStringTrimLeft("UTF8_BINARY", "xxasdxx", "x", "asdxx");
    assertStringTrimLeft("UTF8_BINARY", "xa世ax", "x", "a世ax");

    assertStringTrimRight("UTF8_BINARY", "asd", null, "asd");
    assertStringTrimRight("UTF8_BINARY", "  asd  ", null, "  asd");
    assertStringTrimRight("UTF8_BINARY", " a世a ", null, " a世a");
    assertStringTrimRight("UTF8_BINARY", "asd", "x", "asd");
    assertStringTrimRight("UTF8_BINARY", "xxasdxx", "x", "xxasd");
    assertStringTrimRight("UTF8_BINARY", "xa世ax", "x", "xa世a");

    assertStringTrim("UTF8_BINARY_LCASE", "asd", null, "asd");
    assertStringTrim("UTF8_BINARY_LCASE", "  asd  ", null, "asd");
    assertStringTrim("UTF8_BINARY_LCASE", " a世a ", null, "a世a");
    assertStringTrim("UTF8_BINARY_LCASE", "asd", "x", "asd");
    assertStringTrim("UTF8_BINARY_LCASE", "xxasdxx", "x", "asd");
    assertStringTrim("UTF8_BINARY_LCASE", "xa世ax", "x", "a世a");

    assertStringTrimLeft("UTF8_BINARY_LCASE", "asd", null, "asd");
    assertStringTrimLeft("UTF8_BINARY_LCASE", "  asd  ", null, "asd  ");
    assertStringTrimLeft("UTF8_BINARY_LCASE", " a世a ", null, "a世a ");
    assertStringTrimLeft("UTF8_BINARY_LCASE", "asd", "x", "asd");
    assertStringTrimLeft("UTF8_BINARY_LCASE", "xxasdxx", "x", "asdxx");
    assertStringTrimLeft("UTF8_BINARY_LCASE", "xa世ax", "x", "a世ax");

    assertStringTrimRight("UTF8_BINARY_LCASE", "asd", null, "asd");
    assertStringTrimRight("UTF8_BINARY_LCASE", "  asd  ", null, "  asd");
    assertStringTrimRight("UTF8_BINARY_LCASE", " a世a ", null, " a世a");
    assertStringTrimRight("UTF8_BINARY_LCASE", "asd", "x", "asd");
    assertStringTrimRight("UTF8_BINARY_LCASE", "xxasdxx", "x", "xxasd");
    assertStringTrimRight("UTF8_BINARY_LCASE", "xa世ax", "x", "xa世a");

    assertStringTrim("UTF8_BINARY_LCASE", "asd", null, "asd");
    assertStringTrim("UTF8_BINARY_LCASE", "  asd  ", null, "asd");
    assertStringTrim("UTF8_BINARY_LCASE", " a世a ", null, "a世a");
    assertStringTrim("UTF8_BINARY_LCASE", "asd", "x", "asd");
    assertStringTrim("UTF8_BINARY_LCASE", "xxasdxx", "x", "asd");
    assertStringTrim("UTF8_BINARY_LCASE", "xa世ax", "x", "a世a");

    assertStringTrimLeft("UNICODE", "asd", null, "asd");
    assertStringTrimLeft("UNICODE", "  asd  ", null, "asd  ");
    assertStringTrimLeft("UNICODE", " a世a ", null, "a世a ");
    assertStringTrimLeft("UNICODE", "asd", "x", "asd");
    assertStringTrimLeft("UNICODE", "xxasdxx", "x", "asdxx");
    assertStringTrimLeft("UNICODE", "xa世ax", "x", "a世ax");

    assertStringTrimRight("UNICODE", "asd", null, "asd");
    assertStringTrimRight("UNICODE", "  asd  ", null, "  asd");
    assertStringTrimRight("UNICODE", " a世a ", null, " a世a");
    assertStringTrimRight("UNICODE", "asd", "x", "asd");
    assertStringTrimRight("UNICODE", "xxasdxx", "x", "xxasd");
    assertStringTrimRight("UNICODE", "xa世ax", "x", "xa世a");

    // Test cases where trimString has more than one character
    assertStringTrim("UTF8_BINARY", "ddsXXXaa", "asd", "XXX");
    assertStringTrimLeft("UTF8_BINARY", "ddsXXXaa", "asd", "XXXaa");
    assertStringTrimRight("UTF8_BINARY", "ddsXXXaa", "asd", "ddsXXX");

    assertStringTrim("UTF8_BINARY_LCASE", "ddsXXXaa", "asd", "XXX");
    assertStringTrimLeft("UTF8_BINARY_LCASE", "ddsXXXaa", "asd", "XXXaa");
    assertStringTrimRight("UTF8_BINARY_LCASE", "ddsXXXaa", "asd", "ddsXXX");

    assertStringTrim("UNICODE", "ddsXXXaa", "asd", "XXX");
    assertStringTrimLeft("UNICODE", "ddsXXXaa", "asd", "XXXaa");
    assertStringTrimRight("UNICODE", "ddsXXXaa", "asd", "ddsXXX");

    // Test cases specific to collation type
    // uppercase trim, lowercase src
    assertStringTrim("UTF8_BINARY", "asd", "A", "asd");
    assertStringTrim("UTF8_BINARY_LCASE", "asd", "A", "sd");
    assertStringTrim("UNICODE", "asd", "A", "asd");
    assertStringTrim("UNICODE_CI", "asd", "A", "sd");

    // lowercase trim, uppercase src
    assertStringTrim("UTF8_BINARY", "ASD", "a", "ASD");
    assertStringTrim("UTF8_BINARY_LCASE", "ASD", "a", "SD");
    assertStringTrim("UNICODE", "ASD", "a", "ASD");
    assertStringTrim("UNICODE_CI", "ASD", "a", "SD");

    // uppercase and lowercase chars of different byte-length (utf8)
    assertStringTrim("UTF8_BINARY", "ẞaaaẞ", "ß", "ẞaaaẞ");
    assertStringTrimLeft("UTF8_BINARY", "ẞaaaẞ", "ß", "ẞaaaẞ");
    assertStringTrimRight("UTF8_BINARY", "ẞaaaẞ", "ß", "ẞaaaẞ");

    assertStringTrim("UTF8_BINARY_LCASE", "ẞaaaẞ", "ß", "aaa");
    assertStringTrimLeft("UTF8_BINARY_LCASE", "ẞaaaẞ", "ß", "aaaẞ");
    assertStringTrimRight("UTF8_BINARY_LCASE", "ẞaaaẞ", "ß", "ẞaaa");

    assertStringTrim("UNICODE", "ẞaaaẞ", "ß", "ẞaaaẞ");
    assertStringTrimLeft("UNICODE", "ẞaaaẞ", "ß", "ẞaaaẞ");
    assertStringTrimRight("UNICODE", "ẞaaaẞ", "ß", "ẞaaaẞ");

    assertStringTrim("UTF8_BINARY", "ßaaaß", "ẞ", "ßaaaß");
    assertStringTrimLeft("UTF8_BINARY", "ßaaaß", "ẞ", "ßaaaß");
    assertStringTrimRight("UTF8_BINARY", "ßaaaß", "ẞ", "ßaaaß");

    assertStringTrim("UTF8_BINARY_LCASE", "ßaaaß", "ẞ", "aaa");
    assertStringTrimLeft("UTF8_BINARY_LCASE", "ßaaaß", "ẞ", "aaaß");
    assertStringTrimRight("UTF8_BINARY_LCASE", "ßaaaß", "ẞ", "ßaaa");

    assertStringTrim("UNICODE", "ßaaaß", "ẞ", "ßaaaß");
    assertStringTrimLeft("UNICODE", "ßaaaß", "ẞ", "ßaaaß");
    assertStringTrimRight("UNICODE", "ßaaaß", "ẞ", "ßaaaß");

    // different byte-length (utf8) chars trimmed
    assertStringTrim("UTF8_BINARY", "Ëaaaẞ", "Ëẞ", "aaa");
    assertStringTrimLeft("UTF8_BINARY", "Ëaaaẞ", "Ëẞ", "aaaẞ");
    assertStringTrimRight("UTF8_BINARY", "Ëaaaẞ", "Ëẞ", "Ëaaa");

    assertStringTrim("UTF8_BINARY_LCASE", "Ëaaaẞ", "Ëẞ", "aaa");
    assertStringTrimLeft("UTF8_BINARY_LCASE", "Ëaaaẞ", "Ëẞ", "aaaẞ");
    assertStringTrimRight("UTF8_BINARY_LCASE", "Ëaaaẞ", "Ëẞ", "Ëaaa");

    assertStringTrim("UNICODE", "Ëaaaẞ", "Ëẞ", "aaa");
    assertStringTrimLeft("UNICODE", "Ëaaaẞ", "Ëẞ", "aaaẞ");
    assertStringTrimRight("UNICODE", "Ëaaaẞ", "Ëẞ", "Ëaaa");
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
// checkstyle.on: AvoidEscapedUnicodeCharacters
