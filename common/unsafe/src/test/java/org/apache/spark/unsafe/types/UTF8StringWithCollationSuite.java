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
import org.junit.jupiter.api.Test;

import static org.apache.spark.unsafe.types.UTF8String.fromString;
import static org.junit.jupiter.api.Assertions.*;


public class UTF8StringWithCollationSuite {

  private void assertStartsWith(String pattern, String prefix, String collationName, boolean value)
      throws SparkException {
    assertEquals(UTF8String.fromString(pattern).startsWith(UTF8String.fromString(prefix),
        CollationFactory.collationNameToId(collationName)), value);
  }

  private void assertEndsWith(String pattern, String suffix, String collationName, boolean value)
      throws SparkException {
    assertEquals(UTF8String.fromString(pattern).endsWith(UTF8String.fromString(suffix),
        CollationFactory.collationNameToId(collationName)), value);
  }

  @Test
  public void startsWithTest() throws SparkException {
    assertStartsWith("", "", "UTF8_BINARY", true);
    assertStartsWith("c", "", "UTF8_BINARY", true);
    assertStartsWith("", "c", "UTF8_BINARY", false);
    assertStartsWith("abcde", "a", "UTF8_BINARY", true);
    assertStartsWith("abcde", "A", "UTF8_BINARY", false);
    assertStartsWith("abcde", "bcd", "UTF8_BINARY", false);
    assertStartsWith("abcde", "BCD", "UTF8_BINARY", false);
    assertStartsWith("", "", "UNICODE", true);
    assertStartsWith("c", "", "UNICODE", true);
    assertStartsWith("", "c", "UNICODE", false);
    assertStartsWith("abcde", "a", "UNICODE", true);
    assertStartsWith("abcde", "A", "UNICODE", false);
    assertStartsWith("abcde", "bcd", "UNICODE", false);
    assertStartsWith("abcde", "BCD", "UNICODE", false);
    assertStartsWith("", "", "UTF8_BINARY_LCASE", true);
    assertStartsWith("c", "", "UTF8_BINARY_LCASE", true);
    assertStartsWith("", "c", "UTF8_BINARY_LCASE", false);
    assertStartsWith("abcde", "a", "UTF8_BINARY_LCASE", true);
    assertStartsWith("abcde", "A", "UTF8_BINARY_LCASE", true);
    assertStartsWith("abcde", "abc", "UTF8_BINARY_LCASE", true);
    assertStartsWith("abcde", "BCD", "UTF8_BINARY_LCASE", false);
    assertStartsWith("", "", "UNICODE_CI", true);
    assertStartsWith("c", "", "UNICODE_CI", true);
    assertStartsWith("", "c", "UNICODE_CI", false);
    assertStartsWith("abcde", "a", "UNICODE_CI", true);
    assertStartsWith("abcde", "A", "UNICODE_CI", true);
    assertStartsWith("abcde", "abc", "UNICODE_CI", true);
    assertStartsWith("abcde", "BCD", "UNICODE_CI", false);
  }

  @Test
  public void endsWithTest() throws SparkException {
    assertEndsWith("", "", "UTF8_BINARY", true);
    assertEndsWith("c", "", "UTF8_BINARY", true);
    assertEndsWith("", "c", "UTF8_BINARY", false);
    assertEndsWith("abcde", "e", "UTF8_BINARY", true);
    assertEndsWith("abcde", "E", "UTF8_BINARY", false);
    assertEndsWith("abcde", "bcd", "UTF8_BINARY", false);
    assertEndsWith("abcde", "BCD", "UTF8_BINARY", false);
    assertEndsWith("", "", "UNICODE", true);
    assertEndsWith("c", "", "UNICODE", true);
    assertEndsWith("", "c", "UNICODE", false);
    assertEndsWith("abcde", "e", "UNICODE", true);
    assertEndsWith("abcde", "E", "UNICODE", false);
    assertEndsWith("abcde", "bcd", "UNICODE", false);
    assertEndsWith("abcde", "BCD", "UNICODE", false);
    assertEndsWith("", "", "UTF8_BINARY_LCASE", true);
    assertEndsWith("c", "", "UTF8_BINARY_LCASE", true);
    assertEndsWith("", "c", "UTF8_BINARY_LCASE", false);
    assertEndsWith("abcde", "e", "UTF8_BINARY_LCASE", true);
    assertEndsWith("abcde", "E", "UTF8_BINARY_LCASE", true);
    assertEndsWith("abcde", "cde", "UTF8_BINARY_LCASE", true);
    assertEndsWith("abcde", "BCD", "UTF8_BINARY_LCASE", false);
    assertEndsWith("", "", "UNICODE_CI", true);
    assertEndsWith("c", "", "UNICODE_CI", true);
    assertEndsWith("", "c", "UNICODE_CI", false);
    assertEndsWith("abcde", "e", "UNICODE_CI", true);
    assertEndsWith("abcde", "E", "UNICODE_CI", true);
    assertEndsWith("abcde", "cde", "UNICODE_CI", true);
    assertEndsWith("abcde", "BCD", "UNICODE_CI", false);
  }

  private void assertSubStringIndex(String string, String delimiter, int count, int collationId, String expected)
          throws SparkException {
    assertEquals(UTF8String.fromString(string).subStringIndex(UTF8String.fromString(delimiter), count,
            collationId), UTF8String.fromString(expected));
  }

  @Test
  public void subStringIndex() throws SparkException {
    assertSubStringIndex("wwwgapachegorg", "g", -3, 0, "apachegorg");
    assertSubStringIndex("www||apache||org", "||", 2, 0, "www||apache");
    // UTF8_BINARY_LCASE
    assertSubStringIndex("AaAaAaAaAa", "aa", 2, 1, "A");
    assertSubStringIndex("www.apache.org", ".", 3, 1, "www.apache.org");
    assertSubStringIndex("wwwXapachexorg", "x", 2, 1, "wwwXapache");
    assertSubStringIndex("wwwxapacheXorg", "X", 1, 1, "www");
    assertSubStringIndex("www.apache.org", ".", 0, 1, "");
    assertSubStringIndex("www.apache.ORG", ".", -3, 1, "www.apache.ORG");
    assertSubStringIndex("wwwGapacheGorg", "g", 1, 1, "www");
    assertSubStringIndex("wwwGapacheGorg", "g", 3, 1, "wwwGapacheGor");
    assertSubStringIndex("gwwwGapacheGorg", "g", 3, 1, "gwwwGapache");
    assertSubStringIndex("wwwGapacheGorg", "g", -3, 1, "apacheGorg");
    assertSubStringIndex("wwwmapacheMorg", "M", -2, 1, "apacheMorg");
    assertSubStringIndex("www.apache.org", ".", -1, 1, "org");
    assertSubStringIndex("", ".", -2, 1, "");
    // scalastyle:off
    assertSubStringIndex("test大千世界X大千世界", "x", -1, 1, "大千世界");
    assertSubStringIndex("test大千世界X大千世界", "X", 1, 1, "test大千世界");
    assertSubStringIndex("test大千世界大千世界", "千", 2, 1, "test大千世界大");
    // scalastyle:on
    assertSubStringIndex("www||APACHE||org", "||", 2, 1, "www||APACHE");
    assertSubStringIndex("www||APACHE||org", "||", -1, 1, "org");
    // UNICODE
    assertSubStringIndex("AaAaAaAaAa", "Aa", 2, 2, "Aa");
    assertSubStringIndex("wwwYapacheyorg", "y", 3, 2, "wwwYapacheyorg");
    assertSubStringIndex("www.apache.org", ".", 2, 2, "www.apache");
    assertSubStringIndex("wwwYapacheYorg", "Y", 1, 2, "www");
    assertSubStringIndex("wwwYapacheYorg", "y", 1, 2, "wwwYapacheYorg");
    assertSubStringIndex("wwwGapacheGorg", "g", 1, 2, "wwwGapacheGor");
    assertSubStringIndex("GwwwGapacheGorG", "G", 3, 2, "GwwwGapache");
    assertSubStringIndex("wwwGapacheGorG", "G", -3, 2, "apacheGorG");
    assertSubStringIndex("www.apache.org", ".", 0, 2, "");
    assertSubStringIndex("www.apache.org", ".", -3, 2, "www.apache.org");
    assertSubStringIndex("www.apache.org", ".", -2, 2, "apache.org");
    assertSubStringIndex("www.apache.org", ".", -1, 2, "org");
    assertSubStringIndex("", ".", -2, 2, "");
    // scalastyle:off
    assertSubStringIndex("test大千世界X大千世界", "X", -1, 2, "大千世界");
    assertSubStringIndex("test大千世界X大千世界", "X", 1, 2, "test大千世界");
    assertSubStringIndex("大x千世界大千世x界", "x", 1, 2, "大");
    assertSubStringIndex("大x千世界大千世x界", "x", -1, 2, "界");
    assertSubStringIndex("大x千世界大千世x界", "x", -2, 2, "千世界大千世x界");
    assertSubStringIndex("大千世界大千世界", "千", 2, 2, "大千世界大");
    // scalastyle:on
    assertSubStringIndex("www||apache||org", "||", 2, 2, "www||apache");
    // UNICODE_CI
    assertSubStringIndex("AaAaAaAaAa", "aa", 2, 3, "A");
    assertSubStringIndex("www.apache.org", ".", 3, 3, "www.apache.org");
    assertSubStringIndex("wwwXapachexorg", "x", 2, 3, "wwwXapache");
    assertSubStringIndex("wwwxapacheXorg", "X", 1, 3, "www");
    assertSubStringIndex("www.apache.org", ".", 0, 3, "");
    assertSubStringIndex("wwwGapacheGorg", "g", 1, 3, "www");
    assertSubStringIndex("wwwGapacheGorg", "g", 3, 3, "wwwGapacheGor");
    assertSubStringIndex("gwwwGapacheGorg", "g", 3, 3, "gwwwGapache");
    assertSubStringIndex("wwwGapacheGorg", "g", -3, 3, "apacheGorg");
    assertSubStringIndex("www.apache.ORG", ".", -3, 3, "www.apache.ORG");
    assertSubStringIndex("wwwmapacheMorg", "M", -2, 3, "apacheMorg");
    assertSubStringIndex("www.apache.org", ".", -1, 3, "org");
    assertSubStringIndex("", ".", -2, 3, "");
    // scalastyle:off
    assertSubStringIndex("test大千世界X大千世界", "X", -1, 3, "大千世界");
    assertSubStringIndex("test大千世界X大千世界", "X", 1, 3, "test大千世界");
    assertSubStringIndex("test大千世界大千世界", "千", 2, 3, "test大千世界大");
    // scalastyle:on
    assertSubStringIndex("www||APACHE||org", "||", 2, 3, "www||APACHE");
  }


}
