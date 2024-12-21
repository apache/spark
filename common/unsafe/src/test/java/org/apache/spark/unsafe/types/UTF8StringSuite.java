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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UTF8StringBuilder;

import org.junit.jupiter.api.Test;

import static org.apache.spark.unsafe.types.UTF8String.fromString;
import static org.junit.jupiter.api.Assertions.*;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;
import static org.apache.spark.unsafe.types.UTF8String.*;

public class UTF8StringSuite {

  private static void checkBasic(String str, int len) {
    UTF8String s1 = fromString(str);
    UTF8String s2 = fromBytes(str.getBytes(StandardCharsets.UTF_8));
    assertEquals(len, s1.numChars());
    assertEquals(len, s2.numChars());

    assertEquals(str, s1.toString());
    assertEquals(str, s1.toValidString());
    assertEquals(str, s2.toString());
    assertEquals(str, s2.toValidString());
    assertEquals(s1, s2);

    assertEquals(s1.hashCode(), s2.hashCode());

    assertEquals(0, s1.binaryCompare(s2));

    assertTrue(s1.contains(s2));
    assertTrue(s2.contains(s1));
    assertTrue(s1.startsWith(s2));
    assertTrue(s1.endsWith(s2));
  }

  @Test
  public void basicTest() {
    checkBasic("", 0);
    checkBasic("¡", 1); // 2 bytes char
    checkBasic("ку", 2); // 2 * 2 bytes chars
    checkBasic("hello", 5); // 5 * 1 byte chars
    checkBasic("大 千 世 界", 7);
    checkBasic("︽﹋％", 3); // 3 * 3 bytes chars
    checkBasic("\uD83E\uDD19", 1); // 4 bytes char
  }

  @Test
  public void emptyStringTest() {
    assertEquals(EMPTY_UTF8, fromString(""));
    assertEquals(EMPTY_UTF8, fromBytes(new byte[0]));
    assertEquals(0, EMPTY_UTF8.numChars());
    assertEquals(0, EMPTY_UTF8.numBytes());
  }

  @Test
  public void prefix() {
    assertTrue(fromString("a").getPrefix() - fromString("b").getPrefix() < 0);
    assertTrue(fromString("ab").getPrefix() - fromString("b").getPrefix() < 0);
    assertTrue(
      fromString("abbbbbbbbbbbasdf").getPrefix() - fromString("bbbbbbbbbbbbasdf").getPrefix() < 0);
    assertTrue(fromString("").getPrefix() - fromString("a").getPrefix() < 0);
    assertTrue(fromString("你好").getPrefix() - fromString("世界").getPrefix() > 0);

    byte[] buf1 = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    byte[] buf2 = {1, 2, 3};
    UTF8String str1 = fromBytes(buf1, 0, 3);
    UTF8String str2 = fromBytes(buf1, 0, 8);
    UTF8String str3 = fromBytes(buf2);
    assertTrue(str1.getPrefix() - str2.getPrefix() < 0);
    assertEquals(str1.getPrefix(), str3.getPrefix());
  }

  @Test
  public void binaryCompareTo() {
    assertTrue(fromString("").binaryCompare(fromString("a")) < 0);
    assertTrue(fromString("abc").binaryCompare(fromString("ABC")) > 0);
    assertTrue(fromString("abc0").binaryCompare(fromString("abc")) > 0);
    assertTrue(fromString("abcabcabc").binaryCompare(fromString("abcabcabc")) == 0);
    assertTrue(fromString("aBcabcabc").binaryCompare(fromString("Abcabcabc")) > 0);
    assertTrue(fromString("Abcabcabc").binaryCompare(fromString("abcabcabC")) < 0);
    assertTrue(fromString("abcabcabc").binaryCompare(fromString("abcabcabC")) > 0);

    assertTrue(fromString("abc").binaryCompare(fromString("世界")) < 0);
    assertTrue(fromString("你好").binaryCompare(fromString("世界")) > 0);
    assertTrue(fromString("你好123").binaryCompare(fromString("你好122")) > 0);
  }

  protected static void testUpperandLower(String upper, String lower) {
    UTF8String us = fromString(upper);
    UTF8String ls = fromString(lower);
    assertEquals(ls, us.toLowerCase());
    assertEquals(us, ls.toUpperCase());
    assertEquals(us, us.toUpperCase());
    assertEquals(ls, ls.toLowerCase());
  }

  @Test
  public void upperAndLower() {
    testUpperandLower("", "");
    testUpperandLower("0123456", "0123456");
    testUpperandLower("ABCXYZ", "abcxyz");
    testUpperandLower("ЀЁЂѺΏỀ", "ѐёђѻώề");
    testUpperandLower("大千世界 数据砖头", "大千世界 数据砖头");
  }

  @Test
  public void titleCase() {
    assertEquals(fromString(""), fromString("").toTitleCase());
    assertEquals(fromString("Ab Bc Cd"), fromString("ab bc cd").toTitleCase());
    assertEquals(fromString("Ѐ Ё Ђ Ѻ Ώ Ề"), fromString("ѐ ё ђ ѻ ώ ề").toTitleCase());
    assertEquals(fromString("大千世界 数据砖头"), fromString("大千世界 数据砖头").toTitleCase());
  }

  @Test
  public void concatTest() {
    assertEquals(EMPTY_UTF8, concat());
    assertNull(concat((UTF8String) null));
    assertEquals(EMPTY_UTF8, concat(EMPTY_UTF8));
    assertEquals(fromString("ab"), concat(fromString("ab")));
    assertEquals(fromString("ab"), concat(fromString("a"), fromString("b")));
    assertEquals(fromString("abc"), concat(fromString("a"), fromString("b"), fromString("c")));
    assertNull(concat(fromString("a"), null, fromString("c")));
    assertNull(concat(fromString("a"), null, null));
    assertNull(concat(null, null, null));
    assertEquals(fromString("数据砖头"), concat(fromString("数据"), fromString("砖头")));
  }

  @Test
  public void concatWsTest() {
    // Returns null if the separator is null
    assertNull(concatWs(null, (UTF8String) null));
    assertNull(concatWs(null, fromString("a")));

    // If separator is null, concatWs should skip all null inputs and never return null.
    UTF8String sep = fromString("哈哈");
    assertEquals(
      EMPTY_UTF8,
      concatWs(sep, EMPTY_UTF8));
    assertEquals(
      fromString("ab"),
      concatWs(sep, fromString("ab")));
    assertEquals(
      fromString("a哈哈b"),
      concatWs(sep, fromString("a"), fromString("b")));
    assertEquals(
      fromString("a哈哈b哈哈c"),
      concatWs(sep, fromString("a"), fromString("b"), fromString("c")));
    assertEquals(
      fromString("a哈哈c"),
      concatWs(sep, fromString("a"), null, fromString("c")));
    assertEquals(
      fromString("a"),
      concatWs(sep, fromString("a"), null, null));
    assertEquals(
      EMPTY_UTF8,
      concatWs(sep, null, null, null));
    assertEquals(
      fromString("数据哈哈砖头"),
      concatWs(sep, fromString("数据"), fromString("砖头")));
  }

  @Test
  public void contains() {
    assertTrue(EMPTY_UTF8.contains(EMPTY_UTF8));
    assertTrue(fromString("hello").contains(fromString("ello")));
    assertFalse(fromString("hello").contains(fromString("vello")));
    assertFalse(fromString("hello").contains(fromString("hellooo")));
    assertTrue(fromString("大千世界").contains(fromString("千世界")));
    assertFalse(fromString("大千世界").contains(fromString("世千")));
    assertFalse(fromString("大千世界").contains(fromString("大千世界好")));
  }

  @Test
  public void startsWith() {
    assertTrue(EMPTY_UTF8.startsWith(EMPTY_UTF8));
    assertTrue(fromString("hello").startsWith(fromString("hell")));
    assertFalse(fromString("hello").startsWith(fromString("ell")));
    assertFalse(fromString("hello").startsWith(fromString("hellooo")));
    assertTrue(fromString("数据砖头").startsWith(fromString("数据")));
    assertFalse(fromString("大千世界").startsWith(fromString("千")));
    assertFalse(fromString("大千世界").startsWith(fromString("大千世界好")));
  }

  @Test
  public void endsWith() {
    assertTrue(EMPTY_UTF8.endsWith(EMPTY_UTF8));
    assertTrue(fromString("hello").endsWith(fromString("ello")));
    assertFalse(fromString("hello").endsWith(fromString("ellov")));
    assertFalse(fromString("hello").endsWith(fromString("hhhello")));
    assertTrue(fromString("大千世界").endsWith(fromString("世界")));
    assertFalse(fromString("大千世界").endsWith(fromString("世")));
    assertFalse(fromString("数据砖头").endsWith(fromString("我的数据砖头")));
  }

  @Test
  public void substring() {
    assertEquals(EMPTY_UTF8, fromString("hello").substring(0, 0));
    assertEquals(fromString("el"), fromString("hello").substring(1, 3));
    assertEquals(fromString("数"), fromString("数据砖头").substring(0, 1));
    assertEquals(fromString("据砖"), fromString("数据砖头").substring(1, 3));
    assertEquals(fromString("头"), fromString("数据砖头").substring(3, 5));
    assertEquals(fromString("ߵ梷"), fromString("ߵ梷").substring(0, 2));
  }

  @Test
  public void trims() {
    assertEquals(fromString("1"), fromString("1").trim());
    assertEquals(fromString("1"), fromString("1\t").trimAll());

    assertEquals(fromString("1中文").toString(), fromString("1中文").trimAll().toString());
    assertEquals(fromString("1"), fromString("1\u0003").trimAll());
    assertEquals(fromString("1"), fromString("1\u007F").trimAll());

    assertEquals(fromString("hello"), fromString("  hello ").trim());
    assertEquals(fromString("hello "), fromString("  hello ").trimLeft());
    assertEquals(fromString("  hello"), fromString("  hello ").trimRight());

    assertEquals(EMPTY_UTF8, EMPTY_UTF8.trim());
    assertEquals(EMPTY_UTF8, fromString("  ").trim());
    assertEquals(EMPTY_UTF8, fromString("  ").trimLeft());
    assertEquals(EMPTY_UTF8, fromString("  ").trimRight());

    assertEquals(fromString("数据砖头"), fromString("  数据砖头 ").trim());
    assertEquals(fromString("数据砖头 "), fromString("  数据砖头 ").trimLeft());
    assertEquals(fromString("  数据砖头"), fromString("  数据砖头 ").trimRight());

    assertEquals(fromString("数据砖头"), fromString("数据砖头").trim());
    assertEquals(fromString("数据砖头"), fromString("数据砖头").trimLeft());
    assertEquals(fromString("数据砖头"), fromString("数据砖头").trimRight());

    char[] charsLessThan0x20 = new char[10];
    Arrays.fill(charsLessThan0x20, (char)(' ' - 1));
    String stringStartingWithSpace =
      new String(charsLessThan0x20) + "hello" + new String(charsLessThan0x20);
    assertEquals(fromString(stringStartingWithSpace), fromString(stringStartingWithSpace).trim());
    assertEquals(fromString(stringStartingWithSpace),
      fromString(stringStartingWithSpace).trimLeft());
    assertEquals(fromString(stringStartingWithSpace),
      fromString(stringStartingWithSpace).trimRight());
  }

  @Test
  public void indexOf() {
    assertEquals(0, EMPTY_UTF8.indexOf(EMPTY_UTF8, 0));
    assertEquals(-1, EMPTY_UTF8.indexOf(fromString("l"), 0));
    assertEquals(0, fromString("hello").indexOf(EMPTY_UTF8, 0));
    assertEquals(2, fromString("hello").indexOf(EMPTY_UTF8, 2));
    assertEquals(0, fromString("hello").indexOf(EMPTY_UTF8, 9));
    assertEquals(2, fromString("hello").indexOf(fromString("l"), 0));
    assertEquals(3, fromString("hello").indexOf(fromString("l"), 3));
    assertEquals(-1, fromString("hello").indexOf(fromString("a"), 0));
    assertEquals(2, fromString("hello").indexOf(fromString("ll"), 0));
    assertEquals(-1, fromString("hello").indexOf(fromString("ll"), 4));
    assertEquals(1, fromString("数据砖头").indexOf(fromString("据砖"), 0));
    assertEquals(-1, fromString("数据砖头").indexOf(fromString("数"), 3));
    assertEquals(0, fromString("数据砖头").indexOf(fromString("数"), 0));
    assertEquals(3, fromString("数据砖头").indexOf(fromString("头"), 0));
  }

  @Test
  public void substring_index() {
    assertEquals(fromString("www.apache.org"),
      fromString("www.apache.org").subStringIndex(fromString("."), 3));
    assertEquals(fromString("www.apache"),
      fromString("www.apache.org").subStringIndex(fromString("."), 2));
    assertEquals(fromString("www"),
      fromString("www.apache.org").subStringIndex(fromString("."), 1));
    assertEquals(fromString(""),
      fromString("www.apache.org").subStringIndex(fromString("."), 0));
    assertEquals(fromString("org"),
      fromString("www.apache.org").subStringIndex(fromString("."), -1));
    assertEquals(fromString("apache.org"),
      fromString("www.apache.org").subStringIndex(fromString("."), -2));
    assertEquals(fromString("www.apache.org"),
      fromString("www.apache.org").subStringIndex(fromString("."), -3));
    // str is empty string
    assertEquals(fromString(""),
      fromString("").subStringIndex(fromString("."), 1));
    // empty string delim
    assertEquals(fromString(""),
      fromString("www.apache.org").subStringIndex(fromString(""), 1));
    // delim does not exist in str
    assertEquals(fromString("www.apache.org"),
      fromString("www.apache.org").subStringIndex(fromString("#"), 2));
    // delim is 2 chars
    assertEquals(fromString("www||apache"),
      fromString("www||apache||org").subStringIndex(fromString("||"), 2));
    assertEquals(fromString("apache||org"),
      fromString("www||apache||org").subStringIndex(fromString("||"), -2));
    // non ascii chars
    assertEquals(fromString("大千世界大"),
      fromString("大千世界大千世界").subStringIndex(fromString("千"), 2));
    // overlapped delim
    assertEquals(fromString("||"), fromString("||||||").subStringIndex(fromString("|||"), 3));
    assertEquals(fromString("|||"), fromString("||||||").subStringIndex(fromString("|||"), -4));
  }

  @Test
  public void reverse() {
    assertEquals(fromString("olleh"), fromString("hello").reverse());
    assertEquals(EMPTY_UTF8, EMPTY_UTF8.reverse());
    assertEquals(fromString("者行孙"), fromString("孙行者").reverse());
    assertEquals(fromString("者行孙 olleh"), fromString("hello 孙行者").reverse());
  }

  @Test
  public void repeat() {
    assertEquals(fromString("数d数d数d数d数d"), fromString("数d").repeat(5));
    assertEquals(fromString("数d"), fromString("数d").repeat(1));
    assertEquals(EMPTY_UTF8, fromString("数d").repeat(-1));
  }

  @Test
  public void pad() {
    assertEquals(fromString("hel"), fromString("hello").lpad(3, fromString("????")));
    assertEquals(fromString("hello"), fromString("hello").lpad(5, fromString("????")));
    assertEquals(fromString("?hello"), fromString("hello").lpad(6, fromString("????")));
    assertEquals(fromString("???????hello"), fromString("hello").lpad(12, fromString("????")));
    assertEquals(fromString("?????hello"), fromString("hello").lpad(10, fromString("?????")));
    assertEquals(fromString("???????"), EMPTY_UTF8.lpad(7, fromString("?????")));

    assertEquals(fromString("hel"), fromString("hello").rpad(3, fromString("????")));
    assertEquals(fromString("hello"), fromString("hello").rpad(5, fromString("????")));
    assertEquals(fromString("hello?"), fromString("hello").rpad(6, fromString("????")));
    assertEquals(fromString("hello???????"), fromString("hello").rpad(12, fromString("????")));
    assertEquals(fromString("hello?????"), fromString("hello").rpad(10, fromString("?????")));
    assertEquals(fromString("???????"), EMPTY_UTF8.rpad(7, fromString("?????")));

    assertEquals(fromString("数据砖"), fromString("数据砖头").lpad(3, fromString("????")));
    assertEquals(fromString("?数据砖头"), fromString("数据砖头").lpad(5, fromString("????")));
    assertEquals(fromString("??数据砖头"), fromString("数据砖头").lpad(6, fromString("????")));
    assertEquals(fromString("孙行数据砖头"), fromString("数据砖头").lpad(6, fromString("孙行者")));
    assertEquals(fromString("孙行者数据砖头"), fromString("数据砖头").lpad(7, fromString("孙行者")));
    assertEquals(
      fromString("孙行者孙行者孙行数据砖头"),
      fromString("数据砖头").lpad(12, fromString("孙行者")));

    assertEquals(fromString("数据砖"), fromString("数据砖头").rpad(3, fromString("????")));
    assertEquals(fromString("数据砖头?"), fromString("数据砖头").rpad(5, fromString("????")));
    assertEquals(fromString("数据砖头??"), fromString("数据砖头").rpad(6, fromString("????")));
    assertEquals(fromString("数据砖头孙行"), fromString("数据砖头").rpad(6, fromString("孙行者")));
    assertEquals(fromString("数据砖头孙行者"), fromString("数据砖头").rpad(7, fromString("孙行者")));
    assertEquals(
      fromString("数据砖头孙行者孙行者孙行"),
      fromString("数据砖头").rpad(12, fromString("孙行者")));

    assertEquals(EMPTY_UTF8, fromString("数据砖头").lpad(-10, fromString("孙行者")));
    assertEquals(EMPTY_UTF8, fromString("数据砖头").lpad(-10, EMPTY_UTF8));
    assertEquals(fromString("数据砖头"), fromString("数据砖头").lpad(5, EMPTY_UTF8));
    assertEquals(fromString("数据砖"), fromString("数据砖头").lpad(3, EMPTY_UTF8));
    assertEquals(EMPTY_UTF8, EMPTY_UTF8.lpad(3, EMPTY_UTF8));

    assertEquals(EMPTY_UTF8, fromString("数据砖头").rpad(-10, fromString("孙行者")));
    assertEquals(EMPTY_UTF8, fromString("数据砖头").rpad(-10, EMPTY_UTF8));
    assertEquals(fromString("数据砖头"), fromString("数据砖头").rpad(5, EMPTY_UTF8));
    assertEquals(fromString("数据砖"), fromString("数据砖头").rpad(3, EMPTY_UTF8));
    assertEquals(EMPTY_UTF8, EMPTY_UTF8.rpad(3, EMPTY_UTF8));
  }

  @Test
  public void substringSQL() {
    UTF8String e = fromString("example");
    assertEquals(fromString("ex"), e.substringSQL(0, 2));
    assertEquals(fromString("ex"), e.substringSQL(1, 2));
    assertEquals(fromString("example"), e.substringSQL(0, 7));
    assertEquals(fromString("ex"), e.substringSQL(1, 2));
    assertEquals(fromString("example"), e.substringSQL(0, 100));
    assertEquals(fromString("example"), e.substringSQL(1, 100));
    assertEquals(fromString("xa"), e.substringSQL(2, 2));
    assertEquals(fromString("exampl"), e.substringSQL(1, 6));
    assertEquals(fromString("xample"), e.substringSQL(2, 100));
    assertEquals(fromString(""), e.substringSQL(0, 0));
    assertEquals(EMPTY_UTF8, e.substringSQL(100, 4));
    assertEquals(fromString("example"), e.substringSQL(0, Integer.MAX_VALUE));
    assertEquals(fromString("example"), e.substringSQL(1, Integer.MAX_VALUE));
    assertEquals(fromString("xample"), e.substringSQL(2, Integer.MAX_VALUE));
    assertEquals(EMPTY_UTF8, e.substringSQL(-100, -100));
    assertEquals(EMPTY_UTF8, e.substringSQL(-1207959552, -1207959552));
    assertEquals(fromString("pl"), e.substringSQL(-3, 2));
    assertEquals(EMPTY_UTF8, e.substringSQL(Integer.MIN_VALUE, 6));
  }

  @Test
  public void split() {
    UTF8String[] negativeAndZeroLimitCase =
      new UTF8String[]{fromString("ab"), fromString("def"), fromString("ghi"), fromString("")};
    assertArrayEquals(
      negativeAndZeroLimitCase,
      fromString("ab,def,ghi,").split(fromString(","), 0));
    assertArrayEquals(
      negativeAndZeroLimitCase,
      fromString("ab,def,ghi,").split(fromString(","), -1));
    assertArrayEquals(
      new UTF8String[]{fromString("ab"), fromString("def,ghi,")},
      fromString("ab,def,ghi,").split(fromString(","), 2));
    // Split with empty pattern ignores trailing empty spaces.
    assertArrayEquals(
      new UTF8String[]{fromString("a"), fromString("b")},
      fromString("ab").split(fromString(""), 0));
    assertArrayEquals(
      new UTF8String[]{fromString("a"), fromString("b")},
      fromString("ab").split(fromString(""), -1));
    assertArrayEquals(
      new UTF8String[]{fromString("a"), fromString("b")},
      fromString("ab").split(fromString(""), 2));
    assertArrayEquals(
      new UTF8String[]{fromString("a"), fromString("b")},
      fromString("ab").split(fromString(""), 100));
    assertArrayEquals(
      new UTF8String[]{fromString("a")},
      fromString("ab").split(fromString(""), 1));
    assertArrayEquals(
      new UTF8String[]{fromString("")},
      fromString("").split(fromString(""), 0));
  }

  @Test
  public void replace() {
    assertEquals(
      fromString("re123ace"),
      fromString("replace").replace(fromString("pl"), fromString("123")));
    assertEquals(
      fromString("reace"),
      fromString("replace").replace(fromString("pl"), fromString("")));
    assertEquals(
      fromString("replace"),
      fromString("replace").replace(fromString(""), fromString("123")));
    // tests for multiple replacements
    assertEquals(
      fromString("a12ca12c"),
      fromString("abcabc").replace(fromString("b"), fromString("12")));
    assertEquals(
      fromString("adad"),
      fromString("abcdabcd").replace(fromString("bc"), fromString("")));
    // tests for single character search and replacement strings
    assertEquals(
      fromString("AbcAbc"),
      fromString("abcabc").replace(fromString("a"), fromString("A")));
    assertEquals(
      fromString("abcabc"),
      fromString("abcabc").replace(fromString("Z"), fromString("A")));
    // Tests with non-ASCII characters
    assertEquals(
      fromString("花ab界"),
      fromString("花花世界").replace(fromString("花世"), fromString("ab")));
    assertEquals(
      fromString("a水c"),
      fromString("a火c").replace(fromString("火"), fromString("水")));
    // Tests for a large number of replacements, triggering UTF8StringBuilder resize
    assertEquals(
      fromString("abcd").repeat(17),
      fromString("a").repeat(17).replace(fromString("a"), fromString("abcd")));
  }

  @Test
  public void levenshteinDistance() {
    assertEquals(0, EMPTY_UTF8.levenshteinDistance(EMPTY_UTF8));
    assertEquals(1, EMPTY_UTF8.levenshteinDistance(fromString("a")));
    assertEquals(7, fromString("aaapppp").levenshteinDistance(EMPTY_UTF8));
    assertEquals(1, fromString("frog").levenshteinDistance(fromString("fog")));
    assertEquals(3, fromString("fly").levenshteinDistance(fromString("ant")));
    assertEquals(7, fromString("elephant").levenshteinDistance(fromString("hippo")));
    assertEquals(7, fromString("hippo").levenshteinDistance(fromString("elephant")));
    assertEquals(8, fromString("hippo").levenshteinDistance(fromString("zzzzzzzz")));
    assertEquals(1, fromString("hello").levenshteinDistance(fromString("hallo")));
    assertEquals(4, fromString("世界千世").levenshteinDistance(fromString("千a世b")));
  }

  @Test
  public void translate() {
    assertEquals(
      fromString("1a2s3ae"),
      fromString("translate").translate(ImmutableMap.of(
        "r", "1",
        "n", "2",
        "l", "3",
        "t", "\0"
      )));
    assertEquals(
      fromString("translate"),
      fromString("translate").translate(new HashMap<>()));
    assertEquals(
      fromString("asae"),
      fromString("translate").translate(ImmutableMap.of(
        "r", "\0",
        "n", "\0",
        "l", "\0",
        "t", "\0"
      )));
    assertEquals(
      fromString("aa世b"),
      fromString("花花世界").translate(ImmutableMap.of(
        "花", "a",
        "界", "b"
      )));
  }

  @Test
  public void createBlankString() {
    assertEquals(fromString(" "), blankString(1));
    assertEquals(fromString("  "), blankString(2));
    assertEquals(fromString("   "), blankString(3));
    assertEquals(fromString(""), blankString(0));
  }

  @Test
  public void findInSet() {
    assertEquals(1, fromString("ab").findInSet(fromString("ab")));
    assertEquals(2, fromString("a,b").findInSet(fromString("b")));
    assertEquals(3, fromString("abc,b,ab,c,def").findInSet(fromString("ab")));
    assertEquals(1, fromString("ab,abc,b,ab,c,def").findInSet(fromString("ab")));
    assertEquals(4, fromString(",,,ab,abc,b,ab,c,def").findInSet(fromString("ab")));
    assertEquals(1, fromString(",ab,abc,b,ab,c,def").findInSet(fromString("")));
    assertEquals(4, fromString("数据砖头,abc,b,ab,c,def").findInSet(fromString("ab")));
    assertEquals(6, fromString("数据砖头,abc,b,ab,c,def").findInSet(fromString("def")));
  }

  @Test
  public void soundex() {
    assertEquals(fromString("R163"), fromString("Robert").soundex());
    assertEquals(fromString("R163"), fromString("Rupert").soundex());
    assertEquals(fromString("R150"), fromString("Rubin").soundex());
    assertEquals(fromString("A261"), fromString("Ashcraft").soundex());
    assertEquals(fromString("A261"), fromString("Ashcroft").soundex());
    assertEquals(fromString("B620"), fromString("Burroughs").soundex());
    assertEquals(fromString("B620"), fromString("Burrows").soundex());
    assertEquals(fromString("E251"), fromString("Ekzampul").soundex());
    assertEquals(fromString("E251"), fromString("Example").soundex());
    assertEquals(fromString("E460"), fromString("Ellery").soundex());
    assertEquals(fromString("E460"), fromString("Euler").soundex());
    assertEquals(fromString("G200"), fromString("Ghosh").soundex());
    assertEquals(fromString("G200"), fromString("Gauss").soundex());
    assertEquals(fromString("G362"), fromString("Gutierrez").soundex());
    assertEquals(fromString("H416"), fromString("Heilbronn").soundex());
    assertEquals(fromString("H416"), fromString("Hilbert").soundex());
    assertEquals(fromString("J250"), fromString("Jackson").soundex());
    assertEquals(fromString("K530"), fromString("Kant").soundex());
    assertEquals(fromString("K530"), fromString("Knuth").soundex());
    assertEquals(fromString("L000"), fromString("Lee").soundex());
    assertEquals(fromString("L222"), fromString("Lukasiewicz").soundex());
    assertEquals(fromString("L222"), fromString("Lissajous").soundex());
    assertEquals(fromString("L300"), fromString("Ladd").soundex());
    assertEquals(fromString("L300"), fromString("Lloyd").soundex());
    assertEquals(fromString("M220"), fromString("Moses").soundex());
    assertEquals(fromString("O600"), fromString("O'Hara").soundex());
    assertEquals(fromString("P236"), fromString("Pfister").soundex());
    assertEquals(fromString("R150"), fromString("Rubin").soundex());
    assertEquals(fromString("R163"), fromString("Robert").soundex());
    assertEquals(fromString("R163"), fromString("Rupert").soundex());
    assertEquals(fromString("S532"), fromString("Soundex").soundex());
    assertEquals(fromString("S532"), fromString("Sownteks").soundex());
    assertEquals(fromString("T522"), fromString("Tymczak").soundex());
    assertEquals(fromString("V532"), fromString("VanDeusen").soundex());
    assertEquals(fromString("W252"), fromString("Washington").soundex());
    assertEquals(fromString("W350"), fromString("Wheaton").soundex());

    assertEquals(fromString("A000"), fromString("a").soundex());
    assertEquals(fromString("A100"), fromString("ab").soundex());
    assertEquals(fromString("A120"), fromString("abc").soundex());
    assertEquals(fromString("A123"), fromString("abcd").soundex());
    assertEquals(fromString(""), fromString("").soundex());
    assertEquals(fromString("123"), fromString("123").soundex());
    assertEquals(fromString("世界千世"), fromString("世界千世").soundex());
  }

  @Test
  public void writeToOutputStreamUnderflow() throws IOException {
    // offset underflow is apparently supported?
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final byte[] test = "01234567".getBytes(StandardCharsets.UTF_8);

    for (int i = 1; i <= Platform.BYTE_ARRAY_OFFSET; ++i) {
      UTF8String.fromAddress(test, Platform.BYTE_ARRAY_OFFSET - i, test.length + i)
          .writeTo(outputStream);
      final ByteBuffer buffer = ByteBuffer.wrap(outputStream.toByteArray(), i, test.length);
      assertEquals("01234567", StandardCharsets.UTF_8.decode(buffer).toString());
      outputStream.reset();
    }
  }

  @Test
  public void writeToOutputStreamSlice() throws IOException {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final byte[] test = "01234567".getBytes(StandardCharsets.UTF_8);

    for (int i = 0; i < test.length; ++i) {
      for (int j = 0; j < test.length - i; ++j) {
        UTF8String.fromAddress(test, Platform.BYTE_ARRAY_OFFSET + i, j)
            .writeTo(outputStream);

        assertArrayEquals(Arrays.copyOfRange(test, i, i + j), outputStream.toByteArray());
        outputStream.reset();
      }
    }
  }

  @Test
  public void writeToOutputStreamOverflow() throws IOException {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final byte[] test = "01234567".getBytes(StandardCharsets.UTF_8);

    final HashSet<Long> offsets = new HashSet<>();
    for (int i = 0; i < 16; ++i) {
      // touch more points around MAX_VALUE
      offsets.add((long) Integer.MAX_VALUE - i);
      // subtract off BYTE_ARRAY_OFFSET to avoid wrapping around to a negative value,
      // which will hit the slower copy path instead of the optimized one
      offsets.add(Long.MAX_VALUE - BYTE_ARRAY_OFFSET - i);
    }

    for (long i = 1; i > 0L; i <<= 1) {
      for (long j = 0; j < 32L; ++j) {
        offsets.add(i + j);
      }
    }

    for (final long offset : offsets) {
      try {
        assertThrows(ArrayIndexOutOfBoundsException.class,
          () -> fromAddress(test, BYTE_ARRAY_OFFSET + offset, test.length).writeTo(outputStream));
      } finally {
        outputStream.reset();
      }
    }
  }

  @Test
  public void writeToOutputStream() throws IOException {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    EMPTY_UTF8.writeTo(outputStream);
    assertEquals("", outputStream.toString(StandardCharsets.UTF_8.name()));
    outputStream.reset();

    fromString("数据砖很重").writeTo(outputStream);
    assertEquals(
        "数据砖很重",
        outputStream.toString(StandardCharsets.UTF_8.name()));
    outputStream.reset();
  }

  @Test
  public void writeToOutputStreamIntArray() throws IOException {
    // verify that writes work on objects that are not byte arrays
    final ByteBuffer buffer = StandardCharsets.UTF_8.encode("大千世界");
    buffer.position(0);
    buffer.order(ByteOrder.nativeOrder());

    final int length = buffer.limit();
    assertEquals(12, length);

    final int ints = length / 4;
    final int[] array = new int[ints];

    for (int i = 0; i < ints; ++i) {
      array[i] = buffer.getInt();
    }

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    fromAddress(array, Platform.INT_ARRAY_OFFSET, length)
        .writeTo(outputStream);
    assertEquals("大千世界", outputStream.toString(StandardCharsets.UTF_8.name()));
  }

  @Test
  public void testToShort() {
    Map<String, Short> inputToExpectedOutput = new HashMap<>();
    inputToExpectedOutput.put("1", (short) 1);
    inputToExpectedOutput.put("+1", (short) 1);
    inputToExpectedOutput.put("-1", (short) -1);
    inputToExpectedOutput.put("0", (short) 0);
    inputToExpectedOutput.put("1111.12345678901234567890", (short) 1111);
    inputToExpectedOutput.put(String.valueOf(Short.MAX_VALUE), Short.MAX_VALUE);
    inputToExpectedOutput.put(String.valueOf(Short.MIN_VALUE), Short.MIN_VALUE);

    Random rand = new Random();
    for (int i = 0; i < 10; i++) {
      short value = (short) rand.nextInt();
      inputToExpectedOutput.put(String.valueOf(value), value);
    }

    IntWrapper wrapper = new IntWrapper();
    for (Map.Entry<String, Short> entry : inputToExpectedOutput.entrySet()) {
      assertTrue(UTF8String.fromString(entry.getKey()).toShort(wrapper), entry.getKey());
      assertEquals((short) entry.getValue(), wrapper.value);
    }

    List<String> negativeInputs =
      Arrays.asList("", "  ", "null", "NULL", "\n", "~1212121", "3276700");

    for (String negativeInput : negativeInputs) {
      assertFalse(UTF8String.fromString(negativeInput).toShort(wrapper), negativeInput);
    }
  }

  @Test
  public void testToByte() {
    Map<String, Byte> inputToExpectedOutput = new HashMap<>();
    inputToExpectedOutput.put("1", (byte) 1);
    inputToExpectedOutput.put("+1",(byte)  1);
    inputToExpectedOutput.put("-1", (byte)  -1);
    inputToExpectedOutput.put("0", (byte)  0);
    inputToExpectedOutput.put("111.12345678901234567890", (byte) 111);
    inputToExpectedOutput.put(String.valueOf(Byte.MAX_VALUE), Byte.MAX_VALUE);
    inputToExpectedOutput.put(String.valueOf(Byte.MIN_VALUE), Byte.MIN_VALUE);

    Random rand = new Random();
    for (int i = 0; i < 10; i++) {
      byte value = (byte) rand.nextInt();
      inputToExpectedOutput.put(String.valueOf(value), value);
    }

    IntWrapper intWrapper = new IntWrapper();
    for (Map.Entry<String, Byte> entry : inputToExpectedOutput.entrySet()) {
      assertTrue(UTF8String.fromString(entry.getKey()).toByte(intWrapper), entry.getKey());
      assertEquals((byte) entry.getValue(), intWrapper.value);
    }

    List<String> negativeInputs =
      Arrays.asList("", "  ", "null", "NULL", "\n", "~1212121", "12345678901234567890");

    for (String negativeInput : negativeInputs) {
      assertFalse(UTF8String.fromString(negativeInput).toByte(intWrapper), negativeInput);
    }
  }

  @Test
  public void testToInt() {
    Map<String, Integer> inputToExpectedOutput = new HashMap<>();
    inputToExpectedOutput.put("1", 1);
    inputToExpectedOutput.put("+1", 1);
    inputToExpectedOutput.put("-1", -1);
    inputToExpectedOutput.put("0", 0);
    inputToExpectedOutput.put("11111.1234567", 11111);
    inputToExpectedOutput.put(String.valueOf(Integer.MAX_VALUE), Integer.MAX_VALUE);
    inputToExpectedOutput.put(String.valueOf(Integer.MIN_VALUE), Integer.MIN_VALUE);

    Random rand = new Random();
    for (int i = 0; i < 10; i++) {
      int value = rand.nextInt();
      inputToExpectedOutput.put(String.valueOf(value), value);
    }

    IntWrapper intWrapper = new IntWrapper();
    for (Map.Entry<String, Integer> entry : inputToExpectedOutput.entrySet()) {
      assertTrue(UTF8String.fromString(entry.getKey()).toInt(intWrapper), entry.getKey());
      assertEquals((int) entry.getValue(), intWrapper.value);
    }

    List<String> negativeInputs =
      Arrays.asList("", "  ", "null", "NULL", "\n", "~1212121", "12345678901234567890");

    for (String negativeInput : negativeInputs) {
      assertFalse(UTF8String.fromString(negativeInput).toInt(intWrapper), negativeInput);
    }
  }

  @Test
  public void testToLong() {
    Map<String, Long> inputToExpectedOutput = new HashMap<>();
    inputToExpectedOutput.put("1", 1L);
    inputToExpectedOutput.put("+1", 1L);
    inputToExpectedOutput.put("-1", -1L);
    inputToExpectedOutput.put("0", 0L);
    inputToExpectedOutput.put("1076753423.12345678901234567890", 1076753423L);
    inputToExpectedOutput.put(String.valueOf(Long.MAX_VALUE), Long.MAX_VALUE);
    inputToExpectedOutput.put(String.valueOf(Long.MIN_VALUE), Long.MIN_VALUE);

    Random rand = new Random();
    for (int i = 0; i < 10; i++) {
      long value = rand.nextLong();
      inputToExpectedOutput.put(String.valueOf(value), value);
    }

    LongWrapper wrapper = new LongWrapper();
    for (Map.Entry<String, Long> entry : inputToExpectedOutput.entrySet()) {
      assertTrue(UTF8String.fromString(entry.getKey()).toLong(wrapper), entry.getKey());
      assertEquals((long) entry.getValue(), wrapper.value);
    }

    List<String> negativeInputs = Arrays.asList("", "  ", "null", "NULL", "\n", "~1212121",
        "1234567890123456789012345678901234");

    for (String negativeInput : negativeInputs) {
      assertFalse(UTF8String.fromString(negativeInput).toLong(wrapper), negativeInput);
    }
  }

  @Test
  public void trimBothWithTrimString() {
    assertEquals(fromString("hello"), fromString("  hello ").trim(fromString(" ")));
    assertEquals(fromString("o"), fromString("  hello ").trim(fromString(" hle")));
    assertEquals(fromString("h e"), fromString("ooh e ooo").trim(fromString("o ")));
    assertEquals(fromString(""), fromString("ooo...oooo").trim(fromString("o.")));
    assertEquals(fromString("b"), fromString("%^b[]@").trim(fromString("][@^%")));

    assertEquals(EMPTY_UTF8, fromString("  ").trim(fromString(" ")));

    assertEquals(fromString("数据砖头"), fromString("  数据砖头 ").trim());
    assertEquals(fromString("数"), fromString("a数b").trim(fromString("ab")));
    assertEquals(fromString(""), fromString("a").trim(fromString("a数b")));
    assertEquals(fromString(""), fromString("数数 数数数").trim(fromString("数 ")));
    assertEquals(fromString("据砖头"), fromString("数]数[数据砖头#数数").trim(fromString("[数]#")));
    assertEquals(fromString("据砖头数数 "), fromString("数数数据砖头数数 ").trim(fromString("数")));
  }

  @Test
  public void trimLeftWithTrimString() {
    assertEquals(fromString("  hello "), fromString("  hello ").trimLeft(fromString("")));
    assertEquals(fromString(""), fromString("a").trimLeft(fromString("a")));
    assertEquals(fromString("b"), fromString("b").trimLeft(fromString("a")));
    assertEquals(fromString("ba"), fromString("ba").trimLeft(fromString("a")));
    assertEquals(fromString(""), fromString("aaaaaaa").trimLeft(fromString("a")));
    assertEquals(fromString("trim"), fromString("oabtrim").trimLeft(fromString("bao")));
    assertEquals(fromString("rim "), fromString("ooootrim ").trimLeft(fromString("otm")));

    assertEquals(EMPTY_UTF8, fromString("  ").trimLeft(fromString(" ")));

    assertEquals(fromString("数据砖头 "), fromString("  数据砖头 ").trimLeft(fromString(" ")));
    assertEquals(fromString("数"), fromString("数").trimLeft(fromString("a")));
    assertEquals(fromString("a"), fromString("a").trimLeft(fromString("数")));
    assertEquals(fromString("砖头数数"), fromString("数数数据砖头数数").trimLeft(fromString("据数")));
    assertEquals(fromString("据砖头数数"), fromString(" 数数数据砖头数数").trimLeft(fromString("数 ")));
    assertEquals(fromString("据砖头数数"), fromString("aa数数数据砖头数数").trimLeft(fromString("a数砖")));
    assertEquals(fromString("$S,.$BR"), fromString(",,,,%$S,.$BR").trimLeft(fromString("%,")));
  }

  @Test
  public void trimRightWithTrimString() {
    assertEquals(fromString("  hello "), fromString("  hello ").trimRight(fromString("")));
    assertEquals(fromString(""), fromString("a").trimRight(fromString("a")));
    assertEquals(fromString("cc"), fromString("ccbaaaa").trimRight(fromString("ba")));
    assertEquals(fromString(""), fromString("aabbbbaaa").trimRight(fromString("ab")));
    assertEquals(fromString("  he"), fromString("  hello ").trimRight(fromString(" ol")));
    assertEquals(fromString("oohell"),
        fromString("oohellooo../*&").trimRight(fromString("./,&%*o")));

    assertEquals(EMPTY_UTF8, fromString("  ").trimRight(fromString(" ")));

    assertEquals(fromString("  数据砖头"), fromString("  数据砖头 ").trimRight(fromString(" ")));
    assertEquals(fromString("数数砖头"), fromString("数数砖头数aa数").trimRight(fromString("a数")));
    assertEquals(fromString(""), fromString("数数数据砖ab").trimRight(fromString("数据砖ab")));
    assertEquals(fromString("头"), fromString("头a???/").trimRight(fromString("数?/*&^%a")));
    assertEquals(fromString("头"), fromString("头数b数数 [").trimRight(fromString(" []数b")));
  }

  @Test
  public void skipWrongFirstByte() {
    int[] wrongFirstBytes = {
      0x80, 0x9F, 0xBF, // Skip Continuation bytes
      0xC0, 0xC2, // 0xC0..0xC1 - disallowed in UTF-8
      // 0xF5..0xFF - disallowed in UTF-8
      0xF5, 0xF6, 0xF7, 0xF8, 0xF9,
      0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF
    };
    byte[] c = new byte[1];

    for (int wrongFirstByte : wrongFirstBytes) {
      c[0] = (byte) wrongFirstByte;
      assertEquals(1, fromBytes(c).numChars());
    }
  }

  private void testMakeValid(String input, String expected) {
    ByteArrayOutputStream exp = new ByteArrayOutputStream();
    for (String hex : expected.split(" ")) exp.write(Integer.parseInt(hex.substring(2), 16));
    ByteArrayOutputStream inp = new ByteArrayOutputStream();
    for (String hex : input.split(" ")) inp.write(Integer.parseInt(hex.substring(2), 16));
    UTF8String expUTF8String = fromBytes(exp.toByteArray());
    UTF8String inpUTF8String = fromBytes(inp.toByteArray());
    assertEquals(expUTF8String, inpUTF8String.makeValid());
    assertEquals(inpUTF8String.toValidString(), inpUTF8String.toString());
  }
  @Test
  public void makeValid() {
    // Basic tests
    assertEquals(EMPTY_UTF8, EMPTY_UTF8.makeValid());
    assertEquals(fromString(""), fromString("").makeValid());
    assertEquals(fromString("abc"), fromString("abc").makeValid());
    assertEquals(fromString("hello"), fromString("hello").makeValid());
    assertEquals(fromString("大千世界"), fromString("大千世界").makeValid());
    assertEquals(fromBytes(new byte[] {}), fromBytes(new byte[] {}).makeValid());
    assertEquals(fromBytes(new byte[] {0x61}), fromBytes(new byte[] {0x61}).makeValid());
    assertEquals(fromBytes(new byte[] {0x7F}), fromBytes(new byte[] {0x7F}).makeValid());
    assertEquals(fromBytes(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBD}),
      fromBytes(new byte[] {(byte) 0xFF}).makeValid());
    assertEquals(fromBytes(new byte[] {(byte) 0xC2, (byte) 0x80}),
      fromBytes(new byte[] {(byte) 0xC2, (byte) 0x80}).makeValid());
    assertEquals(fromBytes(new byte[] {(byte) 0xDF, (byte) 0xBF}),
      fromBytes(new byte[] {(byte) 0xDF, (byte) 0xBF}).makeValid());
    assertEquals(fromString("��"),
      fromBytes(new byte[] {(byte) 0xC0, (byte) 0x80}).makeValid());
    assertEquals(fromString("��"),
      fromBytes(new byte[] {(byte) 0xC1, (byte) 0xBF}).makeValid());
    assertEquals(fromString("��"),
      fromBytes(new byte[] {(byte) 0xDF, (byte) 0xC0}).makeValid());
    assertEquals(fromString("��"),
      fromBytes(new byte[] {(byte) 0xDF, (byte) 0xFF}).makeValid());
    assertEquals(fromString("��"),
      fromBytes(new byte[] {(byte) 0x80, (byte) 0x80}).makeValid());
    assertEquals(fromBytes(new byte[] {(byte) 0xE0, (byte) 0xA0, (byte) 0x80}),
      fromBytes(new byte[] {(byte) 0xE0, (byte) 0xA0, (byte) 0x80}).makeValid());
    assertEquals(fromBytes(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBF}),
      fromBytes(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBF}).makeValid());
    assertEquals(fromBytes(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBF}),
      fromBytes(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBF}).makeValid());
    assertEquals(fromString("���"),
      fromBytes(new byte[] {(byte) 0xE0, (byte) 0x9F, (byte) 0x80}).makeValid());
    assertEquals(fromString("���"),
      fromBytes(new byte[] {(byte) 0xE0, (byte) 0xC0, (byte) 0x80}).makeValid());
    assertEquals(fromString("��"),
      fromBytes(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xC0}).makeValid());
    assertEquals(fromString("���"),
      fromBytes(new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80}).makeValid());
    assertEquals(fromBytes(new byte[] {(byte) 0xF0, (byte) 0x90, (byte) 0x80, (byte) 0x80}),
      fromBytes(new byte[] {(byte) 0xF0, (byte) 0x90, (byte) 0x80, (byte) 0x80}).makeValid());
    assertEquals(fromBytes(new byte[] {(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF}),
      fromBytes(new byte[] {(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF}).makeValid());
    assertEquals(fromString("����"),
      fromBytes(new byte[] {(byte) 0xF0, (byte) 0x8F, (byte) 0x80, (byte) 0x80}).makeValid());
    assertEquals(fromBytes(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBD, (byte) 0x7F}),
      fromBytes(new byte[] {(byte) 0xF0, (byte) 0x90, (byte) 0x80, (byte) 0x7F}).makeValid());
    assertEquals(fromString("����"),
      fromBytes(new byte[] {(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80}).makeValid());
    assertEquals(fromString("��"),
      fromBytes(new byte[] {(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xC0}).makeValid());
    assertEquals(fromString("�����"), fromBytes(new byte[]
      {(byte) 0xF8, (byte) 0x8F, (byte) 0x80, (byte) 0x80, (byte) 0x80}).makeValid());
    assertEquals(fromString("�����"), fromBytes(new byte[]
      {(byte) 0xF8, (byte) 0x90, (byte) 0x80, (byte) 0x80, (byte) 0x80}).makeValid());
    assertEquals(fromString("�����"), fromBytes(new byte[]
      {(byte) 0xFB, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF, (byte) 0xBF}).makeValid());
    assertEquals(fromString("�����"), fromBytes(new byte[]
      {(byte) 0xFB, (byte) 0x90, (byte) 0x80, (byte) 0x80, (byte) 0x80}).makeValid());
    // More tests
    testMakeValid("0x42", "0x42");
    testMakeValid("0x80", "0xEF 0xBF 0xBD");
    testMakeValid("0xF4 0x92 0x12", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD 0x12");
    testMakeValid("0x84 0xA5 0x63 0x64", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD 0x63 0x64");
    testMakeValid("0xF4 0x9B", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testMakeValid("0xE2 0x0C", "0xEF 0xBF 0xBD 0x0C");
    testMakeValid("0xE8 0x3E", "0xEF 0xBF 0xBD 0x3E");
    testMakeValid("0x17 0x1B 0xC2", "0x17 0x1B 0xEF 0xBF 0xBD");
    testMakeValid("0x29 0xB2 0x7C 0xA0", "0x29 0xEF 0xBF 0xBD 0x7C 0xEF 0xBF 0xBD");
    testMakeValid("0x13 0x68 0x28 0x2A 0x83", "0x13 0x68 0x28 0x2A 0xEF 0xBF 0xBD");
    testMakeValid("0x3B 0xFE 0xC2 0xB7 0x9E", "0x3B 0xEF 0xBF 0xBD 0xC2 0xB7 0xEF 0xBF 0xBD");
    testMakeValid("0xFC", "0xEF 0xBF 0xBD");
    testMakeValid("0x8E 0xBD", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testMakeValid("0x6A 0x8B 0x5C 0x5A", "0x6A 0xEF 0xBF 0xBD 0x5C 0x5A");
    testMakeValid("0xB4 0xC0", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testMakeValid("0xB0 0xD3", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testMakeValid("0xAA", "0xEF 0xBF 0xBD");
    testMakeValid("0x1A 0x66", "0x1A 0x66");
    testMakeValid("0x32 0xB5 0x5F", "0x32 0xEF 0xBF 0xBD 0x5F");
    testMakeValid("0x70 0xB6 0x69 0xBC", "0x70 0xEF 0xBF 0xBD 0x69 0xEF 0xBF 0xBD");
    testMakeValid("0xDC 0x15 0x82", "0xEF 0xBF 0xBD 0x15 0xEF 0xBF 0xBD");
    testMakeValid("0x7F 0x3F 0x72 0xBB", "0x7F 0x3F 0x72 0xEF 0xBF 0xBD");
    testMakeValid("0x3C 0x3D 0x1F 0x6C 0x75", "0x3C 0x3D 0x1F 0x6C 0x75");
    testMakeValid("0xBF 0x10 0xF4", "0xEF 0xBF 0xBD 0x10 0xEF 0xBF 0xBD");
    testMakeValid("0xBF 0xDD 0x89", "0xEF 0xBF 0xBD 0xDD 0x89");
    testMakeValid("0x85 0xD4 0x90 0x5E 0x6C", "0xEF 0xBF 0xBD 0xD4 0x90 0x5E 0x6C");
    testMakeValid("0x0A 0x92 0x3C 0x17 0x7D", "0x0A 0xEF 0xBF 0xBD 0x3C 0x17 0x7D");
    testMakeValid("0x49 0x4E 0x2C 0x7B 0x1C", "0x49 0x4E 0x2C 0x7B 0x1C");
    testMakeValid("0x2E 0x0C 0x3E 0x02 0x9D", "0x2E 0x0C 0x3E 0x02 0xEF 0xBF 0xBD");
    testMakeValid("0xF9 0xF2 0x11", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD 0x11");
    testMakeValid("0xD1", "0xEF 0xBF 0xBD");
    testMakeValid("0x74 0x35 0x56 0xC4", "0x74 0x35 0x56 0xEF 0xBF 0xBD");
    testMakeValid("0xBE 0x45 0x7F", "0xEF 0xBF 0xBD 0x45 0x7F");
    testMakeValid("0x2E", "0x2E");
    testMakeValid("0x3B 0xC3", "0x3B 0xEF 0xBF 0xBD");
    testMakeValid("0xF9 0x58 0xE0 0x84", "0xEF 0xBF 0xBD 0x58 0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testMakeValid("0x72 0xF9 0x4F", "0x72 0xEF 0xBF 0xBD 0x4F");
    testMakeValid("0xF2 0x2A 0x38", "0xEF 0xBF 0xBD 0x2A 0x38");
    testMakeValid("0x66 0xFC 0x8D 0x5F", "0x66 0xEF 0xBF 0xBD 0xEF 0xBF 0xBD 0x5F");
    testMakeValid("0x6D", "0x6D");
    testMakeValid("0x6E 0xDF 0xDD", "0x6E 0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testMakeValid("0x50 0xE8 0x45 0xDB", "0x50 0xEF 0xBF 0xBD 0x45 0xEF 0xBF 0xBD");
    testMakeValid("0x71 0x06 0x1F 0x87", "0x71 0x06 0x1F 0xEF 0xBF 0xBD");
    testMakeValid("0x02", "0x02");
    testMakeValid("0x20 0x2B 0x38 0xCB 0xDF", "0x20 0x2B 0x38 0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testMakeValid("0x10", "0x10");
    testMakeValid("0x15", "0x15");
    testMakeValid("0xC5 0xEA 0x2A", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD 0x2A");
    testMakeValid("0x72 0x2D 0xBB 0x06", "0x72 0x2D 0xEF 0xBF 0xBD 0x06");
    testMakeValid("0xB7", "0xEF 0xBF 0xBD");
    testMakeValid("0xB8 0xB5", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testMakeValid("0x9C 0x76 0x17", "0xEF 0xBF 0xBD 0x76 0x17");
  }

  private void testIsValid(String input, String expected) {
    ByteArrayOutputStream inp = new ByteArrayOutputStream();
    for (String hex : input.split(" ")) inp.write(Integer.parseInt(hex.substring(2), 16));
    assertEquals(input.equals(expected), fromBytes(inp.toByteArray()).isValid());
  }
  @Test
  public void isValid() {
    // Basic tests
    assertTrue(EMPTY_UTF8.isValid());
    assertTrue(fromString("").isValid());
    assertTrue(fromString("abc").isValid());
    assertTrue(fromString("hello").isValid());
    assertTrue(fromString("大千世界").isValid());
    assertTrue(fromString("数据砖头").isValid());
    assertTrue(fromBytes(new byte[] {}).isValid());
    assertTrue(fromBytes(new byte[] {0x61}).isValid());
    assertTrue(fromBytes(new byte[] {0x7F}).isValid());
    assertFalse(fromBytes(new byte[] {(byte) 0xFF}).isValid());
    assertTrue(fromBytes(new byte[] {(byte) 0xC2, (byte) 0x80}).isValid());
    assertTrue(fromBytes(new byte[] {(byte) 0xDF, (byte) 0xBF}).isValid());
    assertFalse(fromBytes(new byte[] {(byte) 0xC0, (byte) 0x80}).isValid());
    assertFalse(fromBytes(new byte[] {(byte) 0xC1, (byte) 0xBF}).isValid());
    assertFalse(fromBytes(new byte[] {(byte) 0xDF, (byte) 0xC0}).isValid());
    assertFalse(fromBytes(new byte[] {(byte) 0xDF, (byte) 0xFF}).isValid());
    assertFalse(fromBytes(new byte[] {(byte) 0x80, (byte) 0x80}).isValid());
    assertTrue(fromBytes(new byte[] {(byte) 0xE0, (byte) 0xA0, (byte) 0x80}).isValid());
    assertTrue(fromBytes(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBF}).isValid());
    assertTrue(fromBytes(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBF}).isValid());
    assertFalse(fromBytes(new byte[] {(byte) 0xE0, (byte) 0x9F, (byte) 0x80}).isValid());
    assertFalse(fromBytes(new byte[] {(byte) 0xE0, (byte) 0xC0, (byte) 0x80}).isValid());
    assertFalse(fromBytes(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xC0}).isValid());
    assertFalse(fromBytes(new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80}).isValid());
    assertTrue(fromBytes(
      new byte[] {(byte) 0xF0, (byte) 0x90, (byte) 0x80, (byte) 0x80}).isValid());
    assertTrue(fromBytes(
      new byte[] {(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF}).isValid());
    assertFalse(fromBytes(
      new byte[] {(byte) 0xF0, (byte) 0x8F, (byte) 0x80, (byte) 0x80}).isValid());
    assertFalse(fromBytes(
      new byte[] {(byte) 0xF0, (byte) 0x90, (byte) 0x80, (byte) 0x7F}).isValid());
    assertFalse(fromBytes(
      new byte[] {(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80}).isValid());
    assertFalse(fromBytes(
      new byte[] {(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xC0}).isValid());
    assertFalse(fromBytes(
      new byte[] {(byte) 0xF8, (byte) 0x8F, (byte) 0x80, (byte) 0x80, (byte) 0x80}).isValid());
    assertFalse(fromBytes(
      new byte[] {(byte) 0xF8, (byte) 0x90, (byte) 0x80, (byte) 0x80, (byte) 0x80}).isValid());
    assertFalse(fromBytes(
      new byte[] {(byte) 0xFB, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF, (byte) 0xBF}).isValid());
    assertFalse(fromBytes(
      new byte[] {(byte) 0xFB, (byte) 0x90, (byte) 0x80, (byte) 0x80, (byte) 0x80}).isValid());
    // More tests
    testIsValid("0x42", "0x42");
    testIsValid("0x80", "0xEF 0xBF 0xBD");
    testIsValid("0xF4 0x92 0x12", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD 0x12");
    testIsValid("0x84 0xA5 0x63 0x64", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD 0x63 0x64");
    testIsValid("0xF4 0x9B", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testIsValid("0xE2 0x0C", "0xEF 0xBF 0xBD 0x0C");
    testIsValid("0xE8 0x3E", "0xEF 0xBF 0xBD 0x3E");
    testIsValid("0x17 0x1B 0xC2", "0x17 0x1B 0xEF 0xBF 0xBD");
    testIsValid("0x29 0xB2 0x7C 0xA0", "0x29 0xEF 0xBF 0xBD 0x7C 0xEF 0xBF 0xBD");
    testIsValid("0x13 0x68 0x28 0x2A 0x83", "0x13 0x68 0x28 0x2A 0xEF 0xBF 0xBD");
    testIsValid("0x3B 0xFE 0xC2 0xB7 0x9E", "0x3B 0xEF 0xBF 0xBD 0xC2 0xB7 0xEF 0xBF 0xBD");
    testIsValid("0xFC", "0xEF 0xBF 0xBD");
    testIsValid("0x8E 0xBD", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testIsValid("0x6A 0x8B 0x5C 0x5A", "0x6A 0xEF 0xBF 0xBD 0x5C 0x5A");
    testIsValid("0xB4 0xC0", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testIsValid("0xB0 0xD3", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testIsValid("0xAA", "0xEF 0xBF 0xBD");
    testIsValid("0x1A 0x66", "0x1A 0x66");
    testIsValid("0x32 0xB5 0x5F", "0x32 0xEF 0xBF 0xBD 0x5F");
    testIsValid("0x70 0xB6 0x69 0xBC", "0x70 0xEF 0xBF 0xBD 0x69 0xEF 0xBF 0xBD");
    testIsValid("0xDC 0x15 0x82", "0xEF 0xBF 0xBD 0x15 0xEF 0xBF 0xBD");
    testIsValid("0x7F 0x3F 0x72 0xBB", "0x7F 0x3F 0x72 0xEF 0xBF 0xBD");
    testIsValid("0x3C 0x3D 0x1F 0x6C 0x75", "0x3C 0x3D 0x1F 0x6C 0x75");
    testIsValid("0xBF 0x10 0xF4", "0xEF 0xBF 0xBD 0x10 0xEF 0xBF 0xBD");
    testIsValid("0xBF 0xDD 0x89", "0xEF 0xBF 0xBD 0xDD 0x89");
    testIsValid("0x85 0xD4 0x90 0x5E 0x6C", "0xEF 0xBF 0xBD 0xD4 0x90 0x5E 0x6C");
    testIsValid("0x0A 0x92 0x3C 0x17 0x7D", "0x0A 0xEF 0xBF 0xBD 0x3C 0x17 0x7D");
    testIsValid("0x49 0x4E 0x2C 0x7B 0x1C", "0x49 0x4E 0x2C 0x7B 0x1C");
    testIsValid("0x2E 0x0C 0x3E 0x02 0x9D", "0x2E 0x0C 0x3E 0x02 0xEF 0xBF 0xBD");
    testIsValid("0xF9 0xF2 0x11", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD 0x11");
    testIsValid("0xD1", "0xEF 0xBF 0xBD");
    testIsValid("0x74 0x35 0x56 0xC4", "0x74 0x35 0x56 0xEF 0xBF 0xBD");
    testIsValid("0xBE 0x45 0x7F", "0xEF 0xBF 0xBD 0x45 0x7F");
    testIsValid("0x2E", "0x2E");
    testIsValid("0x3B 0xC3", "0x3B 0xEF 0xBF 0xBD");
    testIsValid("0xF9 0x58 0xE0 0x84", "0xEF 0xBF 0xBD 0x58 0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testIsValid("0x72 0xF9 0x4F", "0x72 0xEF 0xBF 0xBD 0x4F");
    testIsValid("0xF2 0x2A 0x38", "0xEF 0xBF 0xBD 0x2A 0x38");
    testIsValid("0x66 0xFC 0x8D 0x5F", "0x66 0xEF 0xBF 0xBD 0xEF 0xBF 0xBD 0x5F");
    testIsValid("0x6D", "0x6D");
    testIsValid("0x6E 0xDF 0xDD", "0x6E 0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testIsValid("0x50 0xE8 0x45 0xDB", "0x50 0xEF 0xBF 0xBD 0x45 0xEF 0xBF 0xBD");
    testIsValid("0x71 0x06 0x1F 0x87", "0x71 0x06 0x1F 0xEF 0xBF 0xBD");
    testIsValid("0x02", "0x02");
    testIsValid("0x20 0x2B 0x38 0xCB 0xDF", "0x20 0x2B 0x38 0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testIsValid("0x10", "0x10");
    testIsValid("0x15", "0x15");
    testIsValid("0xC5 0xEA 0x2A", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD 0x2A");
    testIsValid("0x72 0x2D 0xBB 0x06", "0x72 0x2D 0xEF 0xBF 0xBD 0x06");
    testIsValid("0xB7", "0xEF 0xBF 0xBD");
    testIsValid("0xB8 0xB5", "0xEF 0xBF 0xBD 0xEF 0xBF 0xBD");
    testIsValid("0x9C 0x76 0x17", "0xEF 0xBF 0xBD 0x76 0x17");
  }

  @Test
  public void testGetByte() {
    // Valid UTF-8 string
    String validString = "abcde";
    UTF8String validUTF8String = fromString(validString);
    // Valid byte index handling
    for (int i = 0; i < validString.length(); ++i) {
      assertEquals(validString.charAt(i), validUTF8String.getByte(i));
    }
    // Invalid byte index handling
    assertEquals(0, validUTF8String.getByte(-1));
    assertEquals(0, validUTF8String.getByte(validString.length()));
    assertEquals(0, validUTF8String.getByte(validString.length() + 1));

    // Invalid UTF-8 string
    byte[] invalidString = new byte[] {(byte) 0x41, (byte) 0x42, (byte) 0x80};
    UTF8String invalidUTF8String = fromBytes(invalidString);
    // Valid byte index handling
    for (int i = 0; i < invalidString.length; ++i) {
      assertEquals(invalidString[i], invalidUTF8String.getByte(i));
    }
    // Invalid byte index handling
    assertEquals(0, invalidUTF8String.getByte(-1));
    assertEquals(0, invalidUTF8String.getByte(invalidString.length));
    assertEquals(0, invalidUTF8String.getByte(invalidString.length + 1));
  }

  @Test
  public void testGetChar() {
    // Valid UTF-8 string
    String str = "abcde";
    UTF8String s = fromString(str);
    // Valid character index handling
    for (int i = 0; i < str.length(); ++i) {
      assertEquals(str.charAt(i), s.getChar(i));
    }
    // Invalid character index handling
    assertThrows(IndexOutOfBoundsException.class, () -> s.getChar(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> s.getChar(str.length()));
    assertThrows(IndexOutOfBoundsException.class, () -> s.getChar(str.length() + 1));

    // Invalid UTF-8 string
    byte[] invalidString = new byte[] {(byte) 0x41, (byte) 0x42, (byte) 0x80};
    UTF8String invalidUTF8String = fromBytes(invalidString);
    // Valid byte index handling
    for (int i = 0; i < invalidString.length; ++i) {
      if (Character.isValidCodePoint(invalidString[i])) {
        assertEquals(invalidString[i], invalidUTF8String.getChar(i));
      } else {
        assertEquals(0, invalidUTF8String.getChar(i));
      }
    }
    // Invalid byte index handling
    assertThrows(IndexOutOfBoundsException.class, () -> s.getChar(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> s.getChar(str.length()));
    assertThrows(IndexOutOfBoundsException.class, () -> s.getChar(str.length() + 1));
  }

  @Test
  public void testCodePointFrom() {
    // Valid UTF-8 string
    String str = "abcde";
    UTF8String s = fromString(str);
    // Valid character index handling
    for (int i = 0; i < str.length(); ++i) {
      assertEquals(str.charAt(i), s.codePointFrom(i));
    }
    // Invalid character index handling
    assertThrows(IndexOutOfBoundsException.class, () -> s.codePointFrom(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> s.codePointFrom(str.length()));
    assertThrows(IndexOutOfBoundsException.class, () -> s.codePointFrom(str.length() + 1));

    // Invalid UTF-8 string
    byte[] invalidString = new byte[] {(byte) 0x41, (byte) 0x42, (byte) 0x80};
    UTF8String invalidUTF8String = fromBytes(invalidString);
    // Valid byte index handling
    for (int i = 0; i < invalidString.length; ++i) {
      if (Character.isValidCodePoint(invalidString[i])) {
        assertEquals(invalidString[i], invalidUTF8String.codePointFrom(i));
      } else {
        assertEquals(0, invalidUTF8String.codePointFrom(i));
      }
    }
    // Invalid byte index handling
    assertThrows(IndexOutOfBoundsException.class, () -> s.codePointFrom(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> s.codePointFrom(str.length()));
    assertThrows(IndexOutOfBoundsException.class, () -> s.codePointFrom(str.length() + 1));
  }

  @Test
  public void utf8StringCodePoints() {
    String s = "aéह 日å!";
    UTF8String s0 = fromString(s);
    for (int i = 0; i < s.length(); ++i) {
      assertEquals(s.codePointAt(i), s0.getChar(i));
    }

    UTF8String s1 = fromBytes(new byte[] {0x41, (byte) 0xC3, (byte) 0xB1, (byte) 0xE2,
      (byte) 0x82, (byte) 0xAC, (byte) 0xF0, (byte) 0x90, (byte) 0x8D, (byte) 0x88});
    // numBytesForFirstByte
    assertEquals(1, UTF8String.numBytesForFirstByte(s1.getByte(0)));
    assertEquals(2, UTF8String.numBytesForFirstByte(s1.getByte(1)));
    assertEquals(3, UTF8String.numBytesForFirstByte(s1.getByte(3)));
    assertEquals(4, UTF8String.numBytesForFirstByte(s1.getByte(6)));
    // getByte
    assertEquals((byte) 0x41, s1.getByte(0));
    assertEquals((byte) 0xC3, s1.getByte(1));
    assertEquals((byte) 0xE2, s1.getByte(3));
    assertEquals((byte) 0xF0, s1.getByte(6));
    // codePointFrom
    assertEquals(0x41, s1.codePointFrom(0));
    assertEquals(0xF1, s1.codePointFrom(1));
    assertEquals(0x20AC, s1.codePointFrom(3));
    assertEquals(0x10348, s1.codePointFrom(6));
    assertThrows(IndexOutOfBoundsException.class, () -> s1.codePointFrom(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> s1.codePointFrom(99));
    // getChar
    assertEquals(0x41, s1.getChar(0));
    assertEquals(0xF1, s1.getChar(1));
    assertEquals(0x20AC, s1.getChar(2));
    assertEquals(0x10348, s1.getChar(3));
    assertThrows(IndexOutOfBoundsException.class, () -> s1.getChar(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> s1.getChar(99));

    UTF8String s2 = fromString("Añ€𐍈");
    // numBytesForFirstByte
    assertEquals(1, UTF8String.numBytesForFirstByte(s2.getByte(0)));
    assertEquals(2, UTF8String.numBytesForFirstByte(s2.getByte(1)));
    assertEquals(3, UTF8String.numBytesForFirstByte(s2.getByte(3)));
    assertEquals(4, UTF8String.numBytesForFirstByte(s2.getByte(6)));
    // getByte
    assertEquals((byte) 0x41, s2.getByte(0));
    assertEquals((byte) 0xC3, s2.getByte(1));
    assertEquals((byte) 0xE2, s2.getByte(3));
    assertEquals((byte) 0xF0, s2.getByte(6));
    // codePointFrom
    assertEquals(0x41, s2.codePointFrom(0));
    assertEquals(0xF1, s2.codePointFrom(1));
    assertEquals(0x20AC, s2.codePointFrom(3));
    assertEquals(0x10348, s2.codePointFrom(6));
    assertThrows(IndexOutOfBoundsException.class, () -> s2.codePointFrom(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> s2.codePointFrom(99));
    // getChar
    assertEquals(0x41, s2.getChar(0));
    assertEquals(0xF1, s2.getChar(1));
    assertEquals(0x20AC, s2.getChar(2));
    assertEquals(0x10348, s2.getChar(3));
    assertThrows(IndexOutOfBoundsException.class, () -> s2.getChar(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> s2.getChar(99));

    UTF8String s3 = EMPTY_UTF8;
    // codePointFrom
    assertThrows(IndexOutOfBoundsException.class, () -> s3.codePointFrom(0));
    assertThrows(IndexOutOfBoundsException.class, () -> s3.codePointFrom(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> s3.codePointFrom(99));
    // getChar
    assertThrows(IndexOutOfBoundsException.class, () -> s3.getChar(0));
    assertThrows(IndexOutOfBoundsException.class, () -> s3.getChar(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> s3.getChar(99));
  }

  private void testCodePointIterator(UTF8String utf8String) {
    CodePointIteratorType iteratorMode = utf8String.isValid() ?
      CodePointIteratorType.CODE_POINT_ITERATOR_ASSUME_VALID :
      CodePointIteratorType.CODE_POINT_ITERATOR_MAKE_VALID;
    Iterator<Integer> iterator = utf8String.codePointIterator(iteratorMode);
    for (int i = 0; i < utf8String.numChars(); ++i) {
      assertTrue(iterator.hasNext());
      int codePoint = (utf8String.isValid() ? utf8String : utf8String.makeValid()).getChar(i);
      assertEquals(codePoint, (int) iterator.next());
    }
    assertFalse(iterator.hasNext());
  }
  @Test
  public void codePointIterator() {
    // Valid UTF8 strings.
    testCodePointIterator(fromString(""));
    testCodePointIterator(fromString("abc"));
    testCodePointIterator(fromString("a!2&^R"));
    testCodePointIterator(fromString("aéह 日å!"));
    testCodePointIterator(fromBytes(new byte[] {(byte) 0x41}));
    testCodePointIterator(fromBytes(new byte[] {(byte) 0xC2, (byte) 0xA3}));
    testCodePointIterator(fromBytes(new byte[] {(byte) 0xE2, (byte) 0x82, (byte) 0xAC}));
    // Invalid UTF8 strings.
    testCodePointIterator(fromBytes(new byte[] {(byte) 0xFF}));
    testCodePointIterator(fromBytes(new byte[] {(byte) 0x80}));
    testCodePointIterator(fromBytes(new byte[] {(byte) 0xC2, (byte) 0x80}));
    testCodePointIterator(fromBytes(new byte[] {(byte) 0xE2, (byte) 0x82, (byte) 0x80}));
    testCodePointIterator(fromBytes(new byte[] {(byte) 0x41, (byte) 0x80, (byte) 0x42}));
    testCodePointIterator(fromBytes(new byte[] {
      (byte) 0x41, (byte) 0xC2, (byte) 0x80, (byte) 0x42}));
    testCodePointIterator(fromBytes(new byte[] {
      (byte) 0x41, (byte) 0xE2, (byte) 0x82, (byte) 0x80, (byte) 0x42}));
  }

  private void testReverseCodePointIterator(UTF8String utf8String) {
    CodePointIteratorType iteratorMode = utf8String.isValid() ?
      CodePointIteratorType.CODE_POINT_ITERATOR_ASSUME_VALID :
      CodePointIteratorType.CODE_POINT_ITERATOR_MAKE_VALID;
    Iterator<Integer> iterator = utf8String.codePointIterator(iteratorMode);
    for (int i = 0; i < utf8String.numChars(); ++i) {
      assertTrue(iterator.hasNext());
      int codePoint = (utf8String.isValid() ? utf8String : utf8String.makeValid()).getChar(i);
      assertEquals(codePoint, (int) iterator.next());
    }
    assertFalse(iterator.hasNext());
  }
  @Test
  public void reverseCodePointIterator() {
    // Valid UTF8 strings
    testReverseCodePointIterator(fromString(""));
    testReverseCodePointIterator(fromString("abc"));
    testReverseCodePointIterator(fromString("a!2&^R"));
    testReverseCodePointIterator(fromString("aéह 日å!"));
    testReverseCodePointIterator(fromBytes(new byte[] {(byte) 0x41}));
    testReverseCodePointIterator(fromBytes(new byte[] {(byte) 0xC2, (byte) 0xA3}));
    testReverseCodePointIterator(fromBytes(new byte[] {(byte) 0xE2, (byte) 0x82, (byte) 0xAC}));
    // Invalid UTF8 strings
    testReverseCodePointIterator(fromBytes(new byte[] {(byte) 0xFF}));
    testReverseCodePointIterator(fromBytes(new byte[] {(byte) 0x80}));
    testReverseCodePointIterator(fromBytes(new byte[] {(byte) 0xC2, (byte) 0x80}));
    testReverseCodePointIterator(fromBytes(new byte[] {(byte) 0xE2, (byte) 0x82, (byte) 0x80}));
    testReverseCodePointIterator(fromBytes(new byte[] {(byte) 0x41, (byte) 0x80, (byte) 0x42}));
    testReverseCodePointIterator(fromBytes(new byte[] {
      (byte) 0x41, (byte) 0xC2, (byte) 0x80, (byte) 0x42}));
    testReverseCodePointIterator(fromBytes(new byte[] {
      (byte) 0x41, (byte) 0xE2, (byte) 0x82, (byte) 0x80, (byte) 0x42}));
  }

  @Test
  public void toBinaryString() {
    assertEquals(ZERO_UTF8, UTF8String.toBinaryString(0));
    assertEquals(UTF8String.fromString("1"), UTF8String.toBinaryString(1));
    assertEquals(UTF8String.fromString("10"), UTF8String.toBinaryString(2));
    assertEquals(UTF8String.fromString("100"), UTF8String.toBinaryString(4));
    assertEquals(UTF8String.fromString("111"), UTF8String.toBinaryString(7));
    assertEquals(
      UTF8String.fromString("1111111111111111111111111111111111111111111111111111111111110011"),
      UTF8String.toBinaryString(-13));
    assertEquals(
      UTF8String.fromString("1000000000000000000000000000000000000000000000000000000000000000"),
      UTF8String.toBinaryString(Long.MIN_VALUE));
    assertEquals(
      UTF8String.fromString("111111111111111111111111111111111111111111111111111111111111111"),
      UTF8String.toBinaryString(Long.MAX_VALUE));
  }

  /**
   * This tests whether appending a codepoint to a 'UTF8StringBuilder' correctly appends every
   * single codepoint. We test it against an already existing 'StringBuilder.appendCodePoint' and
   * 'UTF8String.fromString'. We skip testing the surrogate codepoints because at some point while
   * converting the surrogate codepoint to 'UTF8String' (via 'StringBuilder' and 'UTF8String') we
   * get an ill-formated byte sequence (probably because 'String' is in UTF-16 format, and a single
   * surrogate codepoint is handled differently in UTF-16 than in UTF-8, so somewhere during those
   * conversions some different behaviour happens).
   */
  @Test
  public void testAppendCodepointToUTF8StringBuilder() {
    int surrogateRangeLowerBound = 0xD800;
    int surrogateRangeUpperBound = 0xDFFF;
    for (int i = Character.MIN_CODE_POINT; i <= Character.MAX_CODE_POINT; ++i) {
      if(surrogateRangeLowerBound <= i && i <= surrogateRangeUpperBound) continue;
      UTF8StringBuilder usb = new UTF8StringBuilder();
      usb.appendCodePoint(i);
      StringBuilder sb = new StringBuilder();
      sb.appendCodePoint(i);
      assert(usb.build().equals(UTF8String.fromString(sb.toString())));
    }
  }
}
