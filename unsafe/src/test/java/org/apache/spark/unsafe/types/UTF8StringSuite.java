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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static junit.framework.Assert.*;

import static org.apache.spark.unsafe.types.UTF8String.*;

public class UTF8StringSuite {

  private void checkBasic(String str, int len) throws UnsupportedEncodingException {
    UTF8String s1 = fromString(str);
    UTF8String s2 = fromBytes(str.getBytes("utf8"));
    assertEquals(s1.numChars(), len);
    assertEquals(s2.numChars(), len);

    assertEquals(s1.toString(), str);
    assertEquals(s2.toString(), str);
    assertEquals(s1, s2);

    assertEquals(s1.hashCode(), s2.hashCode());

    assertEquals(s1.compareTo(s2), 0);

    assertEquals(s1.contains(s2), true);
    assertEquals(s2.contains(s1), true);
    assertEquals(s1.startsWith(s1), true);
    assertEquals(s1.endsWith(s1), true);
  }

  @Test
  public void basicTest() throws UnsupportedEncodingException {
    checkBasic("", 0);
    checkBasic("hello", 5);
    checkBasic("大 千 世 界", 7);
  }

  @Test
  public void emptyStringTest() {
    assertEquals(fromString(""), EMPTY_UTF8);
    assertEquals(fromBytes(new byte[0]), EMPTY_UTF8);
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
    UTF8String str1 = UTF8String.fromBytes(buf1, 0, 3);
    UTF8String str2 = UTF8String.fromBytes(buf1, 0, 8);
    UTF8String str3 = UTF8String.fromBytes(buf2);
    assertTrue(str1.getPrefix() - str2.getPrefix() < 0);
    assertEquals(str1.getPrefix(), str3.getPrefix());
  }

  @Test
  public void compareTo() {
    assertTrue(fromString("").compareTo(fromString("a")) < 0);
    assertTrue(fromString("abc").compareTo(fromString("ABC")) > 0);
    assertTrue(fromString("abc0").compareTo(fromString("abc")) > 0);
    assertTrue(fromString("abcabcabc").compareTo(fromString("abcabcabc")) == 0);
    assertTrue(fromString("aBcabcabc").compareTo(fromString("Abcabcabc")) > 0);
    assertTrue(fromString("Abcabcabc").compareTo(fromString("abcabcabC")) < 0);
    assertTrue(fromString("abcabcabc").compareTo(fromString("abcabcabC")) > 0);

    assertTrue(fromString("abc").compareTo(fromString("世界")) < 0);
    assertTrue(fromString("你好").compareTo(fromString("世界")) > 0);
    assertTrue(fromString("你好123").compareTo(fromString("你好122")) > 0);
  }

  protected void testUpperandLower(String upper, String lower) {
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
    assertEquals(null, concat((UTF8String) null));
    assertEquals(EMPTY_UTF8, concat(EMPTY_UTF8));
    assertEquals(fromString("ab"), concat(fromString("ab")));
    assertEquals(fromString("ab"), concat(fromString("a"), fromString("b")));
    assertEquals(fromString("abc"), concat(fromString("a"), fromString("b"), fromString("c")));
    assertEquals(null, concat(fromString("a"), null, fromString("c")));
    assertEquals(null, concat(fromString("a"), null, null));
    assertEquals(null, concat(null, null, null));
    assertEquals(fromString("数据砖头"), concat(fromString("数据"), fromString("砖头")));
  }

  @Test
  public void concatWsTest() {
    // Returns null if the separator is null
    assertEquals(null, concatWs(null, (UTF8String)null));
    assertEquals(null, concatWs(null, fromString("a")));

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
    assertEquals(fromString("hello"), fromString("  hello ").trim());
    assertEquals(fromString("hello "), fromString("  hello ").trimLeft());
    assertEquals(fromString("  hello"), fromString("  hello ").trimRight());

    assertEquals(EMPTY_UTF8, fromString("  ").trim());
    assertEquals(EMPTY_UTF8, fromString("  ").trimLeft());
    assertEquals(EMPTY_UTF8, fromString("  ").trimRight());

    assertEquals(fromString("数据砖头"), fromString("  数据砖头 ").trim());
    assertEquals(fromString("数据砖头 "), fromString("  数据砖头 ").trimLeft());
    assertEquals(fromString("  数据砖头"), fromString("  数据砖头 ").trimRight());

    assertEquals(fromString("数据砖头"), fromString("数据砖头").trim());
    assertEquals(fromString("数据砖头"), fromString("数据砖头").trimLeft());
    assertEquals(fromString("数据砖头"), fromString("数据砖头").trimRight());
  }

  @Test
  public void indexOf() {
    assertEquals(0, EMPTY_UTF8.indexOf(EMPTY_UTF8, 0));
    assertEquals(-1, EMPTY_UTF8.indexOf(fromString("l"), 0));
    assertEquals(0, fromString("hello").indexOf(EMPTY_UTF8, 0));
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
    assertEquals(e.substringSQL(0, 2), fromString("ex"));
    assertEquals(e.substringSQL(1, 2), fromString("ex"));
    assertEquals(e.substringSQL(0, 7), fromString("example"));
    assertEquals(e.substringSQL(1, 2), fromString("ex"));
    assertEquals(e.substringSQL(0, 100), fromString("example"));
    assertEquals(e.substringSQL(1, 100), fromString("example"));
    assertEquals(e.substringSQL(2, 2), fromString("xa"));
    assertEquals(e.substringSQL(1, 6), fromString("exampl"));
    assertEquals(e.substringSQL(2, 100), fromString("xample"));
    assertEquals(e.substringSQL(0, 0), fromString(""));
    assertEquals(e.substringSQL(100, 4), EMPTY_UTF8);
    assertEquals(e.substringSQL(0, Integer.MAX_VALUE), fromString("example"));
    assertEquals(e.substringSQL(1, Integer.MAX_VALUE), fromString("example"));
    assertEquals(e.substringSQL(2, Integer.MAX_VALUE), fromString("xample"));
  }

  @Test
  public void split() {
    assertTrue(Arrays.equals(fromString("ab,def,ghi").split(fromString(","), -1),
      new UTF8String[]{fromString("ab"), fromString("def"), fromString("ghi")}));
    assertTrue(Arrays.equals(fromString("ab,def,ghi").split(fromString(","), 2),
      new UTF8String[]{fromString("ab"), fromString("def,ghi")}));
    assertTrue(Arrays.equals(fromString("ab,def,ghi").split(fromString(","), 2),
      new UTF8String[]{fromString("ab"), fromString("def,ghi")}));
  }
  
  @Test
  public void levenshteinDistance() {
    assertEquals(EMPTY_UTF8.levenshteinDistance(EMPTY_UTF8), 0);
    assertEquals(EMPTY_UTF8.levenshteinDistance(fromString("a")), 1);
    assertEquals(fromString("aaapppp").levenshteinDistance(EMPTY_UTF8), 7);
    assertEquals(fromString("frog").levenshteinDistance(fromString("fog")), 1);
    assertEquals(fromString("fly").levenshteinDistance(fromString("ant")),3);
    assertEquals(fromString("elephant").levenshteinDistance(fromString("hippo")), 7);
    assertEquals(fromString("hippo").levenshteinDistance(fromString("elephant")), 7);
    assertEquals(fromString("hippo").levenshteinDistance(fromString("zzzzzzzz")), 8);
    assertEquals(fromString("hello").levenshteinDistance(fromString("hallo")),1);
    assertEquals(fromString("世界千世").levenshteinDistance(fromString("千a世b")),4);
  }

  @Test
  public void translate() {
    assertEquals(
      fromString("1a2s3ae"),
      fromString("translate").translate(ImmutableMap.of(
        'r', '1',
        'n', '2',
        'l', '3',
        't', '\0'
      )));
    assertEquals(
      fromString("translate"),
      fromString("translate").translate(new HashMap<Character, Character>()));
    assertEquals(
      fromString("asae"),
      fromString("translate").translate(ImmutableMap.of(
        'r', '\0',
        'n', '\0',
        'l', '\0',
        't', '\0'
      )));
    assertEquals(
      fromString("aa世b"),
      fromString("花花世界").translate(ImmutableMap.of(
        '花', 'a',
        '界', 'b'
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
    assertEquals(fromString("ab").findInSet(fromString("ab")), 1);
    assertEquals(fromString("a,b").findInSet(fromString("b")), 2);
    assertEquals(fromString("abc,b,ab,c,def").findInSet(fromString("ab")), 3);
    assertEquals(fromString("ab,abc,b,ab,c,def").findInSet(fromString("ab")), 1);
    assertEquals(fromString(",,,ab,abc,b,ab,c,def").findInSet(fromString("ab")), 4);
    assertEquals(fromString(",ab,abc,b,ab,c,def").findInSet(fromString("")), 1);
    assertEquals(fromString("数据砖头,abc,b,ab,c,def").findInSet(fromString("ab")), 4);
    assertEquals(fromString("数据砖头,abc,b,ab,c,def").findInSet(fromString("def")), 6);
  }

  @Test
  public void soundex() {
    assertEquals(fromString("Robert").soundex(), fromString("R163"));
    assertEquals(fromString("Rupert").soundex(), fromString("R163"));
    assertEquals(fromString("Rubin").soundex(), fromString("R150"));
    assertEquals(fromString("Ashcraft").soundex(), fromString("A261"));
    assertEquals(fromString("Ashcroft").soundex(), fromString("A261"));
    assertEquals(fromString("Burroughs").soundex(), fromString("B620"));
    assertEquals(fromString("Burrows").soundex(), fromString("B620"));
    assertEquals(fromString("Ekzampul").soundex(), fromString("E251"));
    assertEquals(fromString("Example").soundex(), fromString("E251"));
    assertEquals(fromString("Ellery").soundex(), fromString("E460"));
    assertEquals(fromString("Euler").soundex(), fromString("E460"));
    assertEquals(fromString("Ghosh").soundex(), fromString("G200"));
    assertEquals(fromString("Gauss").soundex(), fromString("G200"));
    assertEquals(fromString("Gutierrez").soundex(), fromString("G362"));
    assertEquals(fromString("Heilbronn").soundex(), fromString("H416"));
    assertEquals(fromString("Hilbert").soundex(), fromString("H416"));
    assertEquals(fromString("Jackson").soundex(), fromString("J250"));
    assertEquals(fromString("Kant").soundex(), fromString("K530"));
    assertEquals(fromString("Knuth").soundex(), fromString("K530"));
    assertEquals(fromString("Lee").soundex(), fromString("L000"));
    assertEquals(fromString("Lukasiewicz").soundex(), fromString("L222"));
    assertEquals(fromString("Lissajous").soundex(), fromString("L222"));
    assertEquals(fromString("Ladd").soundex(), fromString("L300"));
    assertEquals(fromString("Lloyd").soundex(), fromString("L300"));
    assertEquals(fromString("Moses").soundex(), fromString("M220"));
    assertEquals(fromString("O'Hara").soundex(), fromString("O600"));
    assertEquals(fromString("Pfister").soundex(), fromString("P236"));
    assertEquals(fromString("Rubin").soundex(), fromString("R150"));
    assertEquals(fromString("Robert").soundex(), fromString("R163"));
    assertEquals(fromString("Rupert").soundex(), fromString("R163"));
    assertEquals(fromString("Soundex").soundex(), fromString("S532"));
    assertEquals(fromString("Sownteks").soundex(), fromString("S532"));
    assertEquals(fromString("Tymczak").soundex(), fromString("T522"));
    assertEquals(fromString("VanDeusen").soundex(), fromString("V532"));
    assertEquals(fromString("Washington").soundex(), fromString("W252"));
    assertEquals(fromString("Wheaton").soundex(), fromString("W350"));

    assertEquals(fromString("a").soundex(), fromString("A000"));
    assertEquals(fromString("ab").soundex(), fromString("A100"));
    assertEquals(fromString("abc").soundex(), fromString("A120"));
    assertEquals(fromString("abcd").soundex(), fromString("A123"));
    assertEquals(fromString("").soundex(), fromString(""));
    assertEquals(fromString("123").soundex(), fromString("123"));
    assertEquals(fromString("世界千世").soundex(), fromString("世界千世"));
  }
}
