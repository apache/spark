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
  public void compareTo() {
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
  public void concatTest() {
    assertEquals(concat(), fromString(""));
    assertEquals(concat(null), fromString(""));
    assertEquals(concat(fromString("")), fromString(""));
    assertEquals(concat(fromString("ab")), fromString("ab"));
    assertEquals(concat(fromString("a"), fromString("b")), fromString("ab"));
    assertEquals(concat(fromString("a"), fromString("b"), fromString("c")), fromString("abc"));
    assertEquals(concat(fromString("a"), null, fromString("c")), fromString("ac"));
    assertEquals(concat(fromString("a"), null, null), fromString("a"));
    assertEquals(concat(null, null, null), fromString(""));
    assertEquals(concat(fromString("数据"), fromString("砖头")), fromString("数据砖头"));
  }

  @Test
  public void contains() {
    assertTrue(fromString("").contains(fromString("")));
    assertTrue(fromString("hello").contains(fromString("ello")));
    assertFalse(fromString("hello").contains(fromString("vello")));
    assertFalse(fromString("hello").contains(fromString("hellooo")));
    assertTrue(fromString("大千世界").contains(fromString("千世界")));
    assertFalse(fromString("大千世界").contains(fromString("世千")));
    assertFalse(fromString("大千世界").contains(fromString("大千世界好")));
  }

  @Test
  public void startsWith() {
    assertTrue(fromString("").startsWith(fromString("")));
    assertTrue(fromString("hello").startsWith(fromString("hell")));
    assertFalse(fromString("hello").startsWith(fromString("ell")));
    assertFalse(fromString("hello").startsWith(fromString("hellooo")));
    assertTrue(fromString("数据砖头").startsWith(fromString("数据")));
    assertFalse(fromString("大千世界").startsWith(fromString("千")));
    assertFalse(fromString("大千世界").startsWith(fromString("大千世界好")));
  }

  @Test
  public void endsWith() {
    assertTrue(fromString("").endsWith(fromString("")));
    assertTrue(fromString("hello").endsWith(fromString("ello")));
    assertFalse(fromString("hello").endsWith(fromString("ellov")));
    assertFalse(fromString("hello").endsWith(fromString("hhhello")));
    assertTrue(fromString("大千世界").endsWith(fromString("世界")));
    assertFalse(fromString("大千世界").endsWith(fromString("世")));
    assertFalse(fromString("数据砖头").endsWith(fromString("我的数据砖头")));
  }

  @Test
  public void substring() {
    assertEquals(fromString(""), fromString("hello").substring(0, 0));
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

    assertEquals(fromString(""), fromString("  ").trim());
    assertEquals(fromString(""), fromString("  ").trimLeft());
    assertEquals(fromString(""), fromString("  ").trimRight());

    assertEquals(fromString("数据砖头"), fromString("  数据砖头 ").trim());
    assertEquals(fromString("数据砖头 "), fromString("  数据砖头 ").trimLeft());
    assertEquals(fromString("  数据砖头"), fromString("  数据砖头 ").trimRight());

    assertEquals(fromString("数据砖头"), fromString("数据砖头").trim());
    assertEquals(fromString("数据砖头"), fromString("数据砖头").trimLeft());
    assertEquals(fromString("数据砖头"), fromString("数据砖头").trimRight());
  }

  @Test
  public void indexOf() {
    assertEquals(0, fromString("").indexOf(fromString(""), 0));
    assertEquals(-1, fromString("").indexOf(fromString("l"), 0));
    assertEquals(0, fromString("hello").indexOf(fromString(""), 0));
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
  public void reverse() {
    assertEquals(fromString("olleh"), fromString("hello").reverse());
    assertEquals(fromString(""), fromString("").reverse());
    assertEquals(fromString("者行孙"), fromString("孙行者").reverse());
    assertEquals(fromString("者行孙 olleh"), fromString("hello 孙行者").reverse());
  }

  @Test
  public void repeat() {
    assertEquals(fromString("数d数d数d数d数d"), fromString("数d").repeat(5));
    assertEquals(fromString("数d"), fromString("数d").repeat(1));
    assertEquals(fromString(""), fromString("数d").repeat(-1));
  }

  @Test
  public void pad() {
    assertEquals(fromString("hel"), fromString("hello").lpad(3, fromString("????")));
    assertEquals(fromString("hello"), fromString("hello").lpad(5, fromString("????")));
    assertEquals(fromString("?hello"), fromString("hello").lpad(6, fromString("????")));
    assertEquals(fromString("???????hello"), fromString("hello").lpad(12, fromString("????")));
    assertEquals(fromString("?????hello"), fromString("hello").lpad(10, fromString("?????")));
    assertEquals(fromString("???????"), fromString("").lpad(7, fromString("?????")));

    assertEquals(fromString("hel"), fromString("hello").rpad(3, fromString("????")));
    assertEquals(fromString("hello"), fromString("hello").rpad(5, fromString("????")));
    assertEquals(fromString("hello?"), fromString("hello").rpad(6, fromString("????")));
    assertEquals(fromString("hello???????"), fromString("hello").rpad(12, fromString("????")));
    assertEquals(fromString("hello?????"), fromString("hello").rpad(10, fromString("?????")));
    assertEquals(fromString("???????"), fromString("").rpad(7, fromString("?????")));


    assertEquals(fromString("数据砖"), fromString("数据砖头").lpad(3, fromString("????")));
    assertEquals(fromString("?数据砖头"), fromString("数据砖头").lpad(5, fromString("????")));
    assertEquals(fromString("??数据砖头"), fromString("数据砖头").lpad(6, fromString("????")));
    assertEquals(fromString("孙行数据砖头"), fromString("数据砖头").lpad(6, fromString("孙行者")));
    assertEquals(fromString("孙行者数据砖头"), fromString("数据砖头").lpad(7, fromString("孙行者")));
    assertEquals(fromString("孙行者孙行者孙行数据砖头"), fromString("数据砖头").lpad(12, fromString("孙行者")));

    assertEquals(fromString("数据砖"), fromString("数据砖头").rpad(3, fromString("????")));
    assertEquals(fromString("数据砖头?"), fromString("数据砖头").rpad(5, fromString("????")));
    assertEquals(fromString("数据砖头??"), fromString("数据砖头").rpad(6, fromString("????")));
    assertEquals(fromString("数据砖头孙行"), fromString("数据砖头").rpad(6, fromString("孙行者")));
    assertEquals(fromString("数据砖头孙行者"), fromString("数据砖头").rpad(7, fromString("孙行者")));
    assertEquals(fromString("数据砖头孙行者孙行者孙行"), fromString("数据砖头").rpad(12, fromString("孙行者")));
  }
  
  @Test
  public void levenshteinDistance() {
    assertEquals(
        UTF8String.fromString("").levenshteinDistance(UTF8String.fromString("")), 0);
    assertEquals(
        UTF8String.fromString("").levenshteinDistance(UTF8String.fromString("a")), 1);
    assertEquals(
        UTF8String.fromString("aaapppp").levenshteinDistance(UTF8String.fromString("")), 7);
    assertEquals(
        UTF8String.fromString("frog").levenshteinDistance(UTF8String.fromString("fog")), 1);
    assertEquals(
        UTF8String.fromString("fly").levenshteinDistance(UTF8String.fromString("ant")),3);
    assertEquals(
        UTF8String.fromString("elephant").levenshteinDistance(UTF8String.fromString("hippo")), 7);
    assertEquals(
        UTF8String.fromString("hippo").levenshteinDistance(UTF8String.fromString("elephant")), 7);
    assertEquals(
        UTF8String.fromString("hippo").levenshteinDistance(UTF8String.fromString("zzzzzzzz")), 8);
    assertEquals(
        UTF8String.fromString("hello").levenshteinDistance(UTF8String.fromString("hallo")),1);
    assertEquals(
        UTF8String.fromString("世界千世").levenshteinDistance(UTF8String.fromString("千a世b")),4);
  }
}
