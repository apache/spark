/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import static org.junit.Assert.assertArrayEquals;

public class TestStringUtils extends TestCase {
  final private static String NULL_STR = null;
  final private static String EMPTY_STR = "";
  final private static String STR_WO_SPECIAL_CHARS = "AB";
  final private static String STR_WITH_COMMA = "A,B";
  final private static String ESCAPED_STR_WITH_COMMA = "A\\,B";
  final private static String STR_WITH_ESCAPE = "AB\\";
  final private static String ESCAPED_STR_WITH_ESCAPE = "AB\\\\";
  final private static String STR_WITH_BOTH2 = ",A\\,,B\\\\,";
  final private static String ESCAPED_STR_WITH_BOTH2 = 
    "\\,A\\\\\\,\\,B\\\\\\\\\\,";
  
  public void testEscapeString() throws Exception {
    assertEquals(NULL_STR, StringUtils.escapeString(NULL_STR));
    assertEquals(EMPTY_STR, StringUtils.escapeString(EMPTY_STR));
    assertEquals(STR_WO_SPECIAL_CHARS,
        StringUtils.escapeString(STR_WO_SPECIAL_CHARS));
    assertEquals(ESCAPED_STR_WITH_COMMA,
        StringUtils.escapeString(STR_WITH_COMMA));
    assertEquals(ESCAPED_STR_WITH_ESCAPE,
        StringUtils.escapeString(STR_WITH_ESCAPE));
    assertEquals(ESCAPED_STR_WITH_BOTH2, 
        StringUtils.escapeString(STR_WITH_BOTH2));
  }
  
  public void testSplit() throws Exception {
    assertEquals(NULL_STR, StringUtils.split(NULL_STR));
    String[] splits = StringUtils.split(EMPTY_STR);
    assertEquals(0, splits.length);
    splits = StringUtils.split(",,");
    assertEquals(0, splits.length);
    splits = StringUtils.split(STR_WO_SPECIAL_CHARS);
    assertEquals(1, splits.length);
    assertEquals(STR_WO_SPECIAL_CHARS, splits[0]);
    splits = StringUtils.split(STR_WITH_COMMA);
    assertEquals(2, splits.length);
    assertEquals("A", splits[0]);
    assertEquals("B", splits[1]);
    splits = StringUtils.split(ESCAPED_STR_WITH_COMMA);
    assertEquals(1, splits.length);
    assertEquals(ESCAPED_STR_WITH_COMMA, splits[0]);
    splits = StringUtils.split(STR_WITH_ESCAPE);
    assertEquals(1, splits.length);
    assertEquals(STR_WITH_ESCAPE, splits[0]);
    splits = StringUtils.split(STR_WITH_BOTH2);
    assertEquals(3, splits.length);
    assertEquals(EMPTY_STR, splits[0]);
    assertEquals("A\\,", splits[1]);
    assertEquals("B\\\\", splits[2]);
    splits = StringUtils.split(ESCAPED_STR_WITH_BOTH2);
    assertEquals(1, splits.length);
    assertEquals(ESCAPED_STR_WITH_BOTH2, splits[0]);    
  }
  
  public void testUnescapeString() throws Exception {
    assertEquals(NULL_STR, StringUtils.unEscapeString(NULL_STR));
    assertEquals(EMPTY_STR, StringUtils.unEscapeString(EMPTY_STR));
    assertEquals(STR_WO_SPECIAL_CHARS,
        StringUtils.unEscapeString(STR_WO_SPECIAL_CHARS));
    try {
      StringUtils.unEscapeString(STR_WITH_COMMA);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertEquals(STR_WITH_COMMA,
        StringUtils.unEscapeString(ESCAPED_STR_WITH_COMMA));
    try {
      StringUtils.unEscapeString(STR_WITH_ESCAPE);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertEquals(STR_WITH_ESCAPE,
        StringUtils.unEscapeString(ESCAPED_STR_WITH_ESCAPE));
    try {
      StringUtils.unEscapeString(STR_WITH_BOTH2);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertEquals(STR_WITH_BOTH2,
        StringUtils.unEscapeString(ESCAPED_STR_WITH_BOTH2));
  }
  
  public void testTraditionalBinaryPrefix() throws Exception {
    String[] symbol = {"k", "m", "g", "t", "p", "e"};
    long m = 1024;
    for(String s : symbol) {
      assertEquals(0, StringUtils.TraditionalBinaryPrefix.string2long(0 + s));
      assertEquals(m, StringUtils.TraditionalBinaryPrefix.string2long(1 + s));
      m *= 1024;
    }
    
    assertEquals(0L, StringUtils.TraditionalBinaryPrefix.string2long("0"));
    assertEquals(-1259520L, StringUtils.TraditionalBinaryPrefix.string2long("-1230k"));
    assertEquals(956703965184L, StringUtils.TraditionalBinaryPrefix.string2long("891g"));
  }

  public void testGetTrimmedStrings() throws Exception {
    String compactDirList = "/spindle1/hdfs,/spindle2/hdfs,/spindle3/hdfs";
    String spacedDirList = "/spindle1/hdfs, /spindle2/hdfs, /spindle3/hdfs";
    String pathologicalDirList1 = " /spindle1/hdfs  ,  /spindle2/hdfs ,/spindle3/hdfs ";
    String pathologicalDirList2 = " /spindle1/hdfs  ,  /spindle2/hdfs ,/spindle3/hdfs , ";
    String emptyList1 = "";
    String emptyList2 = "   ";
    
    String[] expectedArray = {"/spindle1/hdfs", "/spindle2/hdfs", "/spindle3/hdfs"};
    String[] emptyArray = {};
    
    assertArrayEquals(expectedArray, StringUtils.getTrimmedStrings(compactDirList));
    assertArrayEquals(expectedArray, StringUtils.getTrimmedStrings(spacedDirList));
    assertArrayEquals(expectedArray, StringUtils.getTrimmedStrings(pathologicalDirList1));
    assertArrayEquals(expectedArray, StringUtils.getTrimmedStrings(pathologicalDirList2));
    
    assertArrayEquals(emptyArray, StringUtils.getTrimmedStrings(emptyList1));
    assertArrayEquals(emptyArray, StringUtils.getTrimmedStrings(emptyList2));
  } 

  public void testJoin() {
    List<String> s = new ArrayList<String>();
    s.add("a");
    s.add("b");
    s.add("c");
    assertEquals("", StringUtils.join(":", s.subList(0, 0)));
    assertEquals("a", StringUtils.join(":", s.subList(0, 1)));
    assertEquals("a:b", StringUtils.join(":", s.subList(0, 2)));
    assertEquals("a:b:c", StringUtils.join(":", s.subList(0, 3)));
  }

  public void testCamelize() {
    // common use cases
    assertEquals("Map", StringUtils.camelize("MAP"));
    assertEquals("JobSetup", StringUtils.camelize("JOB_SETUP"));
    assertEquals("SomeStuff", StringUtils.camelize("some_stuff"));

    // sanity checks for ascii alphabet against unexpected locale issues.
    assertEquals("Aa", StringUtils.camelize("aA"));
    assertEquals("Bb", StringUtils.camelize("bB"));
    assertEquals("Cc", StringUtils.camelize("cC"));
    assertEquals("Dd", StringUtils.camelize("dD"));
    assertEquals("Ee", StringUtils.camelize("eE"));
    assertEquals("Ff", StringUtils.camelize("fF"));
    assertEquals("Gg", StringUtils.camelize("gG"));
    assertEquals("Hh", StringUtils.camelize("hH"));
    assertEquals("Ii", StringUtils.camelize("iI"));
    assertEquals("Jj", StringUtils.camelize("jJ"));
    assertEquals("Kk", StringUtils.camelize("kK"));
    assertEquals("Ll", StringUtils.camelize("lL"));
    assertEquals("Mm", StringUtils.camelize("mM"));
    assertEquals("Nn", StringUtils.camelize("nN"));
    assertEquals("Oo", StringUtils.camelize("oO"));
    assertEquals("Pp", StringUtils.camelize("pP"));
    assertEquals("Qq", StringUtils.camelize("qQ"));
    assertEquals("Rr", StringUtils.camelize("rR"));
    assertEquals("Ss", StringUtils.camelize("sS"));
    assertEquals("Tt", StringUtils.camelize("tT"));
    assertEquals("Uu", StringUtils.camelize("uU"));
    assertEquals("Vv", StringUtils.camelize("vV"));
    assertEquals("Ww", StringUtils.camelize("wW"));
    assertEquals("Xx", StringUtils.camelize("xX"));
    assertEquals("Yy", StringUtils.camelize("yY"));
    assertEquals("Zz", StringUtils.camelize("zZ"));
  }
}
