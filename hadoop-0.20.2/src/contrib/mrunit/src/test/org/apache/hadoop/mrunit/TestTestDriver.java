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
package org.apache.hadoop.mrunit;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

public class TestTestDriver extends TestCase {

  /**
   * Test method for
   * {@link org.apache.hadoop.mrunit.TestDriver#parseTabbedPair(java.lang.String)}.
   */
  @Test
  public void testParseTabbedPair1() {
    Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\tbar");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "bar");
  }

  @Test
  public void testParseTabbedPair2() {
    Pair<Text, Text> pr = TestDriver.parseTabbedPair("   foo\tbar");
    assertEquals(pr.getFirst().toString(), "   foo");
    assertEquals(pr.getSecond().toString(), "bar");
  }

  @Test
  public void testParseTabbedPair3() {
    Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\tbar   ");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "bar   ");
  }

  @Test
  public void testParseTabbedPair4() {
    Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo    \tbar");
    assertEquals(pr.getFirst().toString(), "foo    ");
    assertEquals(pr.getSecond().toString(), "bar");
  }

  @Test
  public void testParseTabbedPair5() {
    Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\t  bar");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "  bar");
  }

  @Test
  public void testParseTabbedPair6() {
    Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\t\tbar");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "\tbar");
  }

  @Test
  public void testParseTabbedPair7() {
    Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\tbar\n");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "bar\n");
  }

  @Test
  public void testParseTabbedPair8() {
    Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\t  bar\tbaz");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "  bar\tbaz");
  }

  /**
   * Test method for
   * {@link
   * org.apache.hadoop.mrunit.TestDriver#parseCommaDelimitedList(java.lang.String)}.
   */
  @Test
  public void testParseCommaDelimList1() {
    List<Text> out = TestDriver.parseCommaDelimitedList("foo");
    ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList2() {
    List<Text> out = TestDriver.parseCommaDelimitedList("foo,bar");
    ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList3() {
    List<Text> out = TestDriver.parseCommaDelimitedList("foo   ,bar");
    ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList4() {
    List<Text> out = TestDriver.parseCommaDelimitedList("foo,   bar");
    ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList5() {
    List<Text> out = TestDriver.parseCommaDelimitedList("   foo,bar");
    ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList6() {
    List<Text> out = TestDriver.parseCommaDelimitedList("foo,bar   ");
    ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList7() {
    List<Text> out = TestDriver.parseCommaDelimitedList("foo,bar, baz");
    ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    verify.add(new Text("baz"));
    assertListEquals(out, verify);
  }

  // note: we decide that correct behavior is that this does *not*
  // add a tailing empty element by itself.
  @Test
  public void testParseCommaDelimList8() {
    List<Text> out = TestDriver.parseCommaDelimitedList("foo,bar,");
    ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  // but this one does.
  @Test
  public void testParseCommaDelimList8a() {
    List<Text> out = TestDriver.parseCommaDelimitedList("foo,bar,,");
    ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    verify.add(new Text(""));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList9() {
    List<Text> out = TestDriver.parseCommaDelimitedList("foo,,bar");
    ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text(""));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList10() {
    List<Text> out = TestDriver.parseCommaDelimitedList(",foo,bar");
    ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text(""));
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

}

