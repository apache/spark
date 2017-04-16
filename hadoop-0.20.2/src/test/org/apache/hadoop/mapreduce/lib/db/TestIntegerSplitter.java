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
package org.apache.hadoop.mapreduce.lib.db;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

public class TestIntegerSplitter extends TestCase {
  private long [] toLongArray(List<Long> in) {
    long [] out = new long[in.size()];
    for (int i = 0; i < in.size(); i++) {
      out[i] = in.get(i).longValue();
    }

    return out;
  }

  public String formatLongArray(long [] ar) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    boolean first = true;
    for (long val : ar) {
      if (!first) {
        sb.append(", ");
      }

      sb.append(Long.toString(val));
      first = false;
    }

    sb.append("]");
    return sb.toString();
  }

  public void assertLongArrayEquals(long [] expected, long [] actual) {
    for (int i = 0; i < expected.length; i++) {
      try {
        assertEquals("Failure at position " + i + "; got " + actual[i]
            + " instead of " + expected[i] + "; actual array is " + formatLongArray(actual),
            expected[i], actual[i]);
      } catch (ArrayIndexOutOfBoundsException oob) {
        fail("Expected array with " + expected.length + " elements; got " + actual.length
            + ". Actual array is " + formatLongArray(actual));
      }
    }

    if (actual.length > expected.length) {
      fail("Actual array has " + actual.length + " elements; expected " + expected.length
          + ". ACtual array is " + formatLongArray(actual));
    }
  }

  public void testEvenSplits() throws SQLException {
    List<Long> splits = new IntegerSplitter().split(10, 0, 100);
    long [] expected = { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
    assertLongArrayEquals(expected, toLongArray(splits));
  }

  public void testOddSplits() throws SQLException {
    List<Long> splits = new IntegerSplitter().split(10, 0, 95);
    long [] expected = { 0, 9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 95 };
    assertLongArrayEquals(expected, toLongArray(splits));

  }

  public void testSingletonSplit() throws SQLException {
    List<Long> splits = new IntegerSplitter().split(1, 5, 5);
    long [] expected = { 5, 5 };
    assertLongArrayEquals(expected, toLongArray(splits));
  }

  public void testSingletonSplit2() throws SQLException {
    // Same test, but overly-high numSplits
    List<Long> splits = new IntegerSplitter().split(5, 5, 5);
    long [] expected = { 5, 5 };
    assertLongArrayEquals(expected, toLongArray(splits));
  }

  public void testTooManySplits() throws SQLException {
    List<Long> splits = new IntegerSplitter().split(5, 3, 5);
    long [] expected = { 3, 4, 5 };
    assertLongArrayEquals(expected, toLongArray(splits));
  }

}

