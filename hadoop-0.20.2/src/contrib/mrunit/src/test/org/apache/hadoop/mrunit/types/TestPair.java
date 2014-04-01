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
package org.apache.hadoop.mrunit.types;

import junit.framework.TestCase;

import org.junit.Test;

public class TestPair extends TestCase {

  public static final int VAL_A = 3;
  public static final int VAL_B = 4;
  public static final int VAL_C = 65;
  public static final int VAL_D = 98;

  /**
   * Test method for
   * {@link
   * org.apache.hadoop.mrunit.types.Pair#equals(org.apache.hadoop.mrunit.types.Pair)}.
   */
  @Test
  public void testEqualsTrulyEqual() {
    assertTrue(
      new Pair<Integer, Integer>(Integer.valueOf(VAL_A), Integer.valueOf(VAL_B))
      .equals(new Pair<Integer, Integer>(
              Integer.valueOf(VAL_A), Integer.valueOf(VAL_B))));
  }

  @Test
  public void testEqualsInequalFirst() {
    assertFalse(new Pair<Integer, Integer>(
                Integer.valueOf(VAL_A), Integer.valueOf(VAL_B))
            .equals(new Pair<Integer, Integer>(Integer.valueOf(VAL_C),
                    Integer.valueOf(VAL_B))));
  }

  @Test
  public void testEqualsInequalSecond() {
    assertFalse(new Pair<Integer, Integer>(
                Integer.valueOf(VAL_A), Integer.valueOf(VAL_B))
            .equals(new Pair<Integer, Integer>(Integer.valueOf(VAL_A),
                    Integer.valueOf(VAL_D))));
  }

  @Test
  public void testEqualsNullNull() {
    assertTrue(new Pair<Integer, Integer>(null, null)
            .equals(new Pair<Integer, Integer>(null, null)));
  }

  @Test
  public void testEqualsNullIntFirst0() {
    assertTrue(new Pair<Integer, Integer>(Integer.valueOf(VAL_A), null)
            .equals(new Pair<Integer, Integer>(Integer.valueOf(VAL_A), null)));
  }

  @Test
  public void testEqualsNullIntFirst1() {
    assertFalse(new Pair<Integer, Integer>(null, null)
            .equals(new Pair<Integer, Integer>(Integer.valueOf(VAL_A), null)));
  }

  @Test
  public void testEqualsNullIntFirst2() {
    assertFalse(new Pair<Integer, Integer>(Integer.valueOf(VAL_A), null)
            .equals(new Pair<Integer, Integer>(null, null)));
  }

  @Test
  public void testEqualsNullIntSecond0() {
    assertTrue(new Pair<Integer, Integer>(null, Integer.valueOf(VAL_A))
            .equals(new Pair<Integer, Integer>(null, Integer.valueOf(VAL_A))));
  }

  @Test
  public void testEqualsNullIntSecond1() {
    assertFalse(new Pair<Integer, Integer>(null, null)
            .equals(new Pair<Integer, Integer>(null, Integer.valueOf(VAL_A))));
  }

  @Test
  public void testEqualsNullIntSecond2() {
    assertFalse(new Pair<Integer, Integer>(null, Integer.valueOf(VAL_A))
            .equals(new Pair<Integer, Integer>(null, null)));
  }

  /**
   * Test method for
   * {@link
   * org.apache.hadoop.mrunit.types.Pair#compareTo(org.apache.hadoop.mrunit.types.Pair)}.
   */
  @Test
  public void testCompareToTrulyEqual() {
    assertTrue(0 == new Pair<Integer, Integer>(Integer.valueOf(VAL_A),
                                               Integer.valueOf(VAL_B))
            .compareTo(new Pair<Integer, Integer>(Integer.valueOf(VAL_A),
                    Integer.valueOf(VAL_B))));
  }

  @Test
  public void testCompareToInequalFirst() {
    assertFalse(0 == new Pair<Integer, Integer>(Integer.valueOf(VAL_A),
            Integer.valueOf(VAL_B)).compareTo(
                new Pair<Integer, Integer>(Integer.valueOf(
                                           VAL_C), Integer.valueOf(VAL_B))));
  }

  @Test
  public void testCompareToInequalSecond() {
    assertFalse(0 == new Pair<Integer, Integer>(Integer.valueOf(VAL_A),
            Integer.valueOf(VAL_B)).compareTo(
            new Pair<Integer, Integer>(Integer.valueOf(VAL_A),
                                       Integer.valueOf(VAL_D))));
  }

  @Test
  public void testCompareToNullNull() {
    try {
      new Pair<Integer, Integer>(null, null)
              .compareTo(new Pair<Integer, Integer>(null, null));
      fail();
    } catch (NullPointerException npe) {
      // expected this; ok.
    }
  }

  @Test
  public void testCompareToNullIntFirst0() {
    try {
      new Pair<Integer, Integer>(Integer.valueOf(VAL_A), null)
          .compareTo(new Pair<Integer, Integer>(Integer.valueOf(VAL_A), null));
      fail();
    } catch (NullPointerException npe) {
      // expected this; ok.
    }
  }

  @Test
  public void testCompareToNullIntFirst1() {
    try {
      new Pair<Integer, Integer>(null, null)
          .compareTo(new Pair<Integer, Integer>(Integer.valueOf(VAL_A), null));
      fail();
    } catch (NullPointerException npe) {
      // expected this; ok.
    }
  }

  @Test
  public void testCompareToNullIntFirst2() {
    try {
      new Pair<Integer, Integer>(Integer.valueOf(VAL_A), null)
          .compareTo(new Pair<Integer, Integer>(null, null));
      fail();
    } catch (NullPointerException npe) {
      // expected this; ok.
    }
  }

  @Test
  public void testCompareToNullIntSecond0() {
    try {
      new Pair<Integer, Integer>(null, Integer.valueOf(VAL_A))
          .compareTo(new Pair<Integer, Integer>(null, Integer.valueOf(VAL_A)));
      fail();
    } catch (NullPointerException npe) {
      // expected this; ok.
    }
  }

  @Test
  public void testCompareToNullIntSecond1() {
    try {
      new Pair<Integer, Integer>(null, null)
          .compareTo(new Pair<Integer, Integer>(null, Integer.valueOf(VAL_A)));
      fail();
    } catch (NullPointerException npe) {
      // expected this; ok.
    }
  }

  @Test
  public void testCompareToNullIntSecond2() {
    try {
      new Pair<Integer, Integer>(null, Integer.valueOf(VAL_A))
          .compareTo(new Pair<Integer, Integer>(null, null));
      fail();
    } catch (NullPointerException npe) {
      // expected this; ok.
    }
  }

}
