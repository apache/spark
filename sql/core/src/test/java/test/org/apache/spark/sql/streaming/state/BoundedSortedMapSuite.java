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
package test.org.apache.spark.sql.streaming.state;

import org.apache.spark.sql.streaming.state.BoundedSortedMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

public class BoundedSortedMapSuite {

  @Test
  public void testAddElementBelowBoundedCount() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 2);

    map.put(2, 2);
    map.put(1, 1);

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(Integer.valueOf(1), map.firstKey());
    Assert.assertEquals(Integer.valueOf(2), map.lastKey());
  }

  @Test
  public void testAddSmallestElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 2);

    map.put(2, 2);
    map.put(1, 1);

    map.put(0, 0);

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(Integer.valueOf(0), map.firstKey());
    Assert.assertEquals(Integer.valueOf(1), map.lastKey());
  }

  @Test
  public void testAddMiddleOfElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 2);

    map.put(3, 3);
    map.put(1, 1);

    // 3 is being cut off
    map.put(2, 2);

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(Integer.valueOf(1), map.firstKey());
    Assert.assertEquals(Integer.valueOf(2), map.lastKey());
  }

  @Test
  public void testAddBiggestOfElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 2);

    map.put(2, 2);
    map.put(1, 1);

    // 3 is not added
    Assert.assertNull(map.put(3, 3));

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(Integer.valueOf(1), map.firstKey());
    Assert.assertEquals(Integer.valueOf(2), map.lastKey());
  }

  @Test
  public void testReversedOrderAddElementBelowBoundedCount() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 2);

    map.put(2, 2);
    map.put(1, 1);

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(Integer.valueOf(2), map.firstKey());
    Assert.assertEquals(Integer.valueOf(1), map.lastKey());
  }

  @Test
  public void testReversedOrderAddBiggestElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 2);

    map.put(2, 2);
    map.put(1, 1);

    // 1 is being cut off
    map.put(3, 3);

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(Integer.valueOf(3), map.firstKey());
    Assert.assertEquals(Integer.valueOf(2), map.lastKey());
  }

  @Test
  public void testReversedOrderAddMiddleOfElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 2);

    map.put(3, 3);
    map.put(1, 1);

    // 1 is being cut off
    map.put(2, 2);

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(Integer.valueOf(3), map.firstKey());
    Assert.assertEquals(Integer.valueOf(2), map.lastKey());
  }

  @Test
  public void testReversedOrderAddSmallestOfElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 2);

    map.put(3, 3);
    map.put(2, 2);

    // 1 is not added
    Assert.assertNull(map.put(1, 1));

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(Integer.valueOf(3), map.firstKey());
    Assert.assertEquals(Integer.valueOf(2), map.lastKey());
  }
}
