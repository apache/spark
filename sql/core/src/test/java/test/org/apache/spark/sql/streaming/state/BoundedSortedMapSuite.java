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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.streaming.state.BoundedSortedMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class BoundedSortedMapSuite {

  @Test
  public void testAddElementBelowBoundedCount() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 2);

    map.put(2, 2);
    map.put(1, 1);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(1, 1), IntPair.of(2, 2));
    assertMap(map, expected);
  }

  @Test
  public void testAddSmallestElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 2);

    map.put(2, 2);
    map.put(1, 1);

    map.put(0, 0);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(0, 0), IntPair.of(1, 1));
    assertMap(map, expected);
  }

  @Test
  public void testAddMiddleOfElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 2);

    map.put(3, 3);
    map.put(1, 1);

    // 3 is being cut off
    map.put(2, 2);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(1, 1), IntPair.of(2, 2));
    assertMap(map, expected);
  }

  @Test
  public void testAddBiggestOfElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 2);

    map.put(2, 2);
    map.put(1, 1);

    // 3 is not added
    map.put(3, 3);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(1, 1), IntPair.of(2, 2));
    assertMap(map, expected);
  }

  @Test
  public void testOverwriteBiggestOfElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 2);

    map.put(2, 2);
    map.put(1, 1);

    map.put(2, 3);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(1, 1), IntPair.of(2, 3));
    assertMap(map, expected);
  }

  @Test
  public void testReversedOrderAddElementBelowBoundedCount() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 2);

    map.put(2, 2);
    map.put(1, 1);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(2, 2), IntPair.of(1, 1));
    assertMap(map, expected);
  }

  @Test
  public void testReversedOrderAddBiggestElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 2);

    map.put(2, 2);
    map.put(1, 1);

    // 1 is being cut off
    map.put(3, 3);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(3, 3), IntPair.of(2, 2));
    assertMap(map, expected);
  }

  @Test
  public void testReversedOrderAddMiddleOfElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 2);

    map.put(3, 3);
    map.put(1, 1);

    // 1 is being cut off
    map.put(2, 2);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(3, 3), IntPair.of(2, 2));
    assertMap(map, expected);
  }

  @Test
  public void testReversedOrderAddSmallestOfElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 2);

    map.put(3, 3);
    map.put(2, 2);

    // 1 is not added
    map.put(1, 1);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(3, 3), IntPair.of(2, 2));
    assertMap(map, expected);
  }

  @Test
  public void testReversedOrderOverwriteSmallestOfElementToFullOfMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 2);

    map.put(3, 3);
    map.put(2, 2);

    map.put(2, 3);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(3, 3), IntPair.of(2, 3));
    assertMap(map, expected);
  }

  @Test
  public void testPutAllNonSortedMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 3);

    map.put(1, 1);
    map.put(2, 2);

    Map<Integer, Integer> param = new HashMap<>();
    param.put(2, 12);
    param.put(1, 11);
    param.put(3, 13);
    param.put(4, 14);

    map.putAll(param);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(1, 11), IntPair.of(2, 12),
        IntPair.of(3, 13));
    assertMap(map, expected);
  }

  @Test
  public void testPutAllSortedMapWithSameOrderingAllKeysAreSmallerThanFirstKey() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 3);

    map.put(7, 7);
    map.put(8, 8);

    SortedMap<Integer, Integer> param = new TreeMap<>(Comparator.naturalOrder());
    param.put(1, 11);
    param.put(2, 12);
    param.put(3, 13);
    param.put(4, 14);

    map.putAll(param);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(1, 11), IntPair.of(2, 12),
        IntPair.of(3, 13));
    assertMap(map, expected);
  }

  @Test
  public void testPutAllSortedMapWithSameOrderingAllKeysAreBiggerThanLastKey() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 3);

    map.put(1, 1);
    map.put(2, 2);

    SortedMap<Integer, Integer> param = new TreeMap<>(Comparator.naturalOrder());
    param.put(3, 13);
    param.put(4, 14);
    param.put(5, 15);
    param.put(6, 16);

    map.putAll(param);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(1, 1), IntPair.of(2, 2),
        IntPair.of(3, 13));
    assertMap(map, expected);
  }

  @Test
  public void testPutAllSortedMapWithSameOrderingOverlappedKeyRange() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 4);

    map.put(1, 1);
    map.put(3, 3);

    SortedMap<Integer, Integer> param = new TreeMap<>(Comparator.naturalOrder());
    param.put(2, 12);
    param.put(4, 14);
    param.put(6, 16);

    map.putAll(param);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(1, 1), IntPair.of(2, 12),
        IntPair.of(3, 3), IntPair.of(4, 14));
    assertMap(map, expected);
  }

  @Test
  public void testPutAllSortedMapWithDifferentOrdering() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.naturalOrder(), 4);

    map.put(1, 1);
    map.put(3, 3);

    SortedMap<Integer, Integer> param = new TreeMap<>(Comparator.reverseOrder());
    param.put(6, 16);
    param.put(4, 14);
    param.put(2, 12);

    map.putAll(param);

    // respecting map's ordering
    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(1, 1), IntPair.of(2, 12),
        IntPair.of(3, 3), IntPair.of(4, 14));
    assertMap(map, expected);
  }

  @Test
  public void testReversedOrderPutAllNonSortedMap() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 3);

    map.put(2, 2);
    map.put(1, 1);

    Map<Integer, Integer> param = new HashMap<>();
    param.put(2, 12);
    param.put(1, 11);
    param.put(3, 13);
    param.put(4, 14);

    map.putAll(param);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(4, 14), IntPair.of(3, 13),
        IntPair.of(2, 12));
    assertMap(map, expected);
  }

  @Test
  public void testReversedOrderPutAllSortedMapWithSameOrderingAllKeysAreSmallerThanLastKey() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 3);

    map.put(9, 9);
    map.put(8, 8);
    map.put(7, 7);

    SortedMap<Integer, Integer> param = new TreeMap<>(Comparator.reverseOrder());
    param.put(1, 11);
    param.put(2, 12);
    param.put(3, 13);
    param.put(4, 14);

    map.putAll(param);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(9, 9), IntPair.of(8, 8),
        IntPair.of(7, 7));
    assertMap(map, expected);
  }

  @Test
  public void testReversedOrderPutAllSortedMapWithSameOrderingAllKeysAreBiggerThanFirstKey() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 3);

    map.put(2, 2);
    map.put(1, 1);

    SortedMap<Integer, Integer> param = new TreeMap<>(Comparator.reverseOrder());
    param.put(3, 13);
    param.put(4, 14);
    param.put(5, 15);
    param.put(6, 16);

    map.putAll(param);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(6, 16), IntPair.of(5, 15),
        IntPair.of(4, 14));
    assertMap(map, expected);
  }

  @Test
  public void testReversedOrderPutAllSortedMapWithSameOrderingOverlappedKeyRange() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 4);

    map.put(3, 3);
    map.put(1, 1);

    SortedMap<Integer, Integer> param = new TreeMap<>(Comparator.reverseOrder());
    param.put(6, 16);
    param.put(4, 14);
    param.put(2, 12);

    map.putAll(param);

    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(6, 16), IntPair.of(4, 14),
        IntPair.of(3, 3), IntPair.of(2, 12));
    assertMap(map, expected);
  }

  @Test
  public void testReversedOrderPutAllSortedMapWithDifferentOrdering() {
    BoundedSortedMap<Integer, Integer> map = new BoundedSortedMap<Integer, Integer>(
        Comparator.reverseOrder(), 4);

    map.put(3, 3);
    map.put(1, 1);

    SortedMap<Integer, Integer> param = new TreeMap<>(Comparator.naturalOrder());
    param.put(2, 12);
    param.put(4, 14);
    param.put(6, 16);

    map.putAll(param);

    // respecting map's ordering
    List<Pair<Integer, Integer>> expected = Lists.newArrayList(IntPair.of(6, 16), IntPair.of(4, 14),
        IntPair.of(3, 3), IntPair.of(2, 12));
    assertMap(map, expected);
  }

  private void assertMap(Map<Integer, Integer> map, List<Pair<Integer, Integer>> expectedEntities) {
    Assert.assertEquals(expectedEntities.size(), map.size());
    Assert.assertEquals(expectedEntities.stream().map(Pair::getKey).collect(Collectors.toSet()),
        map.keySet());

    expectedEntities.forEach(entity -> Assert.assertEquals(entity.getValue(),
        map.get(entity.getKey())));
  }

  private static class IntPair {
    static ImmutablePair of(Integer key, Integer value) {
      return new ImmutablePair<>(key, value);
    }
  }
}
