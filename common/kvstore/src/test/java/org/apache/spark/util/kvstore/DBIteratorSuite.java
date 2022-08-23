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

package org.apache.spark.util.kvstore;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

public abstract class DBIteratorSuite {

  private static final Logger LOG = LoggerFactory.getLogger(DBIteratorSuite.class);

  private static final int MIN_ENTRIES = 42;
  private static final int MAX_ENTRIES = 1024;
  private static final Random RND = new Random();

  private static List<CustomType1> allEntries;
  private static List<CustomType1> clashingEntries;
  private static KVStore db;

  private interface BaseComparator extends Comparator<CustomType1> {
    /**
     * Returns a comparator that falls back to natural order if this comparator's ordering
     * returns equality for two elements. Used to mimic how the index sorts things internally.
     */
    default BaseComparator fallback() {
      return (t1, t2) -> {
        int result = BaseComparator.this.compare(t1, t2);
        if (result != 0) {
          return result;
        }

        return t1.key.compareTo(t2.key);
      };
    }

    /** Reverses the order of this comparator. */
    default BaseComparator reverse() {
      return (t1, t2) -> -BaseComparator.this.compare(t1, t2);
    }
  }

  private static final BaseComparator NATURAL_ORDER = (t1, t2) -> t1.key.compareTo(t2.key);
  private static final BaseComparator REF_INDEX_ORDER = (t1, t2) -> t1.id.compareTo(t2.id);
  private static final BaseComparator COPY_INDEX_ORDER = (t1, t2) -> t1.name.compareTo(t2.name);
  private static final BaseComparator NUMERIC_INDEX_ORDER =
      (t1, t2) -> Integer.compare(t1.num, t2.num);
  private static final BaseComparator CHILD_INDEX_ORDER = (t1, t2) -> t1.child.compareTo(t2.child);

  /**
   * Implementations should override this method; it is called only once, before all tests are
   * run. Any state can be safely stored in static variables and cleaned up in a @AfterClass
   * handler.
   */
  protected abstract KVStore createStore() throws Exception;

  @BeforeClass
  public static void setupClass() {
    long seed = RND.nextLong();
    LOG.info("Random seed: {}", seed);
    RND.setSeed(seed);
  }

  @AfterClass
  public static void cleanupData() throws Exception {
    allEntries = null;
    db = null;
  }

  @Before
  public void setup() throws Exception {
    if (db != null) {
      return;
    }

    db = createStore();

    int count = RND.nextInt(MAX_ENTRIES) + MIN_ENTRIES;

    allEntries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      CustomType1 t = new CustomType1();
      t.key = "key" + i;
      t.id = "id" + i;
      t.name = "name" + RND.nextInt(MAX_ENTRIES);
      // Force one item to have an integer value of zero to test the fix for SPARK-23103.
      t.num = (i != 0) ? (int) RND.nextLong() : 0;
      t.child = "child" + (i % MIN_ENTRIES);
      allEntries.add(t);
    }

    // Shuffle the entries to avoid the insertion order matching the natural ordering. Just in case.
    Collections.shuffle(allEntries, RND);
    for (CustomType1 e : allEntries) {
      db.write(e);
    }

    // Pick the first generated value, and forcefully create a few entries that will clash
    // with the indexed values (id and name), to make sure the index behaves correctly when
    // multiple entities are indexed by the same value.
    //
    // This also serves as a test for the test code itself, to make sure it's sorting indices
    // the same way the store is expected to.
    CustomType1 first = allEntries.get(0);
    clashingEntries = new ArrayList<>();

    int clashCount = RND.nextInt(MIN_ENTRIES) + 1;
    for (int i = 0; i < clashCount; i++) {
      CustomType1 t = new CustomType1();
      t.key = "n-key" + (count + i);
      t.id = first.id;
      t.name = first.name;
      t.num = first.num;
      t.child = first.child;
      allEntries.add(t);
      clashingEntries.add(t);
      db.write(t);
    }

    // Create another entry that could cause problems: take the first entry, and make its indexed
    // name be an extension of the existing ones, to make sure the implementation sorts these
    // correctly even considering the separator character (shorter strings first).
    CustomType1 t = new CustomType1();
    t.key = "extended-key-0";
    t.id = first.id;
    t.name = first.name + "a";
    t.num = first.num;
    t.child = first.child;
    allEntries.add(t);
    db.write(t);
  }

  @Test
  public void naturalIndex() throws Exception {
    testIteration(NATURAL_ORDER, view(), null, null);
  }

  @Test
  public void refIndex() throws Exception {
    testIteration(REF_INDEX_ORDER, view().index("id"), null, null);
  }

  @Test
  public void copyIndex() throws Exception {
    testIteration(COPY_INDEX_ORDER, view().index("name"), null, null);
  }

  @Test
  public void numericIndex() throws Exception {
    testIteration(NUMERIC_INDEX_ORDER, view().index("int"), null, null);
  }

  @Test
  public void childIndex() throws Exception {
    CustomType1 any = pickLimit();
    testIteration(CHILD_INDEX_ORDER, view().index("child").parent(any.id), null, null);
  }

  @Test
  public void naturalIndexDescending() throws Exception {
    testIteration(NATURAL_ORDER, view().reverse(), null, null);
  }

  @Test
  public void refIndexDescending() throws Exception {
    testIteration(REF_INDEX_ORDER, view().index("id").reverse(), null, null);
  }

  @Test
  public void copyIndexDescending() throws Exception {
    testIteration(COPY_INDEX_ORDER, view().index("name").reverse(), null, null);
  }

  @Test
  public void numericIndexDescending() throws Exception {
    testIteration(NUMERIC_INDEX_ORDER, view().index("int").reverse(), null, null);
  }

  @Test
  public void childIndexDescending() throws Exception {
    CustomType1 any = pickLimit();
    testIteration(CHILD_INDEX_ORDER, view().index("child").parent(any.id).reverse(), null, null);
  }

  @Test
  public void naturalIndexWithStart() throws Exception {
    CustomType1 first = pickLimit();
    testIteration(NATURAL_ORDER, view().first(first.key), first, null);
  }

  @Test
  public void refIndexWithStart() throws Exception {
    CustomType1 first = pickLimit();
    testIteration(REF_INDEX_ORDER, view().index("id").first(first.id), first, null);
  }

  @Test
  public void copyIndexWithStart() throws Exception {
    CustomType1 first = pickLimit();
    testIteration(COPY_INDEX_ORDER, view().index("name").first(first.name), first, null);
  }

  @Test
  public void numericIndexWithStart() throws Exception {
    CustomType1 first = pickLimit();
    testIteration(NUMERIC_INDEX_ORDER, view().index("int").first(first.num), first, null);
  }

  @Test
  public void childIndexWithStart() throws Exception {
    CustomType1 any = pickLimit();
    testIteration(CHILD_INDEX_ORDER, view().index("child").parent(any.id).first(any.child), null,
      null);
  }

  @Test
  public void naturalIndexDescendingWithStart() throws Exception {
    CustomType1 first = pickLimit();
    testIteration(NATURAL_ORDER, view().reverse().first(first.key), first, null);
  }

  @Test
  public void refIndexDescendingWithStart() throws Exception {
    CustomType1 first = pickLimit();
    testIteration(REF_INDEX_ORDER, view().reverse().index("id").first(first.id), first, null);
  }

  @Test
  public void copyIndexDescendingWithStart() throws Exception {
    CustomType1 first = pickLimit();
    testIteration(COPY_INDEX_ORDER, view().reverse().index("name").first(first.name), first, null);
  }

  @Test
  public void numericIndexDescendingWithStart() throws Exception {
    CustomType1 first = pickLimit();
    testIteration(NUMERIC_INDEX_ORDER, view().reverse().index("int").first(first.num), first, null);
  }

  @Test
  public void childIndexDescendingWithStart() throws Exception {
    CustomType1 any = pickLimit();
    testIteration(CHILD_INDEX_ORDER,
      view().index("child").parent(any.id).first(any.child).reverse(), null, null);
  }

  @Test
  public void naturalIndexWithSkip() throws Exception {
    testIteration(NATURAL_ORDER, view().skip(pickCount()), null, null);
  }

  @Test
  public void refIndexWithSkip() throws Exception {
    testIteration(REF_INDEX_ORDER, view().index("id").skip(pickCount()), null, null);
  }

  @Test
  public void copyIndexWithSkip() throws Exception {
    testIteration(COPY_INDEX_ORDER, view().index("name").skip(pickCount()), null, null);
  }

  @Test
  public void childIndexWithSkip() throws Exception {
    CustomType1 any = pickLimit();
    testIteration(CHILD_INDEX_ORDER, view().index("child").parent(any.id).skip(pickCount()),
      null, null);
  }

  @Test
  public void naturalIndexWithMax() throws Exception {
    testIteration(NATURAL_ORDER, view().max(pickCount()), null, null);
  }

  @Test
  public void copyIndexWithMax() throws Exception {
    testIteration(COPY_INDEX_ORDER, view().index("name").max(pickCount()), null, null);
  }

  @Test
  public void childIndexWithMax() throws Exception {
    CustomType1 any = pickLimit();
    testIteration(CHILD_INDEX_ORDER, view().index("child").parent(any.id).max(pickCount()), null,
      null);
  }

  @Test
  public void naturalIndexWithLast() throws Exception {
    CustomType1 last = pickLimit();
    testIteration(NATURAL_ORDER, view().last(last.key), null, last);
  }

  @Test
  public void refIndexWithLast() throws Exception {
    CustomType1 last = pickLimit();
    testIteration(REF_INDEX_ORDER, view().index("id").last(last.id), null, last);
  }

  @Test
  public void copyIndexWithLast() throws Exception {
    CustomType1 last = pickLimit();
    testIteration(COPY_INDEX_ORDER, view().index("name").last(last.name), null, last);
  }

  @Test
  public void numericIndexWithLast() throws Exception {
    CustomType1 last = pickLimit();
    testIteration(NUMERIC_INDEX_ORDER, view().index("int").last(last.num), null, last);
  }

  @Test
  public void childIndexWithLast() throws Exception {
    CustomType1 any = pickLimit();
    testIteration(CHILD_INDEX_ORDER, view().index("child").parent(any.id).last(any.child), null,
      null);
  }

  @Test
  public void naturalIndexDescendingWithLast() throws Exception {
    CustomType1 last = pickLimit();
    testIteration(NATURAL_ORDER, view().reverse().last(last.key), null, last);
  }

  @Test
  public void refIndexDescendingWithLast() throws Exception {
    CustomType1 last = pickLimit();
    testIteration(REF_INDEX_ORDER, view().reverse().index("id").last(last.id), null, last);
  }

  @Test
  public void copyIndexDescendingWithLast() throws Exception {
    CustomType1 last = pickLimit();
    testIteration(COPY_INDEX_ORDER, view().reverse().index("name").last(last.name),
      null, last);
  }

  @Test
  public void numericIndexDescendingWithLast() throws Exception {
    CustomType1 last = pickLimit();
    testIteration(NUMERIC_INDEX_ORDER, view().reverse().index("int").last(last.num),
      null, last);
   }

  @Test
  public void childIndexDescendingWithLast() throws Exception {
    CustomType1 any = pickLimit();
    testIteration(CHILD_INDEX_ORDER, view().index("child").parent(any.id).last(any.child).reverse(),
      null, null);
  }

  @Test
  public void testRefWithIntNaturalKey() throws Exception {
    IntKeyType i = new IntKeyType();
    i.key = 1;
    i.id = "1";
    i.values = Arrays.asList("1");

    db.write(i);

    try(KVStoreIterator<?> it = db.view(i.getClass()).closeableIterator()) {
      Object read = it.next();
      assertEquals(i, read);
    }
  }

  private CustomType1 pickLimit() {
    // Picks an element that has clashes with other elements in the given index.
    return clashingEntries.get(RND.nextInt(clashingEntries.size()));
  }

  private int pickCount() {
    int count = RND.nextInt(allEntries.size() / 2);
    return Math.max(count, 1);
  }

  /**
   * Compares the two values and falls back to comparing the natural key of CustomType1
   * if they're the same, to mimic the behavior of the indexing code.
   */
  private <T extends Comparable<T>> int compareWithFallback(
      T v1,
      T v2,
      CustomType1 ct1,
      CustomType1 ct2) {
    int result = v1.compareTo(v2);
    if (result != 0) {
      return result;
    }

    return ct1.key.compareTo(ct2.key);
  }

  private void testIteration(
      final BaseComparator order,
      final KVStoreView<CustomType1> params,
      final CustomType1 first,
      final CustomType1 last) throws Exception {
    List<CustomType1> indexOrder = sortBy(order.fallback());
    if (!params.ascending) {
      indexOrder = Lists.reverse(indexOrder);
    }

    Iterable<CustomType1> expected = indexOrder;
    BaseComparator expectedOrder = params.ascending ? order : order.reverse();

    if (params.parent != null) {
      expected = Iterables.filter(expected, v -> params.parent.equals(v.id));
    }

    if (first != null) {
      expected = Iterables.filter(expected, v -> expectedOrder.compare(first, v) <= 0);
    }

    if (last != null) {
      expected = Iterables.filter(expected, v -> expectedOrder.compare(v, last) <= 0);
    }

    if (params.skip > 0) {
      expected = Iterables.skip(expected, (int) params.skip);
    }

    if (params.max != Long.MAX_VALUE) {
      expected = Iterables.limit(expected, (int) params.max);
    }

    List<CustomType1> actual = collect(params);
    compareLists(expected, actual);
  }

  /** Could use assertEquals(), but that creates hard to read errors for large lists. */
  private void compareLists(Iterable<?> expected, List<?> actual) {
    Iterator<?> expectedIt = expected.iterator();
    Iterator<?> actualIt = actual.iterator();

    int count = 0;
    while (expectedIt.hasNext()) {
      if (!actualIt.hasNext()) {
        break;
      }
      count++;
      assertEquals(expectedIt.next(), actualIt.next());
    }

    String message;
    Object[] remaining;
    int expectedCount = count;
    int actualCount = count;

    if (expectedIt.hasNext()) {
      remaining = Iterators.toArray(expectedIt, Object.class);
      expectedCount += remaining.length;
      message = "missing";
    } else {
      remaining = Iterators.toArray(actualIt, Object.class);
      actualCount += remaining.length;
      message = "stray";
    }

    assertEquals(String.format("Found %s elements: %s", message, Arrays.asList(remaining)),
      expectedCount, actualCount);
  }

  private KVStoreView<CustomType1> view() throws Exception {
    // SPARK-38896: this `view` will be closed in
    // the `collect(KVStoreView<CustomType1> view)` method.
    return db.view(CustomType1.class);
  }

  private List<CustomType1> collect(KVStoreView<CustomType1> view) throws Exception {
    try (KVStoreIterator<CustomType1> iterator = view.closeableIterator()) {
      return Lists.newArrayList(iterator);
    }
  }

  private List<CustomType1> sortBy(Comparator<CustomType1> comp) {
    List<CustomType1> copy = new ArrayList<>(allEntries);
    Collections.sort(copy, comp);
    return copy;
  }

}
