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

package org.apache.spark.kvstore;

import java.io.File;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * This class should really be called "LevelDBIteratorSuite" but for some reason I don't know,
 * sbt does not run the tests if it has that name.
 */
public class DBIteratorSuite {

  private static final int MIN_ENTRIES = 42;
  private static final int MAX_ENTRIES = 1024;
  private static final Random RND = new Random();

  private static List<CustomType1> allEntries;
  private static List<CustomType1> clashingEntries;
  private static LevelDB db;
  private static File dbpath;

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

  private final BaseComparator NATURAL_ORDER = (t1, t2) -> t1.key.compareTo(t2.key);
  private final BaseComparator REF_INDEX_ORDER = (t1, t2) -> t1.id.compareTo(t2.id);
  private final BaseComparator COPY_INDEX_ORDER = (t1, t2) -> t1.name.compareTo(t2.name);
  private final BaseComparator NUMERIC_INDEX_ORDER = (t1, t2) -> t1.num - t2.num;

  @BeforeClass
  public static void setup() throws Exception {
    dbpath = File.createTempFile("test.", ".ldb");
    dbpath.delete();
    db = new LevelDB(dbpath);

    int count = RND.nextInt(MAX_ENTRIES) + MIN_ENTRIES;

    // Instead of generating sequential IDs, generate random unique IDs to avoid the insertion
    // order matching the natural ordering. Just in case.
    boolean[] usedIDs = new boolean[count];

    allEntries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      CustomType1 t = new CustomType1();

      int id;
      do {
        id = RND.nextInt(count);
      } while (usedIDs[id]);

      usedIDs[id] = true;
      t.key = "key" + id;
      t.id = "id" + i;
      t.name = "name" + RND.nextInt(MAX_ENTRIES);
      t.num = RND.nextInt(MAX_ENTRIES);
      allEntries.add(t);
      db.write(t);
    }

    // Pick the first generated value, and forcefully create a few entries that will clash
    // with the indexed values (id and name), to make sure the index behaves correctly when
    // multiple entities are indexed by the same value.
    //
    // This also serves as a test for the test code itself, to make sure it's sorting indices
    // the same way the store is expected to.
    CustomType1 first = allEntries.get(0);
    clashingEntries = new ArrayList<>();
    for (int i = 0; i < RND.nextInt(MIN_ENTRIES) + 1; i++) {
      CustomType1 t = new CustomType1();
      t.key = "n-key" + (count + i);
      t.id = first.id;
      t.name = first.name;
      t.num = first.num;
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
    allEntries.add(t);
    db.write(t);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    allEntries = null;
    if (db != null) {
      db.close();
    }
    if (dbpath != null) {
      FileUtils.deleteQuietly(dbpath);
    }
  }

  @Test
  public void naturalIndex() throws Exception {
    testIteration(NATURAL_ORDER, view(), null);
  }

  @Test
  public void refIndex() throws Exception {
    testIteration(REF_INDEX_ORDER, view().index("id"), null);
  }

  @Test
  public void copyIndex() throws Exception {
    testIteration(COPY_INDEX_ORDER, view().index("name"), null);
  }

  @Test
  public void numericIndex() throws Exception {
    testIteration(NUMERIC_INDEX_ORDER, view().index("int"), null);
  }

  @Test
  public void naturalIndexDescending() throws Exception {
    testIteration(NATURAL_ORDER, view().reverse(), null);
  }

  @Test
  public void refIndexDescending() throws Exception {
    testIteration(REF_INDEX_ORDER, view().index("id").reverse(), null);
  }

  @Test
  public void copyIndexDescending() throws Exception {
    testIteration(COPY_INDEX_ORDER, view().index("name").reverse(), null);
  }

  @Test
  public void numericIndexDescending() throws Exception {
    testIteration(NUMERIC_INDEX_ORDER, view().index("int").reverse(), null);
  }

  @Test
  public void naturalIndexWithStart() throws Exception {
    CustomType1 first = pickFirst();
    testIteration(NATURAL_ORDER, view().first(first.key), first);
  }

  @Test
  public void refIndexWithStart() throws Exception {
    CustomType1 first = pickFirst();
    testIteration(REF_INDEX_ORDER, view().index("id").first(first.id), first);
  }

  @Test
  public void copyIndexWithStart() throws Exception {
    CustomType1 first = pickFirst();
    testIteration(COPY_INDEX_ORDER, view().index("name").first(first.name), first);
  }

  @Test
  public void numericIndexWithStart() throws Exception {
    CustomType1 first = pickFirst();
    testIteration(NUMERIC_INDEX_ORDER, view().index("int").first(first.num), first);
  }

  @Test
  public void naturalIndexDescendingWithStart() throws Exception {
    CustomType1 first = pickFirst();
    testIteration(NATURAL_ORDER, view().reverse().first(first.key), first);
  }

  @Test
  public void refIndexDescendingWithStart() throws Exception {
    CustomType1 first = pickFirst();
    testIteration(REF_INDEX_ORDER, view().reverse().index("id").first(first.id), first);
  }

  @Test
  public void copyIndexDescendingWithStart() throws Exception {
    CustomType1 first = pickFirst();
    testIteration(COPY_INDEX_ORDER, view().reverse().index("name").first(first.name),
      first);
  }

  @Test
  public void numericIndexDescendingWithStart() throws Exception {
    CustomType1 first = pickFirst();
    testIteration(NUMERIC_INDEX_ORDER, view().reverse().index("int").first(first.num),
      first);
  }

  @Test
  public void naturalIndexWithSkip() throws Exception {
    testIteration(NATURAL_ORDER, view().skip(RND.nextInt(allEntries.size() / 2)), null);
  }

  @Test
  public void refIndexWithSkip() throws Exception {
    testIteration(REF_INDEX_ORDER, view().index("id").skip(RND.nextInt(allEntries.size() / 2)),
      null);
  }

  @Test
  public void copyIndexWithSkip() throws Exception {
    testIteration(COPY_INDEX_ORDER, view().index("name").skip(RND.nextInt(allEntries.size() / 2)),
      null);
  }

  @Test
  public void testRefWithIntNaturalKey() throws Exception {
    LevelDBSuite.IntKeyType i = new LevelDBSuite.IntKeyType();
    i.key = 1;
    i.id = "1";
    i.values = Arrays.asList("1");

    db.write(i);

    try(KVStoreIterator<?> it = db.view(i.getClass()).closeableIterator()) {
      Object read = it.next();
      assertEquals(i, read);
    }
  }

  private CustomType1 pickFirst() {
    // Picks a first element that has clashes with other elements in the given index.
    return clashingEntries.get(RND.nextInt(clashingEntries.size()));
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
      final CustomType1 first) throws Exception {
    List<CustomType1> indexOrder = sortBy(order.fallback());
    if (!params.ascending) {
      indexOrder = Lists.reverse(indexOrder);
    }

    Iterable<CustomType1> expected = indexOrder;
    if (first != null) {
      final BaseComparator expectedOrder = params.ascending ? order : order.reverse();
      expected = Iterables.filter(expected, v -> expectedOrder.compare(first, v) <= 0);
    }

    if (params.skip > 0) {
      expected = Iterables.skip(expected, (int) params.skip);
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
    return db.view(CustomType1.class);
  }

  private List<CustomType1> collect(KVStoreView<CustomType1> view) throws Exception {
    return Arrays.asList(Iterables.toArray(view, CustomType1.class));
  }

  private List<CustomType1> sortBy(Comparator<CustomType1> comp) {
    List<CustomType1> copy = new ArrayList<>(allEntries);
    Collections.sort(copy, comp);
    return copy;
  }

}
