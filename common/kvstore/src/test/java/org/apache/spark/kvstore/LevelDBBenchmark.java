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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

/**
 * A set of small benchmarks for the LevelDB implementation.
 *
 * The benchmarks are run over two different types (one with just a natural index, and one
 * with a ref index), over a set of 2^20 elements, and the following tests are performed:
 *
 * - write (then update) elements in sequential natural key order
 * - write (then update) elements in random natural key order
 * - iterate over natural index, ascending and descending
 * - iterate over ref index, ascending and descending
 */
@Ignore
public class LevelDBBenchmark {

  private static final int COUNT = 1024;
  private static final AtomicInteger IDGEN = new AtomicInteger();
  private static final MetricRegistry metrics = new MetricRegistry();
  private static final Timer dbCreation = metrics.timer("dbCreation");
  private static final Timer dbClose = metrics.timer("dbClose");

  private LevelDB db;
  private File dbpath;

  @Before
  public void setup() throws Exception {
    dbpath = File.createTempFile("test.", ".ldb");
    dbpath.delete();
    try(Timer.Context ctx = dbCreation.time()) {
      db = new LevelDB(dbpath);
    }
  }

  @After
  public void cleanup() throws Exception {
    if (db != null) {
      try(Timer.Context ctx = dbClose.time()) {
        db.close();
      }
    }
    if (dbpath != null) {
      FileUtils.deleteQuietly(dbpath);
    }
  }

  @AfterClass
  public static void report() {
    if (metrics.getTimers().isEmpty()) {
      return;
    }

    int headingPrefix = 0;
    for (Map.Entry<String, Timer> e : metrics.getTimers().entrySet()) {
      headingPrefix = Math.max(e.getKey().length(), headingPrefix);
    }
    headingPrefix += 4;

    StringBuilder heading = new StringBuilder();
    for (int i = 0; i < headingPrefix; i++) {
      heading.append(" ");
    }
    heading.append("\tcount");
    heading.append("\tmean");
    heading.append("\tmin");
    heading.append("\tmax");
    heading.append("\t95th");
    System.out.println(heading);

    for (Map.Entry<String, Timer> e : metrics.getTimers().entrySet()) {
      StringBuilder row = new StringBuilder();
      row.append(e.getKey());
      for (int i = 0; i < headingPrefix - e.getKey().length(); i++) {
        row.append(" ");
      }

      Snapshot s = e.getValue().getSnapshot();
      row.append("\t").append(e.getValue().getCount());
      row.append("\t").append(toMs(s.getMean()));
      row.append("\t").append(toMs(s.getMin()));
      row.append("\t").append(toMs(s.getMax()));
      row.append("\t").append(toMs(s.get95thPercentile()));

      System.out.println(row);
    }

    Slf4jReporter.forRegistry(metrics).outputTo(LoggerFactory.getLogger(LevelDBBenchmark.class))
      .build().report();
  }

  private static String toMs(double nanos) {
    return String.format("%.3f", nanos / 1000 / 1000);
  }

  @Test
  public void sequentialWritesNoIndex() throws Exception {
    List<SimpleType> entries = createSimpleType();
    writeAll(entries, false, "sequentialWritesNoIndex");
    writeAll(entries, false, "sequentialUpdatesNoIndex");
    deleteNoIndex(entries, false, "sequentialDeleteNoIndex");
  }

  @Test
  public void sequentialSyncWritesNoIndex() throws Exception {
    List<SimpleType> entries = createSimpleType();
    writeAll(entries, true, "sequentialSyncWritesNoIndex");
    writeAll(entries, true, "sequentialSyncUpdatesNoIndex");
    deleteNoIndex(entries, true, "sequentialSyncDeleteNoIndex");
  }

  @Test
  public void randomWritesNoIndex() throws Exception {
    List<SimpleType> entries = createSimpleType();

    Collections.shuffle(entries);
    writeAll(entries, false, "randomWritesNoIndex");

    Collections.shuffle(entries);
    writeAll(entries, false, "randomUpdatesNoIndex");

    Collections.shuffle(entries);
    deleteNoIndex(entries, false, "randomDeletesNoIndex");
  }

  @Test
  public void randomSyncWritesNoIndex() throws Exception {
    List<SimpleType> entries = createSimpleType();

    Collections.shuffle(entries);
    writeAll(entries, true, "randomSyncWritesNoIndex");

    Collections.shuffle(entries);
    writeAll(entries, true, "randomSyncUpdatesNoIndex");

    Collections.shuffle(entries);
    deleteNoIndex(entries, true, "randomSyncDeletesNoIndex");
  }

  @Test
  public void sequentialWritesIndexedType() throws Exception {
    List<IndexedType> entries = createIndexedType();
    writeAll(entries, false, "sequentialWritesIndexed");
    writeAll(entries, false, "sequentialUpdatesIndexed");
    deleteIndexed(entries, false, "sequentialDeleteIndexed");
  }

  @Test
  public void sequentialSyncWritesIndexedType() throws Exception {
    List<IndexedType> entries = createIndexedType();
    writeAll(entries, true, "sequentialSyncWritesIndexed");
    writeAll(entries, true, "sequentialSyncUpdatesIndexed");
    deleteIndexed(entries, true, "sequentialSyncDeleteIndexed");
  }

  @Test
  public void randomWritesIndexedTypeAndIteration() throws Exception {
    List<IndexedType> entries = createIndexedType();

    Collections.shuffle(entries);
    writeAll(entries, false, "randomWritesIndexed");

    Collections.shuffle(entries);
    writeAll(entries, false, "randomUpdatesIndexed");

    // Run iteration benchmarks here since we've gone through the trouble of writing all
    // the data already.
    KVStoreView<?> view = db.view(IndexedType.class);
    iterate(view, "naturalIndex");
    iterate(view.reverse(), "naturalIndexDescending");
    iterate(view.index("name"), "refIndex");
    iterate(view.index("name").reverse(), "refIndexDescending");

    Collections.shuffle(entries);
    deleteIndexed(entries, false, "randomDeleteIndexed");
  }

  @Test
  public void randomSyncWritesIndexedTypeAndIteration() throws Exception {
    List<IndexedType> entries = createIndexedType();

    Collections.shuffle(entries);
    writeAll(entries, true, "randomSyncWritesIndexed");

    Collections.shuffle(entries);
    deleteIndexed(entries, true, "randomSyncDeleteIndexed");
  }

  private void iterate(KVStoreView<?> view, String name) throws Exception {
    Timer create = metrics.timer(name + "CreateIterator");
    Timer iter = metrics.timer(name + "Iteration");
    KVStoreIterator<?> it = null;
    {
      // Create the iterator several times, just to have multiple data points.
      for (int i = 0; i < 1024; i++) {
        if (it != null) {
          it.close();
        }
        try(Timer.Context ctx = create.time()) {
          it = view.closeableIterator();
        }
      }
    }

    for (; it.hasNext(); ) {
      try(Timer.Context ctx = iter.time()) {
        it.next();
      }
    }
  }

  private void writeAll(List<?> entries, boolean sync, String timerName) throws Exception {
    Timer timer = newTimer(timerName);
    for (Object o : entries) {
      try(Timer.Context ctx = timer.time()) {
        db.write(o, sync);
      }
    }
  }

  private void deleteNoIndex(List<SimpleType> entries, boolean sync, String timerName)
      throws Exception {
    Timer delete = newTimer(timerName);
    for (SimpleType i : entries) {
      try(Timer.Context ctx = delete.time()) {
        db.delete(i.getClass(), i.key, sync);
      }
    }
  }

  private void deleteIndexed(List<IndexedType> entries, boolean sync, String timerName)
      throws Exception {
    Timer delete = newTimer(timerName);
    for (IndexedType i : entries) {
      try(Timer.Context ctx = delete.time()) {
        db.delete(i.getClass(), i.key, sync);
      }
    }
  }

  private List<SimpleType> createSimpleType() {
    List<SimpleType> entries = new ArrayList<>();
    for (int i = 0; i < COUNT; i++) {
      SimpleType t = new SimpleType();
      t.key = IDGEN.getAndIncrement();
      t.name = "name" + (t.key % 1024);
      entries.add(t);
    }
    return entries;
  }

  private List<IndexedType> createIndexedType() {
    List<IndexedType> entries = new ArrayList<>();
    for (int i = 0; i < COUNT; i++) {
      IndexedType t = new IndexedType();
      t.key = IDGEN.getAndIncrement();
      t.name = "name" + (t.key % 1024);
      entries.add(t);
    }
    return entries;
  }

  private Timer newTimer(String name) {
    assertNull("Timer already exists: " + name, metrics.getTimers().get(name));
    return metrics.timer(name);
  }

  public static class SimpleType {

    @KVIndex
    public int key;

    public String name;

  }

  public static class IndexedType {

    @KVIndex
    public int key;

    @KVIndex("name")
    public String name;

  }

}
