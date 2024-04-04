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

package test.org.apache.spark.sql;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.streaming.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import com.google.common.base.Objects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.RowFactory.create;
import org.apache.spark.sql.catalyst.encoders.OuterScopes;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class JavaDatasetSuite implements Serializable {
  private transient TestSparkSession spark;
  private transient JavaSparkContext jsc;

  @BeforeEach
  public void setUp() {
    // Trigger static initializer of TestData
    spark = new TestSparkSession();
    jsc = new JavaSparkContext(spark.sparkContext());
    spark.loadTestData();
  }

  @AfterEach
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  private <T1, T2> Tuple2<T1, T2> tuple2(T1 t1, T2 t2) {
    return new Tuple2<>(t1, t2);
  }

  @Test
  public void testCollect() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    List<String> collected = ds.collectAsList();
    Assertions.assertEquals(Arrays.asList("hello", "world"), collected);
  }

  @Test
  public void testTake() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    List<String> collected = ds.takeAsList(1);
    Assertions.assertEquals(Arrays.asList("hello"), collected);
  }

  @Test
  public void testToLocalIterator() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    Iterator<String> iter = ds.toLocalIterator();
    Assertions.assertEquals("hello", iter.next());
    Assertions.assertEquals("world", iter.next());
    Assertions.assertFalse(iter.hasNext());
  }

  // SPARK-15632: typed filter should preserve the underlying logical schema
  @Test
  public void testTypedFilterPreservingSchema() {
    Dataset<Long> ds = spark.range(10);
    Dataset<Long> ds2 = ds.filter((FilterFunction<Long>) value -> value > 3);
    Assertions.assertEquals(ds.schema(), ds2.schema());
  }

  @Test
  public void testBeanWithSet() {
    BeanWithSet bean = new BeanWithSet();
    Set<Long> fields = asSet(1L, 2L, 3L);
    bean.setFields(fields);
    List<BeanWithSet> objects = Collections.singletonList(bean);

    Dataset<BeanWithSet> ds = spark.createDataset(objects, Encoders.bean(BeanWithSet.class));
    Dataset<Row> df = ds.toDF();

    Dataset<BeanWithSet> mapped =
            df.map((MapFunction<Row, BeanWithSet>) row -> {
              BeanWithSet obj = new BeanWithSet();
              obj.setFields(new HashSet<>(row.<Long>getList(row.fieldIndex("fields"))));
              return obj;
            }, Encoders.bean(BeanWithSet.class));

    Assertions.assertEquals(objects, mapped.collectAsList());
  }

  @Test
  public void testCommonOperation() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    Assertions.assertEquals("hello", ds.first());

    Dataset<String> filtered = ds.filter((FilterFunction<String>) v -> v.startsWith("h"));
    Assertions.assertEquals(Arrays.asList("hello"), filtered.collectAsList());


    Dataset<Integer> mapped =
      ds.map((MapFunction<String, Integer>) String::length, Encoders.INT());
    Assertions.assertEquals(Arrays.asList(5, 5), mapped.collectAsList());

    Dataset<String> parMapped = ds.mapPartitions((MapPartitionsFunction<String, String>) it -> {
      List<String> ls = new LinkedList<>();
      while (it.hasNext()) {
        ls.add(it.next().toUpperCase(Locale.ROOT));
      }
      return ls.iterator();
    }, Encoders.STRING());
    Assertions.assertEquals(Arrays.asList("HELLO", "WORLD"), parMapped.collectAsList());

    Dataset<String> flatMapped = ds.flatMap((FlatMapFunction<String, String>) s -> {
      List<String> ls = new LinkedList<>();
      for (char c : s.toCharArray()) {
        ls.add(String.valueOf(c));
      }
      return ls.iterator();
    }, Encoders.STRING());
    Assertions.assertEquals(
      Arrays.asList("h", "e", "l", "l", "o", "w", "o", "r", "l", "d"),
      flatMapped.collectAsList());
  }

  @Test
  public void testForeach() {
    LongAccumulator accum = jsc.sc().longAccumulator();
    List<String> data = Arrays.asList("a", "b", "c");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

    ds.foreach((ForeachFunction<String>) s -> accum.add(1));
    Assertions.assertEquals(3, accum.value().intValue());
  }

  @Test
  public void testReduce() {
    List<Integer> data = Arrays.asList(1, 2, 3);
    Dataset<Integer> ds = spark.createDataset(data, Encoders.INT());

    int reduced = ds.reduce((ReduceFunction<Integer>) (v1, v2) -> v1 + v2);
    Assertions.assertEquals(6, reduced);
  }

  @Test
  public void testInitialStateForTransformWithState() {
    List<String> data = Arrays.asList("a", "xy", "foo", "bar");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    Dataset<Tuple2<Integer, String>> initialStateDS = spark.createDataset(
      Arrays.asList(new Tuple2<Integer, String>(2, "pq")),
      Encoders.tuple(Encoders.INT(), Encoders.STRING())
    );

    KeyValueGroupedDataset<Integer, Tuple2<Integer, String>> kvInitStateDS =
      initialStateDS.groupByKey(
        (MapFunction<Tuple2<Integer, String>, Integer>) f -> f._1, Encoders.INT());

    KeyValueGroupedDataset<Integer, String> kvInitStateMappedDS = kvInitStateDS.mapValues(
      (MapFunction<Tuple2<Integer, String>, String>) f -> f._2,
      Encoders.STRING()
    );

    KeyValueGroupedDataset<Integer, String> grouped =
      ds.groupByKey((MapFunction<String, Integer>) String::length, Encoders.INT());

    Dataset<String> transformWithStateMapped = grouped.transformWithState(
      new TestStatefulProcessorWithInitialState(),
      TimeoutMode.NoTimeouts(),
      OutputMode.Append(),
      kvInitStateMappedDS,
      Encoders.STRING(),
      Encoders.STRING());

    Assertions.assertEquals(asSet("1a", "2pqxy", "3foobar"),
      toSet(transformWithStateMapped.collectAsList()));
  }

  @Test
  public void testInitialStateFlatMapGroupsWithState() {
    List<String> data = Arrays.asList("a", "foo", "bar");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    Dataset<Tuple2<Integer, Long>> initialStateDS = spark.createDataset(
      Arrays.asList(new Tuple2<Integer, Long>(2, 2L)),
      Encoders.tuple(Encoders.INT(), Encoders.LONG())
    );

    KeyValueGroupedDataset<Integer, Tuple2<Integer, Long>> kvInitStateDS =
      initialStateDS.groupByKey(
        (MapFunction<Tuple2<Integer, Long>, Integer>) f -> f._1, Encoders.INT());

    KeyValueGroupedDataset<Integer, Long> kvInitStateMappedDS = kvInitStateDS.mapValues(
      (MapFunction<Tuple2<Integer, Long>, Long>) f -> f._2,
      Encoders.LONG()
    );

    KeyValueGroupedDataset<Integer, String> grouped =
      ds.groupByKey((MapFunction<String, Integer>) String::length, Encoders.INT());

    Dataset<String> flatMapped2 = grouped.flatMapGroupsWithState(
      (FlatMapGroupsWithStateFunction<Integer, String, Long, String>) (key, values, s) -> {
        StringBuilder sb = new StringBuilder(key.toString());
        while (values.hasNext()) {
          sb.append(values.next());
        }
        return Collections.singletonList(sb.toString()).iterator();
      },
      OutputMode.Append(),
      Encoders.LONG(),
      Encoders.STRING(),
      GroupStateTimeout.NoTimeout(),
      kvInitStateMappedDS);

    Assertions.assertEquals(asSet("1a", "2", "3foobar"), toSet(flatMapped2.collectAsList()));
    Dataset<String> mapped2 = grouped.mapGroupsWithState(
      (MapGroupsWithStateFunction<Integer, String, Long, String>) (key, values, s) -> {
        StringBuilder sb = new StringBuilder(key.toString());
        while (values.hasNext()) {
          sb.append(values.next());
        }
        return sb.toString();
      },
      Encoders.LONG(),
      Encoders.STRING(),
      GroupStateTimeout.NoTimeout(),
      kvInitStateMappedDS);
    Assertions.assertEquals(asSet("1a", "2", "3foobar"), toSet(mapped2.collectAsList()));
  }

  @Test
  public void testIllegalTestGroupStateCreations() {
    // SPARK-35800: test code throws upon illegal TestGroupState create() calls
    Assertions.assertThrows(
      IllegalArgumentException.class,
      () -> TestGroupState.create(
        Optional.of(5), GroupStateTimeout.EventTimeTimeout(), 0L, Optional.of(-1000L), false),
      "eventTimeWatermarkMs must be 0 or positive if present");

    Assertions.assertThrows(
      IllegalArgumentException.class,
      () -> TestGroupState.create(
        Optional.of(5), GroupStateTimeout.EventTimeTimeout(), -100L, Optional.of(1000L), false),
      "batchProcessingTimeMs must be 0 or positive");

    Assertions.assertThrows(
      UnsupportedOperationException.class,
      () -> TestGroupState.create(
        Optional.of(5), GroupStateTimeout.NoTimeout(), 100L, Optional.empty(), true),
      "hasTimedOut is true however there's no timeout configured");
  }

  @Test
  public void testMappingFunctionWithTestGroupState() throws Exception {
    // SPARK-35800: test the mapping function with injected TestGroupState instance
    MapGroupsWithStateFunction<Integer, Integer, Integer, Integer> mappingFunction =
      (MapGroupsWithStateFunction<Integer, Integer, Integer, Integer>) (key, values, state) -> {
        if (state.hasTimedOut()) {
          state.remove();
          return 0;
        }

        int existingState = 0;
        if (state.exists()) {
          existingState = state.get();
        } else {
          // Set state timeout timestamp upon initialization
          state.setTimeoutTimestamp(1500L);
        }

        while (values.hasNext()) {
          existingState += values.next();
        }
        state.update(existingState);

        return state.get();
    };

    TestGroupState<Integer> prevState = TestGroupState.create(
      Optional.empty(), GroupStateTimeout.EventTimeTimeout(), 0L, Optional.of(1000L), false);

    Assertions.assertFalse(prevState.isUpdated());
    Assertions.assertFalse(prevState.isRemoved());
    Assertions.assertFalse(prevState.exists());
    Assertions.assertEquals(Optional.empty(), prevState.getTimeoutTimestampMs());

    Integer[] values = {1, 3, 5};
    mappingFunction.call(1, Arrays.asList(values).iterator(), prevState);

    Assertions.assertTrue(prevState.isUpdated());
    Assertions.assertFalse(prevState.isRemoved());
    Assertions.assertTrue(prevState.exists());
    Assertions.assertEquals(Integer.valueOf(9), prevState.get());
    Assertions.assertEquals(0L, prevState.getCurrentProcessingTimeMs());
    Assertions.assertEquals(1000L, prevState.getCurrentWatermarkMs());
    Assertions.assertEquals(Optional.of(1500L), prevState.getTimeoutTimestampMs());

    mappingFunction.call(1, Arrays.asList(values).iterator(), prevState);

    Assertions.assertTrue(prevState.isUpdated());
    Assertions.assertFalse(prevState.isRemoved());
    Assertions.assertTrue(prevState.exists());
    Assertions.assertEquals(Integer.valueOf(18), prevState.get());

    prevState = TestGroupState.create(
      Optional.of(9), GroupStateTimeout.EventTimeTimeout(), 0L, Optional.of(1000L), true);

    mappingFunction.call(1, Arrays.asList(values).iterator(), prevState);

    Assertions.assertFalse(prevState.isUpdated());
    Assertions.assertTrue(prevState.isRemoved());
    Assertions.assertFalse(prevState.exists());
  }

  @Test
  public void testTransformWithState() {
    List<String> data = Arrays.asList("a", "foo", "bar");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    KeyValueGroupedDataset<Integer, String> grouped =
      ds.groupByKey((MapFunction<String, Integer>) String::length, Encoders.INT());

    StatefulProcessor<Integer, String, String> testStatefulProcessor = new TestStatefulProcessor();
    Dataset<String> transformWithStateMapped = grouped.transformWithState(
      testStatefulProcessor,
      TimeoutMode.NoTimeouts(),
      OutputMode.Append(),
      Encoders.STRING());

    Assertions.assertEquals(asSet("1a", "3foobar"),
      toSet(transformWithStateMapped.collectAsList()));
  }

  @Test
  public void testGroupByKey() {
    List<String> data = Arrays.asList("a", "foo", "bar");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    KeyValueGroupedDataset<Integer, String> grouped =
      ds.groupByKey((MapFunction<String, Integer>) String::length, Encoders.INT());

    Dataset<String> mapped = grouped.mapGroups(
      (MapGroupsFunction<Integer, String, String>) (key, values) -> {
        StringBuilder sb = new StringBuilder(key.toString());
        while (values.hasNext()) {
          sb.append(values.next());
        }
        return sb.toString();
      }, Encoders.STRING());

    Assertions.assertEquals(asSet("1a", "3foobar"), toSet(mapped.collectAsList()));

    Dataset<String> flatMapped = grouped.flatMapGroups(
        (FlatMapGroupsFunction<Integer, String, String>) (key, values) -> {
          StringBuilder sb = new StringBuilder(key.toString());
          while (values.hasNext()) {
            sb.append(values.next());
          }
          return Collections.singletonList(sb.toString()).iterator();
        },
        Encoders.STRING());

    Assertions.assertEquals(asSet("1a", "3foobar"), toSet(flatMapped.collectAsList()));
    Dataset<String> flatMapSorted = grouped.flatMapSortedGroups(
        new Column[] { ds.col("value") },
        (FlatMapGroupsFunction<Integer, String, String>) (key, values) -> {
          StringBuilder sb = new StringBuilder(key.toString());
          while (values.hasNext()) {
            sb.append(values.next());
          }
          return Collections.singletonList(sb.toString()).iterator();
        },
        Encoders.STRING());

    Assertions.assertEquals(asSet("1a", "3barfoo"), toSet(flatMapSorted.collectAsList()));

    Dataset<String> mapped2 = grouped.mapGroupsWithState(
        (MapGroupsWithStateFunction<Integer, String, Long, String>) (key, values, s) -> {
          StringBuilder sb = new StringBuilder(key.toString());
          while (values.hasNext()) {
            sb.append(values.next());
          }
          return sb.toString();
        },
        Encoders.LONG(),
        Encoders.STRING());

    Assertions.assertEquals(asSet("1a", "3foobar"), toSet(mapped2.collectAsList()));

    Dataset<String> flatMapped2 = grouped.flatMapGroupsWithState(
        (FlatMapGroupsWithStateFunction<Integer, String, Long, String>) (key, values, s) -> {
          StringBuilder sb = new StringBuilder(key.toString());
          while (values.hasNext()) {
            sb.append(values.next());
          }
          return Collections.singletonList(sb.toString()).iterator();
        },
        OutputMode.Append(),
        Encoders.LONG(),
        Encoders.STRING(),
        GroupStateTimeout.NoTimeout());

    Assertions.assertEquals(asSet("1a", "3foobar"), toSet(flatMapped2.collectAsList()));

    Dataset<Tuple2<Integer, String>> reduced =
      grouped.reduceGroups((ReduceFunction<String>) (v1, v2) -> v1 + v2);

    Assertions.assertEquals(
      asSet(tuple2(1, "a"), tuple2(3, "foobar")),
      toSet(reduced.collectAsList()));

    List<Integer> data2 = Arrays.asList(2, 6, 7, 10);
    Dataset<Integer> ds2 = spark.createDataset(data2, Encoders.INT());
    KeyValueGroupedDataset<Integer, Integer> grouped2 = ds2.groupByKey(
        (MapFunction<Integer, Integer>) v -> v / 2,
      Encoders.INT());

    Dataset<String> cogrouped = grouped.cogroup(
      grouped2,
      (CoGroupFunction<Integer, String, Integer, String>) (key, left, right) -> {
        StringBuilder sb = new StringBuilder(key.toString());
        while (left.hasNext()) {
          sb.append(left.next());
        }
        sb.append("#");
        while (right.hasNext()) {
          sb.append(right.next());
        }
        return Collections.singletonList(sb.toString()).iterator();
      },
      Encoders.STRING());

    Assertions.assertEquals(asSet("1a#2", "3foobar#67", "5#10"), toSet(cogrouped.collectAsList()));

    Dataset<String> cogroupSorted = grouped.cogroupSorted(
      grouped2,
      new Column[] { ds.col("value") },
      new Column[] { ds2.col("value").desc() },
      (CoGroupFunction<Integer, String, Integer, String>) (key, left, right) -> {
        StringBuilder sb = new StringBuilder(key.toString());
        while (left.hasNext()) {
          sb.append(left.next());
        }
        sb.append("#");
        while (right.hasNext()) {
          sb.append(right.next());
        }
        return Collections.singletonList(sb.toString()).iterator();
      },
      Encoders.STRING());

    Assertions.assertEquals(
      asSet("1a#2", "3barfoo#76", "5#10"),toSet(cogroupSorted.collectAsList()));
  }

  @Test
  public void testObservation() {
    // SPARK-34806: tests the Observation Java API and Dataset.observe(Observation, Column, Column*)
    Observation namedObservation = new Observation("named");
    Observation unnamedObservation = new Observation();

    Dataset<Long> df = spark
      .range(100)
      .observe(
        namedObservation,
        min(col("id")).as("min_val"),
        max(col("id")).as("max_val"),
        sum(col("id")).as("sum_val"),
        count(when(pmod(col("id"), lit(2)).$eq$eq$eq(0), 1)).as("num_even")
      )
      .observe(
        unnamedObservation,
        avg(col("id")).cast("int").as("avg_val")
      );

    df.collect();
    Map<String, Object> namedMetrics = null;
    Map<String, Object> unnamedMetrics = null;

    try {
      namedMetrics = namedObservation.getAsJava();
      unnamedMetrics = unnamedObservation.getAsJava();
    } catch (InterruptedException e) {
      Assertions.fail();
    }
    Map<String, Object> expectedNamedMetrics = new HashMap<String, Object>() {{
      put("min_val", 0L);
      put("max_val", 99L);
      put("sum_val", 4950L);
      put("num_even", 50L);
    }};
    Assertions.assertEquals(expectedNamedMetrics, namedMetrics);

    Map<String, Object> expectedUnnamedMetrics = new HashMap<String, Object>() {{
      put("avg_val", 49);
    }};
    Assertions.assertEquals(expectedUnnamedMetrics, unnamedMetrics);

    // we can get the result multiple times
    try {
      namedMetrics = namedObservation.getAsJava();
      unnamedMetrics = unnamedObservation.getAsJava();
    } catch (InterruptedException e) {
      Assertions.fail();
    }
    Assertions.assertEquals(expectedNamedMetrics, namedMetrics);
    Assertions.assertEquals(expectedUnnamedMetrics, unnamedMetrics);
  }

  @Test
  public void testSelect() {
    List<Integer> data = Arrays.asList(2, 6);
    Dataset<Integer> ds = spark.createDataset(data, Encoders.INT());

    Dataset<Tuple2<Integer, String>> selected = ds.select(
      expr("value + 1"),
      col("value").cast("string")).as(Encoders.tuple(Encoders.INT(), Encoders.STRING()));

    Assertions.assertEquals(
      Arrays.asList(tuple2(3, "2"), tuple2(7, "6")),
      selected.collectAsList());
  }

  @Test
  public void testSetOperation() {
    List<String> data = Arrays.asList("abc", "abc", "xyz");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

    Assertions.assertEquals(asSet("abc", "xyz"), toSet(ds.distinct().collectAsList()));

    List<String> data2 = Arrays.asList("xyz", "foo", "foo");
    Dataset<String> ds2 = spark.createDataset(data2, Encoders.STRING());

    Dataset<String> intersected = ds.intersect(ds2);
    Assertions.assertEquals(Arrays.asList("xyz"), intersected.collectAsList());

    Dataset<String> unioned = ds.union(ds2).union(ds);
    Assertions.assertEquals(
      Arrays.asList("abc", "abc", "xyz", "xyz", "foo", "foo", "abc", "abc", "xyz"),
      unioned.collectAsList());

    Dataset<String> subtracted = ds.except(ds2);
    Assertions.assertEquals(Arrays.asList("abc"), subtracted.collectAsList());
  }

  private static <T> Set<T> toSet(List<T> records) {
    return new HashSet<>(records);
  }

  @SafeVarargs
  @SuppressWarnings("varargs")
  private static <T> Set<T> asSet(T... records) {
    return toSet(Arrays.asList(records));
  }

  @Test
  public void testJoin() {
    List<Integer> data = Arrays.asList(1, 2, 3);
    Dataset<Integer> ds = spark.createDataset(data, Encoders.INT()).as("a");
    List<Integer> data2 = Arrays.asList(2, 3, 4);
    Dataset<Integer> ds2 = spark.createDataset(data2, Encoders.INT()).as("b");

    Dataset<Tuple2<Integer, Integer>> joined =
      ds.joinWith(ds2, col("a.value").equalTo(col("b.value")));
    Assertions.assertEquals(
      Arrays.asList(tuple2(2, 2), tuple2(3, 3)),
      joined.collectAsList());
  }

  private static final Comparator<Tuple2<String, Integer>> comparatorStringAndIntTuple =
      new Comparator<Tuple2<String, Integer>>() {
        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
          if (o1._1.compareTo(o2._1) != 0) {
            return o1._1.compareTo(o2._1);
          }
          return o1._2.compareTo(o2._2);
        }

        @Override
        public boolean equals(Object obj) {
          return super.equals(obj);
        }
      };

  private void assertEqualsUnorderly(
      List<Tuple2<String, Integer>> expected,
      List<Tuple2<String, Integer>> actual) {
    Assertions.assertEquals(
        expected.stream().sorted(comparatorStringAndIntTuple).collect(Collectors.toList()),
        actual.stream().sorted(comparatorStringAndIntTuple).collect(Collectors.toList())
    );
  }

  @Test
  public void testDropDuplicates() {
    List<Tuple2<String, Integer>> data = Arrays.asList(
        new Tuple2<>("a", 1), new Tuple2<>("a", 2),
        new Tuple2<>("b", 1), new Tuple2<>("a", 1)
    );
    Dataset<Tuple2<String, Integer>> ds = spark.createDataset(data,
        Encoders.tuple(Encoders.STRING(), Encoders.INT()));

    assertEqualsUnorderly(
        Arrays.asList(tuple2("a", 1), tuple2("a", 2), tuple2("b", 1)),
        ds.dropDuplicates().collectAsList()
    );

    assertEqualsUnorderly(
        Arrays.asList(tuple2("a", 1), tuple2("b", 1)),
        ds.dropDuplicates("_1").collectAsList()
    );

    assertEqualsUnorderly(
        Arrays.asList(tuple2("a", 1), tuple2("b", 1)),
        ds.dropDuplicates(new String[] { "_1" }).collectAsList()
    );

    assertEqualsUnorderly(
        Arrays.asList(tuple2("a", 1), tuple2("a", 2)),
        ds.dropDuplicates("_2").collectAsList()
    );

    assertEqualsUnorderly(
        Arrays.asList(tuple2("a", 1), tuple2("a", 2)),
        ds.dropDuplicates(new String[] { "_2" }).collectAsList()
    );

    assertEqualsUnorderly(
        Arrays.asList(tuple2("a", 1), tuple2("a", 2), tuple2("b", 1)),
        ds.dropDuplicates("_1", "_2").collectAsList()
    );

    assertEqualsUnorderly(
        Arrays.asList(tuple2("a", 1), tuple2("a", 2), tuple2("b", 1)),
        ds.dropDuplicates(new String[] { "_1", "_2" }).collectAsList()
    );
  }

  @Test
  public void testTupleEncoder() {
    Encoder<Tuple2<Integer, String>> encoder2 = Encoders.tuple(Encoders.INT(), Encoders.STRING());
    List<Tuple2<Integer, String>> data2 = Arrays.asList(tuple2(1, "a"), tuple2(2, "b"));
    Dataset<Tuple2<Integer, String>> ds2 = spark.createDataset(data2, encoder2);
    Assertions.assertEquals(data2, ds2.collectAsList());

    Encoder<Tuple3<Integer, Long, String>> encoder3 =
      Encoders.tuple(Encoders.INT(), Encoders.LONG(), Encoders.STRING());
    List<Tuple3<Integer, Long, String>> data3 =
      Arrays.asList(new Tuple3<>(1, 2L, "a"));
    Dataset<Tuple3<Integer, Long, String>> ds3 = spark.createDataset(data3, encoder3);
    Assertions.assertEquals(data3, ds3.collectAsList());

    Encoder<Tuple4<Integer, String, Long, String>> encoder4 =
      Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.LONG(), Encoders.STRING());
    List<Tuple4<Integer, String, Long, String>> data4 =
      Arrays.asList(new Tuple4<>(1, "b", 2L, "a"));
    Dataset<Tuple4<Integer, String, Long, String>> ds4 = spark.createDataset(data4, encoder4);
    Assertions.assertEquals(data4, ds4.collectAsList());

    Encoder<Tuple5<Integer, String, Long, String, Boolean>> encoder5 =
      Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.LONG(), Encoders.STRING(),
        Encoders.BOOLEAN());
    List<Tuple5<Integer, String, Long, String, Boolean>> data5 =
      Arrays.asList(new Tuple5<>(1, "b", 2L, "a", true));
    Dataset<Tuple5<Integer, String, Long, String, Boolean>> ds5 =
      spark.createDataset(data5, encoder5);
    Assertions.assertEquals(data5, ds5.collectAsList());
  }

  @Test
  public void testTupleEncoderSchema() {
    Encoder<Tuple2<String, Tuple2<String,String>>> encoder =
      Encoders.tuple(Encoders.STRING(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
    List<Tuple2<String, Tuple2<String, String>>> data = Arrays.asList(tuple2("1", tuple2("a", "b")),
      tuple2("2", tuple2("c", "d")));
    Dataset<Row> ds1 = spark.createDataset(data, encoder).toDF("value1", "value2");

    JavaPairRDD<String, Tuple2<String, String>> pairRDD = jsc.parallelizePairs(data);
    Dataset<Row> ds2 = spark.createDataset(JavaPairRDD.toRDD(pairRDD), encoder)
      .toDF("value1", "value2");

    Assertions.assertEquals(ds1.schema(), ds2.schema());
    Assertions.assertEquals(ds1.select(expr("value2._1")).collectAsList(),
      ds2.select(expr("value2._1")).collectAsList());
  }

  @Test
  public void testNestedTupleEncoder() {
    // test ((int, string), string)
    Encoder<Tuple2<Tuple2<Integer, String>, String>> encoder =
      Encoders.tuple(Encoders.tuple(Encoders.INT(), Encoders.STRING()), Encoders.STRING());
    List<Tuple2<Tuple2<Integer, String>, String>> data =
      Arrays.asList(tuple2(tuple2(1, "a"), "a"), tuple2(tuple2(2, "b"), "b"));
    Dataset<Tuple2<Tuple2<Integer, String>, String>> ds = spark.createDataset(data, encoder);
    Assertions.assertEquals(data, ds.collectAsList());

    // test (int, (string, string, long))
    Encoder<Tuple2<Integer, Tuple3<String, String, Long>>> encoder2 =
      Encoders.tuple(Encoders.INT(),
        Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.LONG()));
    List<Tuple2<Integer, Tuple3<String, String, Long>>> data2 =
      Arrays.asList(tuple2(1, new Tuple3<>("a", "b", 3L)));
    Dataset<Tuple2<Integer, Tuple3<String, String, Long>>> ds2 =
      spark.createDataset(data2, encoder2);
    Assertions.assertEquals(data2, ds2.collectAsList());

    // test (int, ((string, long), string))
    Encoder<Tuple2<Integer, Tuple2<Tuple2<String, Long>, String>>> encoder3 =
      Encoders.tuple(Encoders.INT(),
        Encoders.tuple(Encoders.tuple(Encoders.STRING(), Encoders.LONG()), Encoders.STRING()));
    List<Tuple2<Integer, Tuple2<Tuple2<String, Long>, String>>> data3 =
      Arrays.asList(tuple2(1, tuple2(tuple2("a", 2L), "b")));
    Dataset<Tuple2<Integer, Tuple2<Tuple2<String, Long>, String>>> ds3 =
      spark.createDataset(data3, encoder3);
    Assertions.assertEquals(data3, ds3.collectAsList());
  }

  @Test
  public void testPrimitiveEncoder() {
    Encoder<Tuple5<Double, BigDecimal, Date, Timestamp, Float>> encoder =
      Encoders.tuple(Encoders.DOUBLE(), Encoders.DECIMAL(), Encoders.DATE(), Encoders.TIMESTAMP(),
        Encoders.FLOAT());
    List<Tuple5<Double, BigDecimal, Date, Timestamp, Float>> data =
      Arrays.asList(new Tuple5<>(
        1.7976931348623157E308, new BigDecimal("0.922337203685477589"),
          Date.valueOf("1970-01-01"), new Timestamp(System.currentTimeMillis()), Float.MAX_VALUE));
    Dataset<Tuple5<Double, BigDecimal, Date, Timestamp, Float>> ds =
      spark.createDataset(data, encoder);
    Assertions.assertEquals(data, ds.collectAsList());
  }

  @Test
  public void testLocalDateAndInstantEncoders() {
    Encoder<Tuple2<LocalDate, Instant>> encoder =
      Encoders.tuple(Encoders.LOCALDATE(), Encoders.INSTANT());
    List<Tuple2<LocalDate, Instant>> data =
      Arrays.asList(new Tuple2<>(LocalDate.ofEpochDay(0), Instant.ofEpochSecond(0)));
    Dataset<Tuple2<LocalDate, Instant>> ds = spark.createDataset(data, encoder);
    Assertions.assertEquals(data, ds.collectAsList());
  }

  @Test
  public void testLocalDateTimeEncoder() {
    Encoder<LocalDateTime> encoder = Encoders.LOCALDATETIME();
    List<LocalDateTime> data = Arrays.asList(LocalDateTime.of(1, 1, 1, 1, 1));
    Dataset<LocalDateTime> ds = spark.createDataset(data, encoder);
    Assertions.assertEquals(data, ds.collectAsList());
  }

  @Test
  public void testDurationEncoder() {
    Encoder<Duration> encoder = Encoders.DURATION();
    List<Duration> data = Arrays.asList(Duration.ofDays(0));
    Dataset<Duration> ds = spark.createDataset(data, encoder);
    Assertions.assertEquals(data, ds.collectAsList());
  }

  @Test
  public void testPeriodEncoder() {
    Encoder<Period> encoder = Encoders.PERIOD();
    List<Period> data = Arrays.asList(Period.ofYears(10));
    Dataset<Period> ds = spark.createDataset(data, encoder);
    Assertions.assertEquals(data, ds.collectAsList());
  }

  public static class KryoSerializable {
    String value;

    KryoSerializable(String value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;

      return this.value.equals(((KryoSerializable) other).value);
    }

    @Override
    public int hashCode() {
      return this.value.hashCode();
    }
  }

  public static class JavaSerializable implements Serializable {
    String value;

    JavaSerializable(String value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;

      return this.value.equals(((JavaSerializable) other).value);
    }

    @Override
    public int hashCode() {
      return this.value.hashCode();
    }
  }

  @Test
  public void testKryoEncoder() {
    Encoder<KryoSerializable> encoder = Encoders.kryo(KryoSerializable.class);
    List<KryoSerializable> data = Arrays.asList(
      new KryoSerializable("hello"), new KryoSerializable("world"));
    Dataset<KryoSerializable> ds = spark.createDataset(data, encoder);
    Assertions.assertEquals(data, ds.collectAsList());
  }

  @Test
  public void testJavaEncoder() {
    Encoder<JavaSerializable> encoder = Encoders.javaSerialization(JavaSerializable.class);
    List<JavaSerializable> data = Arrays.asList(
      new JavaSerializable("hello"), new JavaSerializable("world"));
    Dataset<JavaSerializable> ds = spark.createDataset(data, encoder);
    Assertions.assertEquals(data, ds.collectAsList());
  }

  @Test
  public void testRandomSplit() {
    List<String> data = Arrays.asList("hello", "world", "from", "spark");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    double[] arraySplit = {1, 2, 3};

    List<Dataset<String>> randomSplit =  ds.randomSplitAsList(arraySplit, 1);
    Assertions.assertEquals(randomSplit.size(), 3, "wrong number of splits");
  }

  /**
   * For testing error messages when creating an encoder on a private class. This is done
   * here since we cannot create truly private classes in Scala.
   */
  private static class PrivateClassTest { }

  @Test
  public void testJavaEncoderErrorMessageForPrivateClass() {
    Assertions.assertThrows(UnsupportedOperationException.class,
      () -> Encoders.javaSerialization(PrivateClassTest.class));
  }

  @Test
  public void testKryoEncoderErrorMessageForPrivateClass() {
    Assertions.assertThrows(UnsupportedOperationException.class,
      () -> Encoders.kryo(PrivateClassTest.class));
  }

  public static class SimpleJavaBean implements Serializable {
    private boolean a;
    private int b;
    private byte[] c;
    private String[] d;
    private List<String> e;
    private List<Long> f;
    private Map<Integer, String> g;
    private Map<List<Long>, Map<String, String>> h;

    public boolean isA() {
      return a;
    }

    public void setA(boolean a) {
      this.a = a;
    }

    public int getB() {
      return b;
    }

    public void setB(int b) {
      this.b = b;
    }

    public byte[] getC() {
      return c;
    }

    public void setC(byte[] c) {
      this.c = c;
    }

    public String[] getD() {
      return d;
    }

    public void setD(String[] d) {
      this.d = d;
    }

    public List<String> getE() {
      return e;
    }

    public void setE(List<String> e) {
      this.e = e;
    }

    public List<Long> getF() {
      return f;
    }

    public void setF(List<Long> f) {
      this.f = f;
    }

    public Map<Integer, String> getG() {
      return g;
    }

    public void setG(Map<Integer, String> g) {
      this.g = g;
    }

    public Map<List<Long>, Map<String, String>> getH() {
      return h;
    }

    public void setH(Map<List<Long>, Map<String, String>> h) {
      this.h = h;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SimpleJavaBean that = (SimpleJavaBean) o;

      if (a != that.a) return false;
      if (b != that.b) return false;
      if (!Arrays.equals(c, that.c)) return false;
      if (!Arrays.equals(d, that.d)) return false;
      if (!e.equals(that.e)) return false;
      if (!f.equals(that.f)) return false;
      if (!g.equals(that.g)) return false;
      return h.equals(that.h);

    }

    @Override
    public int hashCode() {
      int result = (a ? 1 : 0);
      result = 31 * result + b;
      result = 31 * result + Arrays.hashCode(c);
      result = 31 * result + Arrays.hashCode(d);
      result = 31 * result + e.hashCode();
      result = 31 * result + f.hashCode();
      result = 31 * result + g.hashCode();
      result = 31 * result + h.hashCode();
      return result;
    }
  }

  public static class SimpleJavaBean2 implements Serializable {
    private Timestamp a;
    private Date b;
    private java.math.BigDecimal c;

    public Timestamp getA() { return a; }

    public void setA(Timestamp a) { this.a = a; }

    public Date getB() { return b; }

    public void setB(Date b) { this.b = b; }

    public java.math.BigDecimal getC() { return c; }

    public void setC(java.math.BigDecimal c) { this.c = c; }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SimpleJavaBean2 that = (SimpleJavaBean2) o;

      if (!a.equals(that.a)) return false;
      if (!b.equals(that.b)) return false;
      return c.equals(that.c);
    }

    @Override
    public int hashCode() {
      int result = a.hashCode();
      result = 31 * result + b.hashCode();
      result = 31 * result + c.hashCode();
      return result;
    }
  }

  public static class NestedJavaBean implements Serializable {
    private SimpleJavaBean a;

    public SimpleJavaBean getA() {
      return a;
    }

    public void setA(SimpleJavaBean a) {
      this.a = a;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NestedJavaBean that = (NestedJavaBean) o;

      return a.equals(that.a);
    }

    @Override
    public int hashCode() {
      return a.hashCode();
    }
  }

  @Test
  public void testJavaBeanEncoder() {
    OuterScopes.addOuterScope(this);
    SimpleJavaBean obj1 = new SimpleJavaBean();
    obj1.setA(true);
    obj1.setB(3);
    obj1.setC(new byte[]{1, 2});
    obj1.setD(new String[]{"hello", null});
    obj1.setE(Arrays.asList("a", "b"));
    obj1.setF(Arrays.asList(100L, null, 200L));
    Map<Integer, String> map1 = new HashMap<>();
    map1.put(1, "a");
    map1.put(2, "b");
    obj1.setG(map1);
    Map<String, String> nestedMap1 = new HashMap<>();
    nestedMap1.put("x", "1");
    nestedMap1.put("y", "2");
    Map<List<Long>, Map<String, String>> complexMap1 = new HashMap<>();
    complexMap1.put(Arrays.asList(1L, 2L), nestedMap1);
    obj1.setH(complexMap1);

    SimpleJavaBean obj2 = new SimpleJavaBean();
    obj2.setA(false);
    obj2.setB(30);
    obj2.setC(new byte[]{3, 4});
    obj2.setD(new String[]{null, "world"});
    obj2.setE(Arrays.asList("x", "y"));
    obj2.setF(Arrays.asList(300L, null, 400L));
    Map<Integer, String> map2 = new HashMap<>();
    map2.put(3, "c");
    map2.put(4, "d");
    obj2.setG(map2);
    Map<String, String> nestedMap2 = new HashMap<>();
    nestedMap2.put("q", "1");
    nestedMap2.put("w", "2");
    Map<List<Long>, Map<String, String>> complexMap2 = new HashMap<>();
    complexMap2.put(Arrays.asList(3L, 4L), nestedMap2);
    obj2.setH(complexMap2);

    List<SimpleJavaBean> data = Arrays.asList(obj1, obj2);
    Dataset<SimpleJavaBean> ds = spark.createDataset(data, Encoders.bean(SimpleJavaBean.class));
    Assertions.assertEquals(data, ds.collectAsList());

    NestedJavaBean obj3 = new NestedJavaBean();
    obj3.setA(obj1);

    List<NestedJavaBean> data2 = Arrays.asList(obj3);
    Dataset<NestedJavaBean> ds2 = spark.createDataset(data2, Encoders.bean(NestedJavaBean.class));
    Assertions.assertEquals(data2, ds2.collectAsList());

    Row row1 = new GenericRow(new Object[]{
      true,
      3,
      new byte[]{1, 2},
      new String[]{"hello", null},
      Arrays.asList("a", "b"),
      Arrays.asList(100L, null, 200L),
      map1,
      complexMap1});
    Row row2 = new GenericRow(new Object[]{
      false,
      30,
      new byte[]{3, 4},
      new String[]{null, "world"},
      Arrays.asList("x", "y"),
      Arrays.asList(300L, null, 400L),
      map2,
      complexMap2});
    StructType schema = new StructType()
      .add("a", BooleanType, false)
      .add("b", IntegerType, false)
      .add("c", BinaryType)
      .add("d", createArrayType(StringType))
      .add("e", createArrayType(StringType))
      .add("f", createArrayType(LongType))
      .add("g", createMapType(IntegerType, StringType))
      .add("h",createMapType(createArrayType(LongType), createMapType(StringType, StringType)));
    Dataset<SimpleJavaBean> ds3 = spark.createDataFrame(Arrays.asList(row1, row2), schema)
      .as(Encoders.bean(SimpleJavaBean.class));
    Assertions.assertEquals(data, ds3.collectAsList());
  }

  @Test
  public void testJavaBeanEncoder2() {
    // This is a regression test of SPARK-12404
    OuterScopes.addOuterScope(this);
    SimpleJavaBean2 obj = new SimpleJavaBean2();
    obj.setA(new Timestamp(0));
    obj.setB(new Date(0));
    obj.setC(java.math.BigDecimal.valueOf(1));
    Dataset<SimpleJavaBean2> ds =
      spark.createDataset(Arrays.asList(obj), Encoders.bean(SimpleJavaBean2.class));
    ds.collect();
  }

  public static class SmallBean implements Serializable {
    private String a;

    private int b;

    public int getB() {
      return b;
    }

    public void setB(int b) {
      this.b = b;
    }

    public String getA() {
      return a;
    }

    public void setA(String a) {
      this.a = a;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SmallBean smallBean = (SmallBean) o;
      return b == smallBean.b && com.google.common.base.Objects.equal(a, smallBean.a);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(a, b);
    }
  }

  public static class NestedSmallBean implements Serializable {
    private SmallBean f;

    public SmallBean getF() {
      return f;
    }

    public void setF(SmallBean f) {
      this.f = f;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NestedSmallBean that = (NestedSmallBean) o;
      return Objects.equal(f, that.f);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(f);
    }
  }

  public static class NestedSmallBeanWithNonNullField implements Serializable {
    private SmallBean nonNull_f;
    private SmallBean nullable_f;
    private Map<String, SmallBean> childMap;

    @Nonnull
    public SmallBean getNonNull_f() {
      return nonNull_f;
    }

    public void setNonNull_f(SmallBean f) {
      this.nonNull_f = f;
    }

    public SmallBean getNullable_f() {
      return nullable_f;
    }

    public void setNullable_f(SmallBean f) {
      this.nullable_f = f;
    }

    @Nonnull
    public Map<String, SmallBean> getChildMap() { return childMap; }
    public void setChildMap(Map<String, SmallBean> childMap) {
      this.childMap = childMap;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NestedSmallBeanWithNonNullField that = (NestedSmallBeanWithNonNullField) o;
      return Objects.equal(nullable_f, that.nullable_f) &&
        Objects.equal(nonNull_f, that.nonNull_f) && Objects.equal(childMap, that.childMap);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(nullable_f, nonNull_f, childMap);
    }
  }

  public static class NestedSmallBean2 implements Serializable {
    private NestedSmallBeanWithNonNullField f;

    @Nonnull
    public NestedSmallBeanWithNonNullField getF() {
      return f;
    }

    public void setF(NestedSmallBeanWithNonNullField f) {
      this.f = f;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NestedSmallBean2 that = (NestedSmallBean2) o;
      return Objects.equal(f, that.f);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(f);
    }
  }

  @Test
  public void testRuntimeNullabilityCheck() {
    OuterScopes.addOuterScope(this);

    StructType schema = new StructType()
      .add("f", new StructType()
        .add("a", StringType, true)
        .add("b", IntegerType, true), true);

    // Shouldn't throw runtime exception since it passes nullability check.
    {
      Row row = new GenericRow(new Object[] {
          new GenericRow(new Object[] {
              "hello", 1
          })
      });

      Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row), schema);
      Dataset<NestedSmallBean> ds = df.as(Encoders.bean(NestedSmallBean.class));

      SmallBean smallBean = new SmallBean();
      smallBean.setA("hello");
      smallBean.setB(1);

      NestedSmallBean nestedSmallBean = new NestedSmallBean();
      nestedSmallBean.setF(smallBean);

      Assertions.assertEquals(Collections.singletonList(nestedSmallBean), ds.collectAsList());
    }

    // Shouldn't throw runtime exception when parent object (`ClassData`) is null
    {
      Row row = new GenericRow(new Object[] { null });

      Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row), schema);
      Dataset<NestedSmallBean> ds = df.as(Encoders.bean(NestedSmallBean.class));

      NestedSmallBean nestedSmallBean = new NestedSmallBean();
      Assertions.assertEquals(Collections.singletonList(nestedSmallBean), ds.collectAsList());
    }

    {
      Row row = new GenericRow(new Object[] {
          new GenericRow(new Object[] {
              "hello", null
          })
      });

      Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row), schema);
      Dataset<NestedSmallBean> ds = df.as(Encoders.bean(NestedSmallBean.class));

      Assertions.assertThrows(RuntimeException.class, ds::collect,
        "Null value appeared in non-nullable field");
    }
  }

  public static class Nesting3 implements Serializable {
    private Integer field3_1;
    private Double field3_2;
    private String field3_3;

    public Nesting3() {
    }

    public Nesting3(Integer field3_1, Double field3_2, String field3_3) {
      this.field3_1 = field3_1;
      this.field3_2 = field3_2;
      this.field3_3 = field3_3;
    }

    private Nesting3(Builder builder) {
      setField3_1(builder.field3_1);
      setField3_2(builder.field3_2);
      setField3_3(builder.field3_3);
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public Integer getField3_1() {
      return field3_1;
    }

    public void setField3_1(Integer field3_1) {
      this.field3_1 = field3_1;
    }

    public Double getField3_2() {
      return field3_2;
    }

    public void setField3_2(Double field3_2) {
      this.field3_2 = field3_2;
    }

    public String getField3_3() {
      return field3_3;
    }

    public void setField3_3(String field3_3) {
      this.field3_3 = field3_3;
    }

    public static final class Builder {
      private Integer field3_1 = 0;
      private Double field3_2 = 0.0;
      private String field3_3 = "value";

      private Builder() {
      }

      public Builder field3_1(Integer field3_1) {
        this.field3_1 = field3_1;
        return this;
      }

      public Builder field3_2(Double field3_2) {
        this.field3_2 = field3_2;
        return this;
      }

      public Builder field3_3(String field3_3) {
        this.field3_3 = field3_3;
        return this;
      }

      public Nesting3 build() {
        return new Nesting3(this);
      }
    }
  }

  public static class Nesting2 implements Serializable {
    private Nesting3 field2_1;
    private Nesting3 field2_2;
    private Nesting3 field2_3;

    public Nesting2() {
    }

    public Nesting2(Nesting3 field2_1, Nesting3 field2_2, Nesting3 field2_3) {
      this.field2_1 = field2_1;
      this.field2_2 = field2_2;
      this.field2_3 = field2_3;
    }

    private Nesting2(Builder builder) {
      setField2_1(builder.field2_1);
      setField2_2(builder.field2_2);
      setField2_3(builder.field2_3);
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public Nesting3 getField2_1() {
      return field2_1;
    }

    public void setField2_1(Nesting3 field2_1) {
      this.field2_1 = field2_1;
    }

    public Nesting3 getField2_2() {
      return field2_2;
    }

    public void setField2_2(Nesting3 field2_2) {
      this.field2_2 = field2_2;
    }

    public Nesting3 getField2_3() {
      return field2_3;
    }

    public void setField2_3(Nesting3 field2_3) {
      this.field2_3 = field2_3;
    }


    public static final class Builder {
      private Nesting3 field2_1 = Nesting3.newBuilder().build();
      private Nesting3 field2_2 = Nesting3.newBuilder().build();
      private Nesting3 field2_3 = Nesting3.newBuilder().build();

      private Builder() {
      }

      public Builder field2_1(Nesting3 field2_1) {
        this.field2_1 = field2_1;
        return this;
      }

      public Builder field2_2(Nesting3 field2_2) {
        this.field2_2 = field2_2;
        return this;
      }

      public Builder field2_3(Nesting3 field2_3) {
        this.field2_3 = field2_3;
        return this;
      }

      public Nesting2 build() {
        return new Nesting2(this);
      }
    }
  }

  public static class Nesting1 implements Serializable {
    private Nesting2 field1_1;
    private Nesting2 field1_2;
    private Nesting2 field1_3;

    public Nesting1() {
    }

    public Nesting1(Nesting2 field1_1, Nesting2 field1_2, Nesting2 field1_3) {
      this.field1_1 = field1_1;
      this.field1_2 = field1_2;
      this.field1_3 = field1_3;
    }

    private Nesting1(Builder builder) {
      setField1_1(builder.field1_1);
      setField1_2(builder.field1_2);
      setField1_3(builder.field1_3);
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public Nesting2 getField1_1() {
      return field1_1;
    }

    public void setField1_1(Nesting2 field1_1) {
      this.field1_1 = field1_1;
    }

    public Nesting2 getField1_2() {
      return field1_2;
    }

    public void setField1_2(Nesting2 field1_2) {
      this.field1_2 = field1_2;
    }

    public Nesting2 getField1_3() {
      return field1_3;
    }

    public void setField1_3(Nesting2 field1_3) {
      this.field1_3 = field1_3;
    }


    public static final class Builder {
      private Nesting2 field1_1 = Nesting2.newBuilder().build();
      private Nesting2 field1_2 = Nesting2.newBuilder().build();
      private Nesting2 field1_3 = Nesting2.newBuilder().build();

      private Builder() {
      }

      public Builder field1_1(Nesting2 field1_1) {
        this.field1_1 = field1_1;
        return this;
      }

      public Builder field1_2(Nesting2 field1_2) {
        this.field1_2 = field1_2;
        return this;
      }

      public Builder field1_3(Nesting2 field1_3) {
        this.field1_3 = field1_3;
        return this;
      }

      public Nesting1 build() {
        return new Nesting1(this);
      }
    }
  }

  public static class NestedComplicatedJavaBean implements Serializable {
    private Nesting1 field1;
    private Nesting1 field2;
    private Nesting1 field3;
    private Nesting1 field4;
    private Nesting1 field5;
    private Nesting1 field6;
    private Nesting1 field7;
    private Nesting1 field8;
    private Nesting1 field9;
    private Nesting1 field10;

    public NestedComplicatedJavaBean() {
    }

    private NestedComplicatedJavaBean(Builder builder) {
      setField1(builder.field1);
      setField2(builder.field2);
      setField3(builder.field3);
      setField4(builder.field4);
      setField5(builder.field5);
      setField6(builder.field6);
      setField7(builder.field7);
      setField8(builder.field8);
      setField9(builder.field9);
      setField10(builder.field10);
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public Nesting1 getField1() {
      return field1;
    }

    public void setField1(Nesting1 field1) {
      this.field1 = field1;
    }

    public Nesting1 getField2() {
      return field2;
    }

    public void setField2(Nesting1 field2) {
      this.field2 = field2;
    }

    public Nesting1 getField3() {
      return field3;
    }

    public void setField3(Nesting1 field3) {
      this.field3 = field3;
    }

    public Nesting1 getField4() {
      return field4;
    }

    public void setField4(Nesting1 field4) {
      this.field4 = field4;
    }

    public Nesting1 getField5() {
      return field5;
    }

    public void setField5(Nesting1 field5) {
      this.field5 = field5;
    }

    public Nesting1 getField6() {
      return field6;
    }

    public void setField6(Nesting1 field6) {
      this.field6 = field6;
    }

    public Nesting1 getField7() {
      return field7;
    }

    public void setField7(Nesting1 field7) {
      this.field7 = field7;
    }

    public Nesting1 getField8() {
      return field8;
    }

    public void setField8(Nesting1 field8) {
      this.field8 = field8;
    }

    public Nesting1 getField9() {
      return field9;
    }

    public void setField9(Nesting1 field9) {
      this.field9 = field9;
    }

    public Nesting1 getField10() {
      return field10;
    }

    public void setField10(Nesting1 field10) {
      this.field10 = field10;
    }

    public static final class Builder {
      private Nesting1 field1 = Nesting1.newBuilder().build();
      private Nesting1 field2 = Nesting1.newBuilder().build();
      private Nesting1 field3 = Nesting1.newBuilder().build();
      private Nesting1 field4 = Nesting1.newBuilder().build();
      private Nesting1 field5 = Nesting1.newBuilder().build();
      private Nesting1 field6 = Nesting1.newBuilder().build();
      private Nesting1 field7 = Nesting1.newBuilder().build();
      private Nesting1 field8 = Nesting1.newBuilder().build();
      private Nesting1 field9 = Nesting1.newBuilder().build();
      private Nesting1 field10 = Nesting1.newBuilder().build();

      private Builder() {
      }

      public Builder field1(Nesting1 field1) {
        this.field1 = field1;
        return this;
      }

      public Builder field2(Nesting1 field2) {
        this.field2 = field2;
        return this;
      }

      public Builder field3(Nesting1 field3) {
        this.field3 = field3;
        return this;
      }

      public Builder field4(Nesting1 field4) {
        this.field4 = field4;
        return this;
      }

      public Builder field5(Nesting1 field5) {
        this.field5 = field5;
        return this;
      }

      public Builder field6(Nesting1 field6) {
        this.field6 = field6;
        return this;
      }

      public Builder field7(Nesting1 field7) {
        this.field7 = field7;
        return this;
      }

      public Builder field8(Nesting1 field8) {
        this.field8 = field8;
        return this;
      }

      public Builder field9(Nesting1 field9) {
        this.field9 = field9;
        return this;
      }

      public Builder field10(Nesting1 field10) {
        this.field10 = field10;
        return this;
      }

      public NestedComplicatedJavaBean build() {
        return new NestedComplicatedJavaBean(this);
      }
    }
  }

  @Test
  public void test() {
    /* SPARK-15285 Large numbers of Nested JavaBeans generates more than 64KB java bytecode */
    List<NestedComplicatedJavaBean> data = new ArrayList<>();
    data.add(NestedComplicatedJavaBean.newBuilder().build());

    NestedComplicatedJavaBean obj3 = new NestedComplicatedJavaBean();

    Dataset<NestedComplicatedJavaBean> ds =
      spark.createDataset(data, Encoders.bean(NestedComplicatedJavaBean.class));
    ds.collectAsList();
  }

  public enum MyEnum {
    A("www.elgoog.com"),
    B("www.google.com");

    private final String url;

    MyEnum(String url) {
      this.url = url;
    }

    public String getUrl() {
      return url;
    }
  }

  public static class BeanWithEnum {
    MyEnum enumField;
    String regularField;

    public String getRegularField() {
      return regularField;
    }

    public void setRegularField(String regularField) {
      this.regularField = regularField;
    }

    public MyEnum getEnumField() {
      return enumField;
    }

    public void setEnumField(MyEnum field) {
      this.enumField = field;
    }

    public BeanWithEnum(MyEnum enumField, String regularField) {
      this.enumField = enumField;
      this.regularField = regularField;
    }

    public BeanWithEnum() {
    }

    public String toString() {
      return "BeanWithEnum(" + enumField  + ", " + regularField + ")";
    }

    public int hashCode() {
      return Objects.hashCode(enumField, regularField);
    }

    public boolean equals(Object other) {
      if (other instanceof BeanWithEnum beanWithEnum) {
        return beanWithEnum.regularField.equals(regularField)
          && beanWithEnum.enumField.equals(enumField);
      }
      return false;
    }
  }

  @Test
  public void testBeanWithEnum() {
    List<BeanWithEnum> data = Arrays.asList(new BeanWithEnum(MyEnum.A, "mira avenue"),
            new BeanWithEnum(MyEnum.B, "flower boulevard"));
    Encoder<BeanWithEnum> encoder = Encoders.bean(BeanWithEnum.class);
    Dataset<BeanWithEnum> ds = spark.createDataset(data, encoder);
    Assertions.assertEquals(data, ds.collectAsList());
  }

  public static class EmptyBean implements Serializable {}

  @Test
  public void testEmptyBean() {
    EmptyBean bean = new EmptyBean();
    List<EmptyBean> data = Arrays.asList(bean);
    Dataset<EmptyBean> df = spark.createDataset(data, Encoders.bean(EmptyBean.class));
    Assertions.assertEquals(0, df.schema().length());
    Assertions.assertEquals(1, df.collectAsList().size());
  }

  public static class ReadOnlyPropertyBean implements Serializable {
      public boolean isEmpty() {
        return true;
      }
  }

  @Test
  public void testReadOnlyPropertyBean() {
    ReadOnlyPropertyBean bean = new ReadOnlyPropertyBean();
    List<ReadOnlyPropertyBean> data = Arrays.asList(bean);
    Dataset<ReadOnlyPropertyBean> df = spark.createDataset(data,
            Encoders.bean(ReadOnlyPropertyBean.class));
    Assertions.assertEquals(1, df.schema().length());
    Assertions.assertEquals(1, df.collectAsList().size());

  }

  public class CircularReference1Bean implements Serializable {
    private CircularReference2Bean child;

    public CircularReference2Bean getChild() {
      return child;
    }

    public void setChild(CircularReference2Bean child) {
      this.child = child;
    }
  }

  public class CircularReference2Bean implements Serializable {
    private CircularReference1Bean child;

    public CircularReference1Bean getChild() {
      return child;
    }

    public void setChild(CircularReference1Bean child) {
      this.child = child;
    }
  }

  public static class CircularReference3Bean implements Serializable {
    private CircularReference3Bean[] child;

    public CircularReference3Bean[] getChild() {
      return child;
    }

    public void setChild(CircularReference3Bean[] child) {
      this.child = child;
    }
  }

  public class CircularReference4Bean implements Serializable {
    private Map<String, CircularReference5Bean> child;

    public Map<String, CircularReference5Bean> getChild() {
      return child;
    }

    public void setChild(Map<String, CircularReference5Bean> child) {
      this.child = child;
    }
  }

  public class CircularReference5Bean implements Serializable {
    private String id;
    private List<CircularReference4Bean> child;

    public String getId() {
      return id;
    }

    public List<CircularReference4Bean> getChild() {
      return child;
    }

    public void setId(String id) {
      this.id = id;
    }

    public void setChild(List<CircularReference4Bean> child) {
      this.child = child;
    }
  }

  @Test
  public void testCircularReferenceBean1() {
    CircularReference1Bean bean = new CircularReference1Bean();
    Assertions.assertThrows(UnsupportedOperationException.class,
      () -> spark.createDataset(Arrays.asList(bean), Encoders.bean(CircularReference1Bean.class)));
  }

  @Test
  public void testCircularReferenceBean2() {
    CircularReference3Bean bean = new CircularReference3Bean();
    Assertions.assertThrows(UnsupportedOperationException.class,
      () -> spark.createDataset(Arrays.asList(bean), Encoders.bean(CircularReference3Bean.class)));
  }

  @Test
  public void testCircularReferenceBean3() {
    CircularReference4Bean bean = new CircularReference4Bean();
    Assertions.assertThrows(UnsupportedOperationException.class,
      () -> spark.createDataset(Arrays.asList(bean), Encoders.bean(CircularReference4Bean.class)));
  }

  @Test
  public void testNullInTopLevelBean() {
    NestedSmallBean bean = new NestedSmallBean();
    // We cannot set null in top-level bean
    Assertions.assertThrows(RuntimeException.class,
      () -> spark.createDataset(Arrays.asList(bean, null), Encoders.bean(NestedSmallBean.class)));
  }

  @Test
  public void testSerializeNull() {
    NestedSmallBean bean = new NestedSmallBean();
    Encoder<NestedSmallBean> encoder = Encoders.bean(NestedSmallBean.class);
    List<NestedSmallBean> beans = Arrays.asList(bean);
    Dataset<NestedSmallBean> ds1 = spark.createDataset(beans, encoder);
    Assertions.assertEquals(beans, ds1.collectAsList());
    Dataset<NestedSmallBean> ds2 =
      ds1.map((MapFunction<NestedSmallBean, NestedSmallBean>) b -> b, encoder);
    Assertions.assertEquals(beans, ds2.collectAsList());
  }

  @Test
  public void testNonNullField() {
    NestedSmallBeanWithNonNullField bean1 = new NestedSmallBeanWithNonNullField();
    SmallBean smallBean = new SmallBean();
    bean1.setNonNull_f(smallBean);
    Map<String, SmallBean> map = new HashMap<>();
    bean1.setChildMap(map);

    Encoder<NestedSmallBeanWithNonNullField> encoder1 =
            Encoders.bean(NestedSmallBeanWithNonNullField.class);
    List<NestedSmallBeanWithNonNullField> beans1 = Arrays.asList(bean1);
    Dataset<NestedSmallBeanWithNonNullField> ds1 = spark.createDataset(beans1, encoder1);

    StructType schema = ds1.schema();
    Assertions.assertFalse(schema.apply("nonNull_f").nullable());
    Assertions.assertTrue(schema.apply("nullable_f").nullable());
    Assertions.assertFalse(schema.apply("childMap").nullable());

    Assertions.assertEquals(beans1, ds1.collectAsList());
    Dataset<NestedSmallBeanWithNonNullField> ds2 = ds1.map(
      (MapFunction<NestedSmallBeanWithNonNullField, NestedSmallBeanWithNonNullField>) b -> b,
      encoder1);
    Assertions.assertEquals(beans1, ds2.collectAsList());

    // Nonnull nested fields
    NestedSmallBean2 bean2 = new NestedSmallBean2();
    bean2.setF(bean1);

    Encoder<NestedSmallBean2> encoder2 =
            Encoders.bean(NestedSmallBean2.class);
    List<NestedSmallBean2> beans2 = Arrays.asList(bean2);
    Dataset<NestedSmallBean2> ds3 = spark.createDataset(beans2, encoder2);

    StructType nestedSchema = (StructType) ds3.schema()
      .fields()[ds3.schema().fieldIndex("f")]
      .dataType();
    Assertions.assertFalse(nestedSchema.apply("nonNull_f").nullable());
    Assertions.assertTrue(nestedSchema.apply("nullable_f").nullable());
    Assertions.assertFalse(nestedSchema.apply("childMap").nullable());

    Assertions.assertEquals(beans2, ds3.collectAsList());

    Dataset<NestedSmallBean2> ds4 = ds3.map(
      (MapFunction<NestedSmallBean2, NestedSmallBean2>) b -> b,
      encoder2);
    Assertions.assertEquals(beans2, ds4.collectAsList());
  }

  @Test
  public void testSpecificLists() {
    SpecificListsBean bean = new SpecificListsBean();
    ArrayList<Integer> arrayList = new ArrayList<>();
    arrayList.add(1);
    bean.setArrayList(arrayList);
    LinkedList<Integer> linkedList = new LinkedList<>();
    linkedList.add(1);
    bean.setLinkedList(linkedList);
    bean.setList(Collections.singletonList(1));
    List<SpecificListsBean> beans = Collections.singletonList(bean);
    Dataset<SpecificListsBean> dataset =
      spark.createDataset(beans, Encoders.bean(SpecificListsBean.class));
    Assertions.assertEquals(beans, dataset.collectAsList());
  }

  @Test
  public void testRowEncoder() {
    final StructType schema = new StructType()
        .add("a", "int")
        .add("b", "string");
    final Dataset<Row> df = spark.range(3)
        .map(new MapFunction<Long, Row>() {
               @Override
               public Row call(Long i) {
                 return create(i.intValue(), "s" + i);
               }
             },
            Encoders.row(schema))
        .filter(col("a").geq(1));
    final List<Row> expected = Arrays.asList(create(1, "s1"), create(2, "s2"));
    Assertions.assertEquals(expected, df.collectAsList());
  }

  public static class BeanWithSet implements Serializable {
    private Set<Long> fields;

    public Set<Long> getFields() {
      return fields;
    }

    public void setFields(Set<Long> fields) {
      this.fields = fields;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BeanWithSet that = (BeanWithSet) o;
      return Objects.equal(fields, that.fields);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(fields);
    }
  }

  public static class SpecificListsBean implements Serializable {
    private ArrayList<Integer> arrayList;
    private LinkedList<Integer> linkedList;
    private List<Integer> list;

    public ArrayList<Integer> getArrayList() {
      return arrayList;
    }

    public void setArrayList(ArrayList<Integer> arrayList) {
      this.arrayList = arrayList;
    }

    public LinkedList<Integer> getLinkedList() {
      return linkedList;
    }

    public void setLinkedList(LinkedList<Integer> linkedList) {
      this.linkedList = linkedList;
    }

    public List<Integer> getList() {
      return list;
    }

    public void setList(List<Integer> list) {
      this.list = list;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SpecificListsBean that = (SpecificListsBean) o;
      return Objects.equal(arrayList, that.arrayList) &&
        Objects.equal(linkedList, that.linkedList) &&
        Objects.equal(list, that.list);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(arrayList, linkedList, list);
    }
  }
}
