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

package test.org.apache.spark;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.network.util.JavaUtils;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.collection.JavaConverters;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.apache.spark.rdd.RDD;
import org.apache.spark.resource.ExecutorResourceRequests;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.ResourceProfileBuilder;
import org.apache.spark.resource.TaskResourceRequests;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.StatCounter;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaAPISuite implements Serializable {
  private transient JavaSparkContext sc;
  private transient File tempDir;

  @Before
  public void setUp() throws IOException {
    sc = new JavaSparkContext("local", "JavaAPISuite");
    tempDir = JavaUtils.createTempDir();
    tempDir.deleteOnExit();
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sparkContextUnion() {
    // Union of non-specialized JavaRDDs
    List<String> strings = Arrays.asList("Hello", "World");
    JavaRDD<String> s1 = sc.parallelize(strings);
    JavaRDD<String> s2 = sc.parallelize(strings);
    // Varargs
    JavaRDD<String> sUnion = sc.union(s1, s2);
    assertEquals(4, sUnion.count());

    // Union of JavaDoubleRDDs
    List<Double> doubles = Arrays.asList(1.0, 2.0);
    JavaDoubleRDD d1 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD d2 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD dUnion = sc.union(d1, d2);
    assertEquals(4, dUnion.count());

    // Union of JavaPairRDDs
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
    pairs.add(new Tuple2<>(1, 2));
    pairs.add(new Tuple2<>(3, 4));
    JavaPairRDD<Integer, Integer> p1 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> p2 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> pUnion = sc.union(p1, p2);
    assertEquals(4, pUnion.count());
  }

  @Test
  public void intersection() {
    List<Integer> ints1 = Arrays.asList(1, 10, 2, 3, 4, 5);
    List<Integer> ints2 = Arrays.asList(1, 6, 2, 3, 7, 8);
    JavaRDD<Integer> s1 = sc.parallelize(ints1);
    JavaRDD<Integer> s2 = sc.parallelize(ints2);

    JavaRDD<Integer> intersections = s1.intersection(s2);
    assertEquals(3, intersections.count());

    JavaRDD<Integer> empty = sc.emptyRDD();
    JavaRDD<Integer> emptyIntersection = empty.intersection(s2);
    assertEquals(0, emptyIntersection.count());

    List<Double> doubles = Arrays.asList(1.0, 2.0);
    JavaDoubleRDD d1 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD d2 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD dIntersection = d1.intersection(d2);
    assertEquals(2, dIntersection.count());

    List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
    pairs.add(new Tuple2<>(1, 2));
    pairs.add(new Tuple2<>(3, 4));
    JavaPairRDD<Integer, Integer> p1 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> p2 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> pIntersection = p1.intersection(p2);
    assertEquals(2, pIntersection.count());
  }

  @Test
  public void sample() {
    List<Integer> ints = IntStream.iterate(1, x -> x + 1)
      .limit(20)
      .boxed()
      .collect(Collectors.toList());
    JavaRDD<Integer> rdd = sc.parallelize(ints);
    // the seeds here are "magic" to make this work out nicely
    JavaRDD<Integer> sample20 = rdd.sample(true, 0.2, 8);
    assertEquals(2, sample20.count());
    JavaRDD<Integer> sample20WithoutReplacement = rdd.sample(false, 0.2, 2);
    assertEquals(4, sample20WithoutReplacement.count());
  }

  @Test
  public void randomSplit() {
    List<Integer> ints = new ArrayList<>(1000);
    for (int i = 0; i < 1000; i++) {
      ints.add(i);
    }
    JavaRDD<Integer> rdd = sc.parallelize(ints);
    JavaRDD<Integer>[] splits = rdd.randomSplit(new double[] { 0.4, 0.6, 1.0 }, 31);
    // the splits aren't perfect -- not enough data for them to be -- just check they're about right
    assertEquals(3, splits.length);
    long s0 = splits[0].count();
    long s1 = splits[1].count();
    long s2 = splits[2].count();
    assertTrue(s0 + " not within expected range", s0 > 150 && s0 < 250);
    assertTrue(s1 + " not within expected range", s1 > 250 && s1 < 350);
    assertTrue(s2 + " not within expected range", s2 > 430 && s2 < 570);
  }

  @Test
  public void sortByKey() {
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
    pairs.add(new Tuple2<>(0, 4));
    pairs.add(new Tuple2<>(3, 2));
    pairs.add(new Tuple2<>(-1, 1));

    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs);

    // Default comparator
    JavaPairRDD<Integer, Integer> sortedRDD = rdd.sortByKey();
    assertEquals(new Tuple2<>(-1, 1), sortedRDD.first());
    List<Tuple2<Integer, Integer>> sortedPairs = sortedRDD.collect();
    assertEquals(new Tuple2<>(0, 4), sortedPairs.get(1));
    assertEquals(new Tuple2<>(3, 2), sortedPairs.get(2));

    // Custom comparator
    sortedRDD = rdd.sortByKey(Collections.reverseOrder(), false);
    assertEquals(new Tuple2<>(-1, 1), sortedRDD.first());
    sortedPairs = sortedRDD.collect();
    assertEquals(new Tuple2<>(0, 4), sortedPairs.get(1));
    assertEquals(new Tuple2<>(3, 2), sortedPairs.get(2));
  }

  @Test
  public void repartitionAndSortWithinPartitions() {
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
    pairs.add(new Tuple2<>(0, 5));
    pairs.add(new Tuple2<>(3, 8));
    pairs.add(new Tuple2<>(2, 6));
    pairs.add(new Tuple2<>(0, 8));
    pairs.add(new Tuple2<>(3, 8));
    pairs.add(new Tuple2<>(1, 3));

    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs);

    Partitioner partitioner = new Partitioner() {
      @Override
      public int numPartitions() {
        return 2;
      }
      @Override
      public int getPartition(Object key) {
        return (Integer) key % 2;
      }
    };

    JavaPairRDD<Integer, Integer> repartitioned =
        rdd.repartitionAndSortWithinPartitions(partitioner);
    assertTrue(repartitioned.partitioner().isPresent());
    assertEquals(repartitioned.partitioner().get(), partitioner);
    List<List<Tuple2<Integer, Integer>>> partitions = repartitioned.glom().collect();
    assertEquals(partitions.get(0),
        Arrays.asList(new Tuple2<>(0, 5), new Tuple2<>(0, 8), new Tuple2<>(2, 6)));
    assertEquals(partitions.get(1),
        Arrays.asList(new Tuple2<>(1, 3), new Tuple2<>(3, 8), new Tuple2<>(3, 8)));
  }

  @Test
  public void filterByRange() {
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
    pairs.add(new Tuple2<>(0, 5));
    pairs.add(new Tuple2<>(1, 8));
    pairs.add(new Tuple2<>(2, 6));
    pairs.add(new Tuple2<>(3, 8));
    pairs.add(new Tuple2<>(4, 8));

    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs).sortByKey();

    // Default comparator
    JavaPairRDD<Integer, Integer> filteredRDD = rdd.filterByRange(3, 11);
    List<Tuple2<Integer, Integer>> filteredPairs = filteredRDD.collect();
    assertEquals(filteredPairs.size(), 2);
    assertEquals(filteredPairs.get(0), new Tuple2<>(3, 8));
    assertEquals(filteredPairs.get(1), new Tuple2<>(4, 8));

    // Custom comparator
    filteredRDD = rdd.filterByRange(Collections.reverseOrder(), 3, -2);
    filteredPairs = filteredRDD.collect();
    assertEquals(filteredPairs.size(), 4);
    assertEquals(filteredPairs.get(0), new Tuple2<>(0, 5));
    assertEquals(filteredPairs.get(1), new Tuple2<>(1, 8));
    assertEquals(filteredPairs.get(2), new Tuple2<>(2, 6));
    assertEquals(filteredPairs.get(3), new Tuple2<>(3, 8));
  }

  @Test
  public void emptyRDD() {
    JavaRDD<String> rdd = sc.emptyRDD();
    assertEquals("Empty RDD shouldn't have any values", 0, rdd.count());
  }

  @Test
  public void sortBy() {
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
    pairs.add(new Tuple2<>(0, 4));
    pairs.add(new Tuple2<>(3, 2));
    pairs.add(new Tuple2<>(-1, 1));

    JavaRDD<Tuple2<Integer, Integer>> rdd = sc.parallelize(pairs);

    // compare on first value
    JavaRDD<Tuple2<Integer, Integer>> sortedRDD = rdd.sortBy(Tuple2::_1, true, 2);

    assertEquals(new Tuple2<>(-1, 1), sortedRDD.first());
    List<Tuple2<Integer, Integer>> sortedPairs = sortedRDD.collect();
    assertEquals(new Tuple2<>(0, 4), sortedPairs.get(1));
    assertEquals(new Tuple2<>(3, 2), sortedPairs.get(2));

    // compare on second value
    sortedRDD = rdd.sortBy(Tuple2::_2, true, 2);
    assertEquals(new Tuple2<>(-1, 1), sortedRDD.first());
    sortedPairs = sortedRDD.collect();
    assertEquals(new Tuple2<>(3, 2), sortedPairs.get(1));
    assertEquals(new Tuple2<>(0, 4), sortedPairs.get(2));
  }

  @Test
  public void foreach() {
    LongAccumulator accum = sc.sc().longAccumulator();
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello", "World"));
    rdd.foreach(s -> accum.add(1));
    assertEquals(2, accum.value().intValue());
  }

  @Test
  public void foreachPartition() {
    LongAccumulator accum = sc.sc().longAccumulator();
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello", "World"));
    rdd.foreachPartition(iter -> {
      while (iter.hasNext()) {
        iter.next();
        accum.add(1);
      }
    });
    assertEquals(2, accum.value().intValue());
  }

  @Test
  public void toLocalIterator() {
    List<Integer> correct = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> rdd = sc.parallelize(correct);
    List<Integer> result = Lists.newArrayList(rdd.toLocalIterator());
    assertEquals(correct, result);
  }

  @Test
  public void zipWithUniqueId() {
    List<Integer> dataArray = Arrays.asList(1, 2, 3, 4);
    JavaPairRDD<Integer, Long> zip = sc.parallelize(dataArray).zipWithUniqueId();
    JavaRDD<Long> indexes = zip.values();
    assertEquals(4, new HashSet<>(indexes.collect()).size());
  }

  @Test
  public void zipWithIndex() {
    List<Integer> dataArray = Arrays.asList(1, 2, 3, 4);
    JavaPairRDD<Integer, Long> zip = sc.parallelize(dataArray).zipWithIndex();
    JavaRDD<Long> indexes = zip.values();
    List<Long> correctIndexes = Arrays.asList(0L, 1L, 2L, 3L);
    assertEquals(correctIndexes, indexes.collect());
  }

  @Test
  public void lookup() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Apples", "Fruit"),
      new Tuple2<>("Oranges", "Fruit"),
      new Tuple2<>("Oranges", "Citrus")
    ));
    assertEquals(2, categories.lookup("Oranges").size());
    assertEquals(2, Iterables.size(categories.groupByKey().lookup("Oranges").get(0)));
  }

  @Test
  public void groupBy() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function<Integer, Boolean> isOdd = x -> x % 2 == 0;
    JavaPairRDD<Boolean, Iterable<Integer>> oddsAndEvens = rdd.groupBy(isOdd);
    assertEquals(2, oddsAndEvens.count());
    assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds

    oddsAndEvens = rdd.groupBy(isOdd, 1);
    assertEquals(2, oddsAndEvens.count());
    assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds
  }

  @Test
  public void groupByOnPairRDD() {
    // Regression test for SPARK-4459
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function<Tuple2<Integer, Integer>, Boolean> areOdd =
      x -> (x._1() % 2 == 0) && (x._2() % 2 == 0);
    JavaPairRDD<Integer, Integer> pairRDD = rdd.zip(rdd);
    JavaPairRDD<Boolean, Iterable<Tuple2<Integer, Integer>>> oddsAndEvens = pairRDD.groupBy(areOdd);
    assertEquals(2, oddsAndEvens.count());
    assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds

    oddsAndEvens = pairRDD.groupBy(areOdd, 1);
    assertEquals(2, oddsAndEvens.count());
    assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds
  }

  @Test
  public void keyByOnPairRDD() {
    // Regression test for SPARK-4459
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function<Tuple2<Integer, Integer>, String> sumToString = x -> String.valueOf(x._1() + x._2());
    JavaPairRDD<Integer, Integer> pairRDD = rdd.zip(rdd);
    JavaPairRDD<String, Tuple2<Integer, Integer>> keyed = pairRDD.keyBy(sumToString);
    assertEquals(7, keyed.count());
    assertEquals(1, (long) keyed.lookup("2").get(0)._1());
  }

  @Test
  public void cogroup() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Apples", "Fruit"),
      new Tuple2<>("Oranges", "Fruit"),
      new Tuple2<>("Oranges", "Citrus")
      ));
    JavaPairRDD<String, Integer> prices = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", 2),
      new Tuple2<>("Apples", 3)
    ));
    JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> cogrouped =
        categories.cogroup(prices);
    assertEquals("[Fruit, Citrus]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._1()));
    assertEquals("[2]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._2()));

    cogrouped.collect();
  }

  @Test
  public void cogroup3() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Apples", "Fruit"),
      new Tuple2<>("Oranges", "Fruit"),
      new Tuple2<>("Oranges", "Citrus")
      ));
    JavaPairRDD<String, Integer> prices = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", 2),
      new Tuple2<>("Apples", 3)
    ));
    JavaPairRDD<String, Integer> quantities = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", 21),
      new Tuple2<>("Apples", 42)
    ));

    JavaPairRDD<String, Tuple3<Iterable<String>, Iterable<Integer>, Iterable<Integer>>> cogrouped =
        categories.cogroup(prices, quantities);
    assertEquals("[Fruit, Citrus]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._1()));
    assertEquals("[2]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._2()));
    assertEquals("[42]", Iterables.toString(cogrouped.lookup("Apples").get(0)._3()));


    cogrouped.collect();
  }

  @Test
  public void cogroup4() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Apples", "Fruit"),
      new Tuple2<>("Oranges", "Fruit"),
      new Tuple2<>("Oranges", "Citrus")
      ));
    JavaPairRDD<String, Integer> prices = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", 2),
      new Tuple2<>("Apples", 3)
    ));
    JavaPairRDD<String, Integer> quantities = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", 21),
      new Tuple2<>("Apples", 42)
    ));
    JavaPairRDD<String, String> countries = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", "BR"),
      new Tuple2<>("Apples", "US")
    ));

    JavaPairRDD<String, Tuple4<Iterable<String>, Iterable<Integer>, Iterable<Integer>,
        Iterable<String>>> cogrouped = categories.cogroup(prices, quantities, countries);
    assertEquals("[Fruit, Citrus]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._1()));
    assertEquals("[2]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._2()));
    assertEquals("[42]", Iterables.toString(cogrouped.lookup("Apples").get(0)._3()));
    assertEquals("[BR]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._4()));

    cogrouped.collect();
  }

  @Test
  public void leftOuterJoin() {
    JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>(1, 1),
      new Tuple2<>(1, 2),
      new Tuple2<>(2, 1),
      new Tuple2<>(3, 1)
      ));
    JavaPairRDD<Integer, Character> rdd2 = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>(1, 'x'),
      new Tuple2<>(2, 'y'),
      new Tuple2<>(2, 'z'),
      new Tuple2<>(4, 'w')
    ));
    List<Tuple2<Integer,Tuple2<Integer,Optional<Character>>>> joined =
      rdd1.leftOuterJoin(rdd2).collect();
    assertEquals(5, joined.size());
    Tuple2<Integer,Tuple2<Integer,Optional<Character>>> firstUnmatched =
      rdd1.leftOuterJoin(rdd2).filter(tup -> !tup._2()._2().isPresent()).first();
    assertEquals(3, firstUnmatched._1().intValue());
  }

  @Test
  public void foldReduce() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function2<Integer, Integer, Integer> add = (a, b) -> a + b;

    int sum = rdd.fold(0, add);
    assertEquals(33, sum);

    sum = rdd.reduce(add);
    assertEquals(33, sum);
  }

  @Test
  public void treeReduce() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(-5, -4, -3, -2, -1, 1, 2, 3, 4), 10);
    Function2<Integer, Integer, Integer> add = (a, b) -> a + b;
    for (int depth = 1; depth <= 10; depth++) {
      int sum = rdd.treeReduce(add, depth);
      assertEquals(-5, sum);
    }
  }

  @Test
  public void treeAggregate() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(-5, -4, -3, -2, -1, 1, 2, 3, 4), 10);
    Function2<Integer, Integer, Integer> add = (a, b) -> a + b;
    for (int depth = 1; depth <= 10; depth++) {
      int sum = rdd.treeAggregate(0, add, add, depth);
      assertEquals(-5, sum);
    }
  }

  // Since SPARK-36419
  @Test
  public void treeAggregateWithFinalAggregateOnExecutor() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(-5, -4, -3, -2, -1, 1, 2, 3, 4), 10);
    Function2<Integer, Integer, Integer> add = (a, b) -> a + b;
    for (int depth = 1; depth <= 10; depth++) {
      int sum = rdd.treeAggregate(0, add, add, depth, true);
      assertEquals(-5, sum);
    }
  }

  @Test
  public void aggregateByKey() {
    JavaPairRDD<Integer, Integer> pairs = sc.parallelizePairs(
      Arrays.asList(
        new Tuple2<>(1, 1),
        new Tuple2<>(1, 1),
        new Tuple2<>(3, 2),
        new Tuple2<>(5, 1),
        new Tuple2<>(5, 3)), 2);

    Map<Integer, HashSet<Integer>> sets = pairs.aggregateByKey(new HashSet<Integer>(),
       (a, b) -> {
         a.add(b);
         return a;
       },
       (a, b) -> {
         a.addAll(b);
         return a;
       }).collectAsMap();
    assertEquals(3, sets.size());
    assertEquals(new HashSet<>(Arrays.asList(1)), sets.get(1));
    assertEquals(new HashSet<>(Arrays.asList(2)), sets.get(3));
    assertEquals(new HashSet<>(Arrays.asList(1, 3)), sets.get(5));
  }

  @Test
  public void foldByKey() {
    List<Tuple2<Integer, Integer>> pairs = Arrays.asList(
      new Tuple2<>(2, 1),
      new Tuple2<>(2, 1),
      new Tuple2<>(1, 1),
      new Tuple2<>(3, 2),
      new Tuple2<>(3, 1)
    );
    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> sums = rdd.foldByKey(0, (a, b) -> a + b);
    assertEquals(1, sums.lookup(1).get(0).intValue());
    assertEquals(2, sums.lookup(2).get(0).intValue());
    assertEquals(3, sums.lookup(3).get(0).intValue());
  }

  @Test
  public void reduceByKey() {
    List<Tuple2<Integer, Integer>> pairs = Arrays.asList(
      new Tuple2<>(2, 1),
      new Tuple2<>(2, 1),
      new Tuple2<>(1, 1),
      new Tuple2<>(3, 2),
      new Tuple2<>(3, 1)
    );
    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> counts = rdd.reduceByKey((a, b) -> a + b);
    assertEquals(1, counts.lookup(1).get(0).intValue());
    assertEquals(2, counts.lookup(2).get(0).intValue());
    assertEquals(3, counts.lookup(3).get(0).intValue());

    Map<Integer, Integer> localCounts = counts.collectAsMap();
    assertEquals(1, localCounts.get(1).intValue());
    assertEquals(2, localCounts.get(2).intValue());
    assertEquals(3, localCounts.get(3).intValue());

    localCounts = rdd.reduceByKeyLocally((a, b) -> a + b);
    assertEquals(1, localCounts.get(1).intValue());
    assertEquals(2, localCounts.get(2).intValue());
    assertEquals(3, localCounts.get(3).intValue());
  }

  @Test
  public void approximateResults() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Map<Integer, Long> countsByValue = rdd.countByValue();
    assertEquals(2, countsByValue.get(1).longValue());
    assertEquals(1, countsByValue.get(13).longValue());

    PartialResult<Map<Integer, BoundedDouble>> approx = rdd.countByValueApprox(1);
    Map<Integer, BoundedDouble> finalValue = approx.getFinalValue();
    assertEquals(2.0, finalValue.get(1).mean(), 0.01);
    assertEquals(1.0, finalValue.get(13).mean(), 0.01);
  }

  @Test
  public void take() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    assertEquals(1, rdd.first().intValue());
    rdd.take(2);
    rdd.takeSample(false, 2, 42);
  }

  @Test
  public void isEmpty() {
    assertTrue(sc.emptyRDD().isEmpty());
    assertTrue(sc.parallelize(new ArrayList<Integer>()).isEmpty());
    assertFalse(sc.parallelize(Arrays.asList(1)).isEmpty());
    assertTrue(sc.parallelize(Arrays.asList(1, 2, 3), 3).filter(i -> i < 0).isEmpty());
    assertFalse(sc.parallelize(Arrays.asList(1, 2, 3)).filter(i -> i > 1).isEmpty());
  }

  @Test
  public void cartesian() {
    JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 1.0, 2.0, 3.0, 5.0, 8.0));
    JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList("Hello", "World"));
    JavaPairRDD<String, Double> cartesian = stringRDD.cartesian(doubleRDD);
    assertEquals(new Tuple2<>("Hello", 1.0), cartesian.first());
  }

  @Test
  public void javaDoubleRDD() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 1.0, 2.0, 3.0, 5.0, 8.0));
    JavaDoubleRDD distinct = rdd.distinct();
    assertEquals(5, distinct.count());
    JavaDoubleRDD filter = rdd.filter(x -> x > 2.0);
    assertEquals(3, filter.count());
    JavaDoubleRDD union = rdd.union(rdd);
    assertEquals(12, union.count());
    union = union.cache();
    assertEquals(12, union.count());

    assertEquals(20, rdd.sum(), 0.01);
    StatCounter stats = rdd.stats();
    assertEquals(20, stats.sum(), 0.01);
    assertEquals(20/6.0, rdd.mean(), 0.01);
    assertEquals(20/6.0, rdd.mean(), 0.01);
    assertEquals(6.22222, rdd.variance(), 0.01);
    assertEquals(rdd.variance(), rdd.popVariance(), 1e-14);
    assertEquals(7.46667, rdd.sampleVariance(), 0.01);
    assertEquals(2.49444, rdd.stdev(), 0.01);
    assertEquals(rdd.stdev(), rdd.popStdev(), 1e-14);
    assertEquals(2.73252, rdd.sampleStdev(), 0.01);

    rdd.first();
    rdd.take(5);
  }

  @Test
  public void javaDoubleRDDHistoGram() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    // Test using generated buckets
    Tuple2<double[], long[]> results = rdd.histogram(2);
    double[] expected_buckets = {1.0, 2.5, 4.0};
    long[] expected_counts = {2, 2};
    assertArrayEquals(expected_buckets, results._1(), 0.1);
    assertArrayEquals(expected_counts, results._2());
    // Test with provided buckets
    long[] histogram = rdd.histogram(expected_buckets);
    assertArrayEquals(expected_counts, histogram);
    // SPARK-5744
    assertArrayEquals(
      new long[] {0},
      sc.parallelizeDoubles(new ArrayList<>(0), 1).histogram(new double[]{0.0, 1.0}));
  }

  private static class DoubleComparator implements Comparator<Double>, Serializable {
    @Override
    public int compare(Double o1, Double o2) {
      return o1.compareTo(o2);
    }
  }

  @Test
  public void max() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    double max = rdd.max(new DoubleComparator());
    assertEquals(4.0, max, 0.001);
  }

  @Test
  public void min() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    double max = rdd.min(new DoubleComparator());
    assertEquals(1.0, max, 0.001);
  }

  @Test
  public void naturalMax() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    double max = rdd.max();
    assertEquals(4.0, max, 0.0);
  }

  @Test
  public void naturalMin() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    double max = rdd.min();
    assertEquals(1.0, max, 0.0);
  }

  @Test
  public void takeOrdered() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    assertEquals(Arrays.asList(1.0, 2.0), rdd.takeOrdered(2, new DoubleComparator()));
    assertEquals(Arrays.asList(1.0, 2.0), rdd.takeOrdered(2));
  }

  @Test
  public void top() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    List<Integer> top2 = rdd.top(2);
    assertEquals(Arrays.asList(4, 3), top2);
  }

  private static class AddInts implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer a, Integer b) {
      return a + b;
    }
  }

  @Test
  public void reduce() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    int sum = rdd.reduce(new AddInts());
    assertEquals(10, sum);
  }

  @Test
  public void reduceOnJavaDoubleRDD() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    double sum = rdd.reduce((v1, v2) -> v1 + v2);
    assertEquals(10.0, sum, 0.001);
  }

  @Test
  public void fold() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    int sum = rdd.fold(0, new AddInts());
    assertEquals(10, sum);
  }

  @Test
  public void aggregate() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    int sum = rdd.aggregate(0, new AddInts(), new AddInts());
    assertEquals(10, sum);
  }

  @Test
  public void map() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    JavaDoubleRDD doubles = rdd.mapToDouble(Integer::doubleValue).cache();
    doubles.collect();
    JavaPairRDD<Integer, Integer> pairs = rdd.mapToPair(x -> new Tuple2<>(x, x)).cache();
    pairs.collect();
    JavaRDD<String> strings = rdd.map(Object::toString).cache();
    strings.collect();
  }

  @Test
  public void flatMap() {
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello World!",
      "The quick brown fox jumps over the lazy dog."));
    JavaRDD<String> words = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
    assertEquals("Hello", words.first());
    assertEquals(11, words.count());

    JavaPairRDD<String, String> pairsRDD = rdd.flatMapToPair(s -> {
        List<Tuple2<String, String>> pairs = new LinkedList<>();
        for (String word : s.split(" ")) {
          pairs.add(new Tuple2<>(word, word));
        }
        return pairs.iterator();
      }
    );
    assertEquals(new Tuple2<>("Hello", "Hello"), pairsRDD.first());
    assertEquals(11, pairsRDD.count());

    JavaDoubleRDD doubles = rdd.flatMapToDouble(s -> {
      List<Double> lengths = new LinkedList<>();
      for (String word : s.split(" ")) {
        lengths.add((double) word.length());
      }
      return lengths.iterator();
    });
    assertEquals(5.0, doubles.first(), 0.01);
    assertEquals(11, pairsRDD.count());
  }

  @Test
  public void mapsFromPairsToPairs() {
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(pairs);

    // Regression test for SPARK-668:
    JavaPairRDD<String, Integer> swapped = pairRDD.flatMapToPair(
      item -> Collections.singletonList(item.swap()).iterator());
    swapped.collect();

    // There was never a bug here, but it's worth testing:
    pairRDD.mapToPair(Tuple2::swap).collect();
  }

  @Test
  public void mapPartitions() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
    JavaRDD<Integer> partitionSums = rdd.mapPartitions(iter -> {
        int sum = 0;
        while (iter.hasNext()) {
          sum += iter.next();
        }
        return Collections.singletonList(sum).iterator();
      });
    assertEquals("[3, 7]", partitionSums.collect().toString());
  }


  @Test
  public void mapPartitionsWithIndex() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
    JavaRDD<Integer> partitionSums = rdd.mapPartitionsWithIndex((index, iter) -> {
        int sum = 0;
        while (iter.hasNext()) {
          sum += iter.next();
        }
        return Collections.singletonList(sum).iterator();
      }, false);
    assertEquals("[3, 7]", partitionSums.collect().toString());
  }

  @Test
  public void getNumPartitions(){
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 3);
    JavaDoubleRDD rdd2 = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0), 2);
    JavaPairRDD<String, Integer> rdd3 = sc.parallelizePairs(
      Arrays.asList(
        new Tuple2<>("a", 1),
        new Tuple2<>("aa", 2),
        new Tuple2<>("aaa", 3)
      ),
      2);
    assertEquals(3, rdd1.getNumPartitions());
    assertEquals(2, rdd2.getNumPartitions());
    assertEquals(2, rdd3.getNumPartitions());
  }

  @Test
  public void repartition() {
    // Shrinking number of partitions
    JavaRDD<Integer> in1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 2);
    JavaRDD<Integer> repartitioned1 = in1.repartition(4);
    List<List<Integer>> result1 = repartitioned1.glom().collect();
    assertEquals(4, result1.size());
    for (List<Integer> l : result1) {
      assertFalse(l.isEmpty());
    }

    // Growing number of partitions
    JavaRDD<Integer> in2 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 4);
    JavaRDD<Integer> repartitioned2 = in2.repartition(2);
    List<List<Integer>> result2 = repartitioned2.glom().collect();
    assertEquals(2, result2.size());
    for (List<Integer> l: result2) {
      assertFalse(l.isEmpty());
    }
  }

  @Test
  public void persist() {
    JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 1.0, 2.0, 3.0, 5.0, 8.0));
    doubleRDD = doubleRDD.persist(StorageLevel.DISK_ONLY());
    assertEquals(20, doubleRDD.sum(), 0.1);

    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(pairs);
    pairRDD = pairRDD.persist(StorageLevel.DISK_ONLY());
    assertEquals("a", pairRDD.first()._2());

    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    rdd = rdd.persist(StorageLevel.DISK_ONLY());
    assertEquals(1, rdd.first().intValue());
  }

  @Test
  public void withResources() {
    ExecutorResourceRequests ereqs = new ExecutorResourceRequests().cores(4);
    TaskResourceRequests treqs = new TaskResourceRequests().cpus(1);
    ResourceProfile rp1 = new ResourceProfileBuilder().require(ereqs).require(treqs).build();
    JavaRDD<Integer> in1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    in1.withResources(rp1);
    assertEquals(rp1, in1.getResourceProfile());
  }

  @Test
  public void iterator() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);
    TaskContext context = TaskContext$.MODULE$.empty();
    assertEquals(1, rdd.iterator(rdd.partitions().get(0), context).next().intValue());
  }

  @Test
  public void glom() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
    assertEquals("[1, 2]", rdd.glom().first().toString());
  }

  // File input / output tests are largely adapted from FileSuite:

  @Test
  public void textFiles() throws IOException {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    rdd.saveAsTextFile(outputDir);
    // Read the plain text file and check it's OK
    File outputFile = new File(outputDir, "part-00000");
    String content = Files.toString(outputFile, StandardCharsets.UTF_8);
    assertEquals("1\n2\n3\n4\n", content);
    // Also try reading it in as a text file RDD
    List<String> expected = Arrays.asList("1", "2", "3", "4");
    JavaRDD<String> readRDD = sc.textFile(outputDir);
    assertEquals(expected, readRDD.collect());
  }

  @Test
  public void wholeTextFiles() throws Exception {
    byte[] content1 = "spark is easy to use.\n".getBytes(StandardCharsets.UTF_8);
    byte[] content2 = "spark is also easy to use.\n".getBytes(StandardCharsets.UTF_8);

    String tempDirName = tempDir.getAbsolutePath();
    String path1 = new Path(tempDirName, "part-00000").toUri().getPath();
    String path2 = new Path(tempDirName, "part-00001").toUri().getPath();

    Files.write(content1, new File(path1));
    Files.write(content2, new File(path2));

    Map<String, String> container = new HashMap<>();
    container.put(path1, new Text(content1).toString());
    container.put(path2, new Text(content2).toString());

    JavaPairRDD<String, String> readRDD = sc.wholeTextFiles(tempDirName, 3);
    List<Tuple2<String, String>> result = readRDD.collect();

    for (Tuple2<String, String> res : result) {
      // Note that the paths from `wholeTextFiles` are in URI format on Windows,
      // for example, file:/C:/a/b/c.
      assertEquals(res._2(), container.get(new Path(res._1()).toUri().getPath()));
    }
  }

  @Test
  public void textFilesCompressed() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    rdd.saveAsTextFile(outputDir, DefaultCodec.class);

    // Try reading it in as a text file RDD
    List<String> expected = Arrays.asList("1", "2", "3", "4");
    JavaRDD<String> readRDD = sc.textFile(outputDir);
    assertEquals(expected, readRDD.collect());
  }

  @Test
  public void sequenceFile() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(pair -> new Tuple2<>(new IntWritable(pair._1()), new Text(pair._2())))
      .saveAsHadoopFile(outputDir, IntWritable.class, Text.class, SequenceFileOutputFormat.class);

    // Try reading the output back as an object file
    JavaPairRDD<Integer, String> readRDD = sc.sequenceFile(outputDir, IntWritable.class,
      Text.class).mapToPair(pair -> new Tuple2<>(pair._1().get(), pair._2().toString()));
    assertEquals(pairs, readRDD.collect());
  }

  @Test
  public void binaryFiles() throws Exception {
    // Reusing the wholeText files example
    byte[] content1 = "spark is easy to use.\n".getBytes(StandardCharsets.UTF_8);

    String tempDirName = tempDir.getAbsolutePath();
    File file1 = new File(tempDirName + "/part-00000");

    FileOutputStream fos1 = new FileOutputStream(file1);

    try (FileChannel channel1 = fos1.getChannel()) {
      ByteBuffer bbuf = ByteBuffer.wrap(content1);
      channel1.write(bbuf);
    }
    JavaPairRDD<String, PortableDataStream> readRDD = sc.binaryFiles(tempDirName, 3);
    List<Tuple2<String, PortableDataStream>> result = readRDD.collect();
    for (Tuple2<String, PortableDataStream> res : result) {
      assertArrayEquals(content1, res._2().toArray());
    }
  }

  @Test
  public void binaryFilesCaching() throws Exception {
    // Reusing the wholeText files example
    byte[] content1 = "spark is easy to use.\n".getBytes(StandardCharsets.UTF_8);

    String tempDirName = tempDir.getAbsolutePath();
    File file1 = new File(tempDirName + "/part-00000");

    FileOutputStream fos1 = new FileOutputStream(file1);

    try (FileChannel channel1 = fos1.getChannel()) {
      ByteBuffer bbuf = ByteBuffer.wrap(content1);
      channel1.write(bbuf);
    }

    JavaPairRDD<String, PortableDataStream> readRDD = sc.binaryFiles(tempDirName).cache();
    readRDD.foreach(pair -> pair._2().toArray()); // force the file to read

    List<Tuple2<String, PortableDataStream>> result = readRDD.collect();
    for (Tuple2<String, PortableDataStream> res : result) {
      assertArrayEquals(content1, res._2().toArray());
    }
  }

  @Test
  public void binaryRecords() throws Exception {
    // Reusing the wholeText files example
    byte[] content1 = "spark isn't always easy to use.\n".getBytes(StandardCharsets.UTF_8);
    int numOfCopies = 10;
    String tempDirName = tempDir.getAbsolutePath();
    File file1 = new File(tempDirName + "/part-00000");

    FileOutputStream fos1 = new FileOutputStream(file1);

    try (FileChannel channel1 = fos1.getChannel()) {
      for (int i = 0; i < numOfCopies; i++) {
        ByteBuffer bbuf = ByteBuffer.wrap(content1);
        channel1.write(bbuf);
      }
    }

    JavaRDD<byte[]> readRDD = sc.binaryRecords(tempDirName, content1.length);
    assertEquals(numOfCopies,readRDD.count());
    List<byte[]> result = readRDD.collect();
    for (byte[] res : result) {
      assertArrayEquals(content1, res);
    }
  }

  @Test
  public void writeWithNewAPIHadoopFile() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(pair -> new Tuple2<>(new IntWritable(pair._1()), new Text(pair._2())))
      .saveAsNewAPIHadoopFile(outputDir, IntWritable.class, Text.class,
        org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);

    JavaPairRDD<IntWritable, Text> output =
      sc.sequenceFile(outputDir, IntWritable.class, Text.class);
    assertEquals(pairs.toString(), output.map(Tuple2::toString).collect().toString());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void readWithNewAPIHadoopFile() throws IOException {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(pair -> new Tuple2<>(new IntWritable(pair._1()), new Text(pair._2())))
      .saveAsHadoopFile(outputDir, IntWritable.class, Text.class, SequenceFileOutputFormat.class);

    JavaPairRDD<IntWritable, Text> output = sc.newAPIHadoopFile(outputDir,
      org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class,
      IntWritable.class, Text.class, Job.getInstance().getConfiguration());
    assertEquals(pairs.toString(), output.map(Tuple2::toString).collect().toString());
  }

  @Test
  public void objectFilesOfInts() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    rdd.saveAsObjectFile(outputDir);
    // Try reading the output back as an object file
    List<Integer> expected = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> readRDD = sc.objectFile(outputDir);
    assertEquals(expected, readRDD.collect());
  }

  @Test
  public void objectFilesOfComplexTypes() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);
    rdd.saveAsObjectFile(outputDir);
    // Try reading the output back as an object file
    JavaRDD<Tuple2<Integer, String>> readRDD = sc.objectFile(outputDir);
    assertEquals(pairs, readRDD.collect());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void hadoopFile() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(pair -> new Tuple2<>(new IntWritable(pair._1()), new Text(pair._2())))
      .saveAsHadoopFile(outputDir, IntWritable.class, Text.class, SequenceFileOutputFormat.class);

    JavaPairRDD<IntWritable, Text> output = sc.hadoopFile(outputDir,
      SequenceFileInputFormat.class, IntWritable.class, Text.class);
    assertEquals(pairs.toString(), output.map(Tuple2::toString).collect().toString());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void hadoopFileCompressed() {
    String outputDir = new File(tempDir, "output_compressed").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(pair -> new Tuple2<>(new IntWritable(pair._1()), new Text(pair._2())))
      .saveAsHadoopFile(outputDir, IntWritable.class, Text.class,
        SequenceFileOutputFormat.class, DefaultCodec.class);

    JavaPairRDD<IntWritable, Text> output = sc.hadoopFile(outputDir,
      SequenceFileInputFormat.class, IntWritable.class, Text.class);

    assertEquals(pairs.toString(), output.map(Tuple2::toString).collect().toString());
  }

  @Test
  public void zip() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    JavaDoubleRDD doubles = rdd.mapToDouble(Integer::doubleValue);
    JavaPairRDD<Integer, Double> zipped = rdd.zip(doubles);
    zipped.count();
  }

  @Test
  public void zipPartitions() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("1", "2", "3", "4"), 2);
    FlatMapFunction2<Iterator<Integer>, Iterator<String>, Integer> sizesFn =
        (i, s) -> Arrays.asList(Iterators.size(i), Iterators.size(s)).iterator();

    JavaRDD<Integer> sizes = rdd1.zipPartitions(rdd2, sizesFn);
    assertEquals("[3, 2, 3, 2]", sizes.collect().toString());
  }

  @Test
  public void keyBy() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2));
    List<Tuple2<String, Integer>> s = rdd.keyBy(Object::toString).collect();
    assertEquals(new Tuple2<>("1", 1), s.get(0));
    assertEquals(new Tuple2<>("2", 2), s.get(1));
  }

  @Test
  public void checkpointAndComputation() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    sc.setCheckpointDir(tempDir.getAbsolutePath());
    assertFalse(rdd.isCheckpointed());
    rdd.checkpoint();
    rdd.count(); // Forces the DAG to cause a checkpoint
    assertTrue(rdd.isCheckpointed());
    assertEquals(Arrays.asList(1, 2, 3, 4, 5), rdd.collect());
  }

  @Test
  public void checkpointAndRestore() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    sc.setCheckpointDir(tempDir.getAbsolutePath());
    assertFalse(rdd.isCheckpointed());
    rdd.checkpoint();
    rdd.count(); // Forces the DAG to cause a checkpoint
    assertTrue(rdd.isCheckpointed());

    assertTrue(rdd.getCheckpointFile().isPresent());
    JavaRDD<Integer> recovered = sc.checkpointFile(rdd.getCheckpointFile().get());
    assertEquals(Arrays.asList(1, 2, 3, 4, 5), recovered.collect());
  }

  @Test
  public void combineByKey() {
    JavaRDD<Integer> originalRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
    Function<Integer, Integer> keyFunction = v1 -> v1 % 3;
    Function<Integer, Integer> createCombinerFunction = v1 -> v1;

    Function2<Integer, Integer, Integer> mergeValueFunction = (v1, v2) -> v1 + v2;

    JavaPairRDD<Integer, Integer> combinedRDD = originalRDD.keyBy(keyFunction)
      .combineByKey(createCombinerFunction, mergeValueFunction, mergeValueFunction);
    Map<Integer, Integer> results = combinedRDD.collectAsMap();
    ImmutableMap<Integer, Integer> expected = ImmutableMap.of(0, 9, 1, 5, 2, 7);
    assertEquals(expected, results);

    Partitioner defaultPartitioner = Partitioner.defaultPartitioner(
      combinedRDD.rdd(),
      JavaConverters.collectionAsScalaIterableConverter(
        Collections.<RDD<?>>emptyList()).asScala().toSeq());
    combinedRDD = originalRDD.keyBy(keyFunction)
      .combineByKey(
        createCombinerFunction,
        mergeValueFunction,
        mergeValueFunction,
        defaultPartitioner,
        false,
        new KryoSerializer(new SparkConf()));
    results = combinedRDD.collectAsMap();
    assertEquals(expected, results);
  }

  @Test
  public void mapOnPairRDD() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4));
    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(i -> new Tuple2<>(i, i % 2));
    JavaPairRDD<Integer, Integer> rdd3 = rdd2.mapToPair(in -> new Tuple2<>(in._2(), in._1()));
    assertEquals(Arrays.asList(
      new Tuple2<>(1, 1),
      new Tuple2<>(0, 2),
      new Tuple2<>(1, 3),
      new Tuple2<>(0, 4)), rdd3.collect());
  }

  @Test
  public void collectPartitions() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7), 3);

    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(i -> new Tuple2<>(i, i % 2));

    List<Integer>[] parts = rdd1.collectPartitions(new int[] {0});
    assertEquals(Arrays.asList(1, 2), parts[0]);

    parts = rdd1.collectPartitions(new int[] {1, 2});
    assertEquals(Arrays.asList(3, 4), parts[0]);
    assertEquals(Arrays.asList(5, 6, 7), parts[1]);

    assertEquals(
      Arrays.asList(new Tuple2<>(1, 1), new Tuple2<>(2, 0)),
      rdd2.collectPartitions(new int[] {0})[0]);

    List<Tuple2<Integer,Integer>>[] parts2 = rdd2.collectPartitions(new int[] {1, 2});
    assertEquals(Arrays.asList(new Tuple2<>(3, 1), new Tuple2<>(4, 0)), parts2[0]);
    assertEquals(
      Arrays.asList(
        new Tuple2<>(5, 1),
        new Tuple2<>(6, 0),
        new Tuple2<>(7, 1)),
      parts2[1]);
  }

  @Test
  public void countApproxDistinct() {
    List<Integer> arrayData = new ArrayList<>();
    int size = 100;
    for (int i = 0; i < 100000; i++) {
      arrayData.add(i % size);
    }
    JavaRDD<Integer> simpleRdd = sc.parallelize(arrayData, 10);
    assertTrue(Math.abs((simpleRdd.countApproxDistinct(0.05) - size) / (size * 1.0)) <= 0.1);
  }

  @Test
  public void countApproxDistinctByKey() {
    List<Tuple2<Integer, Integer>> arrayData = new ArrayList<>();
    for (int i = 10; i < 100; i++) {
      for (int j = 0; j < i; j++) {
        arrayData.add(new Tuple2<>(i, j));
      }
    }
    double relativeSD = 0.001;
    JavaPairRDD<Integer, Integer> pairRdd = sc.parallelizePairs(arrayData);
    List<Tuple2<Integer, Long>> res =  pairRdd.countApproxDistinctByKey(relativeSD, 8).collect();
    for (Tuple2<Integer, Long> resItem : res) {
      double count = resItem._1();
      long resCount = resItem._2();
      double error = Math.abs((resCount - count) / count);
      assertTrue(error < 0.1);
    }
  }

  @Test
  public void collectAsMapWithIntArrayValues() {
    // Regression test for SPARK-1040
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1));
    JavaPairRDD<Integer, int[]> pairRDD = rdd.mapToPair(x -> new Tuple2<>(x, new int[]{x}));
    pairRDD.collect();  // Works fine
    pairRDD.collectAsMap();  // Used to crash with ClassCastException
  }

  @SuppressWarnings("unchecked")
  @Test
  public void collectAsMapAndSerialize() throws Exception {
    JavaPairRDD<String,Integer> rdd =
        sc.parallelizePairs(Arrays.asList(new Tuple2<>("foo", 1)));
    Map<String,Integer> map = rdd.collectAsMap();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    new ObjectOutputStream(bytes).writeObject(map);
    Map<String,Integer> deserializedMap = (Map<String,Integer>)
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())).readObject();
    assertEquals(1, deserializedMap.get("foo").intValue());
  }

  @Test
  public void sampleByKey() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 3);
    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(i -> new Tuple2<>(i % 2, 1));
    Map<Integer, Double> fractions = new HashMap<>();
    fractions.put(0, 0.5);
    fractions.put(1, 1.0);
    JavaPairRDD<Integer, Integer> wr = rdd2.sampleByKey(true, fractions, 1L);
    Map<Integer, Long> wrCounts = wr.countByKey();
    assertEquals(2, wrCounts.size());
    assertTrue(wrCounts.get(0) > 0);
    assertTrue(wrCounts.get(1) > 0);
    JavaPairRDD<Integer, Integer> wor = rdd2.sampleByKey(false, fractions, 1L);
    Map<Integer, Long> worCounts = wor.countByKey();
    assertEquals(2, worCounts.size());
    assertTrue(worCounts.get(0) > 0);
    assertTrue(worCounts.get(1) > 0);
  }

  @Test
  public void sampleByKeyExact() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 3);
    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(i -> new Tuple2<>(i % 2, 1));
    Map<Integer, Double> fractions = new HashMap<>();
    fractions.put(0, 0.5);
    fractions.put(1, 1.0);
    JavaPairRDD<Integer, Integer> wrExact = rdd2.sampleByKeyExact(true, fractions, 1L);
    Map<Integer, Long> wrExactCounts = wrExact.countByKey();
    assertEquals(2, wrExactCounts.size());
    assertEquals(2, (long) wrExactCounts.get(0));
    assertEquals(4, (long) wrExactCounts.get(1));
    JavaPairRDD<Integer, Integer> worExact = rdd2.sampleByKeyExact(false, fractions, 1L);
    Map<Integer, Long> worExactCounts = worExact.countByKey();
    assertEquals(2, worExactCounts.size());
    assertEquals(2, (long) worExactCounts.get(0));
    assertEquals(4, (long) worExactCounts.get(1));
  }

  private static class SomeCustomClass implements Serializable {
    SomeCustomClass() {
      // Intentionally left blank
    }
  }

  @Test
  public void collectUnderlyingScalaRDD() {
    List<SomeCustomClass> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      data.add(new SomeCustomClass());
    }
    JavaRDD<SomeCustomClass> rdd = sc.parallelize(data);
    SomeCustomClass[] collected =
      (SomeCustomClass[]) rdd.rdd().retag(SomeCustomClass.class).collect();
    assertEquals(data.size(), collected.length);
  }

  private static final class BuggyMapFunction<T> implements Function<T, T> {

    @Override
    public T call(T x) {
      throw new IllegalStateException("Custom exception!");
    }
  }

  @Test
  public void collectAsync() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<List<Integer>> future = rdd.collectAsync();
    List<Integer> result = future.get();
    assertEquals(data, result);
    assertFalse(future.isCancelled());
    assertTrue(future.isDone());
    assertEquals(1, future.jobIds().size());
  }

  @Test
  public void takeAsync() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<List<Integer>> future = rdd.takeAsync(1);
    List<Integer> result = future.get();
    assertEquals(1, result.size());
    assertEquals((Integer) 1, result.get(0));
    assertFalse(future.isCancelled());
    assertTrue(future.isDone());
    assertEquals(1, future.jobIds().size());
  }

  @Test
  public void foreachAsync() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<Void> future = rdd.foreachAsync(integer -> {});
    future.get();
    assertFalse(future.isCancelled());
    assertTrue(future.isDone());
    assertEquals(1, future.jobIds().size());
  }

  @Test
  public void countAsync() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<Long> future = rdd.countAsync();
    long count = future.get();
    assertEquals(data.size(), count);
    assertFalse(future.isCancelled());
    assertTrue(future.isDone());
    assertEquals(1, future.jobIds().size());
  }

  @Test
  public void testAsyncActionCancellation() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<Void> future = rdd.foreachAsync(integer -> {
      Thread.sleep(10000);  // To ensure that the job won't finish before it's cancelled.
    });
    future.cancel(true);
    assertTrue(future.isCancelled());
    assertTrue(future.isDone());
    assertThrows(CancellationException.class, () -> future.get(2000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testAsyncActionErrorWrapping() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<Long> future = rdd.map(new BuggyMapFunction<>()).countAsync();
    ExecutionException ee = assertThrows(ExecutionException.class,
      () -> future.get(2, TimeUnit.SECONDS));
    assertTrue(Throwables.getStackTraceAsString(ee).contains("Custom exception!"));
    assertTrue(future.isDone());
  }

  static class Class1 {}
  static class Class2 {}

  @Test
  public void testRegisterKryoClasses() {
    SparkConf conf = new SparkConf();
    conf.registerKryoClasses(new Class<?>[]{ Class1.class, Class2.class });
    assertEquals(
      Class1.class.getName() + "," + Class2.class.getName(),
      conf.get("spark.kryo.classesToRegister"));
  }

  @Test
  public void testGetPersistentRDDs() {
    java.util.Map<Integer, JavaRDD<?>> cachedRddsMap = sc.getPersistentRDDs();
    assertTrue(cachedRddsMap.isEmpty());
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b")).setName("RDD1").cache();
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("c", "d")).setName("RDD2").cache();
    cachedRddsMap = sc.getPersistentRDDs();
    assertEquals(2, cachedRddsMap.size());
    assertEquals("RDD1", cachedRddsMap.get(0).name());
    assertEquals("RDD2", cachedRddsMap.get(1).name());
  }

}
