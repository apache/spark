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

package org.apache.spark;

import java.io.*;
import java.net.URI;
import java.util.*;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.base.Optional;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.StatCounter;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaAPISuite implements Serializable {
  private transient JavaSparkContext sc;
  private transient File tempDir;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaAPISuite");
    tempDir = Files.createTempDir();
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
    Assert.assertEquals(4, sUnion.count());
    // List
    List<JavaRDD<String>> list = new ArrayList<JavaRDD<String>>();
    list.add(s2);
    sUnion = sc.union(s1, list);
    Assert.assertEquals(4, sUnion.count());

    // Union of JavaDoubleRDDs
    List<Double> doubles = Arrays.asList(1.0, 2.0);
    JavaDoubleRDD d1 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD d2 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD dUnion = sc.union(d1, d2);
    Assert.assertEquals(4, dUnion.count());

    // Union of JavaPairRDDs
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<Tuple2<Integer, Integer>>();
    pairs.add(new Tuple2<Integer, Integer>(1, 2));
    pairs.add(new Tuple2<Integer, Integer>(3, 4));
    JavaPairRDD<Integer, Integer> p1 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> p2 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> pUnion = sc.union(p1, p2);
    Assert.assertEquals(4, pUnion.count());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void intersection() {
    List<Integer> ints1 = Arrays.asList(1, 10, 2, 3, 4, 5);
    List<Integer> ints2 = Arrays.asList(1, 6, 2, 3, 7, 8);
    JavaRDD<Integer> s1 = sc.parallelize(ints1);
    JavaRDD<Integer> s2 = sc.parallelize(ints2);

    JavaRDD<Integer> intersections = s1.intersection(s2);
    Assert.assertEquals(3, intersections.count());

    JavaRDD<Integer> empty = sc.emptyRDD();
    JavaRDD<Integer> emptyIntersection = empty.intersection(s2);
    Assert.assertEquals(0, emptyIntersection.count());

    List<Double> doubles = Arrays.asList(1.0, 2.0);
    JavaDoubleRDD d1 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD d2 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD dIntersection = d1.intersection(d2);
    Assert.assertEquals(2, dIntersection.count());

    List<Tuple2<Integer, Integer>> pairs = new ArrayList<Tuple2<Integer, Integer>>();
    pairs.add(new Tuple2<Integer, Integer>(1, 2));
    pairs.add(new Tuple2<Integer, Integer>(3, 4));
    JavaPairRDD<Integer, Integer> p1 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> p2 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> pIntersection = p1.intersection(p2);
    Assert.assertEquals(2, pIntersection.count());
  }

  @Test
  public void sample() {
    List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    JavaRDD<Integer> rdd = sc.parallelize(ints);
    JavaRDD<Integer> sample20 = rdd.sample(true, 0.2, 11);
    // expected 2 but of course result varies randomly a bit
    Assert.assertEquals(3, sample20.count());
    JavaRDD<Integer> sample20NoReplacement = rdd.sample(false, 0.2, 11);
    Assert.assertEquals(2, sample20NoReplacement.count());
  }

  @Test
  public void randomSplit() {
    List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    JavaRDD<Integer> rdd = sc.parallelize(ints);
    JavaRDD<Integer>[] splits = rdd.randomSplit(new double[] { 0.4, 0.6, 1.0 }, 11);
    Assert.assertEquals(3, splits.length);
    Assert.assertEquals(2, splits[0].count());
    Assert.assertEquals(3, splits[1].count());
    Assert.assertEquals(5, splits[2].count());
  }

  @Test
  public void sortByKey() {
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<Tuple2<Integer, Integer>>();
    pairs.add(new Tuple2<Integer, Integer>(0, 4));
    pairs.add(new Tuple2<Integer, Integer>(3, 2));
    pairs.add(new Tuple2<Integer, Integer>(-1, 1));

    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs);

    // Default comparator
    JavaPairRDD<Integer, Integer> sortedRDD = rdd.sortByKey();
    Assert.assertEquals(new Tuple2<Integer, Integer>(-1, 1), sortedRDD.first());
    List<Tuple2<Integer, Integer>> sortedPairs = sortedRDD.collect();
    Assert.assertEquals(new Tuple2<Integer, Integer>(0, 4), sortedPairs.get(1));
    Assert.assertEquals(new Tuple2<Integer, Integer>(3, 2), sortedPairs.get(2));

    // Custom comparator
    sortedRDD = rdd.sortByKey(Collections.<Integer>reverseOrder(), false);
    Assert.assertEquals(new Tuple2<Integer, Integer>(-1, 1), sortedRDD.first());
    sortedPairs = sortedRDD.collect();
    Assert.assertEquals(new Tuple2<Integer, Integer>(0, 4), sortedPairs.get(1));
    Assert.assertEquals(new Tuple2<Integer, Integer>(3, 2), sortedPairs.get(2));
  }

  @Test
  public void emptyRDD() {
    JavaRDD<String> rdd = sc.emptyRDD();
    Assert.assertEquals("Empty RDD shouldn't have any values", 0, rdd.count());
  }

  @Test
  public void sortBy() {
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<Tuple2<Integer, Integer>>();
    pairs.add(new Tuple2<Integer, Integer>(0, 4));
    pairs.add(new Tuple2<Integer, Integer>(3, 2));
    pairs.add(new Tuple2<Integer, Integer>(-1, 1));

    JavaRDD<Tuple2<Integer, Integer>> rdd = sc.parallelize(pairs);

    // compare on first value
    JavaRDD<Tuple2<Integer, Integer>> sortedRDD = rdd.sortBy(new Function<Tuple2<Integer, Integer>, Integer>() {
      public Integer call(Tuple2<Integer, Integer> t) throws Exception {
        return t._1();
      }
    }, true, 2);

    Assert.assertEquals(new Tuple2<Integer, Integer>(-1, 1), sortedRDD.first());
    List<Tuple2<Integer, Integer>> sortedPairs = sortedRDD.collect();
    Assert.assertEquals(new Tuple2<Integer, Integer>(0, 4), sortedPairs.get(1));
    Assert.assertEquals(new Tuple2<Integer, Integer>(3, 2), sortedPairs.get(2));

    // compare on second value
    sortedRDD = rdd.sortBy(new Function<Tuple2<Integer, Integer>, Integer>() {
      public Integer call(Tuple2<Integer, Integer> t) throws Exception {
        return t._2();
      }
    }, true, 2);
    Assert.assertEquals(new Tuple2<Integer, Integer>(-1, 1), sortedRDD.first());
    sortedPairs = sortedRDD.collect();
    Assert.assertEquals(new Tuple2<Integer, Integer>(3, 2), sortedPairs.get(1));
    Assert.assertEquals(new Tuple2<Integer, Integer>(0, 4), sortedPairs.get(2));
  }

  @Test
  public void foreach() {
    final Accumulator<Integer> accum = sc.accumulator(0);
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello", "World"));
    rdd.foreach(new VoidFunction<String>() {
      @Override
      public void call(String s) throws IOException {
        accum.add(1);
      }
    });
    Assert.assertEquals(2, accum.value().intValue());
  }

  @Test
  public void toLocalIterator() {
    List<Integer> correct = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> rdd = sc.parallelize(correct);
    List<Integer> result = Lists.newArrayList(rdd.toLocalIterator());
    Assert.assertEquals(correct, result);
  }

  @Test
  public void zipWithUniqueId() {
    List<Integer> dataArray = Arrays.asList(1, 2, 3, 4);
    JavaPairRDD<Integer, Long> zip = sc.parallelize(dataArray).zipWithUniqueId();
    JavaRDD<Long> indexes = zip.values();
    Assert.assertEquals(4, new HashSet<Long>(indexes.collect()).size());
  }

  @Test
  public void zipWithIndex() {
    List<Integer> dataArray = Arrays.asList(1, 2, 3, 4);
    JavaPairRDD<Integer, Long> zip = sc.parallelize(dataArray).zipWithIndex();
    JavaRDD<Long> indexes = zip.values();
    List<Long> correctIndexes = Arrays.asList(0L, 1L, 2L, 3L);
    Assert.assertEquals(correctIndexes, indexes.collect());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void lookup() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<String, String>("Apples", "Fruit"),
      new Tuple2<String, String>("Oranges", "Fruit"),
      new Tuple2<String, String>("Oranges", "Citrus")
      ));
    Assert.assertEquals(2, categories.lookup("Oranges").size());
    Assert.assertEquals(2, Iterables.size(categories.groupByKey().lookup("Oranges").get(0)));
  }

  @Test
  public void groupBy() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function<Integer, Boolean> isOdd = new Function<Integer, Boolean>() {
      @Override
      public Boolean call(Integer x) {
        return x % 2 == 0;
      }
    };
    JavaPairRDD<Boolean, Iterable<Integer>> oddsAndEvens = rdd.groupBy(isOdd);
    Assert.assertEquals(2, oddsAndEvens.count());
    Assert.assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    Assert.assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds

    oddsAndEvens = rdd.groupBy(isOdd, 1);
    Assert.assertEquals(2, oddsAndEvens.count());
    Assert.assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    Assert.assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds
  }

  @SuppressWarnings("unchecked")
  @Test
  public void cogroup() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<String, String>("Apples", "Fruit"),
      new Tuple2<String, String>("Oranges", "Fruit"),
      new Tuple2<String, String>("Oranges", "Citrus")
      ));
    JavaPairRDD<String, Integer> prices = sc.parallelizePairs(Arrays.asList(
      new Tuple2<String, Integer>("Oranges", 2),
      new Tuple2<String, Integer>("Apples", 3)
    ));
    JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> cogrouped =
        categories.cogroup(prices);
    Assert.assertEquals("[Fruit, Citrus]",
                        Iterables.toString(cogrouped.lookup("Oranges").get(0)._1()));
    Assert.assertEquals("[2]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._2()));

    cogrouped.collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void cogroup3() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<String, String>("Apples", "Fruit"),
      new Tuple2<String, String>("Oranges", "Fruit"),
      new Tuple2<String, String>("Oranges", "Citrus")
      ));
    JavaPairRDD<String, Integer> prices = sc.parallelizePairs(Arrays.asList(
      new Tuple2<String, Integer>("Oranges", 2),
      new Tuple2<String, Integer>("Apples", 3)
    ));
    JavaPairRDD<String, Integer> quantities = sc.parallelizePairs(Arrays.asList(
      new Tuple2<String, Integer>("Oranges", 21),
      new Tuple2<String, Integer>("Apples", 42)
    ));

    JavaPairRDD<String, Tuple3<Iterable<String>, Iterable<Integer>, Iterable<Integer>>> cogrouped =
        categories.cogroup(prices, quantities);
    Assert.assertEquals("[Fruit, Citrus]",
                        Iterables.toString(cogrouped.lookup("Oranges").get(0)._1()));
    Assert.assertEquals("[2]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._2()));
    Assert.assertEquals("[42]", Iterables.toString(cogrouped.lookup("Apples").get(0)._3()));


    cogrouped.collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void cogroup4() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<String, String>("Apples", "Fruit"),
      new Tuple2<String, String>("Oranges", "Fruit"),
      new Tuple2<String, String>("Oranges", "Citrus")
      ));
    JavaPairRDD<String, Integer> prices = sc.parallelizePairs(Arrays.asList(
      new Tuple2<String, Integer>("Oranges", 2),
      new Tuple2<String, Integer>("Apples", 3)
    ));
    JavaPairRDD<String, Integer> quantities = sc.parallelizePairs(Arrays.asList(
      new Tuple2<String, Integer>("Oranges", 21),
      new Tuple2<String, Integer>("Apples", 42)
    ));
    JavaPairRDD<String, String> countries = sc.parallelizePairs(Arrays.asList(
      new Tuple2<String, String>("Oranges", "BR"),
      new Tuple2<String, String>("Apples", "US")
    ));

    JavaPairRDD<String, Tuple4<Iterable<String>, Iterable<Integer>, Iterable<Integer>, Iterable<String>>> cogrouped =
        categories.cogroup(prices, quantities, countries);
    Assert.assertEquals("[Fruit, Citrus]",
                        Iterables.toString(cogrouped.lookup("Oranges").get(0)._1()));
    Assert.assertEquals("[2]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._2()));
    Assert.assertEquals("[42]", Iterables.toString(cogrouped.lookup("Apples").get(0)._3()));
    Assert.assertEquals("[BR]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._4()));

    cogrouped.collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void leftOuterJoin() {
    JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
      new Tuple2<Integer, Integer>(1, 1),
      new Tuple2<Integer, Integer>(1, 2),
      new Tuple2<Integer, Integer>(2, 1),
      new Tuple2<Integer, Integer>(3, 1)
      ));
    JavaPairRDD<Integer, Character> rdd2 = sc.parallelizePairs(Arrays.asList(
      new Tuple2<Integer, Character>(1, 'x'),
      new Tuple2<Integer, Character>(2, 'y'),
      new Tuple2<Integer, Character>(2, 'z'),
      new Tuple2<Integer, Character>(4, 'w')
    ));
    List<Tuple2<Integer,Tuple2<Integer,Optional<Character>>>> joined =
      rdd1.leftOuterJoin(rdd2).collect();
    Assert.assertEquals(5, joined.size());
    Tuple2<Integer,Tuple2<Integer,Optional<Character>>> firstUnmatched =
      rdd1.leftOuterJoin(rdd2).filter(
        new Function<Tuple2<Integer, Tuple2<Integer, Optional<Character>>>, Boolean>() {
          @Override
          public Boolean call(Tuple2<Integer, Tuple2<Integer, Optional<Character>>> tup) {
            return !tup._2()._2().isPresent();
          }
      }).first();
    Assert.assertEquals(3, firstUnmatched._1().intValue());
  }

  @Test
  public void foldReduce() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function2<Integer, Integer, Integer> add = new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    };

    int sum = rdd.fold(0, add);
    Assert.assertEquals(33, sum);

    sum = rdd.reduce(add);
    Assert.assertEquals(33, sum);
  }

  @Test
  public void aggregateByKey() {
    JavaPairRDD<Integer, Integer> pairs = sc.parallelizePairs(
      Arrays.asList(
        new Tuple2<Integer, Integer>(1, 1),
        new Tuple2<Integer, Integer>(1, 1),
        new Tuple2<Integer, Integer>(3, 2),
        new Tuple2<Integer, Integer>(5, 1),
        new Tuple2<Integer, Integer>(5, 3)), 2);

    Map<Integer, Set<Integer>> sets = pairs.aggregateByKey(new HashSet<Integer>(),
      new Function2<Set<Integer>, Integer, Set<Integer>>() {
        @Override
        public Set<Integer> call(Set<Integer> a, Integer b) {
          a.add(b);
          return a;
        }
      },
      new Function2<Set<Integer>, Set<Integer>, Set<Integer>>() {
        @Override
        public Set<Integer> call(Set<Integer> a, Set<Integer> b) {
          a.addAll(b);
          return a;
        }
      }).collectAsMap();
    Assert.assertEquals(3, sets.size());
    Assert.assertEquals(new HashSet<Integer>(Arrays.asList(1)), sets.get(1));
    Assert.assertEquals(new HashSet<Integer>(Arrays.asList(2)), sets.get(3));
    Assert.assertEquals(new HashSet<Integer>(Arrays.asList(1, 3)), sets.get(5));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void foldByKey() {
    List<Tuple2<Integer, Integer>> pairs = Arrays.asList(
      new Tuple2<Integer, Integer>(2, 1),
      new Tuple2<Integer, Integer>(2, 1),
      new Tuple2<Integer, Integer>(1, 1),
      new Tuple2<Integer, Integer>(3, 2),
      new Tuple2<Integer, Integer>(3, 1)
    );
    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> sums = rdd.foldByKey(0,
      new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer a, Integer b) {
          return a + b;
        }
    });
    Assert.assertEquals(1, sums.lookup(1).get(0).intValue());
    Assert.assertEquals(2, sums.lookup(2).get(0).intValue());
    Assert.assertEquals(3, sums.lookup(3).get(0).intValue());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void reduceByKey() {
    List<Tuple2<Integer, Integer>> pairs = Arrays.asList(
      new Tuple2<Integer, Integer>(2, 1),
      new Tuple2<Integer, Integer>(2, 1),
      new Tuple2<Integer, Integer>(1, 1),
      new Tuple2<Integer, Integer>(3, 2),
      new Tuple2<Integer, Integer>(3, 1)
    );
    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> counts = rdd.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer a, Integer b) {
         return a + b;
        }
    });
    Assert.assertEquals(1, counts.lookup(1).get(0).intValue());
    Assert.assertEquals(2, counts.lookup(2).get(0).intValue());
    Assert.assertEquals(3, counts.lookup(3).get(0).intValue());

    Map<Integer, Integer> localCounts = counts.collectAsMap();
    Assert.assertEquals(1, localCounts.get(1).intValue());
    Assert.assertEquals(2, localCounts.get(2).intValue());
    Assert.assertEquals(3, localCounts.get(3).intValue());

    localCounts = rdd.reduceByKeyLocally(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    });
    Assert.assertEquals(1, localCounts.get(1).intValue());
    Assert.assertEquals(2, localCounts.get(2).intValue());
    Assert.assertEquals(3, localCounts.get(3).intValue());
  }

  @Test
  public void approximateResults() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Map<Integer, Long> countsByValue = rdd.countByValue();
    Assert.assertEquals(2, countsByValue.get(1).longValue());
    Assert.assertEquals(1, countsByValue.get(13).longValue());

    PartialResult<Map<Integer, BoundedDouble>> approx = rdd.countByValueApprox(1);
    Map<Integer, BoundedDouble> finalValue = approx.getFinalValue();
    Assert.assertEquals(2.0, finalValue.get(1).mean(), 0.01);
    Assert.assertEquals(1.0, finalValue.get(13).mean(), 0.01);
  }

  @Test
  public void take() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Assert.assertEquals(1, rdd.first().intValue());
    rdd.take(2);
    rdd.takeSample(false, 2, 42);
  }

  @Test
  public void cartesian() {
    JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 1.0, 2.0, 3.0, 5.0, 8.0));
    JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList("Hello", "World"));
    JavaPairRDD<String, Double> cartesian = stringRDD.cartesian(doubleRDD);
    Assert.assertEquals(new Tuple2<String, Double>("Hello", 1.0), cartesian.first());
  }

  @Test
  public void javaDoubleRDD() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 1.0, 2.0, 3.0, 5.0, 8.0));
    JavaDoubleRDD distinct = rdd.distinct();
    Assert.assertEquals(5, distinct.count());
    JavaDoubleRDD filter = rdd.filter(new Function<Double, Boolean>() {
      @Override
      public Boolean call(Double x) {
        return x > 2.0;
      }
    });
    Assert.assertEquals(3, filter.count());
    JavaDoubleRDD union = rdd.union(rdd);
    Assert.assertEquals(12, union.count());
    union = union.cache();
    Assert.assertEquals(12, union.count());

    Assert.assertEquals(20, rdd.sum(), 0.01);
    StatCounter stats = rdd.stats();
    Assert.assertEquals(20, stats.sum(), 0.01);
    Assert.assertEquals(20/6.0, rdd.mean(), 0.01);
    Assert.assertEquals(20/6.0, rdd.mean(), 0.01);
    Assert.assertEquals(6.22222, rdd.variance(), 0.01);
    Assert.assertEquals(7.46667, rdd.sampleVariance(), 0.01);
    Assert.assertEquals(2.49444, rdd.stdev(), 0.01);
    Assert.assertEquals(2.73252, rdd.sampleStdev(), 0.01);

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
    Assert.assertArrayEquals(expected_buckets, results._1, 0.1);
    Assert.assertArrayEquals(expected_counts, results._2);
    // Test with provided buckets
    long[] histogram = rdd.histogram(expected_buckets);
    Assert.assertArrayEquals(expected_counts, histogram);
  }

  @Test
  public void map() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    JavaDoubleRDD doubles = rdd.mapToDouble(new DoubleFunction<Integer>() {
      @Override
      public double call(Integer x) {
        return x.doubleValue();
      }
    }).cache();
    doubles.collect();
    JavaPairRDD<Integer, Integer> pairs = rdd.mapToPair(
        new PairFunction<Integer, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Integer x) {
            return new Tuple2<Integer, Integer>(x, x);
          }
        }).cache();
    pairs.collect();
    JavaRDD<String> strings = rdd.map(new Function<Integer, String>() {
      @Override
      public String call(Integer x) {
        return x.toString();
      }
    }).cache();
    strings.collect();
  }

  @Test
  public void flatMap() {
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello World!",
      "The quick brown fox jumps over the lazy dog."));
    JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
        return Arrays.asList(x.split(" "));
      }
    });
    Assert.assertEquals("Hello", words.first());
    Assert.assertEquals(11, words.count());

    JavaPairRDD<String, String> pairs = rdd.flatMapToPair(
      new PairFlatMapFunction<String, String, String>() {

        @Override
        public Iterable<Tuple2<String, String>> call(String s) {
          List<Tuple2<String, String>> pairs = new LinkedList<Tuple2<String, String>>();
          for (String word : s.split(" ")) {
            pairs.add(new Tuple2<String, String>(word, word));
          }
          return pairs;
        }
      }
    );
    Assert.assertEquals(new Tuple2<String, String>("Hello", "Hello"), pairs.first());
    Assert.assertEquals(11, pairs.count());

    JavaDoubleRDD doubles = rdd.flatMapToDouble(new DoubleFlatMapFunction<String>() {
      @Override
      public Iterable<Double> call(String s) {
        List<Double> lengths = new LinkedList<Double>();
        for (String word : s.split(" ")) {
          lengths.add((double) word.length());
        }
        return lengths;
      }
    });
    Assert.assertEquals(5.0, doubles.first(), 0.01);
    Assert.assertEquals(11, pairs.count());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mapsFromPairsToPairs() {
      List<Tuple2<Integer, String>> pairs = Arrays.asList(
              new Tuple2<Integer, String>(1, "a"),
              new Tuple2<Integer, String>(2, "aa"),
              new Tuple2<Integer, String>(3, "aaa")
      );
      JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(pairs);

      // Regression test for SPARK-668:
      JavaPairRDD<String, Integer> swapped = pairRDD.flatMapToPair(
          new PairFlatMapFunction<Tuple2<Integer, String>, String, Integer>() {
          @Override
          public Iterable<Tuple2<String, Integer>> call(Tuple2<Integer, String> item) {
              return Collections.singletonList(item.swap());
          }
      });
      swapped.collect();

      // There was never a bug here, but it's worth testing:
      pairRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(Tuple2<Integer, String> item) {
              return item.swap();
          }
      }).collect();
  }

  @Test
  public void mapPartitions() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
    JavaRDD<Integer> partitionSums = rdd.mapPartitions(
      new FlatMapFunction<Iterator<Integer>, Integer>() {
        @Override
        public Iterable<Integer> call(Iterator<Integer> iter) {
          int sum = 0;
          while (iter.hasNext()) {
            sum += iter.next();
          }
          return Collections.singletonList(sum);
        }
    });
    Assert.assertEquals("[3, 7]", partitionSums.collect().toString());
  }

  @Test
  public void repartition() {
    // Shrinking number of partitions
    JavaRDD<Integer> in1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 2);
    JavaRDD<Integer> repartitioned1 = in1.repartition(4);
    List<List<Integer>> result1 = repartitioned1.glom().collect();
    Assert.assertEquals(4, result1.size());
    for (List<Integer> l: result1) {
      Assert.assertTrue(l.size() > 0);
    }

    // Growing number of partitions
    JavaRDD<Integer> in2 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 4);
    JavaRDD<Integer> repartitioned2 = in2.repartition(2);
    List<List<Integer>> result2 = repartitioned2.glom().collect();
    Assert.assertEquals(2, result2.size());
    for (List<Integer> l: result2) {
      Assert.assertTrue(l.size() > 0);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void persist() {
    JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 1.0, 2.0, 3.0, 5.0, 8.0));
    doubleRDD = doubleRDD.persist(StorageLevel.DISK_ONLY());
    Assert.assertEquals(20, doubleRDD.sum(), 0.1);

    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<Integer, String>(1, "a"),
      new Tuple2<Integer, String>(2, "aa"),
      new Tuple2<Integer, String>(3, "aaa")
    );
    JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(pairs);
    pairRDD = pairRDD.persist(StorageLevel.DISK_ONLY());
    Assert.assertEquals("a", pairRDD.first()._2());

    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    rdd = rdd.persist(StorageLevel.DISK_ONLY());
    Assert.assertEquals(1, rdd.first().intValue());
  }

  @Test
  public void iterator() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);
    TaskContext context = new TaskContext(0, 0, 0, false, new TaskMetrics());
    Assert.assertEquals(1, rdd.iterator(rdd.partitions().get(0), context).next().intValue());
  }

  @Test
  public void glom() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
    Assert.assertEquals("[1, 2]", rdd.glom().first().toString());
  }

  // File input / output tests are largely adapted from FileSuite:

  @Test
  public void textFiles() throws IOException {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    rdd.saveAsTextFile(outputDir);
    // Read the plain text file and check it's OK
    File outputFile = new File(outputDir, "part-00000");
    String content = Files.toString(outputFile, Charsets.UTF_8);
    Assert.assertEquals("1\n2\n3\n4\n", content);
    // Also try reading it in as a text file RDD
    List<String> expected = Arrays.asList("1", "2", "3", "4");
    JavaRDD<String> readRDD = sc.textFile(outputDir);
    Assert.assertEquals(expected, readRDD.collect());
  }

  @Test
  public void wholeTextFiles() throws Exception {
    byte[] content1 = "spark is easy to use.\n".getBytes("utf-8");
    byte[] content2 = "spark is also easy to use.\n".getBytes("utf-8");

    String tempDirName = tempDir.getAbsolutePath();
    Files.write(content1, new File(tempDirName + "/part-00000"));
    Files.write(content2, new File(tempDirName + "/part-00001"));

    Map<String, String> container = new HashMap<String, String>();
    container.put(tempDirName+"/part-00000", new Text(content1).toString());
    container.put(tempDirName+"/part-00001", new Text(content2).toString());

    JavaPairRDD<String, String> readRDD = sc.wholeTextFiles(tempDirName, 3);
    List<Tuple2<String, String>> result = readRDD.collect();

    for (Tuple2<String, String> res : result) {
      Assert.assertEquals(res._2(), container.get(new URI(res._1()).getPath()));
    }
  }

  @Test
  public void textFilesCompressed() throws IOException {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    rdd.saveAsTextFile(outputDir, DefaultCodec.class);

    // Try reading it in as a text file RDD
    List<String> expected = Arrays.asList("1", "2", "3", "4");
    JavaRDD<String> readRDD = sc.textFile(outputDir);
    Assert.assertEquals(expected, readRDD.collect());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sequenceFile() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<Integer, String>(1, "a"),
      new Tuple2<Integer, String>(2, "aa"),
      new Tuple2<Integer, String>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(new PairFunction<Tuple2<Integer, String>, IntWritable, Text>() {
      @Override
      public Tuple2<IntWritable, Text> call(Tuple2<Integer, String> pair) {
        return new Tuple2<IntWritable, Text>(new IntWritable(pair._1()), new Text(pair._2()));
      }
    }).saveAsHadoopFile(outputDir, IntWritable.class, Text.class, SequenceFileOutputFormat.class);

    // Try reading the output back as an object file
    JavaPairRDD<Integer, String> readRDD = sc.sequenceFile(outputDir, IntWritable.class,
      Text.class).mapToPair(new PairFunction<Tuple2<IntWritable, Text>, Integer, String>() {
      @Override
      public Tuple2<Integer, String> call(Tuple2<IntWritable, Text> pair) {
        return new Tuple2<Integer, String>(pair._1().get(), pair._2().toString());
      }
    });
    Assert.assertEquals(pairs, readRDD.collect());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void writeWithNewAPIHadoopFile() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<Integer, String>(1, "a"),
      new Tuple2<Integer, String>(2, "aa"),
      new Tuple2<Integer, String>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(new PairFunction<Tuple2<Integer, String>, IntWritable, Text>() {
      @Override
      public Tuple2<IntWritable, Text> call(Tuple2<Integer, String> pair) {
        return new Tuple2<IntWritable, Text>(new IntWritable(pair._1()), new Text(pair._2()));
      }
    }).saveAsNewAPIHadoopFile(outputDir, IntWritable.class, Text.class,
      org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);

    JavaPairRDD<IntWritable, Text> output = sc.sequenceFile(outputDir, IntWritable.class,
      Text.class);
    Assert.assertEquals(pairs.toString(), output.map(new Function<Tuple2<IntWritable, Text>,
      String>() {
      @Override
      public String call(Tuple2<IntWritable, Text> x) {
        return x.toString();
      }
    }).collect().toString());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void readWithNewAPIHadoopFile() throws IOException {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<Integer, String>(1, "a"),
      new Tuple2<Integer, String>(2, "aa"),
      new Tuple2<Integer, String>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(new PairFunction<Tuple2<Integer, String>, IntWritable, Text>() {
      @Override
      public Tuple2<IntWritable, Text> call(Tuple2<Integer, String> pair) {
        return new Tuple2<IntWritable, Text>(new IntWritable(pair._1()), new Text(pair._2()));
      }
    }).saveAsHadoopFile(outputDir, IntWritable.class, Text.class, SequenceFileOutputFormat.class);

    JavaPairRDD<IntWritable, Text> output = sc.newAPIHadoopFile(outputDir,
      org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class, IntWritable.class,
      Text.class, new Job().getConfiguration());
    Assert.assertEquals(pairs.toString(), output.map(new Function<Tuple2<IntWritable, Text>,
      String>() {
      @Override
      public String call(Tuple2<IntWritable, Text> x) {
        return x.toString();
      }
    }).collect().toString());
  }

  @Test
  public void objectFilesOfInts() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    rdd.saveAsObjectFile(outputDir);
    // Try reading the output back as an object file
    List<Integer> expected = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> readRDD = sc.objectFile(outputDir);
    Assert.assertEquals(expected, readRDD.collect());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void objectFilesOfComplexTypes() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<Integer, String>(1, "a"),
      new Tuple2<Integer, String>(2, "aa"),
      new Tuple2<Integer, String>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);
    rdd.saveAsObjectFile(outputDir);
    // Try reading the output back as an object file
    JavaRDD<Tuple2<Integer, String>> readRDD = sc.objectFile(outputDir);
    Assert.assertEquals(pairs, readRDD.collect());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void hadoopFile() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<Integer, String>(1, "a"),
      new Tuple2<Integer, String>(2, "aa"),
      new Tuple2<Integer, String>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(new PairFunction<Tuple2<Integer, String>, IntWritable, Text>() {
      @Override
      public Tuple2<IntWritable, Text> call(Tuple2<Integer, String> pair) {
        return new Tuple2<IntWritable, Text>(new IntWritable(pair._1()), new Text(pair._2()));
      }
    }).saveAsHadoopFile(outputDir, IntWritable.class, Text.class, SequenceFileOutputFormat.class);

    JavaPairRDD<IntWritable, Text> output = sc.hadoopFile(outputDir,
      SequenceFileInputFormat.class, IntWritable.class, Text.class);
    Assert.assertEquals(pairs.toString(), output.map(new Function<Tuple2<IntWritable, Text>,
      String>() {
      @Override
      public String call(Tuple2<IntWritable, Text> x) {
        return x.toString();
      }
    }).collect().toString());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void hadoopFileCompressed() {
    String outputDir = new File(tempDir, "output_compressed").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
        new Tuple2<Integer, String>(1, "a"),
        new Tuple2<Integer, String>(2, "aa"),
        new Tuple2<Integer, String>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(new PairFunction<Tuple2<Integer, String>, IntWritable, Text>() {
      @Override
      public Tuple2<IntWritable, Text> call(Tuple2<Integer, String> pair) {
        return new Tuple2<IntWritable, Text>(new IntWritable(pair._1()), new Text(pair._2()));
      }
    }).saveAsHadoopFile(outputDir, IntWritable.class, Text.class, SequenceFileOutputFormat.class,
        DefaultCodec.class);

    JavaPairRDD<IntWritable, Text> output = sc.hadoopFile(outputDir,
        SequenceFileInputFormat.class, IntWritable.class, Text.class);

    Assert.assertEquals(pairs.toString(), output.map(new Function<Tuple2<IntWritable, Text>,
        String>() {
      @Override
      public String call(Tuple2<IntWritable, Text> x) {
        return x.toString();
      }
    }).collect().toString());
  }

  @Test
  public void zip() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    JavaDoubleRDD doubles = rdd.mapToDouble(new DoubleFunction<Integer>() {
      @Override
      public double call(Integer x) {
        return x.doubleValue();
      }
    });
    JavaPairRDD<Integer, Double> zipped = rdd.zip(doubles);
    zipped.count();
  }

  @Test
  public void zipPartitions() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("1", "2", "3", "4"), 2);
    FlatMapFunction2<Iterator<Integer>, Iterator<String>, Integer> sizesFn =
      new FlatMapFunction2<Iterator<Integer>, Iterator<String>, Integer>() {
        @Override
        public Iterable<Integer> call(Iterator<Integer> i, Iterator<String> s) {
          return Arrays.asList(Iterators.size(i), Iterators.size(s));
        }
      };

    JavaRDD<Integer> sizes = rdd1.zipPartitions(rdd2, sizesFn);
    Assert.assertEquals("[3, 2, 3, 2]", sizes.collect().toString());
  }

  @Test
  public void accumulators() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

    final Accumulator<Integer> intAccum = sc.intAccumulator(10);
    rdd.foreach(new VoidFunction<Integer>() {
      @Override
      public void call(Integer x) {
        intAccum.add(x);
      }
    });
    Assert.assertEquals((Integer) 25, intAccum.value());

    final Accumulator<Double> doubleAccum = sc.doubleAccumulator(10.0);
    rdd.foreach(new VoidFunction<Integer>() {
      @Override
      public void call(Integer x) {
        doubleAccum.add((double) x);
      }
    });
    Assert.assertEquals((Double) 25.0, doubleAccum.value());

    // Try a custom accumulator type
    AccumulatorParam<Float> floatAccumulatorParam = new AccumulatorParam<Float>() {
      @Override
      public Float addInPlace(Float r, Float t) {
        return r + t;
      }

      @Override
      public Float addAccumulator(Float r, Float t) {
        return r + t;
      }

      @Override
      public Float zero(Float initialValue) {
        return 0.0f;
      }
    };

    final Accumulator<Float> floatAccum = sc.accumulator(10.0f, floatAccumulatorParam);
    rdd.foreach(new VoidFunction<Integer>() {
      @Override
      public void call(Integer x) {
        floatAccum.add((float) x);
      }
    });
    Assert.assertEquals((Float) 25.0f, floatAccum.value());

    // Test the setValue method
    floatAccum.setValue(5.0f);
    Assert.assertEquals((Float) 5.0f, floatAccum.value());
  }

  @Test
  public void keyBy() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2));
    List<Tuple2<String, Integer>> s = rdd.keyBy(new Function<Integer, String>() {
      @Override
      public String call(Integer t) {
        return t.toString();
      }
    }).collect();
    Assert.assertEquals(new Tuple2<String, Integer>("1", 1), s.get(0));
    Assert.assertEquals(new Tuple2<String, Integer>("2", 2), s.get(1));
  }

  @Test
  public void checkpointAndComputation() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    sc.setCheckpointDir(tempDir.getAbsolutePath());
    Assert.assertFalse(rdd.isCheckpointed());
    rdd.checkpoint();
    rdd.count(); // Forces the DAG to cause a checkpoint
    Assert.assertTrue(rdd.isCheckpointed());
    Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), rdd.collect());
  }

  @Test
  public void checkpointAndRestore() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    sc.setCheckpointDir(tempDir.getAbsolutePath());
    Assert.assertFalse(rdd.isCheckpointed());
    rdd.checkpoint();
    rdd.count(); // Forces the DAG to cause a checkpoint
    Assert.assertTrue(rdd.isCheckpointed());

    Assert.assertTrue(rdd.getCheckpointFile().isPresent());
    JavaRDD<Integer> recovered = sc.checkpointFile(rdd.getCheckpointFile().get());
    Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), recovered.collect());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mapOnPairRDD() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4));
    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(
        new PairFunction<Integer, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Integer i) {
            return new Tuple2<Integer, Integer>(i, i % 2);
          }
        });
    JavaPairRDD<Integer, Integer> rdd3 = rdd2.mapToPair(
        new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
      @Override
      public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> in) {
        return new Tuple2<Integer, Integer>(in._2(), in._1());
      }
    });
    Assert.assertEquals(Arrays.asList(
        new Tuple2<Integer, Integer>(1, 1),
        new Tuple2<Integer, Integer>(0, 2),
        new Tuple2<Integer, Integer>(1, 3),
        new Tuple2<Integer, Integer>(0, 4)), rdd3.collect());

  }

  @SuppressWarnings("unchecked")
  @Test
  public void collectPartitions() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7), 3);

    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(
        new PairFunction<Integer, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Integer i) {
            return new Tuple2<Integer, Integer>(i, i % 2);
          }
        });

    List<Integer>[] parts = rdd1.collectPartitions(new int[] {0});
    Assert.assertEquals(Arrays.asList(1, 2), parts[0]);

    parts = rdd1.collectPartitions(new int[] {1, 2});
    Assert.assertEquals(Arrays.asList(3, 4), parts[0]);
    Assert.assertEquals(Arrays.asList(5, 6, 7), parts[1]);

    Assert.assertEquals(Arrays.asList(new Tuple2<Integer, Integer>(1, 1),
                                      new Tuple2<Integer, Integer>(2, 0)),
                        rdd2.collectPartitions(new int[] {0})[0]);

    List<Tuple2<Integer,Integer>>[] parts2 = rdd2.collectPartitions(new int[] {1, 2});
    Assert.assertEquals(Arrays.asList(new Tuple2<Integer, Integer>(3, 1),
                                      new Tuple2<Integer, Integer>(4, 0)),
                        parts2[0]);
    Assert.assertEquals(Arrays.asList(new Tuple2<Integer, Integer>(5, 1),
                                      new Tuple2<Integer, Integer>(6, 0),
                                      new Tuple2<Integer, Integer>(7, 1)),
                        parts2[1]);
  }

  @Test
  public void countApproxDistinct() {
    List<Integer> arrayData = new ArrayList<Integer>();
    int size = 100;
    for (int i = 0; i < 100000; i++) {
      arrayData.add(i % size);
    }
    JavaRDD<Integer> simpleRdd = sc.parallelize(arrayData, 10);
    Assert.assertTrue(Math.abs((simpleRdd.countApproxDistinct(0.05) - size) / (size * 1.0)) <= 0.1);
  }

  @Test
  public void countApproxDistinctByKey() {
    List<Tuple2<Integer, Integer>> arrayData = new ArrayList<Tuple2<Integer, Integer>>();
    for (int i = 10; i < 100; i++) {
      for (int j = 0; j < i; j++) {
        arrayData.add(new Tuple2<Integer, Integer>(i, j));
      }
    }
    double relativeSD = 0.001;
    JavaPairRDD<Integer, Integer> pairRdd = sc.parallelizePairs(arrayData);
    List<Tuple2<Integer, Object>> res =  pairRdd.countApproxDistinctByKey(8, 0).collect();
    for (Tuple2<Integer, Object> resItem : res) {
      double count = (double)resItem._1();
      Long resCount = (Long)resItem._2();
      Double error = Math.abs((resCount - count) / count);
      Assert.assertTrue(error < 0.1);
    }

  }

  @Test
  public void collectAsMapWithIntArrayValues() {
    // Regression test for SPARK-1040
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1));
    JavaPairRDD<Integer, int[]> pairRDD = rdd.mapToPair(
        new PairFunction<Integer, Integer, int[]>() {
          @Override
          public Tuple2<Integer, int[]> call(Integer x) {
            return new Tuple2<Integer, int[]>(x, new int[] { x });
          }
        });
    pairRDD.collect();  // Works fine
    pairRDD.collectAsMap();  // Used to crash with ClassCastException
  }

  @Test
  @SuppressWarnings("unchecked")
  public void sampleByKey() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 3);
    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(
      new PairFunction<Integer, Integer, Integer>() {
        @Override
        public Tuple2<Integer, Integer> call(Integer i) {
          return new Tuple2<Integer, Integer>(i % 2, 1);
        }
      });
    Map<Integer, Object> fractions = Maps.newHashMap();
    fractions.put(0, 0.5);
    fractions.put(1, 1.0);
    JavaPairRDD<Integer, Integer> wr = rdd2.sampleByKey(true, fractions, 1L);
    Map<Integer, Long> wrCounts = (Map<Integer, Long>) (Object) wr.countByKey();
    Assert.assertTrue(wrCounts.size() == 2);
    Assert.assertTrue(wrCounts.get(0) > 0);
    Assert.assertTrue(wrCounts.get(1) > 0);
    JavaPairRDD<Integer, Integer> wor = rdd2.sampleByKey(false, fractions, 1L);
    Map<Integer, Long> worCounts = (Map<Integer, Long>) (Object) wor.countByKey();
    Assert.assertTrue(worCounts.size() == 2);
    Assert.assertTrue(worCounts.get(0) > 0);
    Assert.assertTrue(worCounts.get(1) > 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void sampleByKeyExact() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 3);
    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(
      new PairFunction<Integer, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Integer i) {
              return new Tuple2<Integer, Integer>(i % 2, 1);
          }
      });
    Map<Integer, Object> fractions = Maps.newHashMap();
    fractions.put(0, 0.5);
    fractions.put(1, 1.0);
    JavaPairRDD<Integer, Integer> wrExact = rdd2.sampleByKeyExact(true, fractions, 1L);
    Map<Integer, Long> wrExactCounts = (Map<Integer, Long>) (Object) wrExact.countByKey();
    Assert.assertTrue(wrExactCounts.size() == 2);
    Assert.assertTrue(wrExactCounts.get(0) == 2);
    Assert.assertTrue(wrExactCounts.get(1) == 4);
    JavaPairRDD<Integer, Integer> worExact = rdd2.sampleByKeyExact(false, fractions, 1L);
    Map<Integer, Long> worExactCounts = (Map<Integer, Long>) (Object) worExact.countByKey();
    Assert.assertTrue(worExactCounts.size() == 2);
    Assert.assertTrue(worExactCounts.get(0) == 2);
    Assert.assertTrue(worExactCounts.get(1) == 4);
  }

  private static class SomeCustomClass implements Serializable {
    public SomeCustomClass() {
      // Intentionally left blank
    }
  }

  @Test
  public void collectUnderlyingScalaRDD() {
    List<SomeCustomClass> data = new ArrayList<SomeCustomClass>();
    for (int i = 0; i < 100; i++) {
      data.add(new SomeCustomClass());
    }
    JavaRDD<SomeCustomClass> rdd = sc.parallelize(data);
    SomeCustomClass[] collected = (SomeCustomClass[]) rdd.rdd().retag(SomeCustomClass.class).collect();
    Assert.assertEquals(data.size(), collected.length);
  }

  public void getHadoopInputSplits() {
    String outDir = new File(tempDir, "output").getAbsolutePath();
    sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2).saveAsTextFile(outDir);

    JavaHadoopRDD<LongWritable, Text> hadoopRDD = (JavaHadoopRDD<LongWritable, Text>)
        sc.hadoopFile(outDir, TextInputFormat.class, LongWritable.class, Text.class);
    List<String> inputPaths = hadoopRDD.mapPartitionsWithInputSplit(
        new Function2<InputSplit, Iterator<Tuple2<LongWritable, Text>>, Iterator<String>>() {
      @Override
      public Iterator<String> call(InputSplit split, Iterator<Tuple2<LongWritable, Text>> it)
          throws Exception {
        FileSplit fileSplit = (FileSplit) split;
        return Lists.newArrayList(fileSplit.getPath().toUri().getPath()).iterator();
      }
    }, true).collect();
    Assert.assertEquals(Sets.newHashSet(inputPaths),
        Sets.newHashSet(outDir + "/part-00000", outDir + "/part-00001"));
  }
}
