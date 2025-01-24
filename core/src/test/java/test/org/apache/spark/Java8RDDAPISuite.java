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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import scala.Tuple2;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.util.Utils;

/**
 * Most of these tests replicate org.apache.spark.JavaAPISuite using java 8
 * lambda syntax.
 */
public class Java8RDDAPISuite implements Serializable {
  private static int foreachCalls = 0;
  private transient JavaSparkContext sc;

  @BeforeEach
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaAPISuite");
  }

  @AfterEach
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void foreachWithAnonymousClass() {
    foreachCalls = 0;
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello", "World"));
    rdd.foreach(s -> foreachCalls++);
    Assertions.assertEquals(2, foreachCalls);
  }

  @Test
  public void foreach() {
    foreachCalls = 0;
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello", "World"));
    rdd.foreach(x -> foreachCalls++);
    Assertions.assertEquals(2, foreachCalls);
  }

  @Test
  public void groupBy() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function<Integer, Boolean> isOdd = x -> x % 2 == 0;
    JavaPairRDD<Boolean, Iterable<Integer>> oddsAndEvens = rdd.groupBy(isOdd);
    Assertions.assertEquals(2, oddsAndEvens.count());
    Assertions.assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    Assertions.assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds

    oddsAndEvens = rdd.groupBy(isOdd, 1);
    Assertions.assertEquals(2, oddsAndEvens.count());
    Assertions.assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    Assertions.assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds
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
    List<Tuple2<Integer, Tuple2<Integer, Optional<Character>>>> joined =
      rdd1.leftOuterJoin(rdd2).collect();
    Assertions.assertEquals(5, joined.size());
    Tuple2<Integer, Tuple2<Integer, Optional<Character>>> firstUnmatched =
      rdd1.leftOuterJoin(rdd2).filter(tup -> !tup._2()._2().isPresent()).first();
    Assertions.assertEquals(3, firstUnmatched._1().intValue());
  }

  @Test
  public void foldReduce() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function2<Integer, Integer, Integer> add = (a, b) -> a + b;

    int sum = rdd.fold(0, add);
    Assertions.assertEquals(33, sum);

    sum = rdd.reduce(add);
    Assertions.assertEquals(33, sum);
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
    Assertions.assertEquals(1, sums.lookup(1).get(0).intValue());
    Assertions.assertEquals(2, sums.lookup(2).get(0).intValue());
    Assertions.assertEquals(3, sums.lookup(3).get(0).intValue());
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
    Assertions.assertEquals(1, counts.lookup(1).get(0).intValue());
    Assertions.assertEquals(2, counts.lookup(2).get(0).intValue());
    Assertions.assertEquals(3, counts.lookup(3).get(0).intValue());

    Map<Integer, Integer> localCounts = counts.collectAsMap();
    Assertions.assertEquals(1, localCounts.get(1).intValue());
    Assertions.assertEquals(2, localCounts.get(2).intValue());
    Assertions.assertEquals(3, localCounts.get(3).intValue());

    localCounts = rdd.reduceByKeyLocally((a, b) -> a + b);
    Assertions.assertEquals(1, localCounts.get(1).intValue());
    Assertions.assertEquals(2, localCounts.get(2).intValue());
    Assertions.assertEquals(3, localCounts.get(3).intValue());
  }

  @Test
  public void map() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    JavaDoubleRDD doubles = rdd.mapToDouble(x -> 1.0 * x).cache();
    doubles.collect();
    JavaPairRDD<Integer, Integer> pairs = rdd.mapToPair(x -> new Tuple2<>(x, x))
      .cache();
    pairs.collect();
    JavaRDD<String> strings = rdd.map(Object::toString).cache();
    strings.collect();
  }

  @Test
  public void flatMap() {
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello World!",
      "The quick brown fox jumps over the lazy dog."));
    JavaRDD<String> words = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

    Assertions.assertEquals("Hello", words.first());
    Assertions.assertEquals(11, words.count());

    JavaPairRDD<String, String> pairs = rdd.flatMapToPair(s -> {
      List<Tuple2<String, String>> pairs2 = new LinkedList<>();
      for (String word : s.split(" ")) {
        pairs2.add(new Tuple2<>(word, word));
      }
      return pairs2.iterator();
    });

    Assertions.assertEquals(new Tuple2<>("Hello", "Hello"), pairs.first());
    Assertions.assertEquals(11, pairs.count());

    JavaDoubleRDD doubles = rdd.flatMapToDouble(s -> {
      List<Double> lengths = new LinkedList<>();
      for (String word : s.split(" ")) {
        lengths.add((double) word.length());
      }
      return lengths.iterator();
    });

    Assertions.assertEquals(5.0, doubles.first(), 0.01);
    Assertions.assertEquals(11, pairs.count());
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
    JavaPairRDD<String, Integer> swapped =
      pairRDD.flatMapToPair(x -> Collections.singletonList(x.swap()).iterator());
    swapped.collect();

    // There was never a bug here, but it's worth testing:
    pairRDD.map(Tuple2::swap).collect();
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

    Assertions.assertEquals("[3, 7]", partitionSums.collect().toString());
  }

  @Test
  public void sequenceFile() throws IOException {
    File tempDir = Utils.createTempDir();
    tempDir.deleteOnExit();
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
    JavaPairRDD<Integer, String> readRDD = sc.sequenceFile(outputDir, IntWritable.class, Text.class)
      .mapToPair(pair -> new Tuple2<>(pair._1().get(), pair._2().toString()));
    Assertions.assertEquals(pairs, readRDD.collect());
    Utils.deleteRecursively(tempDir);
  }

  @Test
  public void zip() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    JavaDoubleRDD doubles = rdd.mapToDouble(x -> 1.0 * x);
    JavaPairRDD<Integer, Double> zipped = rdd.zip(doubles);
    zipped.count();
  }

  @Test
  public void zipPartitions2() {
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d"), 2);
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("e", "f", "g", "h"), 2);
    JavaRDD<String> zipped = rdd1.zipPartitions(
      rdd2, ZipPartitionsFunction.ZIP_PARTITIONS_2_FUNCTION);
    Assertions.assertEquals(Arrays.asList("abef", "cdgh"), zipped.collect());
  }

  @Test
  public void zipPartitions3() {
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d"), 2);
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("e", "f", "g", "h"), 2);
    JavaRDD<String> rdd3 = sc.parallelize(Arrays.asList("i", "j", "k", "l"), 2);
    JavaRDD<String> zipped = rdd1.zipPartitions(
      rdd2, rdd3, ZipPartitionsFunction.ZIP_PARTITIONS_3_FUNCTION);
    Assertions.assertEquals(Arrays.asList("abefij", "cdghkl"), zipped.collect());
  }

  @Test
  public void zipPartitions4() {
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d"), 2);
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("e", "f", "g", "h"), 2);
    JavaRDD<String> rdd3 = sc.parallelize(Arrays.asList("i", "j", "k", "l"), 2);
    JavaRDD<String> rdd4 = sc.parallelize(Arrays.asList("m", "n", "o", "p"), 2);
    JavaRDD<String> zipped = rdd1.zipPartitions(
      rdd2, rdd3, rdd4, ZipPartitionsFunction.ZIP_PARTITIONS_4_FUNCTION);
    Assertions.assertEquals(Arrays.asList("abefijmn", "cdghklop"), zipped.collect());
  }

  @Test
  public void zipPartitionsN() {
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d"), 2);
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("e", "f", "g", "h"), 2);
    JavaRDD<String> rdd3 = sc.parallelize(Arrays.asList("i", "j", "k", "l"), 2);
    JavaRDD<String> rdd4 = sc.parallelize(Arrays.asList("m", "n", "o", "p"), 2);
    JavaRDD<String> rdd5 = sc.parallelize(Arrays.asList("q", "r", "s", "t"), 2);
    JavaRDD<String> zipped = rdd1.zipPartitions(
      ZipPartitionsFunction.ZIP_PARTITIONS_N_FUNCTION, rdd2, rdd3, rdd4, rdd5);
    Assertions.assertEquals(Arrays.asList("abefijmnqr", "cdghklopst"), zipped.collect());
  }

  @Test
  public void keyBy() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2));
    List<Tuple2<String, Integer>> s = rdd.keyBy(Object::toString).collect();
    Assertions.assertEquals(new Tuple2<>("1", 1), s.get(0));
    Assertions.assertEquals(new Tuple2<>("2", 2), s.get(1));
  }

  @Test
  public void mapOnPairRDD() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    JavaPairRDD<Integer, Integer> rdd2 =
      rdd1.mapToPair(i -> new Tuple2<>(i, i % 2));
    JavaPairRDD<Integer, Integer> rdd3 =
      rdd2.mapToPair(in -> new Tuple2<>(in._2(), in._1()));
    Assertions.assertEquals(Arrays.asList(
      new Tuple2<>(1, 1),
      new Tuple2<>(0, 2),
      new Tuple2<>(1, 3),
      new Tuple2<>(0, 4)), rdd3.collect());
  }

  @Test
  public void collectPartitions() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7), 3);

    JavaPairRDD<Integer, Integer> rdd2 =
      rdd1.mapToPair(i -> new Tuple2<>(i, i % 2));
    List<Integer>[] parts = rdd1.collectPartitions(new int[]{0});
    Assertions.assertEquals(Arrays.asList(1, 2), parts[0]);

    parts = rdd1.collectPartitions(new int[]{1, 2});
    Assertions.assertEquals(Arrays.asList(3, 4), parts[0]);
    Assertions.assertEquals(Arrays.asList(5, 6, 7), parts[1]);

    Assertions.assertEquals(Arrays.asList(new Tuple2<>(1, 1), new Tuple2<>(2, 0)),
      rdd2.collectPartitions(new int[]{0})[0]);

    List<Tuple2<Integer, Integer>>[] parts2 = rdd2.collectPartitions(new int[]{1, 2});
    Assertions.assertEquals(Arrays.asList(new Tuple2<>(3, 1), new Tuple2<>(4, 0)), parts2[0]);
    Assertions.assertEquals(
      Arrays.asList(new Tuple2<>(5, 1), new Tuple2<>(6, 0), new Tuple2<>(7, 1)), parts2[1]);
  }

  @Test
  public void collectAsMapWithIntArrayValues() {
    // Regression test for SPARK-1040
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1));
    JavaPairRDD<Integer, int[]> pairRDD =
      rdd.mapToPair(x -> new Tuple2<>(x, new int[]{x}));
    pairRDD.collect();  // Works fine
    pairRDD.collectAsMap();  // Used to crash with ClassCastException
  }

  private static class ZipPartitionsFunction
      implements FlatMapFunction<List<Iterator<String>>, String> {

    private static final ZipPartitionsFunction ZIP_PARTITIONS_N_FUNCTION =
        new ZipPartitionsFunction();

    private static final FlatMapFunction2<Iterator<String>, Iterator<String>, String>
        ZIP_PARTITIONS_2_FUNCTION =
            (Iterator<String> i1, Iterator<String> i2) ->
                ZIP_PARTITIONS_N_FUNCTION.call(Arrays.asList(i1, i2));

    private static final FlatMapFunction3<
            Iterator<String>, Iterator<String>, Iterator<String>, String>
        ZIP_PARTITIONS_3_FUNCTION =
            (Iterator<String> i1, Iterator<String> i2, Iterator<String> i3) ->
                ZIP_PARTITIONS_N_FUNCTION.call(Arrays.asList(i1, i2, i3));

    private static final FlatMapFunction4<
            Iterator<String>, Iterator<String>, Iterator<String>, Iterator<String>, String>
        ZIP_PARTITIONS_4_FUNCTION =
            (Iterator<String> i1, Iterator<String> i2, Iterator<String> i3, Iterator<String> i4) ->
                ZIP_PARTITIONS_N_FUNCTION.call(Arrays.asList(i1, i2, i3, i4));

    @Override
    public Iterator<String> call(List<Iterator<String>> iterators) {
      StringBuilder stringBuilder = new StringBuilder();
      for (Iterator<String> iterator : iterators) {
        while (iterator.hasNext()) {
          stringBuilder.append(iterator.next());
        }
      }
      return Collections.singleton(stringBuilder.toString()).iterator();
    }
  }
}
