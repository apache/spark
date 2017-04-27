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

package test.org.apache.spark.streaming;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.JavaTestUtils;
import org.apache.spark.streaming.LocalJavaStreamingContext;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import scala.Tuple2;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

/**
 * Most of these tests replicate org.apache.spark.streaming.JavaAPISuite using java 8
 * lambda syntax.
 */
@SuppressWarnings("unchecked")
public class Java8APISuite extends LocalJavaStreamingContext implements Serializable {

  @Test
  public void testMap() {
    List<List<String>> inputData = Arrays.asList(
      Arrays.asList("hello", "world"),
      Arrays.asList("goodnight", "moon"));

    List<List<Integer>> expected = Arrays.asList(
      Arrays.asList(5, 5),
      Arrays.asList(9, 4));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> letterCount = stream.map(String::length);
    JavaTestUtils.attachTestOutputStream(letterCount);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testFilter() {
    List<List<String>> inputData = Arrays.asList(
      Arrays.asList("giants", "dodgers"),
      Arrays.asList("yankees", "red sox"));

    List<List<String>> expected = Arrays.asList(
      Arrays.asList("giants"),
      Arrays.asList("yankees"));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<String> filtered = stream.filter(s -> s.contains("a"));
    JavaTestUtils.attachTestOutputStream(filtered);
    List<List<String>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testMapPartitions() {
    List<List<String>> inputData = Arrays.asList(
      Arrays.asList("giants", "dodgers"),
      Arrays.asList("yankees", "red sox"));

    List<List<String>> expected = Arrays.asList(
      Arrays.asList("GIANTSDODGERS"),
      Arrays.asList("YANKEESRED SOX"));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<String> mapped = stream.mapPartitions(in -> {
      String out = "";
      while (in.hasNext()) {
        out = out + in.next().toUpperCase(Locale.ROOT);
      }
      return Arrays.asList(out).iterator();
    });
    JavaTestUtils.attachTestOutputStream(mapped);
    List<List<String>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testReduce() {
    List<List<Integer>> inputData = Arrays.asList(
      Arrays.asList(1, 2, 3),
      Arrays.asList(4, 5, 6),
      Arrays.asList(7, 8, 9));

    List<List<Integer>> expected = Arrays.asList(
      Arrays.asList(6),
      Arrays.asList(15),
      Arrays.asList(24));

    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> reduced = stream.reduce((x, y) -> x + y);
    JavaTestUtils.attachTestOutputStream(reduced);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testReduceByWindow() {
    List<List<Integer>> inputData = Arrays.asList(
      Arrays.asList(1, 2, 3),
      Arrays.asList(4, 5, 6),
      Arrays.asList(7, 8, 9));

    List<List<Integer>> expected = Arrays.asList(
      Arrays.asList(6),
      Arrays.asList(21),
      Arrays.asList(39),
      Arrays.asList(24));

    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> reducedWindowed = stream.reduceByWindow(
      (x, y) -> x + y, (x, y) -> x - y, new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reducedWindowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 4, 4);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testTransform() {
    List<List<Integer>> inputData = Arrays.asList(
      Arrays.asList(1, 2, 3),
      Arrays.asList(4, 5, 6),
      Arrays.asList(7, 8, 9));

    List<List<Integer>> expected = Arrays.asList(
      Arrays.asList(3, 4, 5),
      Arrays.asList(6, 7, 8),
      Arrays.asList(9, 10, 11));

    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> transformed = stream.transform(in -> in.map(i -> i + 2));

    JavaTestUtils.attachTestOutputStream(transformed);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testVariousTransform() {
    // tests whether all variations of transform can be called from Java

    List<List<Integer>> inputData = Arrays.asList(Arrays.asList(1));
    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);

    List<List<Tuple2<String, Integer>>> pairInputData =
      Arrays.asList(Arrays.asList(new Tuple2<>("x", 1)));
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(
      JavaTestUtils.attachTestInputStream(ssc, pairInputData, 1));

    JavaDStream<Integer> transformed1 = stream.transform(in -> null);
    JavaDStream<Integer> transformed2 = stream.transform((x, time) -> null);
    JavaPairDStream<String, Integer> transformed3 = stream.transformToPair(x -> null);
    JavaPairDStream<String, Integer> transformed4 = stream.transformToPair((x, time) -> null);
    JavaDStream<Integer> pairTransformed1 = pairStream.transform(x -> null);
    JavaDStream<Integer> pairTransformed2 = pairStream.transform((x, time) -> null);
    JavaPairDStream<String, String> pairTransformed3 = pairStream.transformToPair(x -> null);
    JavaPairDStream<String, String> pairTransformed4 =
      pairStream.transformToPair((x, time) -> null);

  }

  @Test
  public void testTransformWith() {
    List<List<Tuple2<String, String>>> stringStringKVStream1 = Arrays.asList(
      Arrays.asList(
        new Tuple2<>("california", "dodgers"),
        new Tuple2<>("new york", "yankees")),
      Arrays.asList(
        new Tuple2<>("california", "sharks"),
        new Tuple2<>("new york", "rangers")));

    List<List<Tuple2<String, String>>> stringStringKVStream2 = Arrays.asList(
      Arrays.asList(
        new Tuple2<>("california", "giants"),
        new Tuple2<>("new york", "mets")),
      Arrays.asList(
        new Tuple2<>("california", "ducks"),
        new Tuple2<>("new york", "islanders")));


    List<Set<Tuple2<String, Tuple2<String, String>>>> expected = Arrays.asList(
      Sets.newHashSet(
        new Tuple2<>("california",
          new Tuple2<>("dodgers", "giants")),
        new Tuple2<>("new york",
          new Tuple2<>("yankees", "mets"))),
      Sets.newHashSet(
        new Tuple2<>("california",
          new Tuple2<>("sharks", "ducks")),
        new Tuple2<>("new york",
          new Tuple2<>("rangers", "islanders"))));

    JavaDStream<Tuple2<String, String>> stream1 = JavaTestUtils.attachTestInputStream(
      ssc, stringStringKVStream1, 1);
    JavaPairDStream<String, String> pairStream1 = JavaPairDStream.fromJavaDStream(stream1);

    JavaDStream<Tuple2<String, String>> stream2 = JavaTestUtils.attachTestInputStream(
      ssc, stringStringKVStream2, 1);
    JavaPairDStream<String, String> pairStream2 = JavaPairDStream.fromJavaDStream(stream2);

    JavaPairDStream<String, Tuple2<String, String>> joined =
      pairStream1.transformWithToPair(pairStream2,(x, y, z) -> x.join(y));

    JavaTestUtils.attachTestOutputStream(joined);
    List<List<Tuple2<String, Tuple2<String, String>>>> result = JavaTestUtils.runStreams(ssc, 2, 2);
    List<Set<Tuple2<String, Tuple2<String, String>>>> unorderedResult = new ArrayList<>();
    for (List<Tuple2<String, Tuple2<String, String>>> res : result) {
      unorderedResult.add(Sets.newHashSet(res));
    }

    Assert.assertEquals(expected, unorderedResult);
  }


  @Test
  public void testVariousTransformWith() {
    // tests whether all variations of transformWith can be called from Java

    List<List<Integer>> inputData1 = Arrays.asList(Arrays.asList(1));
    List<List<String>> inputData2 = Arrays.asList(Arrays.asList("x"));
    JavaDStream<Integer> stream1 = JavaTestUtils.attachTestInputStream(ssc, inputData1, 1);
    JavaDStream<String> stream2 = JavaTestUtils.attachTestInputStream(ssc, inputData2, 1);

    List<List<Tuple2<String, Integer>>> pairInputData1 =
      Arrays.asList(Arrays.asList(new Tuple2<>("x", 1)));
    List<List<Tuple2<Double, Character>>> pairInputData2 =
      Arrays.asList(Arrays.asList(new Tuple2<>(1.0, 'x')));
    JavaPairDStream<String, Integer> pairStream1 = JavaPairDStream.fromJavaDStream(
      JavaTestUtils.attachTestInputStream(ssc, pairInputData1, 1));
    JavaPairDStream<Double, Character> pairStream2 = JavaPairDStream.fromJavaDStream(
      JavaTestUtils.attachTestInputStream(ssc, pairInputData2, 1));

    JavaDStream<Double> transformed1 = stream1.transformWith(stream2, (x, y, z) -> null);
    JavaDStream<Double> transformed2 = stream1.transformWith(pairStream1,(x, y, z) -> null);

    JavaPairDStream<Double, Double> transformed3 =
      stream1.transformWithToPair(stream2,(x, y, z) -> null);

    JavaPairDStream<Double, Double> transformed4 =
      stream1.transformWithToPair(pairStream1,(x, y, z) -> null);

    JavaDStream<Double> pairTransformed1 = pairStream1.transformWith(stream2,(x, y, z) -> null);

    JavaDStream<Double> pairTransformed2_ =
      pairStream1.transformWith(pairStream1,(x, y, z) -> null);

    JavaPairDStream<Double, Double> pairTransformed3 =
      pairStream1.transformWithToPair(stream2,(x, y, z) -> null);

    JavaPairDStream<Double, Double> pairTransformed4 =
      pairStream1.transformWithToPair(pairStream2,(x, y, z) -> null);
  }

  @Test
  public void testStreamingContextTransform() {
    List<List<Integer>> stream1input = Arrays.asList(
      Arrays.asList(1),
      Arrays.asList(2)
    );

    List<List<Integer>> stream2input = Arrays.asList(
      Arrays.asList(3),
      Arrays.asList(4)
    );

    List<List<Tuple2<Integer, String>>> pairStream1input = Arrays.asList(
      Arrays.asList(new Tuple2<>(1, "x")),
      Arrays.asList(new Tuple2<>(2, "y"))
    );

    List<List<Tuple2<Integer, Tuple2<Integer, String>>>> expected = Arrays.asList(
      Arrays.asList(new Tuple2<>(1, new Tuple2<>(1, "x"))),
      Arrays.asList(new Tuple2<>(2, new Tuple2<>(2, "y")))
    );

    JavaDStream<Integer> stream1 = JavaTestUtils.attachTestInputStream(ssc, stream1input, 1);
    JavaDStream<Integer> stream2 = JavaTestUtils.attachTestInputStream(ssc, stream2input, 1);
    JavaPairDStream<Integer, String> pairStream1 = JavaPairDStream.fromJavaDStream(
      JavaTestUtils.attachTestInputStream(ssc, pairStream1input, 1));

    List<JavaDStream<?>> listOfDStreams1 = Arrays.asList(stream1, stream2);

    // This is just to test whether this transform to JavaStream compiles
    JavaDStream<Long> transformed1 = ssc.transform(
      listOfDStreams1, (List<JavaRDD<?>> listOfRDDs, Time time) -> {
      Assert.assertEquals(2, listOfRDDs.size());
      return null;
    });

    List<JavaDStream<?>> listOfDStreams2 =
      Arrays.asList(stream1, stream2, pairStream1.toJavaDStream());

    JavaPairDStream<Integer, Tuple2<Integer, String>> transformed2 = ssc.transformToPair(
      listOfDStreams2, (List<JavaRDD<?>> listOfRDDs, Time time) -> {
      Assert.assertEquals(3, listOfRDDs.size());
      JavaRDD<Integer> rdd1 = (JavaRDD<Integer>) listOfRDDs.get(0);
      JavaRDD<Integer> rdd2 = (JavaRDD<Integer>) listOfRDDs.get(1);
      JavaRDD<Tuple2<Integer, String>> rdd3 = (JavaRDD<Tuple2<Integer, String>>) listOfRDDs.get(2);
      JavaPairRDD<Integer, String> prdd3 = JavaPairRDD.fromJavaRDD(rdd3);
      PairFunction<Integer, Integer, Integer> mapToTuple =
        (Integer i) -> new Tuple2<>(i, i);
      return rdd1.union(rdd2).mapToPair(mapToTuple).join(prdd3);
    });
    JavaTestUtils.attachTestOutputStream(transformed2);
    List<List<Tuple2<Integer, Tuple2<Integer, String>>>> result =
      JavaTestUtils.runStreams(ssc, 2, 2);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testFlatMap() {
    List<List<String>> inputData = Arrays.asList(
      Arrays.asList("go", "giants"),
      Arrays.asList("boo", "dodgers"),
      Arrays.asList("athletics"));

    List<List<String>> expected = Arrays.asList(
      Arrays.asList("g", "o", "g", "i", "a", "n", "t", "s"),
      Arrays.asList("b", "o", "o", "d", "o", "d", "g", "e", "r", "s"),
      Arrays.asList("a", "t", "h", "l", "e", "t", "i", "c", "s"));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<String> flatMapped = stream.flatMap(
        s -> Arrays.asList(s.split("(?!^)")).iterator());
    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<String>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testPairFlatMap() {
    List<List<String>> inputData = Arrays.asList(
      Arrays.asList("giants"),
      Arrays.asList("dodgers"),
      Arrays.asList("athletics"));

    List<List<Tuple2<Integer, String>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<>(6, "g"),
        new Tuple2<>(6, "i"),
        new Tuple2<>(6, "a"),
        new Tuple2<>(6, "n"),
        new Tuple2<>(6, "t"),
        new Tuple2<>(6, "s")),
      Arrays.asList(
        new Tuple2<>(7, "d"),
        new Tuple2<>(7, "o"),
        new Tuple2<>(7, "d"),
        new Tuple2<>(7, "g"),
        new Tuple2<>(7, "e"),
        new Tuple2<>(7, "r"),
        new Tuple2<>(7, "s")),
      Arrays.asList(
        new Tuple2<>(9, "a"),
        new Tuple2<>(9, "t"),
        new Tuple2<>(9, "h"),
        new Tuple2<>(9, "l"),
        new Tuple2<>(9, "e"),
        new Tuple2<>(9, "t"),
        new Tuple2<>(9, "i"),
        new Tuple2<>(9, "c"),
        new Tuple2<>(9, "s")));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<Integer, String> flatMapped = stream.flatMapToPair(s -> {
      List<Tuple2<Integer, String>> out = new ArrayList<>();
      for (String letter : s.split("(?!^)")) {
        out.add(new Tuple2<>(s.length(), letter));
      }
      return out.iterator();
    });

    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<Tuple2<Integer, String>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  /*
   * Performs an order-invariant comparison of lists representing two RDD streams. This allows
   * us to account for ordering variation within individual RDD's which occurs during windowing.
   */
  public static <T extends Comparable<T>> void assertOrderInvariantEquals(
    List<List<T>> expected, List<List<T>> actual) {
    expected.forEach(Collections::sort);
    List<List<T>> sortedActual = new ArrayList<>();
    actual.forEach(list -> {
        List<T> sortedList = new ArrayList<>(list);
        Collections.sort(sortedList);
        sortedActual.add(sortedList);
    });
    Assert.assertEquals(expected, sortedActual);
  }

  @Test
  public void testPairFilter() {
    List<List<String>> inputData = Arrays.asList(
      Arrays.asList("giants", "dodgers"),
      Arrays.asList("yankees", "red sox"));

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
      Arrays.asList(new Tuple2<>("giants", 6)),
      Arrays.asList(new Tuple2<>("yankees", 7)));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream =
      stream.mapToPair(x -> new Tuple2<>(x, x.length()));
    JavaPairDStream<String, Integer> filtered = pairStream.filter(x -> x._1().contains("a"));
    JavaTestUtils.attachTestOutputStream(filtered);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  List<List<Tuple2<String, String>>> stringStringKVStream = Arrays.asList(
    Arrays.asList(new Tuple2<>("california", "dodgers"),
      new Tuple2<>("california", "giants"),
      new Tuple2<>("new york", "yankees"),
      new Tuple2<>("new york", "mets")),
    Arrays.asList(new Tuple2<>("california", "sharks"),
      new Tuple2<>("california", "ducks"),
      new Tuple2<>("new york", "rangers"),
      new Tuple2<>("new york", "islanders")));

  List<List<Tuple2<String, Integer>>> stringIntKVStream = Arrays.asList(
    Arrays.asList(
      new Tuple2<>("california", 1),
      new Tuple2<>("california", 3),
      new Tuple2<>("new york", 4),
      new Tuple2<>("new york", 1)),
    Arrays.asList(
      new Tuple2<>("california", 5),
      new Tuple2<>("california", 5),
      new Tuple2<>("new york", 3),
      new Tuple2<>("new york", 1)));

  @Test
  public void testPairMap() { // Maps pair -> pair of different type
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<Integer, String>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<>(1, "california"),
        new Tuple2<>(3, "california"),
        new Tuple2<>(4, "new york"),
        new Tuple2<>(1, "new york")),
      Arrays.asList(
        new Tuple2<>(5, "california"),
        new Tuple2<>(5, "california"),
        new Tuple2<>(3, "new york"),
        new Tuple2<>(1, "new york")));

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);
    JavaPairDStream<Integer, String> reversed = pairStream.mapToPair(Tuple2::swap);
    JavaTestUtils.attachTestOutputStream(reversed);
    List<List<Tuple2<Integer, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testPairMapPartitions() { // Maps pair -> pair of different type
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<Integer, String>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<>(1, "california"),
        new Tuple2<>(3, "california"),
        new Tuple2<>(4, "new york"),
        new Tuple2<>(1, "new york")),
      Arrays.asList(
        new Tuple2<>(5, "california"),
        new Tuple2<>(5, "california"),
        new Tuple2<>(3, "new york"),
        new Tuple2<>(1, "new york")));

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);
    JavaPairDStream<Integer, String> reversed = pairStream.mapPartitionsToPair(in -> {
      LinkedList<Tuple2<Integer, String>> out = new LinkedList<>();
      while (in.hasNext()) {
        Tuple2<String, Integer> next = in.next();
        out.add(next.swap());
      }
      return out.iterator();
    });

    JavaTestUtils.attachTestOutputStream(reversed);
    List<List<Tuple2<Integer, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testPairMap2() { // Maps pair -> single
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Integer>> expected = Arrays.asList(
      Arrays.asList(1, 3, 4, 1),
      Arrays.asList(5, 5, 3, 1));

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);
    JavaDStream<Integer> reversed = pairStream.map(Tuple2::_2);
    JavaTestUtils.attachTestOutputStream(reversed);
    List<List<Tuple2<Integer, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testPairToPairFlatMapWithChangingTypes() { // Maps pair -> pair
    List<List<Tuple2<String, Integer>>> inputData = Arrays.asList(
      Arrays.asList(
        new Tuple2<>("hi", 1),
        new Tuple2<>("ho", 2)),
      Arrays.asList(
        new Tuple2<>("hi", 1),
        new Tuple2<>("ho", 2)));

    List<List<Tuple2<Integer, String>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<>(1, "h"),
        new Tuple2<>(1, "i"),
        new Tuple2<>(2, "h"),
        new Tuple2<>(2, "o")),
      Arrays.asList(
        new Tuple2<>(1, "h"),
        new Tuple2<>(1, "i"),
        new Tuple2<>(2, "h"),
        new Tuple2<>(2, "o")));

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);
    JavaPairDStream<Integer, String> flatMapped = pairStream.flatMapToPair(in -> {
      List<Tuple2<Integer, String>> out = new LinkedList<>();
      for (Character s : in._1().toCharArray()) {
        out.add(new Tuple2<>(in._2(), s.toString()));
      }
      return out.iterator();
    });

    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testPairReduceByKey() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<>("california", 4),
        new Tuple2<>("new york", 5)),
      Arrays.asList(
        new Tuple2<>("california", 10),
        new Tuple2<>("new york", 4)));

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(
      ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> reduced = pairStream.reduceByKey((x, y) -> x + y);

    JavaTestUtils.attachTestOutputStream(reduced);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testCombineByKey() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<>("california", 4),
        new Tuple2<>("new york", 5)),
      Arrays.asList(
        new Tuple2<>("california", 10),
        new Tuple2<>("new york", 4)));

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(
      ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> combined = pairStream.combineByKey(i -> i,
      (x, y) -> x + y, (x, y) -> x + y, new HashPartitioner(2));

    JavaTestUtils.attachTestOutputStream(combined);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testReduceByKeyAndWindow() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
      Arrays.asList(new Tuple2<>("california", 4),
        new Tuple2<>("new york", 5)),
      Arrays.asList(new Tuple2<>("california", 14),
        new Tuple2<>("new york", 9)),
      Arrays.asList(new Tuple2<>("california", 10),
        new Tuple2<>("new york", 4)));

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> reduceWindowed =
      pairStream.reduceByKeyAndWindow((x, y) -> x + y, new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reduceWindowed);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testUpdateStateByKey() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
      Arrays.asList(new Tuple2<>("california", 4),
        new Tuple2<>("new york", 5)),
      Arrays.asList(new Tuple2<>("california", 14),
        new Tuple2<>("new york", 9)),
      Arrays.asList(new Tuple2<>("california", 14),
        new Tuple2<>("new york", 9)));

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> updated = pairStream.updateStateByKey((values, state) -> {
      int out = 0;
      if (state.isPresent()) {
        out = out + state.get();
      }
      for (Integer v : values) {
        out = out + v;
      }
      return Optional.of(out);
    });

    JavaTestUtils.attachTestOutputStream(updated);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testReduceByKeyAndWindowWithInverse() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
      Arrays.asList(new Tuple2<>("california", 4),
        new Tuple2<>("new york", 5)),
      Arrays.asList(new Tuple2<>("california", 14),
        new Tuple2<>("new york", 9)),
      Arrays.asList(new Tuple2<>("california", 10),
        new Tuple2<>("new york", 4)));

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> reduceWindowed =
      pairStream.reduceByKeyAndWindow((x, y) -> x + y, (x, y) -> x - y, new Duration(2000),
        new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reduceWindowed);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testPairTransform() {
    List<List<Tuple2<Integer, Integer>>> inputData = Arrays.asList(
      Arrays.asList(
        new Tuple2<>(3, 5),
        new Tuple2<>(1, 5),
        new Tuple2<>(4, 5),
        new Tuple2<>(2, 5)),
      Arrays.asList(
        new Tuple2<>(2, 5),
        new Tuple2<>(3, 5),
        new Tuple2<>(4, 5),
        new Tuple2<>(1, 5)));

    List<List<Tuple2<Integer, Integer>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<>(1, 5),
        new Tuple2<>(2, 5),
        new Tuple2<>(3, 5),
        new Tuple2<>(4, 5)),
      Arrays.asList(
        new Tuple2<>(1, 5),
        new Tuple2<>(2, 5),
        new Tuple2<>(3, 5),
        new Tuple2<>(4, 5)));

    JavaDStream<Tuple2<Integer, Integer>> stream = JavaTestUtils.attachTestInputStream(
      ssc, inputData, 1);
    JavaPairDStream<Integer, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<Integer, Integer> sorted = pairStream.transformToPair(in -> in.sortByKey());

    JavaTestUtils.attachTestOutputStream(sorted);
    List<List<Tuple2<String, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testPairToNormalRDDTransform() {
    List<List<Tuple2<Integer, Integer>>> inputData = Arrays.asList(
      Arrays.asList(
        new Tuple2<>(3, 5),
        new Tuple2<>(1, 5),
        new Tuple2<>(4, 5),
        new Tuple2<>(2, 5)),
      Arrays.asList(
        new Tuple2<>(2, 5),
        new Tuple2<>(3, 5),
        new Tuple2<>(4, 5),
        new Tuple2<>(1, 5)));

    List<List<Integer>> expected = Arrays.asList(
      Arrays.asList(3, 1, 4, 2),
      Arrays.asList(2, 3, 4, 1));

    JavaDStream<Tuple2<Integer, Integer>> stream = JavaTestUtils.attachTestInputStream(
      ssc, inputData, 1);
    JavaPairDStream<Integer, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);
    JavaDStream<Integer> firstParts = pairStream.transform(in -> in.map(x -> x._1()));
    JavaTestUtils.attachTestOutputStream(firstParts);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testMapValues() {
    List<List<Tuple2<String, String>>> inputData = stringStringKVStream;

    List<List<Tuple2<String, String>>> expected = Arrays.asList(
      Arrays.asList(new Tuple2<>("california", "DODGERS"),
        new Tuple2<>("california", "GIANTS"),
        new Tuple2<>("new york", "YANKEES"),
        new Tuple2<>("new york", "METS")),
      Arrays.asList(new Tuple2<>("california", "SHARKS"),
        new Tuple2<>("california", "DUCKS"),
        new Tuple2<>("new york", "RANGERS"),
        new Tuple2<>("new york", "ISLANDERS")));

    JavaDStream<Tuple2<String, String>> stream = JavaTestUtils.attachTestInputStream(
      ssc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, String> mapped =
        pairStream.mapValues(s -> s.toUpperCase(Locale.ROOT));
    JavaTestUtils.attachTestOutputStream(mapped);
    List<List<Tuple2<String, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testFlatMapValues() {
    List<List<Tuple2<String, String>>> inputData = stringStringKVStream;

    List<List<Tuple2<String, String>>> expected = Arrays.asList(
      Arrays.asList(new Tuple2<>("california", "dodgers1"),
        new Tuple2<>("california", "dodgers2"),
        new Tuple2<>("california", "giants1"),
        new Tuple2<>("california", "giants2"),
        new Tuple2<>("new york", "yankees1"),
        new Tuple2<>("new york", "yankees2"),
        new Tuple2<>("new york", "mets1"),
        new Tuple2<>("new york", "mets2")),
      Arrays.asList(new Tuple2<>("california", "sharks1"),
        new Tuple2<>("california", "sharks2"),
        new Tuple2<>("california", "ducks1"),
        new Tuple2<>("california", "ducks2"),
        new Tuple2<>("new york", "rangers1"),
        new Tuple2<>("new york", "rangers2"),
        new Tuple2<>("new york", "islanders1"),
        new Tuple2<>("new york", "islanders2")));

    JavaDStream<Tuple2<String, String>> stream = JavaTestUtils.attachTestInputStream(
      ssc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, String> flatMapped =
      pairStream.flatMapValues(in -> Arrays.asList(in + "1", in + "2"));
    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<Tuple2<String, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);
    Assert.assertEquals(expected, result);
  }

  /**
   * This test is only for testing the APIs. It's not necessary to run it.
   */
  public void testMapWithStateAPI() {
    JavaPairRDD<String, Boolean> initialRDD = null;
    JavaPairDStream<String, Integer> wordsDstream = null;

    Function4<Time, String, Optional<Integer>, State<Boolean>, Optional<Double>> mapFn =
      (time, key, value, state) -> {
        // Use all State's methods here
        state.exists();
        state.get();
        state.isTimingOut();
        state.remove();
        state.update(true);
        return Optional.of(2.0);
      };

    JavaMapWithStateDStream<String, Integer, Boolean, Double> stateDstream =
      wordsDstream.mapWithState(
        StateSpec.function(mapFn)
          .initialState(initialRDD)
          .numPartitions(10)
          .partitioner(new HashPartitioner(10))
          .timeout(Durations.seconds(10)));

    JavaPairDStream<String, Boolean> emittedRecords = stateDstream.stateSnapshots();

    Function3<String, Optional<Integer>, State<Boolean>, Double> mapFn2 =
      (key, value, state) -> {
        state.exists();
        state.get();
        state.isTimingOut();
        state.remove();
        state.update(true);
        return 2.0;
      };

    JavaMapWithStateDStream<String, Integer, Boolean, Double> stateDstream2 =
      wordsDstream.mapWithState(
        StateSpec.function(mapFn2)
          .initialState(initialRDD)
          .numPartitions(10)
          .partitioner(new HashPartitioner(10))
          .timeout(Durations.seconds(10)));

    JavaPairDStream<String, Boolean> mappedDStream = stateDstream2.stateSnapshots();
  }
}
