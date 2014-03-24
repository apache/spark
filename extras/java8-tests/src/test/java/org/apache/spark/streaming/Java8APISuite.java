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

package org.apache.spark.streaming;

import java.io.Serializable;
import java.util.*;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Most of these tests replicate org.apache.spark.streaming.JavaAPISuite using java 8
 * lambda syntax.
 */
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
    JavaDStream<Integer> letterCount = stream.map(s -> s.length());
    JavaTestUtils.attachTestOutputStream(letterCount);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testFilter() {
    List<List<String>> inputData = Arrays.asList(
      Arrays.asList("giants", "dodgers"),
      Arrays.asList("yankees", "red socks"));

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
      Arrays.asList("yankees", "red socks"));

    List<List<String>> expected = Arrays.asList(
      Arrays.asList("GIANTSDODGERS"),
      Arrays.asList("YANKEESRED SOCKS"));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<String> mapped = stream.mapPartitions(in -> {
      String out = "";
      while (in.hasNext()) {
        out = out + in.next().toUpperCase();
      }
      return Lists.newArrayList(out);
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
    JavaDStream<Integer> reducedWindowed = stream.reduceByWindow((x, y) -> x + y,
      (x, y) -> x - y, new Duration(2000), new Duration(1000));
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
      Arrays.asList(Arrays.asList(new Tuple2<String, Integer>("x", 1)));
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
        new Tuple2<String, String>("california", "dodgers"),
        new Tuple2<String, String>("new york", "yankees")),
      Arrays.asList(
        new Tuple2<String, String>("california", "sharks"),
        new Tuple2<String, String>("new york", "rangers")));

    List<List<Tuple2<String, String>>> stringStringKVStream2 = Arrays.asList(
      Arrays.asList(
        new Tuple2<String, String>("california", "giants"),
        new Tuple2<String, String>("new york", "mets")),
      Arrays.asList(
        new Tuple2<String, String>("california", "ducks"),
        new Tuple2<String, String>("new york", "islanders")));


    List<HashSet<Tuple2<String, Tuple2<String, String>>>> expected = Arrays.asList(
      Sets.newHashSet(
        new Tuple2<String, Tuple2<String, String>>("california",
          new Tuple2<String, String>("dodgers", "giants")),
        new Tuple2<String, Tuple2<String, String>>("new york",
          new Tuple2<String, String>("yankees", "mets"))),
      Sets.newHashSet(
        new Tuple2<String, Tuple2<String, String>>("california",
          new Tuple2<String, String>("sharks", "ducks")),
        new Tuple2<String, Tuple2<String, String>>("new york",
          new Tuple2<String, String>("rangers", "islanders"))));

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
    List<HashSet<Tuple2<String, Tuple2<String, String>>>> unorderedResult = Lists.newArrayList();
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
      Arrays.asList(Arrays.asList(new Tuple2<String, Integer>("x", 1)));
    List<List<Tuple2<Double, Character>>> pairInputData2 =
      Arrays.asList(Arrays.asList(new Tuple2<Double, Character>(1.0, 'x')));
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
      Arrays.asList(new Tuple2<Integer, String>(1, "x")),
      Arrays.asList(new Tuple2<Integer, String>(2, "y"))
    );

    List<List<Tuple2<Integer, Tuple2<Integer, String>>>> expected = Arrays.asList(
      Arrays.asList(new Tuple2<Integer, Tuple2<Integer, String>>(1, new Tuple2<Integer, String>(1, "x"))),
      Arrays.asList(new Tuple2<Integer, Tuple2<Integer, String>>(2, new Tuple2<Integer, String>(2, "y")))
    );

    JavaDStream<Integer> stream1 = JavaTestUtils.attachTestInputStream(ssc, stream1input, 1);
    JavaDStream<Integer> stream2 = JavaTestUtils.attachTestInputStream(ssc, stream2input, 1);
    JavaPairDStream<Integer, String> pairStream1 = JavaPairDStream.fromJavaDStream(
      JavaTestUtils.attachTestInputStream(ssc, pairStream1input, 1));

    List<JavaDStream<?>> listOfDStreams1 = Arrays.<JavaDStream<?>>asList(stream1, stream2);

    // This is just to test whether this transform to JavaStream compiles
    JavaDStream<Long> transformed1 = ssc.transform(
      listOfDStreams1, (List<JavaRDD<?>> listOfRDDs, Time time) -> {
      assert (listOfRDDs.size() == 2);
      return null;
    });

    List<JavaDStream<?>> listOfDStreams2 =
      Arrays.<JavaDStream<?>>asList(stream1, stream2, pairStream1.toJavaDStream());

    JavaPairDStream<Integer, Tuple2<Integer, String>> transformed2 = ssc.transformToPair(
      listOfDStreams2, (List<JavaRDD<?>> listOfRDDs, Time time) -> {
      assert (listOfRDDs.size() == 3);
      JavaRDD<Integer> rdd1 = (JavaRDD<Integer>) listOfRDDs.get(0);
      JavaRDD<Integer> rdd2 = (JavaRDD<Integer>) listOfRDDs.get(1);
      JavaRDD<Tuple2<Integer, String>> rdd3 = (JavaRDD<Tuple2<Integer, String>>) listOfRDDs.get(2);
      JavaPairRDD<Integer, String> prdd3 = JavaPairRDD.fromJavaRDD(rdd3);
      PairFunction<Integer, Integer, Integer> mapToTuple =
        (Integer i) -> new Tuple2<Integer, Integer>(i, i);
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
    JavaDStream<String> flatMapped = stream.flatMap(s -> Lists.newArrayList(s.split("(?!^)")));
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
        new Tuple2<Integer, String>(6, "g"),
        new Tuple2<Integer, String>(6, "i"),
        new Tuple2<Integer, String>(6, "a"),
        new Tuple2<Integer, String>(6, "n"),
        new Tuple2<Integer, String>(6, "t"),
        new Tuple2<Integer, String>(6, "s")),
      Arrays.asList(
        new Tuple2<Integer, String>(7, "d"),
        new Tuple2<Integer, String>(7, "o"),
        new Tuple2<Integer, String>(7, "d"),
        new Tuple2<Integer, String>(7, "g"),
        new Tuple2<Integer, String>(7, "e"),
        new Tuple2<Integer, String>(7, "r"),
        new Tuple2<Integer, String>(7, "s")),
      Arrays.asList(
        new Tuple2<Integer, String>(9, "a"),
        new Tuple2<Integer, String>(9, "t"),
        new Tuple2<Integer, String>(9, "h"),
        new Tuple2<Integer, String>(9, "l"),
        new Tuple2<Integer, String>(9, "e"),
        new Tuple2<Integer, String>(9, "t"),
        new Tuple2<Integer, String>(9, "i"),
        new Tuple2<Integer, String>(9, "c"),
        new Tuple2<Integer, String>(9, "s")));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<Integer, String> flatMapped = stream.flatMapToPair(s -> {
      List<Tuple2<Integer, String>> out = Lists.newArrayList();
      for (String letter : s.split("(?!^)")) {
        out.add(new Tuple2<Integer, String>(s.length(), letter));
      }
      return out;
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
    for (List<T> list : expected) {
      Collections.sort(list);
    }
    for (List<T> list : actual) {
      Collections.sort(list);
    }
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testPairFilter() {
    List<List<String>> inputData = Arrays.asList(
      Arrays.asList("giants", "dodgers"),
      Arrays.asList("yankees", "red socks"));

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
      Arrays.asList(new Tuple2<String, Integer>("giants", 6)),
      Arrays.asList(new Tuple2<String, Integer>("yankees", 7)));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream =
      stream.mapToPair(x -> new Tuple2<>(x, x.length()));
    JavaPairDStream<String, Integer> filtered = pairStream.filter(x -> x._1().contains("a"));
    JavaTestUtils.attachTestOutputStream(filtered);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  List<List<Tuple2<String, String>>> stringStringKVStream = Arrays.asList(
    Arrays.asList(new Tuple2<String, String>("california", "dodgers"),
      new Tuple2<String, String>("california", "giants"),
      new Tuple2<String, String>("new york", "yankees"),
      new Tuple2<String, String>("new york", "mets")),
    Arrays.asList(new Tuple2<String, String>("california", "sharks"),
      new Tuple2<String, String>("california", "ducks"),
      new Tuple2<String, String>("new york", "rangers"),
      new Tuple2<String, String>("new york", "islanders")));

  List<List<Tuple2<String, Integer>>> stringIntKVStream = Arrays.asList(
    Arrays.asList(
      new Tuple2<String, Integer>("california", 1),
      new Tuple2<String, Integer>("california", 3),
      new Tuple2<String, Integer>("new york", 4),
      new Tuple2<String, Integer>("new york", 1)),
    Arrays.asList(
      new Tuple2<String, Integer>("california", 5),
      new Tuple2<String, Integer>("california", 5),
      new Tuple2<String, Integer>("new york", 3),
      new Tuple2<String, Integer>("new york", 1)));

  @Test
  public void testPairMap() { // Maps pair -> pair of different type
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<Integer, String>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<Integer, String>(1, "california"),
        new Tuple2<Integer, String>(3, "california"),
        new Tuple2<Integer, String>(4, "new york"),
        new Tuple2<Integer, String>(1, "new york")),
      Arrays.asList(
        new Tuple2<Integer, String>(5, "california"),
        new Tuple2<Integer, String>(5, "california"),
        new Tuple2<Integer, String>(3, "new york"),
        new Tuple2<Integer, String>(1, "new york")));

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);
    JavaPairDStream<Integer, String> reversed = pairStream.mapToPair(x -> x.swap());
    JavaTestUtils.attachTestOutputStream(reversed);
    List<List<Tuple2<Integer, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testPairMapPartitions() { // Maps pair -> pair of different type
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<Integer, String>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<Integer, String>(1, "california"),
        new Tuple2<Integer, String>(3, "california"),
        new Tuple2<Integer, String>(4, "new york"),
        new Tuple2<Integer, String>(1, "new york")),
      Arrays.asList(
        new Tuple2<Integer, String>(5, "california"),
        new Tuple2<Integer, String>(5, "california"),
        new Tuple2<Integer, String>(3, "new york"),
        new Tuple2<Integer, String>(1, "new york")));

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);
    JavaPairDStream<Integer, String> reversed = pairStream.mapPartitionsToPair(in -> {
      LinkedList<Tuple2<Integer, String>> out = new LinkedList<Tuple2<Integer, String>>();
      while (in.hasNext()) {
        Tuple2<String, Integer> next = in.next();
        out.add(next.swap());
      }
      return out;
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

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);
    JavaDStream<Integer> reversed = pairStream.map(in -> in._2());
    JavaTestUtils.attachTestOutputStream(reversed);
    List<List<Tuple2<Integer, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testPairToPairFlatMapWithChangingTypes() { // Maps pair -> pair
    List<List<Tuple2<String, Integer>>> inputData = Arrays.asList(
      Arrays.asList(
        new Tuple2<String, Integer>("hi", 1),
        new Tuple2<String, Integer>("ho", 2)),
      Arrays.asList(
        new Tuple2<String, Integer>("hi", 1),
        new Tuple2<String, Integer>("ho", 2)));

    List<List<Tuple2<Integer, String>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<Integer, String>(1, "h"),
        new Tuple2<Integer, String>(1, "i"),
        new Tuple2<Integer, String>(2, "h"),
        new Tuple2<Integer, String>(2, "o")),
      Arrays.asList(
        new Tuple2<Integer, String>(1, "h"),
        new Tuple2<Integer, String>(1, "i"),
        new Tuple2<Integer, String>(2, "h"),
        new Tuple2<Integer, String>(2, "o")));

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);
    JavaPairDStream<Integer, String> flatMapped = pairStream.flatMapToPair(in -> {
      List<Tuple2<Integer, String>> out = new LinkedList<Tuple2<Integer, String>>();
      for (Character s : in._1().toCharArray()) {
        out.add(new Tuple2<Integer, String>(in._2(), s.toString()));
      }
      return out;
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
        new Tuple2<String, Integer>("california", 4),
        new Tuple2<String, Integer>("new york", 5)),
      Arrays.asList(
        new Tuple2<String, Integer>("california", 10),
        new Tuple2<String, Integer>("new york", 4)));

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
        new Tuple2<String, Integer>("california", 4),
        new Tuple2<String, Integer>("new york", 5)),
      Arrays.asList(
        new Tuple2<String, Integer>("california", 10),
        new Tuple2<String, Integer>("new york", 4)));

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(
      ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> combined = pairStream.<Integer>combineByKey(i -> i,
      (x, y) -> x + y, (x, y) -> x + y, new HashPartitioner(2));

    JavaTestUtils.attachTestOutputStream(combined);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testReduceByKeyAndWindow() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
      Arrays.asList(new Tuple2<String, Integer>("california", 4),
        new Tuple2<String, Integer>("new york", 5)),
      Arrays.asList(new Tuple2<String, Integer>("california", 14),
        new Tuple2<String, Integer>("new york", 9)),
      Arrays.asList(new Tuple2<String, Integer>("california", 10),
        new Tuple2<String, Integer>("new york", 4)));

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
      Arrays.asList(new Tuple2<String, Integer>("california", 4),
        new Tuple2<String, Integer>("new york", 5)),
      Arrays.asList(new Tuple2<String, Integer>("california", 14),
        new Tuple2<String, Integer>("new york", 9)),
      Arrays.asList(new Tuple2<String, Integer>("california", 14),
        new Tuple2<String, Integer>("new york", 9)));

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
      Arrays.asList(new Tuple2<String, Integer>("california", 4),
        new Tuple2<String, Integer>("new york", 5)),
      Arrays.asList(new Tuple2<String, Integer>("california", 14),
        new Tuple2<String, Integer>("new york", 9)),
      Arrays.asList(new Tuple2<String, Integer>("california", 10),
        new Tuple2<String, Integer>("new york", 4)));

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
        new Tuple2<Integer, Integer>(3, 5),
        new Tuple2<Integer, Integer>(1, 5),
        new Tuple2<Integer, Integer>(4, 5),
        new Tuple2<Integer, Integer>(2, 5)),
      Arrays.asList(
        new Tuple2<Integer, Integer>(2, 5),
        new Tuple2<Integer, Integer>(3, 5),
        new Tuple2<Integer, Integer>(4, 5),
        new Tuple2<Integer, Integer>(1, 5)));

    List<List<Tuple2<Integer, Integer>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<Integer, Integer>(1, 5),
        new Tuple2<Integer, Integer>(2, 5),
        new Tuple2<Integer, Integer>(3, 5),
        new Tuple2<Integer, Integer>(4, 5)),
      Arrays.asList(
        new Tuple2<Integer, Integer>(1, 5),
        new Tuple2<Integer, Integer>(2, 5),
        new Tuple2<Integer, Integer>(3, 5),
        new Tuple2<Integer, Integer>(4, 5)));

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
        new Tuple2<Integer, Integer>(3, 5),
        new Tuple2<Integer, Integer>(1, 5),
        new Tuple2<Integer, Integer>(4, 5),
        new Tuple2<Integer, Integer>(2, 5)),
      Arrays.asList(
        new Tuple2<Integer, Integer>(2, 5),
        new Tuple2<Integer, Integer>(3, 5),
        new Tuple2<Integer, Integer>(4, 5),
        new Tuple2<Integer, Integer>(1, 5)));

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
      Arrays.asList(new Tuple2<String, String>("california", "DODGERS"),
        new Tuple2<String, String>("california", "GIANTS"),
        new Tuple2<String, String>("new york", "YANKEES"),
        new Tuple2<String, String>("new york", "METS")),
      Arrays.asList(new Tuple2<String, String>("california", "SHARKS"),
        new Tuple2<String, String>("california", "DUCKS"),
        new Tuple2<String, String>("new york", "RANGERS"),
        new Tuple2<String, String>("new york", "ISLANDERS")));

    JavaDStream<Tuple2<String, String>> stream = JavaTestUtils.attachTestInputStream(
      ssc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, String> mapped = pairStream.mapValues(s -> s.toUpperCase());
    JavaTestUtils.attachTestOutputStream(mapped);
    List<List<Tuple2<String, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testFlatMapValues() {
    List<List<Tuple2<String, String>>> inputData = stringStringKVStream;

    List<List<Tuple2<String, String>>> expected = Arrays.asList(
      Arrays.asList(new Tuple2<String, String>("california", "dodgers1"),
        new Tuple2<String, String>("california", "dodgers2"),
        new Tuple2<String, String>("california", "giants1"),
        new Tuple2<String, String>("california", "giants2"),
        new Tuple2<String, String>("new york", "yankees1"),
        new Tuple2<String, String>("new york", "yankees2"),
        new Tuple2<String, String>("new york", "mets1"),
        new Tuple2<String, String>("new york", "mets2")),
      Arrays.asList(new Tuple2<String, String>("california", "sharks1"),
        new Tuple2<String, String>("california", "sharks2"),
        new Tuple2<String, String>("california", "ducks1"),
        new Tuple2<String, String>("california", "ducks2"),
        new Tuple2<String, String>("new york", "rangers1"),
        new Tuple2<String, String>("new york", "rangers2"),
        new Tuple2<String, String>("new york", "islanders1"),
        new Tuple2<String, String>("new york", "islanders2")));

    JavaDStream<Tuple2<String, String>> stream = JavaTestUtils.attachTestInputStream(
      ssc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);


    JavaPairDStream<String, String> flatMapped = pairStream.flatMapValues(in -> {
      List<String> out = new ArrayList<String>();
      out.add(in + "1");
      out.add(in + "2");
      return out;
    });
    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<Tuple2<String, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);
    Assert.assertEquals(expected, result);
  }

}
