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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.JavaCheckpointTestUtils;
import org.apache.spark.streaming.JavaTestUtils;
import org.apache.spark.streaming.LocalJavaStreamingContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.StreamingContextSuite;
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.io.Files;
import com.google.common.collect.Sets;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.Utils;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaAPISuite extends LocalJavaStreamingContext implements Serializable {

  public static void equalIterator(Iterator<?> a, Iterator<?> b) {
    while (a.hasNext() && b.hasNext()) {
      Assertions.assertEquals(a.next(), b.next());
    }
    Assertions.assertEquals(a.hasNext(), b.hasNext());
  }

  public static void equalIterable(Iterable<?> a, Iterable<?> b) {
      equalIterator(a.iterator(), b.iterator());
  }

  @Test
  public void testInitialization() {
    Assertions.assertNotNull(ssc.sparkContext());
  }

  @Test
  public void testContextState() {
    List<List<Integer>> inputData = Arrays.asList(Arrays.asList(1, 2, 3, 4));
    Assertions.assertEquals(StreamingContextState.INITIALIZED, ssc.getState());
    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaTestUtils.attachTestOutputStream(stream);
    Assertions.assertEquals(StreamingContextState.INITIALIZED, ssc.getState());
    ssc.start();
    Assertions.assertEquals(StreamingContextState.ACTIVE, ssc.getState());
    ssc.stop();
    Assertions.assertEquals(StreamingContextState.STOPPED, ssc.getState());
  }

  @Test
  public void testCount() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3,4),
        Arrays.asList(3,4,5),
        Arrays.asList(3));

    List<List<Long>> expected = Arrays.asList(
        Arrays.asList(4L),
        Arrays.asList(3L),
        Arrays.asList(1L));

    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Long> count = stream.count();
    JavaTestUtils.attachTestOutputStream(count);
    List<List<Long>> result = JavaTestUtils.runStreams(ssc, 3, 3);
    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testMap() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("hello", "world"),
        Arrays.asList("goodnight", "moon"));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(5,5),
        Arrays.asList(9,4));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> letterCount = stream.map(String::length);
    JavaTestUtils.attachTestOutputStream(letterCount);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testWindow() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6,1,2,3),
        Arrays.asList(7,8,9,4,5,6),
        Arrays.asList(7,8,9));

    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> windowed = stream.window(new Duration(2000));
    JavaTestUtils.attachTestOutputStream(windowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 4, 4);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testWindowWithSlideDuration() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9),
        Arrays.asList(10,11,12),
        Arrays.asList(13,14,15),
        Arrays.asList(16,17,18));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,2,3,4,5,6),
        Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12),
        Arrays.asList(7,8,9,10,11,12,13,14,15,16,17,18),
        Arrays.asList(13,14,15,16,17,18));

    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> windowed = stream.window(new Duration(4000), new Duration(2000));
    JavaTestUtils.attachTestOutputStream(windowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 8, 4);

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
  public void testRepartitionMorePartitions() {
    List<List<Integer>> inputData = Arrays.asList(
      Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    JavaDStream<Integer> stream =
        JavaTestUtils.attachTestInputStream(ssc, inputData, 2);
    JavaDStreamLike<Integer,JavaDStream<Integer>,JavaRDD<Integer>> repartitioned =
        stream.repartition(4);
    JavaTestUtils.attachTestOutputStream(repartitioned);
    List<List<List<Integer>>> result = JavaTestUtils.runStreamsWithPartitions(ssc, 2, 2);
    Assertions.assertEquals(2, result.size());
    for (List<List<Integer>> rdd : result) {
      Assertions.assertEquals(4, rdd.size());
      Assertions.assertEquals(
        10, rdd.get(0).size() + rdd.get(1).size() + rdd.get(2).size() + rdd.get(3).size());
    }
  }

  @Test
  public void testRepartitionFewerPartitions() {
    List<List<Integer>> inputData = Arrays.asList(
      Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    JavaDStream<Integer> stream =
        JavaTestUtils.attachTestInputStream(ssc, inputData, 4);
    JavaDStreamLike<Integer,JavaDStream<Integer>,JavaRDD<Integer>> repartitioned =
        stream.repartition(2);
    JavaTestUtils.attachTestOutputStream(repartitioned);
    List<List<List<Integer>>> result = JavaTestUtils.runStreamsWithPartitions(ssc, 2, 2);
    Assertions.assertEquals(2, result.size());
    for (List<List<Integer>> rdd : result) {
      Assertions.assertEquals(2, rdd.size());
      Assertions.assertEquals(10, rdd.get(0).size() + rdd.get(1).size());
    }
  }

  @Test
  public void testGlom() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red sox"));

    List<List<List<String>>> expected = Arrays.asList(
        Arrays.asList(Arrays.asList("giants", "dodgers")),
        Arrays.asList(Arrays.asList("yankees", "red sox")));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<List<String>> glommed = stream.glom();
    JavaTestUtils.attachTestOutputStream(glommed);
    List<List<List<String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
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
        StringBuilder out = new StringBuilder();
        while (in.hasNext()) {
          out.append(in.next().toUpperCase(Locale.ROOT));
        }
        return Arrays.asList(out.toString()).iterator();
      });
    JavaTestUtils.attachTestOutputStream(mapped);
    List<List<String>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
  }

  private static class IntegerSum implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer i1, Integer i2) {
      return i1 + i2;
    }
  }

  private static class IntegerDifference implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer i1, Integer i2) {
      return i1 - i2;
    }
  }

  @Test
  public void testReduce() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(6),
        Arrays.asList(15),
        Arrays.asList(24));

    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> reduced = stream.reduce(new IntegerSum());
    JavaTestUtils.attachTestOutputStream(reduced);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testReduceByWindowWithInverse() {
    testReduceByWindow(true);
  }

  @Test
  public void testReduceByWindowWithoutInverse() {
    testReduceByWindow(false);
  }

  private void testReduceByWindow(boolean withInverse) {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(6),
        Arrays.asList(21),
        Arrays.asList(39),
        Arrays.asList(24));

    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> reducedWindowed;
    if (withInverse) {
      reducedWindowed = stream.reduceByWindow(new IntegerSum(),
                                              new IntegerDifference(),
                                              new Duration(2000),
                                              new Duration(1000));
    } else {
      reducedWindowed = stream.reduceByWindow(new IntegerSum(),
                                              new Duration(2000), new Duration(1000));
    }
    JavaTestUtils.attachTestOutputStream(reducedWindowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 4, 4);

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testQueueStream() {
    ssc.stop();
    // Create a new JavaStreamingContext without checkpointing
    SparkConf conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("test")
        .set("spark.streaming.clock", "org.apache.spark.util.ManualClock");
    ssc = new JavaStreamingContext(conf, new Duration(1000));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    JavaSparkContext jsc = new JavaSparkContext(ssc.ssc().sc());
    JavaRDD<Integer> rdd1 = jsc.parallelize(Arrays.asList(1, 2, 3));
    JavaRDD<Integer> rdd2 = jsc.parallelize(Arrays.asList(4, 5, 6));
    JavaRDD<Integer> rdd3 = jsc.parallelize(Arrays.asList(7,8,9));

    Queue<JavaRDD<Integer>> rdds = new LinkedList<>();
    rdds.add(rdd1);
    rdds.add(rdd2);
    rdds.add(rdd3);

    JavaDStream<Integer> stream = ssc.queueStream(rdds);
    JavaTestUtils.attachTestOutputStream(stream);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 3, 3);
    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testTransform() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(3,4,5),
        Arrays.asList(6,7,8),
        Arrays.asList(9,10,11));

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

    stream.transform(in -> null);

    stream.transform((in, time) -> null);

    stream.transformToPair(in -> null);

    stream.transformToPair((in, time) -> null);

    pairStream.transform(in -> null);

    pairStream.transform((in, time) -> null);

    pairStream.transformToPair(in -> null);

    pairStream.transformToPair((in, time) -> null);

  }

  @SuppressWarnings("unchecked")
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


    List<HashSet<Tuple2<String, Tuple2<String, String>>>> expected = Arrays.asList(
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

    JavaPairDStream<String, Tuple2<String, String>> joined = pairStream1.transformWithToPair(
        pairStream2,
        (rdd1, rdd2, time) -> rdd1.join(rdd2)
    );

    JavaTestUtils.attachTestOutputStream(joined);
    List<List<Tuple2<String, Tuple2<String, String>>>> result = JavaTestUtils.runStreams(ssc, 2, 2);
    List<HashSet<Tuple2<String, Tuple2<String, String>>>> unorderedResult = new ArrayList<>();
    for (List<Tuple2<String, Tuple2<String, String>>> res: result) {
      unorderedResult.add(Sets.newHashSet(res));
    }

    Assertions.assertEquals(expected, unorderedResult);
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

    stream1.transformWith(stream2, (rdd1, rdd2, time) -> null);

    stream1.transformWith(pairStream1, (rdd1, rdd2, time) -> null);

    stream1.transformWithToPair(stream2, (rdd1, rdd2, time) -> null);

    stream1.transformWithToPair(pairStream1, (rdd1, rdd2, time) -> null);

    pairStream1.transformWith(stream2, (rdd1, rdd2, time) -> null);

    pairStream1.transformWith(pairStream1, (rdd1, rdd2, time) -> null);

    pairStream1.transformWithToPair(stream2, (rdd1, rdd2, time) -> null);

    pairStream1.transformWithToPair(pairStream2, (rdd1, rdd2, time) -> null);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStreamingContextTransform(){
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
    ssc.transform(
      listOfDStreams1,
      (listOfRDDs, time) -> {
        Assertions.assertEquals(2, listOfRDDs.size());
        return null;
      }
    );

    List<JavaDStream<?>> listOfDStreams2 =
        Arrays.asList(stream1, stream2, pairStream1.toJavaDStream());

    JavaPairDStream<Integer, Tuple2<Integer, String>> transformed2 = ssc.transformToPair(
      listOfDStreams2,
      (listOfRDDs, time) -> {
        Assertions.assertEquals(3, listOfRDDs.size());
        JavaRDD<Integer> rdd1 = (JavaRDD<Integer>)listOfRDDs.get(0);
        JavaRDD<Integer> rdd2 = (JavaRDD<Integer>)listOfRDDs.get(1);
        JavaRDD<Tuple2<Integer, String>> rdd3 =
          (JavaRDD<Tuple2<Integer, String>>)listOfRDDs.get(2);
        JavaPairRDD<Integer, String> prdd3 = JavaPairRDD.fromJavaRDD(rdd3);
        PairFunction<Integer, Integer, Integer> mapToTuple =
            (PairFunction<Integer, Integer, Integer>) i -> new Tuple2<>(i, i);
        return rdd1.union(rdd2).mapToPair(mapToTuple).join(prdd3);
      }
    );
    JavaTestUtils.attachTestOutputStream(transformed2);
    List<List<Tuple2<Integer, Tuple2<Integer, String>>>> result =
      JavaTestUtils.runStreams(ssc, 2, 2);
    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testFlatMap() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("go", "giants"),
        Arrays.asList("boo", "dodgers"),
        Arrays.asList("athletics"));

    List<List<String>> expected = Arrays.asList(
        Arrays.asList("g","o","g","i","a","n","t","s"),
        Arrays.asList("b", "o", "o", "d","o","d","g","e","r","s"),
        Arrays.asList("a","t","h","l","e","t","i","c","s"));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<String> flatMapped =
      stream.flatMap(x -> Arrays.asList(x.split("(?!^)")).iterator());
    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<String>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testForeachRDD() {
    final LongAccumulator accumRdd = ssc.sparkContext().sc().longAccumulator();
    final LongAccumulator accumEle = ssc.sparkContext().sc().longAccumulator();
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,1,1),
        Arrays.asList(1,1,1));

    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaTestUtils.attachTestOutputStream(stream.count()); // dummy output

    stream.foreachRDD(rdd -> {
      accumRdd.add(1);
      rdd.foreach(i -> accumEle.add(1));
    });

    // This is a test to make sure foreachRDD(VoidFunction2) can be called from Java
    stream.foreachRDD((rdd, time) -> {});

    JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(2, accumRdd.value().intValue());
    Assertions.assertEquals(6, accumEle.value().intValue());
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
    JavaPairDStream<Integer, String> flatMapped = stream.flatMapToPair(in -> {
        List<Tuple2<Integer, String>> out = new ArrayList<>();
        for (String letter : in.split("(?!^)")) {
          out.add(new Tuple2<>(in.length(), letter));
        }
        return out.iterator();
      });
    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<Tuple2<Integer, String>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testUnion() {
    List<List<Integer>> inputData1 = Arrays.asList(
        Arrays.asList(1,1),
        Arrays.asList(2,2),
        Arrays.asList(3,3));

    List<List<Integer>> inputData2 = Arrays.asList(
        Arrays.asList(4,4),
        Arrays.asList(5,5),
        Arrays.asList(6,6));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,1,4,4),
        Arrays.asList(2,2,5,5),
        Arrays.asList(3,3,6,6));

    JavaDStream<Integer> stream1 = JavaTestUtils.attachTestInputStream(ssc, inputData1, 2);
    JavaDStream<Integer> stream2 = JavaTestUtils.attachTestInputStream(ssc, inputData2, 2);

    JavaDStream<Integer> unioned = stream1.union(stream2);
    JavaTestUtils.attachTestOutputStream(unioned);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    assertOrderInvariantEquals(expected, result);
  }

  /*
   * Performs an order-invariant comparison of lists representing two RDD streams. This allows
   * us to account for ordering variation within individual RDD's which occurs during windowing.
   */
  public static <T> void assertOrderInvariantEquals(
      List<List<T>> expected, List<List<T>> actual) {
    List<Set<T>> expectedSets = new ArrayList<>();
    for (List<T> list: expected) {
      expectedSets.add(Set.copyOf(list));
    }
    List<Set<T>> actualSets = new ArrayList<>();
    for (List<T> list: actual) {
      actualSets.add(Set.copyOf(list));
    }
    Assertions.assertEquals(expectedSets, actualSets);
  }


  // PairDStream Functions
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
        stream.mapToPair(in -> new Tuple2<>(in, in.length()));

    JavaPairDStream<String, Integer> filtered = pairStream.filter(in -> in._1().contains("a"));
    JavaTestUtils.attachTestOutputStream(filtered);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
  }

  private final List<List<Tuple2<String, String>>> stringStringKVStream = Arrays.asList(
      Arrays.asList(new Tuple2<>("california", "dodgers"),
                    new Tuple2<>("california", "giants"),
                    new Tuple2<>("new york", "yankees"),
                    new Tuple2<>("new york", "mets")),
      Arrays.asList(new Tuple2<>("california", "sharks"),
                    new Tuple2<>("california", "ducks"),
                    new Tuple2<>("new york", "rangers"),
                    new Tuple2<>("new york", "islanders")));

  private final List<List<Tuple2<String, Integer>>> stringIntKVStream = Arrays.asList(
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

    Assertions.assertEquals(expected, result);
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
        List<Tuple2<Integer, String>> out = new LinkedList<>();
        while (in.hasNext()) {
          Tuple2<String, Integer> next = in.next();
          out.add(next.swap());
        }
        return out.iterator();
      });

    JavaTestUtils.attachTestOutputStream(reversed);
    List<List<Tuple2<Integer, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
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
    JavaDStream<Integer> reversed = pairStream.map(in -> in._2());

    JavaTestUtils.attachTestOutputStream(reversed);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
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
    List<List<Tuple2<Integer, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testPairGroupByKey() {
    List<List<Tuple2<String, String>>> inputData = stringStringKVStream;

    List<List<Tuple2<String, List<String>>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<>("california", Arrays.asList("dodgers", "giants")),
            new Tuple2<>("new york", Arrays.asList("yankees", "mets"))),
        Arrays.asList(
            new Tuple2<>("california", Arrays.asList("sharks", "ducks")),
            new Tuple2<>("new york", Arrays.asList("rangers", "islanders"))));

    JavaDStream<Tuple2<String, String>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Iterable<String>> grouped = pairStream.groupByKey();
    JavaTestUtils.attachTestOutputStream(grouped);
    List<List<Tuple2<String, Iterable<String>>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected.size(), result.size());
    Iterator<List<Tuple2<String, Iterable<String>>>> resultItr = result.iterator();
    Iterator<List<Tuple2<String, List<String>>>> expectedItr = expected.iterator();
    while (resultItr.hasNext() && expectedItr.hasNext()) {
      Iterator<Tuple2<String, Iterable<String>>> resultElements = resultItr.next().iterator();
      Iterator<Tuple2<String, List<String>>> expectedElements = expectedItr.next().iterator();
      while (resultElements.hasNext() && expectedElements.hasNext()) {
        Tuple2<String, Iterable<String>> resultElement = resultElements.next();
        Tuple2<String, List<String>> expectedElement = expectedElements.next();
        Assertions.assertEquals(expectedElement._1(), resultElement._1());
        equalIterable(expectedElement._2(), resultElement._2());
      }
      Assertions.assertEquals(resultElements.hasNext(), expectedElements.hasNext());
    }
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

    JavaPairDStream<String, Integer> reduced = pairStream.reduceByKey(new IntegerSum());

    JavaTestUtils.attachTestOutputStream(reduced);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
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

    JavaPairDStream<String, Integer> combined = pairStream.combineByKey(
        i -> i, new IntegerSum(), new IntegerSum(), new HashPartitioner(2));

    JavaTestUtils.attachTestOutputStream(combined);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testCountByValue() {
    List<List<String>> inputData = Arrays.asList(
      Arrays.asList("hello", "world"),
      Arrays.asList("hello", "moon"),
      Arrays.asList("hello"));

    List<List<Tuple2<String, Long>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<>("hello", 1L),
            new Tuple2<>("world", 1L)),
        Arrays.asList(
            new Tuple2<>("hello", 1L),
            new Tuple2<>("moon", 1L)),
        Arrays.asList(
            new Tuple2<>("hello", 1L)));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Long> counted = stream.countByValue();
    JavaTestUtils.attachTestOutputStream(counted);
    List<List<Tuple2<String, Long>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testGroupByKeyAndWindow() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, List<Integer>>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<>("california", Arrays.asList(1, 3)),
        new Tuple2<>("new york", Arrays.asList(1, 4))
      ),
      Arrays.asList(
        new Tuple2<>("california", Arrays.asList(1, 3, 5, 5)),
        new Tuple2<>("new york", Arrays.asList(1, 1, 3, 4))
      ),
      Arrays.asList(
        new Tuple2<>("california", Arrays.asList(5, 5)),
        new Tuple2<>("new york", Arrays.asList(1, 3))
      )
    );

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Iterable<Integer>> groupWindowed =
        pairStream.groupByKeyAndWindow(new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(groupWindowed);
    List<List<Tuple2<String, List<Integer>>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assertions.assertEquals(expected.size(), result.size());
    for (int i = 0; i < result.size(); i++) {
      Assertions.assertEquals(convert(expected.get(i)), convert(result.get(i)));
    }
  }

  private static Set<Tuple2<String, HashSet<Integer>>>
    convert(List<Tuple2<String, List<Integer>>> listOfTuples) {
    List<Tuple2<String, HashSet<Integer>>> newListOfTuples = new ArrayList<>();
    for (Tuple2<String, List<Integer>> tuple: listOfTuples) {
      newListOfTuples.add(convert(tuple));
    }
    return new HashSet<>(newListOfTuples);
  }

  private static Tuple2<String, HashSet<Integer>> convert(Tuple2<String, List<Integer>> tuple) {
    return new Tuple2<>(tuple._1(), new HashSet<>(tuple._2()));
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
        pairStream.reduceByKeyAndWindow(new IntegerSum(), new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reduceWindowed);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assertions.assertEquals(expected, result);
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
          out += state.get();
        }
        for (Integer v : values) {
          out += v;
        }
        return Optional.of(out);
      });
    JavaTestUtils.attachTestOutputStream(updated);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testUpdateStateByKeyWithInitial() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<Tuple2<String, Integer>> initial = Arrays.asList(
        new Tuple2<>("california", 1),
            new Tuple2<>("new york", 2));

    JavaRDD<Tuple2<String, Integer>> tmpRDD = ssc.sparkContext().parallelize(initial);
    JavaPairRDD<String, Integer> initialRDD = JavaPairRDD.fromJavaRDD(tmpRDD);

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
        Arrays.asList(new Tuple2<>("california", 5),
                      new Tuple2<>("new york", 7)),
        Arrays.asList(new Tuple2<>("california", 15),
                      new Tuple2<>("new york", 11)),
        Arrays.asList(new Tuple2<>("california", 15),
                      new Tuple2<>("new york", 11)));

    JavaDStream<Tuple2<String, Integer>> stream =
      JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> updated = pairStream.updateStateByKey((values, state) -> {
        int out = 0;
        if (state.isPresent()) {
          out += state.get();
        }
        for (Integer v : values) {
          out += v;
        }
        return Optional.of(out);
      }, new HashPartitioner(1), initialRDD);
    JavaTestUtils.attachTestOutputStream(updated);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    assertOrderInvariantEquals(expected, result);
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
        pairStream.reduceByKeyAndWindow(new IntegerSum(), new IntegerDifference(),
                                        new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reduceWindowed);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assertions.assertEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCountByValueAndWindow() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("hello", "world"),
        Arrays.asList("hello", "moon"),
        Arrays.asList("hello"));

    List<HashSet<Tuple2<String, Long>>> expected = Arrays.asList(
        Sets.newHashSet(
            new Tuple2<>("hello", 1L),
            new Tuple2<>("world", 1L)),
        Sets.newHashSet(
            new Tuple2<>("hello", 2L),
            new Tuple2<>("world", 1L),
            new Tuple2<>("moon", 1L)),
        Sets.newHashSet(
            new Tuple2<>("hello", 2L),
            new Tuple2<>("moon", 1L)));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(
        ssc, inputData, 1);
    JavaPairDStream<String, Long> counted =
      stream.countByValueAndWindow(new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(counted);
    List<List<Tuple2<String, Long>>> result = JavaTestUtils.runStreams(ssc, 3, 3);
    List<Set<Tuple2<String, Long>>> unorderedResult = new ArrayList<>();
    for (List<Tuple2<String, Long>> res: result) {
      unorderedResult.add(Sets.newHashSet(res));
    }

    Assertions.assertEquals(expected, unorderedResult);
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
    List<List<Tuple2<Integer, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
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
        Arrays.asList(3,1,4,2),
        Arrays.asList(2,3,4,1));

    JavaDStream<Tuple2<Integer, Integer>> stream = JavaTestUtils.attachTestInputStream(
        ssc, inputData, 1);
    JavaPairDStream<Integer, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaDStream<Integer> firstParts = pairStream.transform(in -> in.map(in2 -> in2._1()));

    JavaTestUtils.attachTestOutputStream(firstParts);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
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

    Assertions.assertEquals(expected, result);
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


    JavaPairDStream<String, String> flatMapped = pairStream.flatMapValues(in -> {
        List<String> out = new ArrayList<>();
        out.add(in + "1");
        out.add(in + "2");
        return out.iterator();
      });

    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<Tuple2<String, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testCoGroup() {
    List<List<Tuple2<String, String>>> stringStringKVStream1 = Arrays.asList(
        Arrays.asList(new Tuple2<>("california", "dodgers"),
                      new Tuple2<>("new york", "yankees")),
        Arrays.asList(new Tuple2<>("california", "sharks"),
                      new Tuple2<>("new york", "rangers")));

    List<List<Tuple2<String, String>>> stringStringKVStream2 = Arrays.asList(
        Arrays.asList(new Tuple2<>("california", "giants"),
                      new Tuple2<>("new york", "mets")),
        Arrays.asList(new Tuple2<>("california", "ducks"),
                      new Tuple2<>("new york", "islanders")));


    List<List<Tuple2<String, Tuple2<List<String>, List<String>>>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<>("california",
                         new Tuple2<>(Arrays.asList("dodgers"), Arrays.asList("giants"))),
            new Tuple2<>("new york",
                         new Tuple2<>(Arrays.asList("yankees"), Arrays.asList("mets")))),
        Arrays.asList(
            new Tuple2<>("california",
                         new Tuple2<>(Arrays.asList("sharks"), Arrays.asList("ducks"))),
            new Tuple2<>("new york",
                         new Tuple2<>(Arrays.asList("rangers"), Arrays.asList("islanders")))));


    JavaDStream<Tuple2<String, String>> stream1 = JavaTestUtils.attachTestInputStream(
        ssc, stringStringKVStream1, 1);
    JavaPairDStream<String, String> pairStream1 = JavaPairDStream.fromJavaDStream(stream1);

    JavaDStream<Tuple2<String, String>> stream2 = JavaTestUtils.attachTestInputStream(
        ssc, stringStringKVStream2, 1);
    JavaPairDStream<String, String> pairStream2 = JavaPairDStream.fromJavaDStream(stream2);

    JavaPairDStream<String, Tuple2<Iterable<String>, Iterable<String>>> grouped =
        pairStream1.cogroup(pairStream2);
    JavaTestUtils.attachTestOutputStream(grouped);
    List<List<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>> result =
        JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected.size(), result.size());
    Iterator<List<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>> resultItr =
        result.iterator();
    Iterator<List<Tuple2<String, Tuple2<List<String>, List<String>>>>> expectedItr =
        expected.iterator();
    while (resultItr.hasNext() && expectedItr.hasNext()) {
      Iterator<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>> resultElements =
          resultItr.next().iterator();
      Iterator<Tuple2<String, Tuple2<List<String>, List<String>>>> expectedElements =
          expectedItr.next().iterator();
      while (resultElements.hasNext() && expectedElements.hasNext()) {
        Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> resultElement =
            resultElements.next();
        Tuple2<String, Tuple2<List<String>, List<String>>> expectedElement =
            expectedElements.next();
        Assertions.assertEquals(expectedElement._1(), resultElement._1());
        equalIterable(expectedElement._2()._1(), resultElement._2()._1());
        equalIterable(expectedElement._2()._2(), resultElement._2()._2());
      }
      Assertions.assertEquals(resultElements.hasNext(), expectedElements.hasNext());
    }
  }

  @Test
  public void testJoin() {
    List<List<Tuple2<String, String>>> stringStringKVStream1 = Arrays.asList(
        Arrays.asList(new Tuple2<>("california", "dodgers"),
                      new Tuple2<>("new york", "yankees")),
        Arrays.asList(new Tuple2<>("california", "sharks"),
                      new Tuple2<>("new york", "rangers")));

    List<List<Tuple2<String, String>>> stringStringKVStream2 = Arrays.asList(
        Arrays.asList(new Tuple2<>("california", "giants"),
                      new Tuple2<>("new york", "mets")),
        Arrays.asList(new Tuple2<>("california", "ducks"),
                      new Tuple2<>("new york", "islanders")));


    List<List<Tuple2<String, Tuple2<String, String>>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<>("california",
                         new Tuple2<>("dodgers", "giants")),
            new Tuple2<>("new york",
                         new Tuple2<>("yankees", "mets"))),
        Arrays.asList(
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

    JavaPairDStream<String, Tuple2<String, String>> joined = pairStream1.join(pairStream2);
    JavaTestUtils.attachTestOutputStream(joined);
    List<List<Tuple2<String, Tuple2<String, String>>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testLeftOuterJoin() {
    List<List<Tuple2<String, String>>> stringStringKVStream1 = Arrays.asList(
        Arrays.asList(new Tuple2<>("california", "dodgers"),
                      new Tuple2<>("new york", "yankees")),
        Arrays.asList(new Tuple2<>("california", "sharks") ));

    List<List<Tuple2<String, String>>> stringStringKVStream2 = Arrays.asList(
        Arrays.asList(new Tuple2<>("california", "giants") ),
        Arrays.asList(new Tuple2<>("new york", "islanders") )

    );

    List<List<Long>> expected = Arrays.asList(Arrays.asList(2L), Arrays.asList(1L));

    JavaDStream<Tuple2<String, String>> stream1 = JavaTestUtils.attachTestInputStream(
        ssc, stringStringKVStream1, 1);
    JavaPairDStream<String, String> pairStream1 = JavaPairDStream.fromJavaDStream(stream1);

    JavaDStream<Tuple2<String, String>> stream2 = JavaTestUtils.attachTestInputStream(
        ssc, stringStringKVStream2, 1);
    JavaPairDStream<String, String> pairStream2 = JavaPairDStream.fromJavaDStream(stream2);

    JavaPairDStream<String, Tuple2<String, Optional<String>>> joined =
        pairStream1.leftOuterJoin(pairStream2);
    JavaDStream<Long> counted = joined.count();
    JavaTestUtils.attachTestOutputStream(counted);
    List<List<Long>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void testCheckpointMasterRecovery() throws InterruptedException, IOException {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("this", "is"),
        Arrays.asList("a", "test"),
        Arrays.asList("counting", "letters"));

    List<List<Integer>> expectedInitial = Arrays.asList(
        Arrays.asList(4,2));
    List<List<Integer>> expectedFinal = Arrays.asList(
        Arrays.asList(1,4),
        Arrays.asList(8,7));

    File tempDir = Utils.createTempDir();
    tempDir.deleteOnExit();
    ssc.checkpoint(tempDir.getAbsolutePath());

    JavaDStream<String> stream = JavaCheckpointTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> letterCount = stream.map(String::length);
    JavaCheckpointTestUtils.attachTestOutputStream(letterCount);
    List<List<Integer>> initialResult = JavaTestUtils.runStreams(ssc, 1, 1);

    assertOrderInvariantEquals(expectedInitial, initialResult);
    Thread.sleep(1000);
    ssc.stop();

    ssc = new JavaStreamingContext(tempDir.getAbsolutePath());
    // Tweak to take into consideration that the last batch before failure
    // will be re-processed after recovery
    List<List<Integer>> finalResult = JavaCheckpointTestUtils.runStreams(ssc, 2, 3);
    assertOrderInvariantEquals(expectedFinal, finalResult.subList(1, 3));
    ssc.stop();
    Utils.deleteRecursively(tempDir);
  }

  @Test
  public void testContextGetOrCreate() throws IOException {
    ssc.stop();

    SparkConf conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("test")
        .set("newContext", "true");

    File emptyDir = Utils.createTempDir();
    emptyDir.deleteOnExit();
    StreamingContextSuite contextSuite = new StreamingContextSuite();
    String corruptedCheckpointDir = contextSuite.createCorruptedCheckpoint();
    String checkpointDir = contextSuite.createValidCheckpoint();

    // Function to create JavaStreamingContext without any output operations
    // (used to detect the new context)
    AtomicBoolean newContextCreated = new AtomicBoolean(false);
    Function0<JavaStreamingContext> creatingFunc = () -> {
      newContextCreated.set(true);
      return new JavaStreamingContext(conf, Seconds.apply(1));
    };

    newContextCreated.set(false);
    ssc = JavaStreamingContext.getOrCreate(emptyDir.getAbsolutePath(), creatingFunc);
    Assertions.assertTrue(newContextCreated.get(), "new context not created");
    ssc.stop();

    newContextCreated.set(false);
    ssc = JavaStreamingContext.getOrCreate(corruptedCheckpointDir, creatingFunc,
        new Configuration(), true);
    Assertions.assertTrue(newContextCreated.get(), "new context not created");
    ssc.stop();

    newContextCreated.set(false);
    ssc = JavaStreamingContext.getOrCreate(checkpointDir, creatingFunc,
        new Configuration());
    Assertions.assertTrue(!newContextCreated.get(), "old context not recovered");
    ssc.stop();

    newContextCreated.set(false);
    JavaSparkContext sc = new JavaSparkContext(conf);
    ssc = JavaStreamingContext.getOrCreate(checkpointDir, creatingFunc,
        new Configuration());
    Assertions.assertTrue(!newContextCreated.get(), "old context not recovered");
    ssc.stop();
  }

  /* TEST DISABLED: Pending a discussion about checkpoint() semantics with TD
  @SuppressWarnings("unchecked")
  @Test
  public void testCheckpointOfIndividualStream() throws InterruptedException {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("this", "is"),
        Arrays.asList("a", "test"),
        Arrays.asList("counting", "letters"));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(4,2),
        Arrays.asList(1,4),
        Arrays.asList(8,7));

    JavaDStream stream = JavaCheckpointTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream letterCount = stream.map(new Function<String, Integer>() {
      @Override
      public Integer call(String s) {
        return s.length();
      }
    });
    JavaCheckpointTestUtils.attachTestOutputStream(letterCount);

    letterCount.checkpoint(new Duration(1000));

    List<List<Integer>> result1 = JavaCheckpointTestUtils.runStreams(ssc, 3, 3);
    assertOrderInvariantEquals(expected, result1);
  }
  */

  // Input stream tests. These mostly just test that we can instantiate a given InputStream with
  // Java arguments and assign it to a JavaDStream without producing type errors. Testing of the
  // InputStream functionality is deferred to the existing Scala tests.
  @Test
  public void testSocketTextStream() {
    ssc.socketTextStream("localhost", 12345);
  }

  @Test
  public void testSocketString() {
    ssc.socketStream(
      "localhost",
      12345,
      in -> {
        List<String> out = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(in, StandardCharsets.UTF_8))) {
          for (String line; (line = reader.readLine()) != null;) {
            out.add(line);
          }
        }
        return out;
      },
      StorageLevel.MEMORY_ONLY());
  }

  @Test
  public void testTextFileStream() throws IOException {
    File testDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    List<List<String>> expected = fileTestPrepare(testDir);

    JavaDStream<String> input = ssc.textFileStream(testDir.toString());
    JavaTestUtils.attachTestOutputStream(input);
    List<List<String>> result = JavaTestUtils.runStreams(ssc, 1, 1);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testFileStream() throws IOException {
    File testDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    List<List<String>> expected = fileTestPrepare(testDir);

    JavaPairInputDStream<LongWritable, Text> inputStream = ssc.fileStream(
      testDir.toString(),
      LongWritable.class,
      Text.class,
      TextInputFormat.class,
      v1 -> Boolean.TRUE,
      true);

    JavaDStream<String> test = inputStream.map(v1 -> v1._2().toString());

    JavaTestUtils.attachTestOutputStream(test);
    List<List<String>> result = JavaTestUtils.runStreams(ssc, 1, 1);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testRawSocketStream() {
    ssc.rawSocketStream("localhost", 12345);
  }

  private static List<List<String>> fileTestPrepare(File testDir) throws IOException {
    File existingFile = new File(testDir, "0");
    Files.asCharSink(existingFile, StandardCharsets.UTF_8).write("0\n");
    Assertions.assertTrue(existingFile.setLastModified(1000));
    Assertions.assertEquals(1000, existingFile.lastModified());
    return Arrays.asList(Arrays.asList("0"));
  }

  @SuppressWarnings("unchecked")
  // SPARK-5795: no logic assertions, just testing that intended API invocations compile
  private void compileSaveAsJavaAPI(JavaPairDStream<LongWritable,Text> pds) {
    pds.saveAsNewAPIHadoopFiles(
        "", "", LongWritable.class, Text.class,
        org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);
    pds.saveAsHadoopFiles(
        "", "", LongWritable.class, Text.class,
        org.apache.hadoop.mapred.SequenceFileOutputFormat.class);
    // Checks that a previous common workaround for this API still compiles
    pds.saveAsNewAPIHadoopFiles(
        "", "", LongWritable.class, Text.class,
        (Class) org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);
    pds.saveAsHadoopFiles(
        "", "", LongWritable.class, Text.class,
        (Class) org.apache.hadoop.mapred.SequenceFileOutputFormat.class);
  }

}
