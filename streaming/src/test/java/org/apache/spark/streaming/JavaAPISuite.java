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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.JavaTestUtils;
import org.apache.spark.streaming.JavaCheckpointTestUtils;
import org.apache.spark.streaming.InputStreamsSuite;

import java.io.*;
import java.util.*;

import akka.actor.Props;
import akka.zeromq.Subscribe;



// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaAPISuite implements Serializable {
  private transient JavaStreamingContext ssc;

  @Before
  public void setUp() {
      System.setProperty("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock");
      ssc = new JavaStreamingContext("local[2]", "test", new Duration(1000));
    ssc.checkpoint("checkpoint");
  }

  @After
  public void tearDown() {
    ssc.stop();
    ssc = null;

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port");
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

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream count = stream.count();
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

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream letterCount = stream.map(new Function<String, Integer>() {
        @Override
        public Integer call(String s) throws Exception {
          return s.length();
        }
    });
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

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream windowed = stream.window(new Duration(2000));
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

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream windowed = stream.window(new Duration(4000), new Duration(2000));
    JavaTestUtils.attachTestOutputStream(windowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 8, 4);

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

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream filtered = stream.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String s) throws Exception {
        return s.contains("a");
      }
    });
    JavaTestUtils.attachTestOutputStream(filtered);
    List<List<String>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testGlom() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<List<String>>> expected = Arrays.asList(
        Arrays.asList(Arrays.asList("giants", "dodgers")),
        Arrays.asList(Arrays.asList("yankees", "red socks")));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream glommed = stream.glom();
    JavaTestUtils.attachTestOutputStream(glommed);
    List<List<List<String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testMapPartitions() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<String>> expected = Arrays.asList(
        Arrays.asList("GIANTSDODGERS"),
        Arrays.asList("YANKEESRED SOCKS"));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream mapped = stream.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
      @Override
      public Iterable<String> call(Iterator<String> in) {
        String out = "";
        while (in.hasNext()) {
          out = out + in.next().toUpperCase();
        }
        return Lists.newArrayList(out);
      }
    });
    JavaTestUtils.attachTestOutputStream(mapped);
    List<List<List<String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  private class IntegerSum extends Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer i1, Integer i2) throws Exception {
      return i1 + i2;
    }
  }

  private class IntegerDifference extends Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer i1, Integer i2) throws Exception {
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

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream reduced = stream.reduce(new IntegerSum());
    JavaTestUtils.attachTestOutputStream(reduced);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testReduceByWindow() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(6),
        Arrays.asList(21),
        Arrays.asList(39),
        Arrays.asList(24));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream reducedWindowed = stream.reduceByWindow(new IntegerSum(),
        new IntegerDifference(), new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reducedWindowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 4, 4);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testQueueStream() {
    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    JavaSparkContext jsc = new JavaSparkContext(ssc.ssc().sc());
    JavaRDD<Integer> rdd1 = ssc.sc().parallelize(Arrays.asList(1,2,3));
    JavaRDD<Integer> rdd2 = ssc.sc().parallelize(Arrays.asList(4,5,6));
    JavaRDD<Integer> rdd3 = ssc.sc().parallelize(Arrays.asList(7,8,9));

    LinkedList<JavaRDD<Integer>> rdds = Lists.newLinkedList();
    rdds.add(rdd1);
    rdds.add(rdd2);
    rdds.add(rdd3);

    JavaDStream<Integer> stream = ssc.queueStream(rdds);
    JavaTestUtils.attachTestOutputStream(stream);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 3, 3);
    Assert.assertEquals(expected, result);
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
    JavaDStream<Integer> transformed =
        stream.transform(new Function<JavaRDD<Integer>, JavaRDD<Integer>>() {
      @Override
      public JavaRDD<Integer> call(JavaRDD<Integer> in) throws Exception {
        return in.map(new Function<Integer, Integer>() {
          @Override
          public Integer call(Integer i) throws Exception {
            return i + 2;
          }
        });
      }});
    JavaTestUtils.attachTestOutputStream(transformed);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    assertOrderInvariantEquals(expected, result);
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

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream flatMapped = stream.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
        return Lists.newArrayList(x.split("(?!^)"));
      }
    });
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

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream flatMapped = stream.flatMap(new PairFlatMapFunction<String, Integer, String>() {
      @Override
      public Iterable<Tuple2<Integer, String>> call(String in) throws Exception {
        List<Tuple2<Integer, String>> out = Lists.newArrayList();
        for (String letter: in.split("(?!^)")) {
          out.add(new Tuple2<Integer, String>(in.length(), letter));
        }
        return out;
      }
    });
    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<Tuple2<Integer, String>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assert.assertEquals(expected, result);
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

    JavaDStream stream1 = JavaTestUtils.attachTestInputStream(ssc, inputData1, 2);
    JavaDStream stream2 = JavaTestUtils.attachTestInputStream(ssc, inputData2, 2);

    JavaDStream unioned = stream1.union(stream2);
    JavaTestUtils.attachTestOutputStream(unioned);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    assertOrderInvariantEquals(expected, result);
  }

  /*
   * Performs an order-invariant comparison of lists representing two RDD streams. This allows
   * us to account for ordering variation within individual RDD's which occurs during windowing.
   */
  public static <T extends Comparable> void assertOrderInvariantEquals(
      List<List<T>> expected, List<List<T>> actual) {
    for (List<T> list: expected) {
      Collections.sort(list);
    }
    for (List<T> list: actual) {
      Collections.sort(list);
    }
    Assert.assertEquals(expected, actual);
  }


  // PairDStream Functions
  @Test
  public void testPairFilter() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
        Arrays.asList(new Tuple2<String, Integer>("giants", 6)),
        Arrays.asList(new Tuple2<String, Integer>("yankees", 7)));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = stream.map(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2 call(String in) throws Exception {
            return new Tuple2<String, Integer>(in, in.length());
          }
        });

    JavaPairDStream<String, Integer> filtered = pairStream.filter(
        new Function<Tuple2<String, Integer>, Boolean>() {
      @Override
      public Boolean call(Tuple2<String, Integer> in) throws Exception {
        return in._1().contains("a");
      }
    });
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

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);
    JavaPairDStream<Integer, String> reversed = pairStream.map(
        new PairFunction<Tuple2<String, Integer>, Integer, String>() {
          @Override
          public Tuple2<Integer, String> call(Tuple2<String, Integer> in) throws Exception {
            return in.swap();
          }
    });

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

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);
    JavaPairDStream<Integer, String> reversed = pairStream.mapPartitions(
        new PairFlatMapFunction<Iterator<Tuple2<String, Integer>>, Integer, String>() {
          @Override
          public Iterable<Tuple2<Integer, String>> call(Iterator<Tuple2<String, Integer>> in) throws Exception {
            LinkedList<Tuple2<Integer, String>> out = new LinkedList<Tuple2<Integer, String>>();
            while (in.hasNext()) {
              Tuple2<String, Integer> next = in.next();
              out.add(next.swap());
            }
            return out;
          }
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
    JavaDStream<Integer> reversed = pairStream.map(
            new Function<Tuple2<String, Integer>, Integer>() {
              @Override
              public Integer call(Tuple2<String, Integer> in) throws Exception {
                return in._2();
              }
            });

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
    JavaPairDStream<Integer, String> flatMapped = pairStream.flatMap(
        new PairFlatMapFunction<Tuple2<String, Integer>, Integer, String>() {
          @Override
          public Iterable<Tuple2<Integer, String>> call(Tuple2<String, Integer> in) throws Exception {
            List<Tuple2<Integer, String>> out = new LinkedList<Tuple2<Integer, String>>();
            for (Character s : in._1().toCharArray()) {
              out.add(new Tuple2<Integer, String>(in._2(), s.toString()));
            }
            return out;
          }
        });
    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testPairGroupByKey() {
    List<List<Tuple2<String, String>>> inputData = stringStringKVStream;

    List<List<Tuple2<String, List<String>>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<String, List<String>>("california", Arrays.asList("dodgers", "giants")),
            new Tuple2<String, List<String>>("new york", Arrays.asList("yankees", "mets"))),
        Arrays.asList(
            new Tuple2<String, List<String>>("california", Arrays.asList("sharks", "ducks")),
            new Tuple2<String, List<String>>("new york", Arrays.asList("rangers", "islanders"))));

    JavaDStream<Tuple2<String, String>> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, List<String>> grouped = pairStream.groupByKey();
    JavaTestUtils.attachTestOutputStream(grouped);
    List<List<Tuple2<String, List<String>>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

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

    JavaPairDStream<String, Integer> reduced = pairStream.reduceByKey(new IntegerSum());

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

    JavaPairDStream<String, Integer> combined = pairStream.<Integer>combineByKey(
        new Function<Integer, Integer>() {
        @Override
          public Integer call(Integer i) throws Exception {
            return i;
          }
        }, new IntegerSum(), new IntegerSum(), new HashPartitioner(2));

    JavaTestUtils.attachTestOutputStream(combined);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testCountByValue() {
    List<List<String>> inputData = Arrays.asList(
      Arrays.asList("hello", "world"),
      Arrays.asList("hello", "moon"),
      Arrays.asList("hello"));

    List<List<Tuple2<String, Long>>> expected = Arrays.asList(
      Arrays.asList(
              new Tuple2<String, Long>("hello", 1L),
              new Tuple2<String, Long>("world", 1L)),
      Arrays.asList(
              new Tuple2<String, Long>("hello", 1L),
              new Tuple2<String, Long>("moon", 1L)),
      Arrays.asList(
              new Tuple2<String, Long>("hello", 1L)));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Long> counted = stream.countByValue();
    JavaTestUtils.attachTestOutputStream(counted);
    List<List<Tuple2<String, Long>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testGroupByKeyAndWindow() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, List<Integer>>>> expected = Arrays.asList(
      Arrays.asList(
        new Tuple2<String, List<Integer>>("california", Arrays.asList(1, 3)),
        new Tuple2<String, List<Integer>>("new york", Arrays.asList(1, 4))
      ),
      Arrays.asList(
        new Tuple2<String, List<Integer>>("california", Arrays.asList(1, 3, 5, 5)),
        new Tuple2<String, List<Integer>>("new york", Arrays.asList(1, 1, 3, 4))
      ),
      Arrays.asList(
        new Tuple2<String, List<Integer>>("california", Arrays.asList(5, 5)),
        new Tuple2<String, List<Integer>>("new york", Arrays.asList(1, 3))
      )
    );

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, List<Integer>> groupWindowed =
        pairStream.groupByKeyAndWindow(new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(groupWindowed);
    List<List<Tuple2<String, List<Integer>>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    assert(result.size() == expected.size());
    for (int i = 0; i < result.size(); i++) {
      assert(convert(result.get(i)).equals(convert(expected.get(i))));
    }
  }

  private HashSet<Tuple2<String, HashSet<Integer>>> convert(List<Tuple2<String, List<Integer>>> listOfTuples) {
    List<Tuple2<String, HashSet<Integer>>> newListOfTuples = new ArrayList<Tuple2<String, HashSet<Integer>>>();
    for (Tuple2<String, List<Integer>> tuple: listOfTuples) {
      newListOfTuples.add(convert(tuple));
    }
    return new HashSet<Tuple2<String, HashSet<Integer>>>(newListOfTuples);
  }

  private Tuple2<String, HashSet<Integer>> convert(Tuple2<String, List<Integer>> tuple) {
    return new Tuple2<String, HashSet<Integer>>(tuple._1(), new HashSet<Integer>(tuple._2()));
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

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> reduceWindowed =
        pairStream.reduceByKeyAndWindow(new IntegerSum(), new Duration(2000), new Duration(1000));
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

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> updated = pairStream.updateStateByKey(
        new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
        @Override
        public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
          int out = 0;
          if (state.isPresent()) {
            out = out + state.get();
          }
          for (Integer v: values) {
            out = out + v;
          }
          return Optional.of(out);
        }
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

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> reduceWindowed =
        pairStream.reduceByKeyAndWindow(new IntegerSum(), new IntegerDifference(), new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reduceWindowed);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testCountByValueAndWindow() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("hello", "world"),
        Arrays.asList("hello", "moon"),
        Arrays.asList("hello"));

    List<List<Tuple2<String, Long>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<String, Long>("hello", 1L),
            new Tuple2<String, Long>("world", 1L)),
        Arrays.asList(
            new Tuple2<String, Long>("hello", 2L),
            new Tuple2<String, Long>("world", 1L),
            new Tuple2<String, Long>("moon", 1L)),
        Arrays.asList(
            new Tuple2<String, Long>("hello", 2L),
            new Tuple2<String, Long>("moon", 1L)));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(
        ssc, inputData, 1);
    JavaPairDStream<String, Long> counted =
      stream.countByValueAndWindow(new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(counted);
    List<List<Tuple2<String, Long>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

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

    JavaPairDStream<Integer, Integer> sorted = pairStream.transform(
        new Function<JavaPairRDD<Integer, Integer>, JavaPairRDD<Integer, Integer>>() {
          @Override
          public JavaPairRDD<Integer, Integer> call(JavaPairRDD<Integer, Integer> in) throws Exception {
            return in.sortByKey();
          }
        });

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
        Arrays.asList(3,1,4,2),
        Arrays.asList(2,3,4,1));

    JavaDStream<Tuple2<Integer, Integer>> stream = JavaTestUtils.attachTestInputStream(
        ssc, inputData, 1);
    JavaPairDStream<Integer, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaDStream<Integer> firstParts = pairStream.transform(
        new Function<JavaPairRDD<Integer, Integer>, JavaRDD<Integer>>() {
          @Override
          public JavaRDD<Integer> call(JavaPairRDD<Integer, Integer> in) throws Exception {
            return in.map(new Function<Tuple2<Integer, Integer>, Integer>() {
              @Override
              public Integer call(Tuple2<Integer, Integer> in) {
                return in._1();
              }
            });
          }
        });

    JavaTestUtils.attachTestOutputStream(firstParts);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

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

    JavaPairDStream<String, String> mapped = pairStream.mapValues(new Function<String, String>() {
      @Override
      public String call(String s) throws Exception {
        return s.toUpperCase();
      }
    });

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


    JavaPairDStream<String, String> flatMapped = pairStream.flatMapValues(
        new Function<String, Iterable<String>>() {
          @Override
          public Iterable<String> call(String in) {
            List<String> out = new ArrayList<String>();
            out.add(in + "1");
            out.add(in + "2");
            return out;
          }
        });

    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<Tuple2<String, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testCoGroup() {
    List<List<Tuple2<String, String>>> stringStringKVStream1 = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "dodgers"),
            new Tuple2<String, String>("new york", "yankees")),
        Arrays.asList(new Tuple2<String, String>("california", "sharks"),
            new Tuple2<String, String>("new york", "rangers")));

    List<List<Tuple2<String, String>>> stringStringKVStream2 = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "giants"),
            new Tuple2<String, String>("new york", "mets")),
        Arrays.asList(new Tuple2<String, String>("california", "ducks"),
            new Tuple2<String, String>("new york", "islanders")));


    List<List<Tuple2<String, Tuple2<List<String>, List<String>>>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<String, Tuple2<List<String>, List<String>>>("california",
                new Tuple2<List<String>, List<String>>(Arrays.asList("dodgers"), Arrays.asList("giants"))),
            new Tuple2<String, Tuple2<List<String>, List<String>>>("new york",
                new Tuple2<List<String>, List<String>>(Arrays.asList("yankees"), Arrays.asList("mets")))),
        Arrays.asList(
            new Tuple2<String, Tuple2<List<String>, List<String>>>("california",
                new Tuple2<List<String>, List<String>>(Arrays.asList("sharks"), Arrays.asList("ducks"))),
            new Tuple2<String, Tuple2<List<String>, List<String>>>("new york",
                new Tuple2<List<String>, List<String>>(Arrays.asList("rangers"), Arrays.asList("islanders")))));


    JavaDStream<Tuple2<String, String>> stream1 = JavaTestUtils.attachTestInputStream(
        ssc, stringStringKVStream1, 1);
    JavaPairDStream<String, String> pairStream1 = JavaPairDStream.fromJavaDStream(stream1);

    JavaDStream<Tuple2<String, String>> stream2 = JavaTestUtils.attachTestInputStream(
        ssc, stringStringKVStream2, 1);
    JavaPairDStream<String, String> pairStream2 = JavaPairDStream.fromJavaDStream(stream2);

    JavaPairDStream<String, Tuple2<List<String>, List<String>>> grouped = pairStream1.cogroup(pairStream2);
    JavaTestUtils.attachTestOutputStream(grouped);
    List<List<Tuple2<String, String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testJoin() {
    List<List<Tuple2<String, String>>> stringStringKVStream1 = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "dodgers"),
            new Tuple2<String, String>("new york", "yankees")),
        Arrays.asList(new Tuple2<String, String>("california", "sharks"),
            new Tuple2<String, String>("new york", "rangers")));

    List<List<Tuple2<String, String>>> stringStringKVStream2 = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "giants"),
            new Tuple2<String, String>("new york", "mets")),
        Arrays.asList(new Tuple2<String, String>("california", "ducks"),
            new Tuple2<String, String>("new york", "islanders")));


    List<List<Tuple2<String, Tuple2<String, String>>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<String, Tuple2<String, String>>("california",
                new Tuple2<String, String>("dodgers", "giants")),
            new Tuple2<String, Tuple2<String, String>>("new york",
                new Tuple2<String, String>("yankees", "mets"))),
        Arrays.asList(
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

    JavaPairDStream<String, Tuple2<String, String>> joined = pairStream1.join(pairStream2);
    JavaTestUtils.attachTestOutputStream(joined);
    List<List<Tuple2<String, Long>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testCheckpointMasterRecovery() throws InterruptedException {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("this", "is"),
        Arrays.asList("a", "test"),
        Arrays.asList("counting", "letters"));

    List<List<Integer>> expectedInitial = Arrays.asList(
        Arrays.asList(4,2));
    List<List<Integer>> expectedFinal = Arrays.asList(
        Arrays.asList(1,4),
        Arrays.asList(8,7));

    File tempDir = Files.createTempDir();
    ssc.checkpoint(tempDir.getAbsolutePath());

    JavaDStream stream = JavaCheckpointTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream letterCount = stream.map(new Function<String, Integer>() {
      @Override
      public Integer call(String s) throws Exception {
        return s.length();
      }
    });
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
  }


  /** TEST DISABLED: Pending a discussion about checkpoint() semantics with TD
  @Test
  public void testCheckpointofIndividualStream() throws InterruptedException {
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
      public Integer call(String s) throws Exception {
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
  public void testKafkaStream() {
    HashMap<String, Integer> topics = Maps.newHashMap();
    JavaDStream test1 = ssc.kafkaStream("localhost:12345", "group", topics);
    JavaDStream test2 = ssc.kafkaStream("localhost:12345", "group", topics,
      StorageLevel.MEMORY_AND_DISK());

    HashMap<String, String> kafkaParams = Maps.newHashMap();
    kafkaParams.put("zk.connect","localhost:12345");
    kafkaParams.put("groupid","consumer-group");
    JavaDStream test3 = ssc.kafkaStream(String.class, StringDecoder.class, kafkaParams, topics,
      StorageLevel.MEMORY_AND_DISK());
  }

  @Test
  public void testSocketTextStream() {
    JavaDStream test = ssc.socketTextStream("localhost", 12345);
  }

  @Test
  public void testSocketString() {
    class Converter extends Function<InputStream, Iterable<String>> {
      public Iterable<String> call(InputStream in) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        List<String> out = new ArrayList<String>();
        try {
          while (true) {
            String line = reader.readLine();
            if (line == null) { break; }
            out.add(line);
          }
        } catch (IOException e) { }
        return out;
      }
    }

    JavaDStream test = ssc.socketStream(
      "localhost",
      12345,
      new Converter(),
      StorageLevel.MEMORY_ONLY());
  }

  @Test
  public void testTextFileStream() {
    JavaDStream test = ssc.textFileStream("/tmp/foo");
  }

  @Test
  public void testRawSocketStream() {
    JavaDStream test = ssc.rawSocketStream("localhost", 12345);
  }

  @Test
  public void testFlumeStream() {
    JavaDStream test = ssc.flumeStream("localhost", 12345, StorageLevel.MEMORY_ONLY());
  }

  @Test
  public void testFileStream() {
    JavaPairDStream<String, String> foo =
      ssc.<String, String, SequenceFileInputFormat>fileStream("/tmp/foo");
  }

  @Test
  public void testTwitterStream() {
    String[] filters = new String[] { "good", "bad", "ugly" };
    JavaDStream test = ssc.twitterStream(filters, StorageLevel.MEMORY_ONLY());
  }

  @Test
  public void testActorStream() {
    JavaDStream test = ssc.actorStream((Props)null, "TestActor", StorageLevel.MEMORY_ONLY());
  }

  @Test
  public void testZeroMQStream() {
    JavaDStream test = ssc.zeroMQStream("url", (Subscribe) null, new Function<byte[][], Iterable<String>>() {
      @Override
      public Iterable<String> call(byte[][] b) throws Exception {
        return null;
      }
    });
  }
}
