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

import scala.Tuple2;

import org.junit.Assert;
import org.junit.Test;
import java.io.*;
import java.util.*;
import java.lang.Iterable;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.collect.Sets;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.util.Utils;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaAPISuite extends LocalJavaStreamingContext implements Serializable {

  public void equalIterator(Iterator<?> a, Iterator<?> b) {
    while (a.hasNext() && b.hasNext()) {
      Assert.assertEquals(a.next(), b.next());
    }
    Assert.assertEquals(a.hasNext(), b.hasNext());
  }

  public void equalIterable(Iterable<?> a, Iterable<?> b) {
      equalIterator(a.iterator(), b.iterator());
  }

  @Test
  public void testInitialization() {
    Assert.assertNotNull(ssc.sc());
  }

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
  @Test
  public void testMap() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("hello", "world"),
        Arrays.asList("goodnight", "moon"));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(5,5),
        Arrays.asList(9,4));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> letterCount = stream.map(new Function<String, Integer>() {
        @Override
        public Integer call(String s) throws Exception {
          return s.length();
        }
    });
    JavaTestUtils.attachTestOutputStream(letterCount);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    assertOrderInvariantEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
  @Test
  public void testFilter() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<String>> expected = Arrays.asList(
        Arrays.asList("giants"),
        Arrays.asList("yankees"));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<String> filtered = stream.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String s) throws Exception {
        return s.contains("a");
      }
    });
    JavaTestUtils.attachTestOutputStream(filtered);
    List<List<String>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    assertOrderInvariantEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
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
    Assert.assertEquals(2, result.size());
    for (List<List<Integer>> rdd : result) {
      Assert.assertEquals(4, rdd.size());
      Assert.assertEquals(
        10, rdd.get(0).size() + rdd.get(1).size() + rdd.get(2).size() + rdd.get(3).size());
    }
  }

  @SuppressWarnings("unchecked")
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
    Assert.assertEquals(2, result.size());
    for (List<List<Integer>> rdd : result) {
      Assert.assertEquals(2, rdd.size());
      Assert.assertEquals(10, rdd.get(0).size() + rdd.get(1).size());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGlom() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<List<String>>> expected = Arrays.asList(
        Arrays.asList(Arrays.asList("giants", "dodgers")),
        Arrays.asList(Arrays.asList("yankees", "red socks")));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<List<String>> glommed = stream.glom();
    JavaTestUtils.attachTestOutputStream(glommed);
    List<List<List<String>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMapPartitions() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<String>> expected = Arrays.asList(
        Arrays.asList("GIANTSDODGERS"),
        Arrays.asList("YANKEESRED SOCKS"));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<String> mapped = stream.mapPartitions(
        new FlatMapFunction<Iterator<String>, String>() {
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
    List<List<String>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  private class IntegerSum implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer i1, Integer i2) throws Exception {
      return i1 + i2;
    }
  }

  private class IntegerDifference implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer i1, Integer i2) throws Exception {
      return i1 - i2;
    }
  }

  @SuppressWarnings("unchecked")
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

    Assert.assertEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
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

    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> reducedWindowed = stream.reduceByWindow(new IntegerSum(),
        new IntegerDifference(), new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reducedWindowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 4, 4);

    Assert.assertEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testQueueStream() {
    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    JavaSparkContext jsc = new JavaSparkContext(ssc.ssc().sc());
    JavaRDD<Integer> rdd1 = ssc.sparkContext().parallelize(Arrays.asList(1, 2, 3));
    JavaRDD<Integer> rdd2 = ssc.sparkContext().parallelize(Arrays.asList(4, 5, 6));
    JavaRDD<Integer> rdd3 = ssc.sparkContext().parallelize(Arrays.asList(7,8,9));

    LinkedList<JavaRDD<Integer>> rdds = Lists.newLinkedList();
    rdds.add(rdd1);
    rdds.add(rdd2);
    rdds.add(rdd3);

    JavaDStream<Integer> stream = ssc.queueStream(rdds);
    JavaTestUtils.attachTestOutputStream(stream);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 3, 3);
    Assert.assertEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
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
    JavaDStream<Integer> transformed = stream.transform(
      new Function<JavaRDD<Integer>, JavaRDD<Integer>>() {
        @Override
        public JavaRDD<Integer> call(JavaRDD<Integer> in) throws Exception {
          return in.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer i) throws Exception {
              return i + 2;
            }
          });
        }
      });

    JavaTestUtils.attachTestOutputStream(transformed);
    List<List<Integer>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    assertOrderInvariantEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testVariousTransform() {
    // tests whether all variations of transform can be called from Java

    List<List<Integer>> inputData = Arrays.asList(Arrays.asList(1));
    JavaDStream<Integer> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);

    List<List<Tuple2<String, Integer>>> pairInputData =
        Arrays.asList(Arrays.asList(new Tuple2<String, Integer>("x", 1)));
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(
        JavaTestUtils.attachTestInputStream(ssc, pairInputData, 1));

    JavaDStream<Integer> transformed1 = stream.transform(
        new Function<JavaRDD<Integer>, JavaRDD<Integer>>() {
          @Override
          public JavaRDD<Integer> call(JavaRDD<Integer> in) throws Exception {
            return null;
          }
        }
    );

    JavaDStream<Integer> transformed2 = stream.transform(
      new Function2<JavaRDD<Integer>, Time, JavaRDD<Integer>>() {
        @Override public JavaRDD<Integer> call(JavaRDD<Integer> in, Time time) throws Exception {
          return null;
        }
      }
    );

    JavaPairDStream<String, Integer> transformed3 = stream.transformToPair(
        new Function<JavaRDD<Integer>, JavaPairRDD<String, Integer>>() {
          @Override public JavaPairRDD<String, Integer> call(JavaRDD<Integer> in) throws Exception {
            return null;
          }
        }
    );

    JavaPairDStream<String, Integer> transformed4 = stream.transformToPair(
        new Function2<JavaRDD<Integer>, Time, JavaPairRDD<String, Integer>>() {
          @Override public JavaPairRDD<String, Integer> call(JavaRDD<Integer> in, Time time) throws Exception {
            return null;
          }
        }
    );

    JavaDStream<Integer> pairTransformed1 = pairStream.transform(
        new Function<JavaPairRDD<String, Integer>, JavaRDD<Integer>>() {
          @Override public JavaRDD<Integer> call(JavaPairRDD<String, Integer> in) throws Exception {
            return null;
          }
        }
    );

    JavaDStream<Integer> pairTransformed2 = pairStream.transform(
        new Function2<JavaPairRDD<String, Integer>, Time, JavaRDD<Integer>>() {
          @Override public JavaRDD<Integer> call(JavaPairRDD<String, Integer> in, Time time) throws Exception {
            return null;
          }
        }
    );

    JavaPairDStream<String, String> pairTransformed3 = pairStream.transformToPair(
        new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, String>>() {
          @Override public JavaPairRDD<String, String> call(JavaPairRDD<String, Integer> in) throws Exception {
            return null;
          }
        }
    );

    JavaPairDStream<String, String> pairTransformed4 = pairStream.transformToPair(
        new Function2<JavaPairRDD<String, Integer>, Time, JavaPairRDD<String, String>>() {
          @Override public JavaPairRDD<String, String> call(JavaPairRDD<String, Integer> in, Time time) throws Exception {
            return null;
          }
        }
    );

  }

  @SuppressWarnings("unchecked")
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

    JavaPairDStream<String, Tuple2<String, String>> joined = pairStream1.transformWithToPair(
        pairStream2,
        new Function3<
            JavaPairRDD<String, String>,
            JavaPairRDD<String, String>,
            Time,
            JavaPairRDD<String, Tuple2<String, String>>
          >() {
          @Override
          public JavaPairRDD<String, Tuple2<String, String>> call(
              JavaPairRDD<String, String> rdd1,
              JavaPairRDD<String, String> rdd2,
              Time time
          ) throws Exception {
            return rdd1.join(rdd2);
          }
        }
    );

    JavaTestUtils.attachTestOutputStream(joined);
    List<List<Tuple2<String, Tuple2<String, String>>>> result = JavaTestUtils.runStreams(ssc, 2, 2);
    List<HashSet<Tuple2<String, Tuple2<String, String>>>> unorderedResult = Lists.newArrayList();
    for (List<Tuple2<String, Tuple2<String, String>>> res: result) {
        unorderedResult.add(Sets.newHashSet(res));
    }

    Assert.assertEquals(expected, unorderedResult);
  }


  @SuppressWarnings("unchecked")
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

    JavaDStream<Double> transformed1 = stream1.transformWith(
        stream2,
        new Function3<JavaRDD<Integer>, JavaRDD<String>, Time, JavaRDD<Double>>() {
          @Override
          public JavaRDD<Double> call(JavaRDD<Integer> rdd1, JavaRDD<String> rdd2, Time time) throws Exception {
            return null;
          }
        }
    );

    JavaDStream<Double> transformed2 = stream1.transformWith(
        pairStream1,
        new Function3<JavaRDD<Integer>, JavaPairRDD<String, Integer>, Time, JavaRDD<Double>>() {
          @Override
          public JavaRDD<Double> call(JavaRDD<Integer> rdd1, JavaPairRDD<String, Integer> rdd2, Time time) throws Exception {
            return null;
          }
        }
    );

    JavaPairDStream<Double, Double> transformed3 = stream1.transformWithToPair(
        stream2,
        new Function3<JavaRDD<Integer>, JavaRDD<String>, Time, JavaPairRDD<Double, Double>>() {
          @Override
          public JavaPairRDD<Double, Double> call(JavaRDD<Integer> rdd1, JavaRDD<String> rdd2, Time time) throws Exception {
            return null;
          }
        }
    );

    JavaPairDStream<Double, Double> transformed4 = stream1.transformWithToPair(
        pairStream1,
        new Function3<JavaRDD<Integer>, JavaPairRDD<String, Integer>, Time, JavaPairRDD<Double, Double>>() {
          @Override
          public JavaPairRDD<Double, Double> call(JavaRDD<Integer> rdd1, JavaPairRDD<String, Integer> rdd2, Time time) throws Exception {
            return null;
          }
        }
    );

    JavaDStream<Double> pairTransformed1 = pairStream1.transformWith(
        stream2,
        new Function3<JavaPairRDD<String, Integer>, JavaRDD<String>, Time, JavaRDD<Double>>() {
          @Override
          public JavaRDD<Double> call(JavaPairRDD<String, Integer> rdd1, JavaRDD<String> rdd2, Time time) throws Exception {
            return null;
          }
        }
    );

    JavaDStream<Double> pairTransformed2_ = pairStream1.transformWith(
        pairStream1,
        new Function3<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>, Time, JavaRDD<Double>>() {
          @Override
          public JavaRDD<Double> call(JavaPairRDD<String, Integer> rdd1, JavaPairRDD<String, Integer> rdd2, Time time) throws Exception {
            return null;
          }
        }
    );

    JavaPairDStream<Double, Double> pairTransformed3 = pairStream1.transformWithToPair(
        stream2,
        new Function3<JavaPairRDD<String, Integer>, JavaRDD<String>, Time, JavaPairRDD<Double, Double>>() {
          @Override
          public JavaPairRDD<Double, Double> call(JavaPairRDD<String, Integer> rdd1, JavaRDD<String> rdd2, Time time) throws Exception {
            return null;
          }
        }
    );

    JavaPairDStream<Double, Double> pairTransformed4 = pairStream1.transformWithToPair(
        pairStream2,
        new Function3<JavaPairRDD<String, Integer>, JavaPairRDD<Double, Character>, Time, JavaPairRDD<Double, Double>>() {
          @Override
          public JavaPairRDD<Double, Double> call(JavaPairRDD<String, Integer> rdd1, JavaPairRDD<Double, Character> rdd2, Time time) throws Exception {
            return null;
          }
        }
    );
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
      listOfDStreams1,
      new Function2<List<JavaRDD<?>>, Time, JavaRDD<Long>>() {
        public JavaRDD<Long> call(List<JavaRDD<?>> listOfRDDs, Time time) {
          assert(listOfRDDs.size() == 2);
          return null;
        }
      }
    );

    List<JavaDStream<?>> listOfDStreams2 =
        Arrays.<JavaDStream<?>>asList(stream1, stream2, pairStream1.toJavaDStream());

    JavaPairDStream<Integer, Tuple2<Integer, String>> transformed2 = ssc.transformToPair(
      listOfDStreams2,
      new Function2<List<JavaRDD<?>>, Time, JavaPairRDD<Integer, Tuple2<Integer, String>>>() {
        public JavaPairRDD<Integer, Tuple2<Integer, String>> call(List<JavaRDD<?>> listOfRDDs, Time time) {
          assert(listOfRDDs.size() == 3);
          JavaRDD<Integer> rdd1 = (JavaRDD<Integer>)listOfRDDs.get(0);
          JavaRDD<Integer> rdd2 = (JavaRDD<Integer>)listOfRDDs.get(1);
          JavaRDD<Tuple2<Integer, String>> rdd3 = (JavaRDD<Tuple2<Integer, String>>)listOfRDDs.get(2);
          JavaPairRDD<Integer, String> prdd3 = JavaPairRDD.fromJavaRDD(rdd3);
          PairFunction<Integer, Integer, Integer> mapToTuple = new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer i) throws Exception {
              return new Tuple2<Integer, Integer>(i, i);
            }
          };
          return rdd1.union(rdd2).mapToPair(mapToTuple).join(prdd3);
        }
      }
    );
    JavaTestUtils.attachTestOutputStream(transformed2);
    List<List<Tuple2<Integer, Tuple2<Integer, String>>>> result = JavaTestUtils.runStreams(ssc, 2, 2);
    Assert.assertEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
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
    JavaDStream<String> flatMapped = stream.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
        return Lists.newArrayList(x.split("(?!^)"));
      }
    });
    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<String>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    assertOrderInvariantEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
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
    JavaPairDStream<Integer, String> flatMapped = stream.flatMapToPair(
      new PairFlatMapFunction<String, Integer, String>() {
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

  @SuppressWarnings("unchecked")
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
  public static <T extends Comparable<T>> void assertOrderInvariantEquals(
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
  @SuppressWarnings("unchecked")
  @Test
  public void testPairFilter() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
        Arrays.asList(new Tuple2<String, Integer>("giants", 6)),
        Arrays.asList(new Tuple2<String, Integer>("yankees", 7)));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = stream.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String in) throws Exception {
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

  @SuppressWarnings("unchecked")
  private List<List<Tuple2<String, String>>> stringStringKVStream = Arrays.asList(
      Arrays.asList(new Tuple2<String, String>("california", "dodgers"),
          new Tuple2<String, String>("california", "giants"),
          new Tuple2<String, String>("new york", "yankees"),
          new Tuple2<String, String>("new york", "mets")),
      Arrays.asList(new Tuple2<String, String>("california", "sharks"),
          new Tuple2<String, String>("california", "ducks"),
          new Tuple2<String, String>("new york", "rangers"),
          new Tuple2<String, String>("new york", "islanders")));

  @SuppressWarnings("unchecked")
  private List<List<Tuple2<String, Integer>>> stringIntKVStream = Arrays.asList(
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

  @SuppressWarnings("unchecked")
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
    JavaPairDStream<Integer, String> reversed = pairStream.mapToPair(
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

  @SuppressWarnings("unchecked")
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
    JavaPairDStream<Integer, String> reversed = pairStream.mapPartitionsToPair(
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

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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
    JavaPairDStream<Integer, String> flatMapped = pairStream.flatMapToPair(
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

  @SuppressWarnings("unchecked")
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

    JavaPairDStream<String, Iterable<String>> grouped = pairStream.groupByKey();
    JavaTestUtils.attachTestOutputStream(grouped);
    List<List<Tuple2<String, Iterable<String>>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected.size(), result.size());
    Iterator<List<Tuple2<String, Iterable<String>>>> resultItr = result.iterator();
    Iterator<List<Tuple2<String, List<String>>>> expectedItr = expected.iterator();
    while (resultItr.hasNext() && expectedItr.hasNext()) {
      Iterator<Tuple2<String, Iterable<String>>> resultElements = resultItr.next().iterator();
      Iterator<Tuple2<String, List<String>>> expectedElements = expectedItr.next().iterator();
      while (resultElements.hasNext() && expectedElements.hasNext()) {
        Tuple2<String, Iterable<String>> resultElement = resultElements.next();
        Tuple2<String, List<String>> expectedElement = expectedElements.next();
        Assert.assertEquals(expectedElement._1(), resultElement._1());
        equalIterable(expectedElement._2(), resultElement._2());
      }
      Assert.assertEquals(resultElements.hasNext(), expectedElements.hasNext());
    }
  }

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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

    JavaPairDStream<String, Iterable<Integer>> groupWindowed =
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

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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
        pairStream.reduceByKeyAndWindow(new IntegerSum(), new IntegerDifference(),
          new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reduceWindowed);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(ssc, 3, 3);

    Assert.assertEquals(expected, result);
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
            new Tuple2<String, Long>("hello", 1L),
            new Tuple2<String, Long>("world", 1L)),
        Sets.newHashSet(
            new Tuple2<String, Long>("hello", 2L),
            new Tuple2<String, Long>("world", 1L),
            new Tuple2<String, Long>("moon", 1L)),
        Sets.newHashSet(
            new Tuple2<String, Long>("hello", 2L),
            new Tuple2<String, Long>("moon", 1L)));

    JavaDStream<String> stream = JavaTestUtils.attachTestInputStream(
        ssc, inputData, 1);
    JavaPairDStream<String, Long> counted =
      stream.countByValueAndWindow(new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(counted);
    List<List<Tuple2<String, Long>>> result = JavaTestUtils.runStreams(ssc, 3, 3);
    List<HashSet<Tuple2<String, Long>>> unorderedResult = Lists.newArrayList();
    for (List<Tuple2<String, Long>> res: result) {
      unorderedResult.add(Sets.newHashSet(res));
    }

    Assert.assertEquals(expected, unorderedResult);
  }

  @SuppressWarnings("unchecked")
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

    JavaPairDStream<Integer, Integer> sorted = pairStream.transformToPair(
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

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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

    JavaPairDStream<String, Tuple2<Iterable<String>, Iterable<String>>> grouped = pairStream1.cogroup(pairStream2);
    JavaTestUtils.attachTestOutputStream(grouped);
    List<List<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected.size(), result.size());
    Iterator<List<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>> resultItr = result.iterator();
    Iterator<List<Tuple2<String, Tuple2<List<String>, List<String>>>>> expectedItr = expected.iterator();
    while (resultItr.hasNext() && expectedItr.hasNext()) {
      Iterator<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>> resultElements = resultItr.next().iterator();
      Iterator<Tuple2<String, Tuple2<List<String>, List<String>>>> expectedElements = expectedItr.next().iterator();
      while (resultElements.hasNext() && expectedElements.hasNext()) {
        Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> resultElement = resultElements.next();
        Tuple2<String, Tuple2<List<String>, List<String>>> expectedElement = expectedElements.next();
        Assert.assertEquals(expectedElement._1(), resultElement._1());
        equalIterable(expectedElement._2()._1(), resultElement._2()._1());
        equalIterable(expectedElement._2()._2(), resultElement._2()._2());
      }
      Assert.assertEquals(resultElements.hasNext(), expectedElements.hasNext());
    }
  }

  @SuppressWarnings("unchecked")
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
    List<List<Tuple2<String, Tuple2<String, String>>>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testLeftOuterJoin() {
    List<List<Tuple2<String, String>>> stringStringKVStream1 = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "dodgers"),
            new Tuple2<String, String>("new york", "yankees")),
        Arrays.asList(new Tuple2<String, String>("california", "sharks") ));

    List<List<Tuple2<String, String>>> stringStringKVStream2 = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "giants") ),
        Arrays.asList(new Tuple2<String, String>("new york", "islanders") )

    );

    List<List<Long>> expected = Arrays.asList(Arrays.asList(2L), Arrays.asList(1L));

    JavaDStream<Tuple2<String, String>> stream1 = JavaTestUtils.attachTestInputStream(
        ssc, stringStringKVStream1, 1);
    JavaPairDStream<String, String> pairStream1 = JavaPairDStream.fromJavaDStream(stream1);

    JavaDStream<Tuple2<String, String>> stream2 = JavaTestUtils.attachTestInputStream(
        ssc, stringStringKVStream2, 1);
    JavaPairDStream<String, String> pairStream2 = JavaPairDStream.fromJavaDStream(stream2);

    JavaPairDStream<String, Tuple2<String, Optional<String>>> joined = pairStream1.leftOuterJoin(pairStream2);
    JavaDStream<Long> counted = joined.count();
    JavaTestUtils.attachTestOutputStream(counted);
    List<List<Long>> result = JavaTestUtils.runStreams(ssc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @SuppressWarnings("unchecked")
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
    tempDir.deleteOnExit();
    ssc.checkpoint(tempDir.getAbsolutePath());

    JavaDStream<String> stream = JavaCheckpointTestUtils.attachTestInputStream(ssc, inputData, 1);
    JavaDStream<Integer> letterCount = stream.map(new Function<String, Integer>() {
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
    Utils.deleteRecursively(tempDir);
  }


  /* TEST DISABLED: Pending a discussion about checkpoint() semantics with TD
  @SuppressWarnings("unchecked")
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
  public void testSocketTextStream() {
      JavaReceiverInputDStream<String> test = ssc.socketTextStream("localhost", 12345);
  }

  @Test
  public void testSocketString() {

    class Converter implements Function<InputStream, Iterable<String>> {
      public Iterable<String> call(InputStream in) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        List<String> out = new ArrayList<String>();
        while (true) {
          String line = reader.readLine();
          if (line == null) { break; }
          out.add(line);
        }
        return out;
      }
    }

    JavaDStream<String> test = ssc.socketStream(
      "localhost",
      12345,
      new Converter(),
      StorageLevel.MEMORY_ONLY());
  }

  @Test
  public void testTextFileStream() {
    JavaDStream<String> test = ssc.textFileStream("/tmp/foo");
  }

  @Test
  public void testRawSocketStream() {
    JavaReceiverInputDStream<String> test = ssc.rawSocketStream("localhost", 12345);
  }
}
