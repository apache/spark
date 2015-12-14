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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.util.ManualClock;
import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

public class JavaMapWithStateSuite extends LocalJavaStreamingContext implements Serializable {

  /**
   * This test is only for testing the APIs. It's not necessary to run it.
   */
  public void testAPI() {
    JavaPairRDD<String, Boolean> initialRDD = null;
    JavaPairDStream<String, Integer> wordsDstream = null;

    final Function4<Time, String, Optional<Integer>, State<Boolean>, Optional<Double>>
        mappingFunc =
        new Function4<Time, String, Optional<Integer>, State<Boolean>, Optional<Double>>() {

          @Override
          public Optional<Double> call(
              Time time, String word, Optional<Integer> one, State<Boolean> state) {
            // Use all State's methods here
            state.exists();
            state.get();
            state.isTimingOut();
            state.remove();
            state.update(true);
            return Optional.of(2.0);
          }
        };

    JavaMapWithStateDStream<String, Integer, Boolean, Double> stateDstream =
        wordsDstream.mapWithState(
            StateSpec.function(mappingFunc)
                .initialState(initialRDD)
                .numPartitions(10)
                .partitioner(new HashPartitioner(10))
                .timeout(Durations.seconds(10)));

    JavaPairDStream<String, Boolean> stateSnapshots = stateDstream.stateSnapshots();

    final Function3<String, Optional<Integer>, State<Boolean>, Double> mappingFunc2 =
        new Function3<String, Optional<Integer>, State<Boolean>, Double>() {

          @Override
          public Double call(String key, Optional<Integer> one, State<Boolean> state) {
            // Use all State's methods here
            state.exists();
            state.get();
            state.isTimingOut();
            state.remove();
            state.update(true);
            return 2.0;
          }
        };

    JavaMapWithStateDStream<String, Integer, Boolean, Double> stateDstream2 =
        wordsDstream.mapWithState(
            StateSpec.<String, Integer, Boolean, Double>function(mappingFunc2)
                .initialState(initialRDD)
                .numPartitions(10)
                .partitioner(new HashPartitioner(10))
                .timeout(Durations.seconds(10)));

    JavaPairDStream<String, Boolean> stateSnapshots2 = stateDstream2.stateSnapshots();
  }

  @Test
  public void testBasicFunction() {
    List<List<String>> inputData = Arrays.asList(
        Collections.<String>emptyList(),
        Arrays.asList("a"),
        Arrays.asList("a", "b"),
        Arrays.asList("a", "b", "c"),
        Arrays.asList("a", "b"),
        Arrays.asList("a"),
        Collections.<String>emptyList()
    );

    List<Set<Integer>> outputData = Arrays.asList(
        Collections.<Integer>emptySet(),
        Sets.newHashSet(1),
        Sets.newHashSet(2, 1),
        Sets.newHashSet(3, 2, 1),
        Sets.newHashSet(4, 3),
        Sets.newHashSet(5),
        Collections.<Integer>emptySet()
    );

    List<Set<Tuple2<String, Integer>>> stateData = Arrays.asList(
        Collections.<Tuple2<String, Integer>>emptySet(),
        Sets.newHashSet(new Tuple2<String, Integer>("a", 1)),
        Sets.newHashSet(new Tuple2<String, Integer>("a", 2), new Tuple2<String, Integer>("b", 1)),
        Sets.newHashSet(
            new Tuple2<String, Integer>("a", 3),
            new Tuple2<String, Integer>("b", 2),
            new Tuple2<String, Integer>("c", 1)),
        Sets.newHashSet(
            new Tuple2<String, Integer>("a", 4),
            new Tuple2<String, Integer>("b", 3),
            new Tuple2<String, Integer>("c", 1)),
        Sets.newHashSet(
            new Tuple2<String, Integer>("a", 5),
            new Tuple2<String, Integer>("b", 3),
            new Tuple2<String, Integer>("c", 1)),
        Sets.newHashSet(
            new Tuple2<String, Integer>("a", 5),
            new Tuple2<String, Integer>("b", 3),
            new Tuple2<String, Integer>("c", 1))
    );

    Function3<String, Optional<Integer>, State<Integer>, Integer> mappingFunc =
        new Function3<String, Optional<Integer>, State<Integer>, Integer>() {

          @Override
          public Integer call(String key, Optional<Integer> value, State<Integer> state) throws Exception {
            int sum = value.or(0) + (state.exists() ? state.get() : 0);
            state.update(sum);
            return sum;
          }
        };
    testOperation(
        inputData,
        StateSpec.<String, Integer, Integer, Integer>function(mappingFunc),
        outputData,
        stateData);
  }

  private <K, S, T> void testOperation(
      List<List<K>> input,
      StateSpec<K, Integer, S, T> mapWithStateSpec,
      List<Set<T>> expectedOutputs,
      List<Set<Tuple2<K, S>>> expectedStateSnapshots) {
    int numBatches = expectedOutputs.size();
    JavaDStream<K> inputStream = JavaTestUtils.attachTestInputStream(ssc, input, 2);
    JavaMapWithStateDStream<K, Integer, S, T> mapWithStateDStream =
        JavaPairDStream.fromJavaDStream(inputStream.map(new Function<K, Tuple2<K, Integer>>() {
          @Override
          public Tuple2<K, Integer> call(K x) throws Exception {
            return new Tuple2<K, Integer>(x, 1);
          }
        })).mapWithState(mapWithStateSpec);

    final List<Set<T>> collectedOutputs =
        Collections.synchronizedList(Lists.<Set<T>>newArrayList());
    mapWithStateDStream.foreachRDD(new Function<JavaRDD<T>, Void>() {
      @Override
      public Void call(JavaRDD<T> rdd) throws Exception {
        collectedOutputs.add(Sets.newHashSet(rdd.collect()));
        return null;
      }
    });
    final List<Set<Tuple2<K, S>>> collectedStateSnapshots =
        Collections.synchronizedList(Lists.<Set<Tuple2<K, S>>>newArrayList());
    mapWithStateDStream.stateSnapshots().foreachRDD(new Function<JavaPairRDD<K, S>, Void>() {
      @Override
      public Void call(JavaPairRDD<K, S> rdd) throws Exception {
        collectedStateSnapshots.add(Sets.newHashSet(rdd.collect()));
        return null;
      }
    });
    BatchCounter batchCounter = new BatchCounter(ssc.ssc());
    ssc.start();
    ((ManualClock) ssc.ssc().scheduler().clock())
        .advance(ssc.ssc().progressListener().batchDuration() * numBatches + 1);
    batchCounter.waitUntilBatchesCompleted(numBatches, 10000);

    Assert.assertEquals(expectedOutputs, collectedOutputs);
    Assert.assertEquals(expectedStateSnapshots, collectedStateSnapshots);
  }
}
