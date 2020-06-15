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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import scala.Tuple2;

import com.google.common.collect.Sets;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.util.ManualClock;
import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
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

    Function4<Time, String, Optional<Integer>, State<Boolean>, Optional<Double>> mappingFunc =
        (time, word, one, state) -> {
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
            StateSpec.function(mappingFunc)
                .initialState(initialRDD)
                .numPartitions(10)
                .partitioner(new HashPartitioner(10))
                .timeout(Durations.seconds(10)));

    stateDstream.stateSnapshots();

    Function3<String, Optional<Integer>, State<Boolean>, Double> mappingFunc2 =
        (key, one, state) -> {
          // Use all State's methods here
          state.exists();
          state.get();
          state.isTimingOut();
          state.remove();
          state.update(true);
          return 2.0;
        };

    JavaMapWithStateDStream<String, Integer, Boolean, Double> stateDstream2 =
        wordsDstream.mapWithState(
            StateSpec.function(mappingFunc2)
                .initialState(initialRDD)
                .numPartitions(10)
                .partitioner(new HashPartitioner(10))
                .timeout(Durations.seconds(10)));

    stateDstream2.stateSnapshots();
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

    @SuppressWarnings("unchecked")
    List<Set<Tuple2<String, Integer>>> stateData = Arrays.asList(
        Collections.<Tuple2<String, Integer>>emptySet(),
        Sets.newHashSet(new Tuple2<>("a", 1)),
        Sets.newHashSet(new Tuple2<>("a", 2), new Tuple2<>("b", 1)),
        Sets.newHashSet(new Tuple2<>("a", 3), new Tuple2<>("b", 2), new Tuple2<>("c", 1)),
        Sets.newHashSet(new Tuple2<>("a", 4), new Tuple2<>("b", 3), new Tuple2<>("c", 1)),
        Sets.newHashSet(new Tuple2<>("a", 5), new Tuple2<>("b", 3), new Tuple2<>("c", 1)),
        Sets.newHashSet(new Tuple2<>("a", 5), new Tuple2<>("b", 3), new Tuple2<>("c", 1))
    );

    Function3<String, Optional<Integer>, State<Integer>, Integer> mappingFunc =
        (key, value, state) -> {
          int sum = value.orElse(0) + (state.exists() ? state.get() : 0);
          state.update(sum);
          return sum;
        };
    testOperation(
        inputData,
        StateSpec.function(mappingFunc),
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
    JavaMapWithStateDStream<K, Integer, S, T> mapWithStateDStream = JavaPairDStream.fromJavaDStream(
      inputStream.map(x -> new Tuple2<>(x, 1))).mapWithState(mapWithStateSpec);

    List<Set<T>> collectedOutputs =
        Collections.synchronizedList(new ArrayList<>());
    mapWithStateDStream.foreachRDD(rdd -> collectedOutputs.add(Sets.newHashSet(rdd.collect())));
    List<Set<Tuple2<K, S>>> collectedStateSnapshots =
        Collections.synchronizedList(new ArrayList<>());
    mapWithStateDStream.stateSnapshots().foreachRDD(rdd ->
        collectedStateSnapshots.add(Sets.newHashSet(rdd.collect())));
    BatchCounter batchCounter = new BatchCounter(ssc.ssc());
    ssc.start();
    ((ManualClock) ssc.ssc().scheduler().clock())
        .advance(ssc.ssc().progressListener().batchDuration() * numBatches + 1);
    batchCounter.waitUntilBatchesCompleted(numBatches, 10000);

    Assert.assertEquals(expectedOutputs, collectedOutputs);
    Assert.assertEquals(expectedStateSnapshots, collectedStateSnapshots);
  }
}
