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
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaTrackStateDStream;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;

public class JavaTrackStateByKeySuite extends LocalJavaStreamingContext implements Serializable {

  /**
   * This test is only for testing the APIs. It's not necessary to run it.
   */
  public void testAPI() {
    // TODO
//    JavaPairRDD<String, Integer> initialRDD = null;
//    JavaPairDStream<String, Integer> wordsDstream = null;
//    final Function4<Time, String, Optional<Integer>, State<Integer>, Optional<String>>
// trackStateFunc =
//        new Function4<Time, String, Optional<Integer>, State<Integer>, Optional<String>>() {
//
//          @Override
//          public Optional<String> call(Time time, String word, Optional<Integer> one,
// State<Integer> state) {
//            // Use all State's methods here
//            state.exists();
//            state.get();
//            state.isTimingOut();
//            state.remove();
//            state.update(10);
//            return "test";
//          }
//        };
//
//    JavaTrackStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
//        wordsDstream.trackStateByKey(
//            StateSpec.function(trackStateFunc)
//                .initialState(initialRDD)
//                .numPartitions(10)
//                .partitioner(new HashPartitioner(10))
//                .timeout(Durations.seconds(10)));
  }
}
