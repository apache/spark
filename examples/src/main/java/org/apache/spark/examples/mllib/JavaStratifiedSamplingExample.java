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

package org.apache.spark.examples.mllib;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

// $example on$
import java.util.*;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
// $example off$

public class JavaStratifiedSamplingExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaStratifiedSamplingExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    List<Tuple2<Integer, Character>> list = Arrays.asList(
        new Tuple2<>(1, 'a'),
        new Tuple2<>(1, 'b'),
        new Tuple2<>(2, 'c'),
        new Tuple2<>(2, 'd'),
        new Tuple2<>(2, 'e'),
        new Tuple2<>(3, 'f')
    );

    JavaPairRDD<Integer, Character> data = jsc.parallelizePairs(list);

    // specify the exact fraction desired from each key Map<K, Double>
    ImmutableMap<Integer, Double> fractions = ImmutableMap.of(1, 0.1, 2, 0.6, 3, 0.3);

    // Get an approximate sample from each stratum
    JavaPairRDD<Integer, Character> approxSample = data.sampleByKey(false, fractions);
    // Get an exact sample from each stratum
    JavaPairRDD<Integer, Character> exactSample = data.sampleByKeyExact(false, fractions);
    // $example off$

    System.out.println("approxSample size is " + approxSample.collect().size());
    for (Tuple2<Integer, Character> t : approxSample.collect()) {
      System.out.println(t._1() + " " + t._2());
    }

    System.out.println("exactSample size is " + exactSample.collect().size());
    for (Tuple2<Integer, Character> t : exactSample.collect()) {
      System.out.println(t._1() + " " + t._2());
    }

    jsc.stop();
  }
}
