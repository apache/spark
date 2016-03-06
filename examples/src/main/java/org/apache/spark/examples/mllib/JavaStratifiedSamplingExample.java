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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

// $example on$
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
// $example off$

public class JavaStratifiedSamplingExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaStratifiedSamplingExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    List<Tuple2<Integer, Character>> list = new ArrayList<>();
    list.add(new Tuple2(1, 'a'));
    list.add(new Tuple2(1, 'b'));
    list.add(new Tuple2(2, 'c'));
    list.add(new Tuple2(2, 'd'));
    list.add(new Tuple2(2, 'e'));
    list.add(new Tuple2(3, 'f'));

    // an RDD of any key value pairs JavaPairRDD<K, V>
    JavaPairRDD<Integer, Character> data = jsc.parallelizePairs(list);

    // specify the exact fraction desired from each key Map<K, Object>
    Map<Integer, Object> fractions = new HashMap<>();

    fractions.put(1, 0.1);
    fractions.put(2, 0.6);
    fractions.put(3, 0.3);

    // Get an exact sample from each stratum
    JavaPairRDD<Integer, Character> approxSample =
      data.sampleByKey(false, fractions); // JavaPairRDD<K, V>
    JavaPairRDD<Integer, Character> exactSample =
      data.sampleByKeyExact(false, fractions); // JavaPairRDD<K, V>
    // $example off$

    approxSample.foreach(new VoidFunction<Tuple2<Integer, Character>>() {
      public void call(Tuple2<Integer, Character> t) throws Exception {
        System.out.println(t._1() + " " + t._2());
      }
    });
    System.out.println();
    exactSample.foreach(new VoidFunction<Tuple2<Integer, Character>>() {
      public void call(Tuple2<Integer, Character> t) throws Exception {
        System.out.println(t._1() + " " + t._2());
      }
    });

    jsc.stop();
  }
}
