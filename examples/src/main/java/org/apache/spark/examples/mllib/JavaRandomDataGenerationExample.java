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

// $example on$
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import static org.apache.spark.mllib.random.RandomRDDs.*;
// $example off$

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class JavaRandomDataGenerationExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaRandomDataGenerationExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    // Generate a random double RDD that contains 1 million i.i.d. values drawn from the
    // standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
    JavaDoubleRDD u = normalJavaRDD(jsc, 1000L, 10);
    // Apply a transform to get a random double RDD following `N(1, 4)`.
    JavaRDD v = u.map(
            new Function<Double, Double>() {
                public Double call(Double x) {
                    return 1.0 + 2.0 * x;
                }
            });
    // $example off$

    u.foreach(new VoidFunction<Double>() {
        public void call(Double d) throws Exception {
            System.out.println(d);
        }
    });

    v.foreach(new VoidFunction<Double>() {
        public void call(Double d) throws Exception {
            System.out.println(d);
        }
    });

    jsc.stop();
  }
}

