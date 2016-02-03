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
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
// $example off$

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.mllib.linalg.Vectors;
import java.util.Arrays;


public class JavaStratifiedSamplingExample {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JavaStratifiedSamplingExample");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        // $example on$
//        JavaPairRDD<K, V> data = ... // an RDD of any key value pairs
//        Map<K, Object> fractions = ... // specify the exact fraction desired from each key
//
//        // Get an exact sample from each stratum
//        JavaPairRDD<K, V> approxSample = data.sampleByKey(false, fractions);
//        JavaPairRDD<K, V> exactSample = data.sampleByKeyExact(false, fractions);
        // $example off$

        jsc.stop();
    }
}
