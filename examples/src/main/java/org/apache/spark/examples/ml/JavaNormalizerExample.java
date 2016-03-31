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

package org.apache.spark.examples.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

// $example on$
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.sql.DataFrame;
// $example off$

public class JavaNormalizerExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaNormalizerExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // $example on$
    DataFrame dataFrame = jsql.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

    // Normalize each Vector using $L^1$ norm.
    Normalizer normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0);

    DataFrame l1NormData = normalizer.transform(dataFrame);
    l1NormData.show();

    // Normalize each Vector using $L^\infty$ norm.
    DataFrame lInfNormData =
      normalizer.transform(dataFrame, normalizer.p().w(Double.POSITIVE_INFINITY));
    lInfNormData.show();
    // $example off$
    jsc.stop();
  }
}