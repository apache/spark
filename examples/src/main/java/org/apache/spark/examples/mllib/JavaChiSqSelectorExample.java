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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.feature.ChiSqSelector;
import org.apache.spark.mllib.feature.ChiSqSelectorModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
// $example off$

public class JavaChiSqSelectorExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaChiSqSelectorExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    JavaRDD<LabeledPoint> points = MLUtils.loadLibSVMFile(jsc.sc(),
      "data/mllib/sample_libsvm_data.txt").toJavaRDD().cache();

    // Discretize data in 16 equal bins since ChiSqSelector requires categorical features
    // Although features are doubles, the ChiSqSelector treats each unique value as a category
    JavaRDD<LabeledPoint> discretizedData = points.map(lp -> {
      double[] discretizedFeatures = new double[lp.features().size()];
      for (int i = 0; i < lp.features().size(); ++i) {
        discretizedFeatures[i] = Math.floor(lp.features().apply(i) / 16);
      }
      return new LabeledPoint(lp.label(), Vectors.dense(discretizedFeatures));
    });

    // Create ChiSqSelector that will select top 50 of 692 features
    ChiSqSelector selector = new ChiSqSelector(50);
    // Create ChiSqSelector model (selecting features)
    ChiSqSelectorModel transformer = selector.fit(discretizedData.rdd());
    // Filter the top 50 features from each feature vector
    JavaRDD<LabeledPoint> filteredData = discretizedData.map(lp ->
      new LabeledPoint(lp.label(), transformer.transform(lp.features())));
    // $example off$

    System.out.println("filtered data: ");
    filteredData.foreach(System.out::println);

    jsc.stop();
  }
}
