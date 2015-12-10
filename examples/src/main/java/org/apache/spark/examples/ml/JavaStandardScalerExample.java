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
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.sql.DataFrame;
// $example off$

public class JavaStandardScalerExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaStandardScalerExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // $example on$
    DataFrame dataFrame = jsql.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

    StandardScaler scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false);

    // Compute summary statistics by fitting the StandardScaler
    StandardScalerModel scalerModel = scaler.fit(dataFrame);

    // Normalize each feature to have unit standard deviation.
    DataFrame scaledData = scalerModel.transform(dataFrame);
    scaledData.show();
    // $example off$
    jsc.stop();
  }
}