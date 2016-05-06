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

// $example on$
import org.apache.spark.ml.feature.MaxAbsScaler;
import org.apache.spark.ml.feature.MaxAbsScalerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$
import org.apache.spark.sql.SparkSession;

public class JavaMaxAbsScalerExample {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaMaxAbsScalerExample")
      .getOrCreate();

    // $example on$
    Dataset<Row> dataFrame = spark
      .read()
      .format("libsvm")
      .load("data/mllib/sample_libsvm_data.txt");
    MaxAbsScaler scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures");

    // Compute summary statistics and generate MaxAbsScalerModel
    MaxAbsScalerModel scalerModel = scaler.fit(dataFrame);

    // rescale each feature to range [-1, 1].
    Dataset<Row> scaledData = scalerModel.transform(dataFrame);
    scaledData.show();
    // $example off$
    spark.stop();
  }

}
