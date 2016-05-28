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

import java.util.List;

import org.apache.spark.ml.regression.IsotonicRegression;
import org.apache.spark.ml.regression.IsotonicRegressionModel;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

// $example off$

public class JavaIsotonicRegressionExample {
  public static void main(String[] args) {
      // Create a SparkSession.
      SparkSession spark = SparkSession
              .builder()
              .appName("JavaIsotonicRegression")
              .getOrCreate();

      IsotonicRegression ir = new IsotonicRegression().setIsotonic(true)
              .setLabelCol("label")
              .setFeaturesCol("features")
              .setWeightCol("weight");

      // Loads data.
      DataFrameReader dataReader = spark.read();
      dataReader.schema(new StructType()
              .add("label", "double")
              .add("features", "double"));

      Dataset<Row> data = dataReader.csv("data/mllib/sample_isotonic_regression_data.txt");
      data = data.select(
              functions.col("label"),
              functions.col("features"),
              functions.lit(1.0).as("weight")
      );

      // Split data into training (60%) and test (40%) sets.
      List<Dataset<Row>> splits = data.randomSplitAsList(new double[]{0.6, 0.4}, 11L);
      Dataset<Row> training = splits.get(0);
      Dataset<Row> test = splits.get(1);

      // Create isotonic regression model from training data.
      // Isotonic parameter defaults to true so it is only shown for demonstration
      IsotonicRegressionModel model = ir.fit(training);

      // Create tuples of predicted and real labels.
      Dataset<Row> result = model.transform(test);

      result.show();
      // $example off$

      spark.stop();
  }
}
