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
// $example on$
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
// $example off$

public class JavaGradientBoostedTreeRegressorExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaGradientBoostedTreeRegressorExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    // Load and parse the data file, converting it to a DataFrame.
    DataFrame data = sqlContext.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    VectorIndexerModel featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data);

    // Split the data into training and test sets (30% held out for testing)
    DataFrame[] splits = data.randomSplit(new double[] {0.7, 0.3});
    DataFrame trainingData = splits[0];
    DataFrame testData = splits[1];

    // Train a GBT model.
    GBTRegressor gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10);

    // Chain indexer and GBT in a Pipeline
    Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {featureIndexer, gbt});

    // Train model.  This also runs the indexer.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
    DataFrame predictions = model.transform(testData);

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(5);

    // Select (prediction, true label) and compute test error
    RegressionEvaluator evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse");
    double rmse = evaluator.evaluate(predictions);
    System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

    GBTRegressionModel gbtModel = (GBTRegressionModel)(model.stages()[1]);
    System.out.println("Learned regression GBT model:\n" + gbtModel.toDebugString());
    // $example off$

    jsc.stop();
  }
}
