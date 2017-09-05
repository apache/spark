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
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.linalg.Matrix;
// $example off$
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class JavaMulticlassClassificationMetricsExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Multi class Classification Metrics Example");
    SparkContext sc = new SparkContext(conf);
    // $example on$
    String path = "data/mllib/sample_multiclass_classification_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

    // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.6, 0.4}, 11L);
    JavaRDD<LabeledPoint> training = splits[0].cache();
    JavaRDD<LabeledPoint> test = splits[1];

    // Run training algorithm to build the model.
    LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
      .setNumClasses(3)
      .run(training.rdd());

    // Compute raw scores on the test set.
    JavaPairRDD<Object, Object> predictionAndLabels = test.mapToPair(p ->
      new Tuple2<>(model.predict(p.features()), p.label()));

    // Get evaluation metrics.
    MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());

    // Confusion matrix
    Matrix confusion = metrics.confusionMatrix();
    System.out.println("Confusion matrix: \n" + confusion);

    // Overall statistics
    System.out.println("Accuracy = " + metrics.accuracy());

    // Stats by labels
    for (int i = 0; i < metrics.labels().length; i++) {
      System.out.format("Class %f precision = %f\n", metrics.labels()[i],metrics.precision(
        metrics.labels()[i]));
      System.out.format("Class %f recall = %f\n", metrics.labels()[i], metrics.recall(
        metrics.labels()[i]));
      System.out.format("Class %f F1 score = %f\n", metrics.labels()[i], metrics.fMeasure(
        metrics.labels()[i]));
    }

    //Weighted stats
    System.out.format("Weighted precision = %f\n", metrics.weightedPrecision());
    System.out.format("Weighted recall = %f\n", metrics.weightedRecall());
    System.out.format("Weighted F1 score = %f\n", metrics.weightedFMeasure());
    System.out.format("Weighted false positive rate = %f\n", metrics.weightedFalsePositiveRate());

    // Save and load model
    model.save(sc, "target/tmp/LogisticRegressionModel");
    LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc,
      "target/tmp/LogisticRegressionModel");
    // $example off$

    sc.stop();
  }
}
