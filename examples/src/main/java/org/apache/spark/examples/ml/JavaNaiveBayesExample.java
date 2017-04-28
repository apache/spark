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
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example off$

/**
 * An example for Naive Bayes Classification.
 */
public class JavaNaiveBayesExample {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaNaiveBayesExample")
      .getOrCreate();

    // $example on$
    // Load training data
    Dataset<Row> dataFrame =
      spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
    // Split the data into train and test
    Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
    Dataset<Row> train = splits[0];
    Dataset<Row> test = splits[1];

    // create the trainer and set its parameters
    NaiveBayes nb = new NaiveBayes();

    // train the model
    NaiveBayesModel model = nb.fit(train);

    // Select example rows to display.
    Dataset<Row> predictions = model.transform(test);
    predictions.show();

    // compute accuracy on the test set
    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy");
    double accuracy = evaluator.evaluate(predictions);
    System.out.println("Test set accuracy = " + accuracy);
    // $example off$

    spark.stop();
  }
}
