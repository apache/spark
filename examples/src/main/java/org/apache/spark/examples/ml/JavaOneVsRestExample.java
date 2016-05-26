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
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$
import org.apache.spark.sql.SparkSession;


/**
 * An example of Multiclass to Binary Reduction with One Vs Rest,
 * using Logistic Regression as the base classifier.
 * Run with
 * <pre>
 * bin/run-example ml.JavaOneVsRestExample
 * </pre>
 */
public class JavaOneVsRestExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaOneVsRestExample")
      .getOrCreate();

    // $example on$
    // load data file.
    Dataset<Row> inputData = spark.read().format("libsvm")
      .load("data/mllib/sample_multiclass_classification_data.txt");

    // generate the train/test split.
    Dataset<Row>[] tmp = inputData.randomSplit(new double[]{0.8, 0.2});
    Dataset<Row> train = tmp[0];
    Dataset<Row> test = tmp[1];

    // configure the base classifier.
    LogisticRegression classifier = new LogisticRegression()
      .setMaxIter(10)
      .setTol(1E-6)
      .setFitIntercept(true);

    // instantiate the One Vs Rest Classifier.
    OneVsRest ovr = new OneVsRest().setClassifier(classifier);

    // train the multiclass model.
    OneVsRestModel ovrModel = ovr.fit(train);

    // score the model on test data.
    Dataset<Row> predictions = ovrModel.transform(test)
      .select("prediction", "label");

    // obtain evaluator.
    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
            .setMetricName("precision");

    // compute the classification error on test data.
    double precision = evaluator.evaluate(predictions);
    System.out.println("Test Error : " + (1 - precision));
    // $example off$

    spark.stop();
  }

}
