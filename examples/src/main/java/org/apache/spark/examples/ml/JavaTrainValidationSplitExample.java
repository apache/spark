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

import java.util.List;

import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.tuning.*;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * A simple example demonstrating model selection using TrainValidationSplit.
 *
 * The example is based on {@link org.apache.spark.examples.ml.JavaSimpleParamsExample}
 * using linear regression.
 *
 * Run with
 * {{{
 * bin/run-example ml.JavaTrainValidationSplitExample
 * }}}
 */
public class JavaTrainValidationSplitExample {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaTrainValidationSplitExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    List<LabeledPoint> localTraining = Lists.newArrayList(
      new LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
      new LabeledPoint(0.0, Vectors.dense(2.0, 1.0, -1.0)),
      new LabeledPoint(0.0, Vectors.dense(2.0, 1.3, 1.0)),
      new LabeledPoint(1.0, Vectors.dense(0.0, 1.2, -0.5)));

    DataFrame training = jsql.createDataFrame(jsc.parallelize(localTraining), LabeledPoint.class);

    LinearRegression lr = new LinearRegression();

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator());

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    ParamMap[] paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam(), new double[]{0.1, 0.01})
      .addGrid(lr.fitIntercept())
      .addGrid(lr.elasticNetParam(), new double[]{0.0, 0.5, 1.0})
      .addGrid(lr.maxIter(), new int[]{10, 100})
      .addGrid(lr.tol(), new double[]{1E-5, 1E-6})
      .build();

    trainValidationSplit.setEstimatorParamMaps(paramGrid);

    // 80% of the data will be used for training and the remaining 20% for validation.
    trainValidationSplit.setTrainRatio(0.8);

    // Run train validation split, and choose the best set of parameters.
    TrainValidationSplitModel model = trainValidationSplit.fit(training);

    // Prepare unlabeled test data.
    List<LabeledPoint> localTest = Lists.newArrayList(
      new LabeledPoint(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      new LabeledPoint(0.0, Vectors.dense(3.0, 2.0, -0.1)),
      new LabeledPoint(1.0, Vectors.dense(0.0, 2.2, -1.5)));

    DataFrame test = jsql.createDataFrame(jsc.parallelize(localTest), LabeledPoint.class);

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    DataFrame results = model.transform(test);
    for (Row r: results.select("features", "label", "prediction").collect()) {
      System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> " + "prediction=" + r.get(2));
    }

    jsc.stop();
  }
}
