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
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.tuning.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example off$

/**
 * A simple example demonstrating model selection using ParamRandomBuilder.
 *
 * Run with
 * {{{
 * bin/run-example ml.JavaModelSelectionViaRandomHyperparametersExample
 * }}}
 */
public class JavaModelSelectionViaRandomHyperparametersExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaModelSelectionViaTrainValidationSplitExample")
                .getOrCreate();

        // $example on$
        Dataset<Row> data = spark.read().format("libsvm")
                .load("data/mllib/sample_linear_regression_data.txt");

        LinearRegression lr = new LinearRegression();

        // We sample the regularization parameter logarithmically over the range [0.01, 1.0].
        // This means that values around 0.01, 0.1 and 1.0 are roughly equally likely.
        // Note that both parameters must be greater than zero as otherwise we'll get an infinity.
        // We sample the the ElasticNet mixing parameter uniformly over the range [0, 1]
        // Note that in real life, you'd choose more than the 5 samples we see below.
        ParamMap[] hyperparameters = new ParamRandomBuilder()
                .addLog10Random(lr.regParam(), 0.01, 1.0, 5)
                .addRandom(lr.elasticNetParam(), 0.0, 1.0, 5)
                .addGrid(lr.fitIntercept())
                .build();

        System.out.println("hyperparameters:");
        for (ParamMap param : hyperparameters) {
            System.out.println(param);
        }

        CrossValidator cv = new CrossValidator()
                .setEstimator(lr)
                .setEstimatorParamMaps(hyperparameters)
                .setEvaluator(new RegressionEvaluator())
                .setNumFolds(3);
        CrossValidatorModel cvModel = cv.fit(data);
        LinearRegression parent = (LinearRegression)cvModel.bestModel().parent();

        System.out.println("Optimal model has\n" + lr.regParam() + " = " + parent.getRegParam()
                + "\n" + lr.elasticNetParam() + " = "+ parent.getElasticNetParam()
                + "\n" + lr.fitIntercept() + " = " + parent.getFitIntercept());
        // $example off$

        spark.stop();
    }
}
