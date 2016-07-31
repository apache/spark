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
import java.util.Arrays;
// $example off$

// $example on$
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$
import org.apache.spark.sql.SparkSession;

/**
 * Java example for Estimator, Transformer, and Param.
 */
public class JavaEstimatorTransformerParamExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaEstimatorTransformerParamExample")
      .getOrCreate();

    // $example on$
    // Prepare training data.
    // We use LabeledPoint, which is a JavaBean. Spark SQL can convert RDDs of JavaBeans into
    // DataFrames, where it uses the bean metadata to infer the schema.
    Dataset<Row> training = spark.createDataFrame(
      Arrays.asList(
        new LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
        new LabeledPoint(0.0, Vectors.dense(2.0, 1.0, -1.0)),
        new LabeledPoint(0.0, Vectors.dense(2.0, 1.3, 1.0)),
        new LabeledPoint(1.0, Vectors.dense(0.0, 1.2, -0.5))
      ), LabeledPoint.class);

    // Create a LogisticRegression instance. This instance is an Estimator.
    LogisticRegression lr = new LogisticRegression();
    // Print out the parameters, documentation, and any default values.
    System.out.println("LogisticRegression parameters:\n" + lr.explainParams() + "\n");

    // We may set parameters using setter methods.
    lr.setMaxIter(10).setRegParam(0.01);

    // Learn a LogisticRegression model. This uses the parameters stored in lr.
    LogisticRegressionModel model1 = lr.fit(training);
    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
    // we can view the parameters it used during fit().
    // This prints the parameter (name: value) pairs, where names are unique IDs for this
    // LogisticRegression instance.
    System.out.println("Model 1 was fit using parameters: " + model1.parent().extractParamMap());

    // We may alternatively specify parameters using a ParamMap.
    ParamMap paramMap = new ParamMap()
      .put(lr.maxIter().w(20))  // Specify 1 Param.
      .put(lr.maxIter(), 30)  // This overwrites the original maxIter.
      .put(lr.regParam().w(0.1), lr.threshold().w(0.55));  // Specify multiple Params.

    // One can also combine ParamMaps.
    ParamMap paramMap2 = new ParamMap()
      .put(lr.probabilityCol().w("myProbability"));  // Change output column name
    ParamMap paramMapCombined = paramMap.$plus$plus(paramMap2);

    // Now learn a new model using the paramMapCombined parameters.
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    LogisticRegressionModel model2 = lr.fit(training, paramMapCombined);
    System.out.println("Model 2 was fit using parameters: " + model2.parent().extractParamMap());

    // Prepare test documents.
    Dataset<Row> test = spark.createDataFrame(Arrays.asList(
      new LabeledPoint(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      new LabeledPoint(0.0, Vectors.dense(3.0, 2.0, -0.1)),
      new LabeledPoint(1.0, Vectors.dense(0.0, 2.2, -1.5))
    ), LabeledPoint.class);

    // Make predictions on test documents using the Transformer.transform() method.
    // LogisticRegression.transform will only use the 'features' column.
    // Note that model2.transform() outputs a 'myProbability' column instead of the usual
    // 'probability' column since we renamed the lr.probabilityCol parameter previously.
    Dataset<Row> results = model2.transform(test);
    Dataset<Row> rows = results.select("features", "label", "myProbability", "prediction");
    for (Row r: rows.collectAsList()) {
      System.out.println("(" + r.get(0) + ", " + r.get(1) + ") -> prob=" + r.get(2)
        + ", prediction=" + r.get(3));
    }
    // $example off$

    spark.stop();
  }
}
