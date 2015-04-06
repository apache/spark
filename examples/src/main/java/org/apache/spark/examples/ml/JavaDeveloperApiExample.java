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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.Classifier;
import org.apache.spark.ml.classification.ClassificationModel;
import org.apache.spark.ml.param.IntParam;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.Params;
import org.apache.spark.ml.param.Params$;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


/**
 * A simple example demonstrating how to write your own learning algorithm using Estimator,
 * Transformer, and other abstractions.
 * This mimics {@link org.apache.spark.ml.classification.LogisticRegression}.
 *
 * Run with
 * <pre>
 * bin/run-example ml.JavaDeveloperApiExample
 * </pre>
 */
public class JavaDeveloperApiExample {

  public static void main(String[] args) throws Exception {
    SparkConf conf = new SparkConf().setAppName("JavaDeveloperApiExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // Prepare training data.
    List<LabeledPoint> localTraining = Lists.newArrayList(
        new LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
        new LabeledPoint(0.0, Vectors.dense(2.0, 1.0, -1.0)),
        new LabeledPoint(0.0, Vectors.dense(2.0, 1.3, 1.0)),
        new LabeledPoint(1.0, Vectors.dense(0.0, 1.2, -0.5)));
    DataFrame training = jsql.createDataFrame(jsc.parallelize(localTraining), LabeledPoint.class);

    // Create a LogisticRegression instance.  This instance is an Estimator.
    MyJavaLogisticRegression lr = new MyJavaLogisticRegression();
    // Print out the parameters, documentation, and any default values.
    System.out.println("MyJavaLogisticRegression parameters:\n" + lr.explainParams() + "\n");

    // We may set parameters using setter methods.
    lr.setMaxIter(10);

    // Learn a LogisticRegression model.  This uses the parameters stored in lr.
    MyJavaLogisticRegressionModel model = lr.fit(training);

    // Prepare test data.
    List<LabeledPoint> localTest = Lists.newArrayList(
        new LabeledPoint(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
        new LabeledPoint(0.0, Vectors.dense(3.0, 2.0, -0.1)),
        new LabeledPoint(1.0, Vectors.dense(0.0, 2.2, -1.5)));
    DataFrame test = jsql.createDataFrame(jsc.parallelize(localTest), LabeledPoint.class);

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    DataFrame results = model.transform(test);
    double sumPredictions = 0;
    for (Row r : results.select("features", "label", "prediction").collect()) {
      sumPredictions += r.getDouble(2);
    }
    if (sumPredictions != 0.0) {
      throw new Exception("MyJavaLogisticRegression predicted something other than 0," +
          " even though all weights are 0!");
    }

    jsc.stop();
  }
}

/**
 * Example of defining a type of {@link Classifier}.
 *
 * NOTE: This is private since it is an example.  In practice, you may not want it to be private.
 */
class MyJavaLogisticRegression
    extends Classifier<Vector, MyJavaLogisticRegression, MyJavaLogisticRegressionModel>
    implements Params {

  /**
   * Param for max number of iterations
   * <p/>
   * NOTE: The usual way to add a parameter to a model or algorithm is to include:
   * - val myParamName: ParamType
   * - def getMyParamName
   * - def setMyParamName
   */
  IntParam maxIter = new IntParam(this, "maxIter", "max number of iterations");

  int getMaxIter() { return (Integer) get(maxIter); }

  public MyJavaLogisticRegression() {
    setMaxIter(100);
  }

  // The parameter setter is in this class since it should return type MyJavaLogisticRegression.
  MyJavaLogisticRegression setMaxIter(int value) {
    return (MyJavaLogisticRegression) set(maxIter, value);
  }

  // This method is used by fit().
  // In Java, we have to make it public since Java does not understand Scala's protected modifier.
  public MyJavaLogisticRegressionModel train(DataFrame dataset, ParamMap paramMap) {
    // Extract columns from data using helper method.
    JavaRDD<LabeledPoint> oldDataset = extractLabeledPoints(dataset, paramMap).toJavaRDD();

    // Do learning to estimate the weight vector.
    int numFeatures = oldDataset.take(1).get(0).features().size();
    Vector weights = Vectors.zeros(numFeatures); // Learning would happen here.

    // Create a model, and return it.
    return new MyJavaLogisticRegressionModel(this, paramMap, weights);
  }
}

/**
 * Example of defining a type of {@link ClassificationModel}.
 *
 * NOTE: This is private since it is an example.  In practice, you may not want it to be private.
 */
class MyJavaLogisticRegressionModel
    extends ClassificationModel<Vector, MyJavaLogisticRegressionModel> implements Params {

  private MyJavaLogisticRegression parent_;
  public MyJavaLogisticRegression parent() { return parent_; }

  private ParamMap fittingParamMap_;
  public ParamMap fittingParamMap() { return fittingParamMap_; }

  private Vector weights_;
  public Vector weights() { return weights_; }

  public MyJavaLogisticRegressionModel(
      MyJavaLogisticRegression parent_,
      ParamMap fittingParamMap_,
      Vector weights_) {
    this.parent_ = parent_;
    this.fittingParamMap_ = fittingParamMap_;
    this.weights_ = weights_;
  }

  // This uses the default implementation of transform(), which reads column "features" and outputs
  // columns "prediction" and "rawPrediction."

  // This uses the default implementation of predict(), which chooses the label corresponding to
  // the maximum value returned by [[predictRaw()]].

  /**
   * Raw prediction for each possible label.
   * The meaning of a "raw" prediction may vary between algorithms, but it intuitively gives
   * a measure of confidence in each possible label (where larger = more confident).
   * This internal method is used to implement [[transform()]] and output [[rawPredictionCol]].
   *
   * @return vector where element i is the raw prediction for label i.
   * This raw prediction may be any real number, where a larger value indicates greater
   * confidence for that label.
   *
   * In Java, we have to make this method public since Java does not understand Scala's protected
   * modifier.
   */
  public Vector predictRaw(Vector features) {
    double margin = BLAS.dot(features, weights_);
    // There are 2 classes (binary classification), so we return a length-2 vector,
    // where index i corresponds to class i (i = 0, 1).
    return Vectors.dense(-margin, margin);
  }

  /**
   * Number of classes the label can take.  2 indicates binary classification.
   */
  public int numClasses() { return 2; }

  /**
   * Create a copy of the model.
   * The copy is shallow, except for the embedded paramMap, which gets a deep copy.
   * <p/>
   * This is used for the defaul implementation of [[transform()]].
   *
   * In Java, we have to make this method public since Java does not understand Scala's protected
   * modifier.
   */
  public MyJavaLogisticRegressionModel copy() {
    MyJavaLogisticRegressionModel m =
        new MyJavaLogisticRegressionModel(parent_, fittingParamMap_, weights_);
    Params$.MODULE$.inheritValues(this.paramMap(), this, m);
    return m;
  }
}
