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

package org.apache.spark.examples.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{Classifier, ClassifierParams, ClassificationModel}
import org.apache.spark.ml.param.{Params, IntParam, ParamMap}
import org.apache.spark.mllib.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


/**
 * A simple example demonstrating how to write your own learning algorithm using Estimator,
 * Transformer, and other abstractions.
 * This mimics [[org.apache.spark.ml.classification.LogisticRegression]].
 * Run with
 * {{{
 * bin/run-example ml.DeveloperApiExample
 * }}}
 */
object DeveloperApiExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DeveloperApiExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Prepare training data.
    val training = sc.parallelize(Seq(
      LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
      LabeledPoint(0.0, Vectors.dense(2.0, 1.0, -1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 1.3, 1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.2, -0.5))))

    // Create a LogisticRegression instance.  This instance is an Estimator.
    val lr = new MyLogisticRegression()
    // Print out the parameters, documentation, and any default values.
    println("MyLogisticRegression parameters:\n" + lr.explainParams() + "\n")

    // We may set parameters using setter methods.
    lr.setMaxIter(10)

    // Learn a LogisticRegression model.  This uses the parameters stored in lr.
    val model = lr.fit(training.toDF())

    // Prepare test data.
    val test = sc.parallelize(Seq(
      LabeledPoint(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      LabeledPoint(0.0, Vectors.dense(3.0, 2.0, -0.1)),
      LabeledPoint(1.0, Vectors.dense(0.0, 2.2, -1.5))))

    // Make predictions on test data.
    val sumPredictions: Double = model.transform(test.toDF())
      .select("features", "label", "prediction")
      .collect()
      .map { case Row(features: Vector, label: Double, prediction: Double) =>
        prediction
      }.sum
    assert(sumPredictions == 0.0,
      "MyLogisticRegression predicted something other than 0, even though all weights are 0!")

    sc.stop()
  }
}

/**
 * Example of defining a parameter trait for a user-defined type of [[Classifier]].
 *
 * NOTE: This is private since it is an example.  In practice, you may not want it to be private.
 */
private trait MyLogisticRegressionParams extends ClassifierParams {

  /**
   * Param for max number of iterations
   *
   * NOTE: The usual way to add a parameter to a model or algorithm is to include:
   *   - val myParamName: ParamType
   *   - def getMyParamName
   *   - def setMyParamName
   * Here, we have a trait to be mixed in with the Estimator and Model (MyLogisticRegression
   * and MyLogisticRegressionModel).  We place the setter (setMaxIter) method in the Estimator
   * class since the maxIter parameter is only used during training (not in the Model).
   */
  val maxIter: IntParam = new IntParam(this, "maxIter", "max number of iterations")
  def getMaxIter: Int = get(maxIter)
}

/**
 * Example of defining a type of [[Classifier]].
 *
 * NOTE: This is private since it is an example.  In practice, you may not want it to be private.
 */
private class MyLogisticRegression
  extends Classifier[Vector, MyLogisticRegression, MyLogisticRegressionModel]
  with MyLogisticRegressionParams {

  setMaxIter(100) // Initialize

  // The parameter setter is in this class since it should return type MyLogisticRegression.
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  // This method is used by fit()
  override protected def train(
      dataset: DataFrame,
      paramMap: ParamMap): MyLogisticRegressionModel = {
    // Extract columns from data using helper method.
    val oldDataset = extractLabeledPoints(dataset, paramMap)

    // Do learning to estimate the weight vector.
    val numFeatures = oldDataset.take(1)(0).features.size
    val weights = Vectors.zeros(numFeatures) // Learning would happen here.

    // Create a model, and return it.
    new MyLogisticRegressionModel(this, paramMap, weights)
  }
}

/**
 * Example of defining a type of [[ClassificationModel]].
 *
 * NOTE: This is private since it is an example.  In practice, you may not want it to be private.
 */
private class MyLogisticRegressionModel(
    override val parent: MyLogisticRegression,
    override val fittingParamMap: ParamMap,
    val weights: Vector)
  extends ClassificationModel[Vector, MyLogisticRegressionModel]
  with MyLogisticRegressionParams {

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
   * @return  vector where element i is the raw prediction for label i.
   *          This raw prediction may be any real number, where a larger value indicates greater
   *          confidence for that label.
   */
  override protected def predictRaw(features: Vector): Vector = {
    val margin = BLAS.dot(features, weights)
    // There are 2 classes (binary classification), so we return a length-2 vector,
    // where index i corresponds to class i (i = 0, 1).
    Vectors.dense(-margin, margin)
  }

  /** Number of classes the label can take.  2 indicates binary classification. */
  override val numClasses: Int = 2

  /**
   * Create a copy of the model.
   * The copy is shallow, except for the embedded paramMap, which gets a deep copy.
   *
   * This is used for the defaul implementation of [[transform()]].
   */
  override protected def copy(): MyLogisticRegressionModel = {
    val m = new MyLogisticRegressionModel(parent, fittingParamMap, weights)
    Params.inheritValues(this.paramMap, this, m)
    m
  }
}
