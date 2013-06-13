package spark.ml

import java.io._

import spark.{RDD, SparkContext}
import spark.SparkContext._

import org.jblas.DoubleMatrix

trait RegressionModel {
  /**
   * Predict values for the given data set using the model trained.
   *
   * @param test_data RDD representing data points to be predicted
   * @return RDD[Double] where each entry contains the corresponding prediction
   */
  def predict(test_data: RDD[Array[Double]]): RDD[Double]
}

abstract class RegressionData(val data: RDD[(Double, Array[Double])]) extends Serializable {

  /**
   * Normalize the provided input data. This function is typically called before
   * training a classifier on the input dataset and should be used to center of scale the data
   * appropriately.
   *
   * @return RDD containing the normalized data
   */
  def normalizeData(): RDD[(Double, Array[Double])]

  /**
   * Scale the trained regression model. This function is usually called after training
   * to adjust the model based on the normalization performed before.
   *
   * @return Regression model that can be used for prediction
   */
  def scaleModel(model: RegressionModel): RegressionModel
}
