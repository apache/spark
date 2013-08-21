package spark.mllib.classification

import spark.RDD

trait ClassificationModel extends Serializable {
  /**
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return RDD[Int] where each entry contains the corresponding prediction
   */
  def predict(testData: RDD[Array[Double]]): RDD[Double]

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return Int prediction from the trained model
   */
  def predict(testData: Array[Double]): Double
}
