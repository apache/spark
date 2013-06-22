package spark.ml.recommendation

import spark.RDD
import spark.SparkContext._

import org.jblas._

class MatrixFactorizationModel(
  val rank: Int,
  val userFeatures: RDD[(Int, Array[Double])],
  val productFeatures: RDD[(Int, Array[Double])])
{
  /** Predict the rating of one user for one product. */
  def predict(user: Int, product: Int): Double = {
    val userVector = new DoubleMatrix(userFeatures.lookup(user).head)
    val productVector = new DoubleMatrix(productFeatures.lookup(product).head)
    userVector.dot(productVector)
  }

  // TODO: Figure out what good bulk prediction methods would look like.
  // Probably want a way to get the top users for a product or vice-versa.
}
