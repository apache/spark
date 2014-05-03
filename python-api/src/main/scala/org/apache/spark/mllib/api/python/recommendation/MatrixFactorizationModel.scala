package org.apache.spark.mllib.api.python.recommendation

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.api.python.PythonMLLibAPI

class MatrixFactorizationModel(
    override val rank: Int,
    override val userFeatures: RDD[(Int, Array[Double])],
    override val productFeatures: RDD[(Int, Array[Double])])
  extends org.apache.spark.mllib.recommendation.MatrixFactorizationModel(rank,
    userFeatures, productFeatures) {

  /**
   * :: DeveloperApi ::
   * Predict the rating of many users for many products.
   * This is a Java stub for python predictAll()
   *
   * @param usersProductsJRDD A JavaRDD with serialized tuples (user, product)
   * @return JavaRDD of serialized Rating objects.
   */
  def predict(usersProductsJRDD: JavaRDD[Array[Byte]]): JavaRDD[Array[Byte]] = {
    val pythonAPI = new PythonMLLibAPI()
    val usersProducts = usersProductsJRDD.rdd.map(xBytes => pythonAPI.unpackTuple(xBytes))
    predict(usersProducts).map(rate => pythonAPI.serializeRating(rate))
  }

}
