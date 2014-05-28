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

package org.apache.spark.mllib.recommendation

import org.jblas.DoubleMatrix

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.api.python.PythonMLLibAPI

/**
 * Model representing the result of matrix factorization.
 *
 * @param rank Rank for the features in this model.
 * @param userFeatures RDD of tuples where each tuple represents the userId and
 *                     the features computed for this user.
 * @param productFeatures RDD of tuples where each tuple represents the productId
 *                        and the features computed for this product.
 */
class MatrixFactorizationModel private[mllib] (
    val rank: Int,
    val userFeatures: RDD[(Int, Array[Double])],
    val productFeatures: RDD[(Int, Array[Double])]) extends Serializable {
  /** Predict the rating of one user for one product. */
  def predict(user: Int, product: Int): Double = {
    val userVector = new DoubleMatrix(userFeatures.lookup(user).head)
    val productVector = new DoubleMatrix(productFeatures.lookup(product).head)
    userVector.dot(productVector)
  }

  /**
    * Predict the rating of many users for many products.
    * The output RDD has an element per each element in the input RDD (including all duplicates)
    * unless a user or product is missing in the training set.
    *
    * @param usersProducts  RDD of (user, product) pairs.
    * @return RDD of Ratings.
    */
  def predict(usersProducts: RDD[(Int, Int)]): RDD[Rating] = {
    val users = userFeatures.join(usersProducts).map{
      case (user, (uFeatures, product)) => (product, (user, uFeatures))
    }
    users.join(productFeatures).map {
      case (product, ((user, uFeatures), pFeatures)) =>
        val userVector = new DoubleMatrix(uFeatures)
        val productVector = new DoubleMatrix(pFeatures)
        Rating(user, product, userVector.dot(productVector))
    }
  }

  /**
   * :: DeveloperApi ::
   * Predict the rating of many users for many products.
   * This is a Java stub for python predictAll()
   *
   * @param usersProductsJRDD A JavaRDD with serialized tuples (user, product)
   * @return JavaRDD of serialized Rating objects.
   */
  @DeveloperApi
  def predict(usersProductsJRDD: JavaRDD[Array[Byte]]): JavaRDD[Array[Byte]] = {
    val pythonAPI = new PythonMLLibAPI()
    val usersProducts = usersProductsJRDD.rdd.map(xBytes => pythonAPI.unpackTuple(xBytes))
    predict(usersProducts).map(rate => pythonAPI.serializeRating(rate))
  }

  // TODO: Figure out what other good bulk prediction methods would look like.
  // Probably want a way to get the top users for a product or vice-versa.
}
