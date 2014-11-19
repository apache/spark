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

import java.lang.{ Integer => JavaInteger }

import org.apache.spark.SparkContext._
import org.apache.spark.api.java.{ JavaPairRDD, JavaRDD }
import org.apache.spark.rdd.RDD

import org.apache.spark.util.collection.Utils
import org.apache.spark.util.BoundedPriorityQueue

import scala.Ordering
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.BLAS

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
    val userVector = Vectors.dense(userFeatures.lookup(user).head)
    val productVector = Vectors.dense(productFeatures.lookup(product).head)
    BLAS.dot(userVector, productVector)
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
    val users = userFeatures.join(usersProducts).map {
      case (user, (uFeatures, product)) => (product, (user, uFeatures))
    }
    users.join(productFeatures).map {
      case (product, ((user, uFeatures), pFeatures)) =>
        val userVector = Vectors.dense(uFeatures)
        val productVector = Vectors.dense(pFeatures)
        Rating(user, product, BLAS.dot(userVector, productVector))
    }
  }

  /**
   * Java-friendly version of [[MatrixFactorizationModel.predict]].
   */
  def predict(usersProducts: JavaPairRDD[JavaInteger, JavaInteger]): JavaRDD[Rating] = {
    predict(usersProducts.rdd.asInstanceOf[RDD[(Int, Int)]]).toJavaRDD()
  }

  /**
   * Recommends products to a user.
   *
   * @param user the user to recommend products to
   * @param num how many products to return. The number returned may be less than this.
   * @return [[Rating]] objects, each of which contains the given user ID, a product ID, and a
   *  "score" in the rating field. Each represents one recommended product, and they are sorted
   *  by score, decreasing. The first returned is the one predicted to be most strongly
   *  recommended to the user. The score is an opaque value that indicates how strongly
   *  recommended the product is.
   */
  def recommendProducts(user: Int, num: Int): Array[Rating] =
    recommend(userFeatures.lookup(user).head, productFeatures, num)
      .map(t => Rating(user, t._1, t._2))

  /**
   * Recommends users to a product. That is, this returns users who are most likely to be
   * interested in a product.
   *
   * @param product the product to recommend users to
   * @param num how many users to return. The number returned may be less than this.
   * @return [[Rating]] objects, each of which contains a user ID, the given product ID, and a
   *  "score" in the rating field. Each represents one recommended user, and they are sorted
   *  by score, decreasing. The first returned is the one predicted to be most strongly
   *  recommended to the product. The score is an opaque value that indicates how strongly
   *  recommended the user is.
   */
  def recommendUsers(product: Int, num: Int): Array[Rating] =
    recommend(productFeatures.lookup(product).head, userFeatures, num)
      .map(t => Rating(t._1, product, t._2))

  /**
   * Recommends topK users/products.
   *
   * @param num how many users to return. The number returned may be less than this.
   * @return [Array[Rating]] objects, each of which contains a userID, the given productID and a
   *  "score" in the rating field. Each represents one recommended user, and they are sorted
   *  by score, decreasing. The first returned is the one predicted to be most strongly
   *  recommended to the product. The score is an opaque value that indicates how strongly
   *  recommended the user is.
   */

  /**
   * Recommend topK products for all users
   */
  def recommendProductsForUsers(num: Int): RDD[(Int, Array[Rating])] = {
    val topK = userFeatures.map { x => (x._1, num) }
    recommendProductsForUsers(topK)
  }

  /**
   * Recommend topK users for all products
   */
  def recommendUsersForProducts(num: Int): RDD[(Int, Array[Rating])] = {
    val topK = productFeatures.map { x => (x._1, num) }
    recommendUsersForProducts(topK)
  }

  val ord = Ordering.by[Rating, Double](x => x.rating)
  case class FeatureTopK(feature: Vector, topK: Int)

  /**
   * Recommend topK products for users in userTopK RDD
   */
  def recommendProductsForUsers(
    userTopK: RDD[(Int, Int)]): RDD[(Int, Array[Rating])] = {
    val userFeaturesTopK = userFeatures.join(userTopK).map {
      case (userId, (userFeature, topK)) =>
        (userId, FeatureTopK(Vectors.dense(userFeature), topK))
    }
    val productVectors = productFeatures.map {
      x => (x._1, Vectors.dense(x._2))
    }.collect

    userFeaturesTopK.map {
      case (userId, userFeatureTopK) => {
        val predictions = productVectors.map {
          case (productId, productVector) =>
            Rating(userId, productId,
              BLAS.dot(userFeatureTopK.feature, productVector))
        }
        (userId, Utils.takeOrdered(predictions.iterator,
          userFeatureTopK.topK)(ord.reverse).toArray)
      }
    }
  }
  
  /**
   * Recommend topK users for products in productTopK RDD
   */
  def recommendUsersForProducts(
    productTopK: RDD[(Int, Int)]): RDD[(Int, Array[Rating])] = {
    val blocks = userFeatures.partitions.size / 2

    val productVectors = productFeatures.join(productTopK).map {
      case (productId, (productFeature, topK)) =>
        (productId, FeatureTopK(Vectors.dense(productFeature), topK))
    }.collect()

    userFeatures.mapPartitions { items =>
      val predictions = productVectors.map {
        x => (x._1, new BoundedPriorityQueue[Rating](x._2.topK)(ord.reverse))
      }.toMap
      while (items.hasNext) {
        val (userId, userFeature) = items.next
        val userVector = Vectors.dense(userFeature)
        for (i <- 0 until productVectors.length) {
          val (productId, productFeatureTopK) = productVectors(i)
          val predicted = Rating(userId, productId,
            BLAS.dot(userVector, productFeatureTopK.feature))
          predictions(productId) ++= Iterator.single(predicted)
        }
      }
      predictions.iterator
    }.reduceByKey({ (queue1, queue2) =>
      queue1 ++= queue2
      queue1
    }, blocks).map {
      case (productId, predictions) =>
        (productId, predictions.toArray)
    }
  }

  private def recommend(
    recommendToFeatures: Array[Double],
    recommendableFeatures: RDD[(Int, Array[Double])],
    num: Int): Array[(Int, Double)] = {
    val recommendToVector = Vectors.dense(recommendToFeatures)
    val scored = recommendableFeatures.map {
      case (id, features) =>
        (id, BLAS.dot(recommendToVector, Vectors.dense(features)))
    }
    scored.top(num)(Ordering.by(_._2))
  }
}
