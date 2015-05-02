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

import java.io.IOException
import java.lang.{Integer => JavaInteger}

import scala.collection.mutable

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel

/**
 * Model representing the result of matrix factorization.
 *
 * Note: If you create the model directly using constructor, please be aware that fast prediction
 * requires cached user/product features and their associated partitioners.
 *
 * @param rank Rank for the features in this model.
 * @param userFeatures RDD of tuples where each tuple represents the userId and
 *                     the features computed for this user.
 * @param productFeatures RDD of tuples where each tuple represents the productId
 *                        and the features computed for this product.
 */
class MatrixFactorizationModel(
    val rank: Int,
    val userFeatures: RDD[(Int, Array[Double])],
    val productFeatures: RDD[(Int, Array[Double])])
  extends Saveable with Serializable with Logging {

  require(rank > 0)
  validateFeatures("User", userFeatures)
  validateFeatures("Product", productFeatures)

  /** Validates factors and warns users if there are performance concerns. */
  private def validateFeatures(name: String, features: RDD[(Int, Array[Double])]): Unit = {
    require(features.first()._2.length == rank,
      s"$name feature dimension does not match the rank $rank.")
    if (features.partitioner.isEmpty) {
      logWarning(s"$name factor does not have a partitioner. "
        + "Prediction on individual records could be slow.")
    }
    if (features.getStorageLevel == StorageLevel.NONE) {
      logWarning(s"$name factor is not cached. Prediction could be slow.")
    }
  }

  /** Predict the rating of one user for one product. */
  def predict(user: Int, product: Int): Double = {
    val userVector = userFeatures.lookup(user).head
    val productVector = productFeatures.lookup(product).head
    blas.ddot(rank, userVector, 1, productVector, 1)
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
        Rating(user, product, blas.ddot(uFeatures.length, uFeatures, 1, pFeatures, 1))
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
    MatrixFactorizationModel.recommend(userFeatures.lookup(user).head, productFeatures, num)
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
    MatrixFactorizationModel.recommend(productFeatures.lookup(product).head, userFeatures, num)
      .map(t => Rating(t._1, product, t._2))

  protected override val formatVersion: String = "1.0"

  override def save(sc: SparkContext, path: String): Unit = {
    MatrixFactorizationModel.SaveLoadV1_0.save(this, path)
  }

  /**
   * Recommends topK products for all users.
   *
   * @param num how many products to return for every user.
   * @return [(Int, Array[Rating])] objects, where every tuple contains a userID and an array of
   * rating objects which contains the same userId, recommended productID and a "score" in the
   * rating field. Semantics of score is same as recommendProducts API
   */
  def recommendProductsForUsers(num: Int): RDD[(Int, Array[Rating])] = {
    MatrixFactorizationModel.recommendForAll(rank, userFeatures, productFeatures, num).map {
      case (user, top) =>
        val ratings = top.map { case (product, rating) => Rating(user, product, rating) }
        (user, ratings)
    }
  }


  /**
   * Recommends topK users for all products.
   *
   * @param num how many users to return for every product.
   * @return [(Int, Array[Rating])] objects, where every tuple contains a productID and an array
   * of rating objects which contains the recommended userId, same productID and a "score" in the
   * rating field. Semantics of score is same as recommendUsers API
   */
  def recommendUsersForProducts(num: Int): RDD[(Int, Array[Rating])] = {
    MatrixFactorizationModel.recommendForAll(rank, productFeatures, userFeatures, num).map {
      case (product, top) =>
        val ratings = top.map { case (user, rating) => Rating(user, product, rating) }
        (product, ratings)
    }
  }
}

object MatrixFactorizationModel extends Loader[MatrixFactorizationModel] {

  import org.apache.spark.mllib.util.Loader._

  /**
   * Makes recommendations for a single user (or product).
   */
  private def recommend(
      recommendToFeatures: Array[Double],
      recommendableFeatures: RDD[(Int, Array[Double])],
      num: Int): Array[(Int, Double)] = {
    val scored = recommendableFeatures.map { case (id, features) =>
      (id, blas.ddot(features.length, recommendToFeatures, 1, features, 1))
    }
    scored.top(num)(Ordering.by(_._2))
  }

  /**
   * Makes recommendations for all users (or products).
   * @param rank rank
   * @param srcFeatures src features to receive recommendations
   * @param dstFeatures dst features used to make recommendations
   * @param num number of recommendations for each record
   * @return an RDD of (srcId: Int, recommendations), where recommendations are stored as an array
   *         of (dstId, rating) pairs.
   */
  private def recommendForAll(
      rank: Int,
      srcFeatures: RDD[(Int, Array[Double])],
      dstFeatures: RDD[(Int, Array[Double])],
      num: Int): RDD[(Int, Array[(Int, Double)])] = {
    val srcBlocks = blockify(rank, srcFeatures)
    val dstBlocks = blockify(rank, dstFeatures)
    val ratings = srcBlocks.cartesian(dstBlocks).flatMap {
      case ((srcIds, srcFactors), (dstIds, dstFactors)) =>
        val m = srcIds.length
        val n = dstIds.length
        val ratings = srcFactors.transpose.multiply(dstFactors)
        val output = new Array[(Int, (Int, Double))](m * n)
        var k = 0
        ratings.foreachActive { (i, j, r) =>
          output(k) = (srcIds(i), (dstIds(j), r))
          k += 1
        }
        output.toSeq
    }
    ratings.topByKey(num)(Ordering.by(_._2))
  }

  /**
   * Blockifies features to use Level-3 BLAS.
   */
  private def blockify(
      rank: Int,
      features: RDD[(Int, Array[Double])]): RDD[(Array[Int], DenseMatrix)] = {
    val blockSize = 4096 // TODO: tune the block size
    val blockStorage = rank * blockSize
    features.mapPartitions { iter =>
      iter.grouped(blockSize).map { grouped =>
        val ids = mutable.ArrayBuilder.make[Int]
        ids.sizeHint(blockSize)
        val factors = mutable.ArrayBuilder.make[Double]
        factors.sizeHint(blockStorage)
        var i = 0
        grouped.foreach { case (id, factor) =>
          ids += id
          factors ++= factor
          i += 1
        }
        (ids.result(), new DenseMatrix(rank, i, factors.result()))
      }
    }
  }

  override def load(sc: SparkContext, path: String): MatrixFactorizationModel = {
    val (loadedClassName, formatVersion, _) = loadMetadata(sc, path)
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    (loadedClassName, formatVersion) match {
      case (className, "1.0") if className == classNameV1_0 =>
        SaveLoadV1_0.load(sc, path)
      case _ =>
        throw new IOException("MatrixFactorizationModel.load did not recognize model with" +
          s"(class: $loadedClassName, version: $formatVersion). Supported:\n" +
          s"  ($classNameV1_0, 1.0)")
    }
  }

  private[recommendation]
  object SaveLoadV1_0 {

    private val thisFormatVersion = "1.0"

    private[recommendation]
    val thisClassName = "org.apache.spark.mllib.recommendation.MatrixFactorizationModel"

    /**
     * Saves a [[MatrixFactorizationModel]], where user features are saved under `data/users` and
     * product features are saved under `data/products`.
     */
    def save(model: MatrixFactorizationModel, path: String): Unit = {
      val sc = model.userFeatures.sparkContext
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~ ("rank" -> model.rank)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(metadataPath(path))
      model.userFeatures.toDF("id", "features").saveAsParquetFile(userPath(path))
      model.productFeatures.toDF("id", "features").saveAsParquetFile(productPath(path))
    }

    def load(sc: SparkContext, path: String): MatrixFactorizationModel = {
      implicit val formats = DefaultFormats
      val sqlContext = new SQLContext(sc)
      val (className, formatVersion, metadata) = loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)
      val rank = (metadata \ "rank").extract[Int]
      val userFeatures = sqlContext.parquetFile(userPath(path))
        .map { case Row(id: Int, features: Seq[_]) =>
          (id, features.asInstanceOf[Seq[Double]].toArray)
        }
      val productFeatures = sqlContext.parquetFile(productPath(path))
        .map { case Row(id: Int, features: Seq[_]) =>
        (id, features.asInstanceOf[Seq[Double]].toArray)
      }
      new MatrixFactorizationModel(rank, userFeatures, productFeatures)
    }

    private def userPath(path: String): String = {
      new Path(dataPath(path), "user").toUri.toString
    }

    private def productPath(path: String): String = {
      new Path(dataPath(path), "product").toUri.toString
    }
  }

}
