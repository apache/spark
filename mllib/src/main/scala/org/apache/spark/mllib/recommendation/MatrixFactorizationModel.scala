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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.google.common.collect.{Ordering => GuavaOrdering}
import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.mllib.linalg.BLAS
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * Model representing the result of matrix factorization.
 *
 * @param rank Rank for the features in this model.
 * @param userFeatures RDD of tuples where each tuple represents the userId and
 *                     the features computed for this user.
 * @param productFeatures RDD of tuples where each tuple represents the productId
 *                        and the features computed for this product.
 *
 * @note If you create the model directly using constructor, please be aware that fast prediction
 * requires cached user/product features and their associated partitioners.
 */
@Since("0.8.0")
class MatrixFactorizationModel @Since("0.8.0") (
    @Since("0.8.0") val rank: Int,
    @Since("0.8.0") val userFeatures: RDD[(Int, Array[Double])],
    @Since("0.8.0") val productFeatures: RDD[(Int, Array[Double])])
  extends Saveable with Serializable with Logging {

  require(rank > 0)
  validateFeatures("User", userFeatures)
  validateFeatures("Product", productFeatures)

  /** Validates factors and warns users if there are performance concerns. */
  private def validateFeatures(name: String, features: RDD[(Int, Array[Double])]): Unit = {
    require(features.first()._2.length == rank,
      s"$name feature dimension does not match the rank $rank.")
    if (features.partitioner.isEmpty) {
      logWarning(log"${MDC(LogKeys.FEATURE_NAME, name)} factor does not have a partitioner. " +
        log"Prediction on individual records could be slow.")
    }
    if (features.getStorageLevel == StorageLevel.NONE) {
      logWarning(log"${MDC(LogKeys.FEATURE_NAME, name)} factor is not cached. " +
        log"Prediction could be slow.")
    }
  }

  /** Predict the rating of one user for one product. */
  @Since("0.8.0")
  def predict(user: Int, product: Int): Double = {
    val userFeatureSeq = userFeatures.lookup(user)
    require(userFeatureSeq.nonEmpty, s"userId: $user not found in the model")
    val productFeatureSeq = productFeatures.lookup(product)
    require(productFeatureSeq.nonEmpty, s"productId: $product not found in the model")

    val userVector = userFeatureSeq.head
    val productVector = productFeatureSeq.head
    BLAS.nativeBLAS.ddot(rank, userVector, 1, productVector, 1)
  }

  /**
   * Return approximate numbers of users and products in the given usersProducts tuples.
   * This method is based on `countApproxDistinct` in class `RDD`.
   *
   * @param usersProducts  RDD of (user, product) pairs.
   * @return approximate numbers of users and products.
   */
  private[this] def countApproxDistinctUserProduct(usersProducts: RDD[(Int, Int)]): (Long, Long) = {
    val zeroCounterUser = new HyperLogLogPlus(4, 0)
    val zeroCounterProduct = new HyperLogLogPlus(4, 0)
    val aggregated = usersProducts.aggregate((zeroCounterUser, zeroCounterProduct))(
      (hllTuple: (HyperLogLogPlus, HyperLogLogPlus), v: (Int, Int)) => {
        hllTuple._1.offer(v._1)
        hllTuple._2.offer(v._2)
        hllTuple
      },
      (h1: (HyperLogLogPlus, HyperLogLogPlus), h2: (HyperLogLogPlus, HyperLogLogPlus)) => {
        h1._1.addAll(h2._1)
        h1._2.addAll(h2._2)
        h1
      })
    (aggregated._1.cardinality(), aggregated._2.cardinality())
  }

  /**
   * Predict the rating of many users for many products.
   * The output RDD has an element per each element in the input RDD (including all duplicates)
   * unless a user or product is missing in the training set.
   *
   * @param usersProducts  RDD of (user, product) pairs.
   * @return RDD of Ratings.
   */
  @Since("0.9.0")
  def predict(usersProducts: RDD[(Int, Int)]): RDD[Rating] = {
    // Previously the partitions of ratings are only based on the given products.
    // So if the usersProducts given for prediction contains only few products or
    // even one product, the generated ratings will be pushed into few or single partition
    // and can't use high parallelism.
    // Here we calculate approximate numbers of users and products. Then we decide the
    // partitions should be based on users or products.
    val (usersCount, productsCount) = countApproxDistinctUserProduct(usersProducts)

    if (usersCount < productsCount) {
      val users = userFeatures.join(usersProducts).map {
        case (user, (uFeatures, product)) => (product, (user, uFeatures))
      }
      users.join(productFeatures).map {
        case (product, ((user, uFeatures), pFeatures)) =>
          Rating(user, product, BLAS.nativeBLAS.ddot(uFeatures.length, uFeatures, 1, pFeatures, 1))
      }
    } else {
      val products = productFeatures.join(usersProducts.map(_.swap)).map {
        case (product, (pFeatures, user)) => (user, (product, pFeatures))
      }
      products.join(userFeatures).map {
        case (user, ((product, pFeatures), uFeatures)) =>
          Rating(user, product, BLAS.nativeBLAS.ddot(uFeatures.length, uFeatures, 1, pFeatures, 1))
      }
    }
  }

  /**
   * Java-friendly version of `MatrixFactorizationModel.predict`.
   */
  @Since("1.2.0")
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
  @Since("1.1.0")
  def recommendProducts(user: Int, num: Int): Array[Rating] = {
    val userFeatureSeq = userFeatures.lookup(user)
    require(userFeatureSeq.nonEmpty, s"userId: $user not found in the model")
    MatrixFactorizationModel.recommend(userFeatureSeq.head, productFeatures, num)
      .map(t => Rating(user, t._1, t._2))
  }

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
  @Since("1.1.0")
  def recommendUsers(product: Int, num: Int): Array[Rating] = {
    val productFeatureSeq = productFeatures.lookup(product)
    require(productFeatureSeq.nonEmpty, s"productId: $product not found in the model")
    MatrixFactorizationModel.recommend(productFeatureSeq.head, userFeatures, num)
      .map(t => Rating(t._1, product, t._2))
  }

  /**
   * Save this model to the given path.
   *
   * This saves:
   *  - human-readable (JSON) model metadata to path/metadata/
   *  - Parquet formatted data to path/data/
   *
   * The model may be loaded using `Loader.load`.
   *
   * @param sc  Spark context used to save model data.
   * @param path  Path specifying the directory in which to save this model.
   *              If the directory already exists, this method throws an exception.
   */
  @Since("1.3.0")
  override def save(sc: SparkContext, path: String): Unit = {
    MatrixFactorizationModel.SaveLoadV1_0.save(this, path)
  }

  /**
   * Recommends top products for all users.
   *
   * @param num how many products to return for every user.
   * @return [(Int, Array[Rating])] objects, where every tuple contains a userID and an array of
   * rating objects which contains the same userId, recommended productID and a "score" in the
   * rating field. Semantics of score is same as recommendProducts API
   */
  @Since("1.4.0")
  def recommendProductsForUsers(num: Int): RDD[(Int, Array[Rating])] = {
    MatrixFactorizationModel.recommendForAll(rank, userFeatures, productFeatures, num).map {
      case (user, top) =>
        val ratings = top.map { case (product, rating) => Rating(user, product, rating) }
        (user, ratings)
    }
  }


  /**
   * Recommends top users for all products.
   *
   * @param num how many users to return for every product.
   * @return [(Int, Array[Rating])] objects, where every tuple contains a productID and an array
   * of rating objects which contains the recommended userId, same productID and a "score" in the
   * rating field. Semantics of score is same as recommendUsers API
   */
  @Since("1.4.0")
  def recommendUsersForProducts(num: Int): RDD[(Int, Array[Rating])] = {
    MatrixFactorizationModel.recommendForAll(rank, productFeatures, userFeatures, num).map {
      case (product, top) =>
        val ratings = top.map { case (user, rating) => Rating(user, product, rating) }
        (product, ratings)
    }
  }
}

@Since("1.3.0")
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
      (id, BLAS.nativeBLAS.ddot(features.length, recommendToFeatures, 1, features, 1))
    }
    scored.top(num)(Ordering.by(_._2))
  }

  /**
   * Makes recommendations for all users (or products).
   *
   * Note: the previous approach used for computing top-k recommendations aimed to group
   * individual factor vectors into blocks, so that Level 3 BLAS operations (gemm) could
   * be used for efficiency. However, this causes excessive GC pressure due to the large
   * arrays required for intermediate result storage, as well as a high sensitivity to the
   * block size used.
   *
   * This approach groups factors into blocks and computes the top-k elements per block,
   * using GEMV (it use less memory compared with GEMM, and is much faster than DOT) and
   * an efficient selection based on [[GuavaOrdering]] (instead of [[BoundedPriorityQueue]]).
   * It then computes the global top-k by aggregating the per block top-k elements with
   * a [[BoundedPriorityQueue]]. This significantly reduces the size of intermediate and
   * shuffle data.
   *
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
    import scala.jdk.CollectionConverters._
    val srcBlocks = blockify(srcFeatures)
    val dstBlocks = blockify(dstFeatures)

    val ratings = srcBlocks.cartesian(dstBlocks)
      .mapPartitions { iter =>
        var scores: Array[Double] = null
        var idxOrd: GuavaOrdering[Int] = null
        iter.flatMap { case ((srcIds, srcMat), (dstIds, dstMat)) =>
          require(srcMat.length == srcIds.length * rank)
          require(dstMat.length == dstIds.length * rank)
          val m = srcIds.length
          val n = dstIds.length
          if (scores == null || scores.length < n) {
            scores = Array.ofDim[Double](n)
            idxOrd = new GuavaOrdering[Int] {
              override def compare(left: Int, right: Int): Int = {
                Ordering[Double].compare(scores(left), scores(right))
              }
            }
          }

          Iterator.range(0, m).flatMap { i =>
            // scores = i-th vec in srcMat * dstMat
            BLAS.javaBLAS.dgemv("T", rank, n, 1.0F, dstMat, 0, rank,
              srcMat, i * rank, 1, 0.0F, scores, 0, 1)

            val srcId = srcIds(i)
            idxOrd.greatestOf(Iterator.range(0, n).asJava, num).asScala
              .iterator.map { j => (srcId, (dstIds(j), scores(j))) }
          }
        }
      }

    ratings.topByKey(num)(Ordering.by(_._2))
  }

  /**
   * Blockifies features to improve the efficiency of cartesian product
   * TODO: SPARK-20443 - expose blockSize as a param?
   */
  private def blockify(
      features: RDD[(Int, Array[Double])],
      blockSize: Int = 4096): RDD[(Array[Int], Array[Double])] = {
    features.mapPartitions { iter =>
      iter.grouped(blockSize)
        .map(block => (block.map(_._1).toArray, block.flatMap(_._2).toArray))
    }
  }

  /**
   * Load a model from the given path.
   *
   * The model should have been saved by `Saveable.save`.
   *
   * @param sc  Spark context used for loading model files.
   * @param path  Path specifying the directory to which the model was saved.
   * @return  Model instance
   */
  @Since("1.3.0")
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
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      import spark.implicits._
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~ ("rank" -> model.rank)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(metadataPath(path))
      model.userFeatures.toDF("id", "features").write.parquet(userPath(path))
      model.productFeatures.toDF("id", "features").write.parquet(productPath(path))
    }

    def load(sc: SparkContext, path: String): MatrixFactorizationModel = {
      implicit val formats: Formats = DefaultFormats
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val (className, formatVersion, metadata) = loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)
      val rank = (metadata \ "rank").extract[Int]
      val userFeatures = spark.read.parquet(userPath(path)).rdd.map {
        case Row(id: Int, features: scala.collection.Seq[_]) =>
          (id, features.asInstanceOf[scala.collection.Seq[Double]].toArray)
      }
      val productFeatures = spark.read.parquet(productPath(path)).rdd.map {
        case Row(id: Int, features: scala.collection.Seq[_]) =>
          (id, features.asInstanceOf[scala.collection.Seq[Double]].toArray)
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
