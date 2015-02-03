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

import scala.collection.mutable

import org.apache.spark.Logging
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.recommendation.{ALS => NewALS}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Out-link information for a user or product block. This includes the original user/product IDs
 * of the elements within this block, and the list of destination blocks that each user or
 * product will need to send its feature vector to.
 */
private[recommendation]
case class OutLinkBlock(elementIds: Array[Int], shouldSend: Array[mutable.BitSet])


/**
 * In-link information for a user (or product) block. This includes the original user/product IDs
 * of the elements within this block, as well as an array of indices and ratings that specify
 * which user in the block will be rated by which products from each product block (or vice-versa).
 * Specifically, if this InLinkBlock is for users, ratingsForBlock(b)(i) will contain two arrays,
 * indices and ratings, for the i'th product that will be sent to us by product block b (call this
 * P). These arrays represent the users that product P had ratings for (by their index in this
 * block), as well as the corresponding rating for each one. We can thus use this information when
 * we get product block b's message to update the corresponding users.
 */
private[recommendation] case class InLinkBlock(
  elementIds: Array[Int], ratingsForBlock: Array[Array[(Array[Int], Array[Double])]])


/**
 * :: Experimental ::
 * A more compact class to represent a rating than Tuple3[Int, Int, Double].
 */
@Experimental
case class Rating(user: Int, product: Int, rating: Double)

/**
 * Alternating Least Squares matrix factorization.
 *
 * ALS attempts to estimate the ratings matrix `R` as the product of two lower-rank matrices,
 * `X` and `Y`, i.e. `X * Yt = R`. Typically these approximations are called 'factor' matrices.
 * The general approach is iterative. During each iteration, one of the factor matrices is held
 * constant, while the other is solved for using least squares. The newly-solved factor matrix is
 * then held constant while solving for the other factor matrix.
 *
 * This is a blocked implementation of the ALS factorization algorithm that groups the two sets
 * of factors (referred to as "users" and "products") into blocks and reduces communication by only
 * sending one copy of each user vector to each product block on each iteration, and only for the
 * product blocks that need that user's feature vector. This is achieved by precomputing some
 * information about the ratings matrix to determine the "out-links" of each user (which blocks of
 * products it will contribute to) and "in-link" information for each product (which of the feature
 * vectors it receives from each user block it will depend on). This allows us to send only an
 * array of feature vectors between each user block and product block, and have the product block
 * find the users' ratings and update the products based on these messages.
 *
 * For implicit preference data, the algorithm used is based on
 * "Collaborative Filtering for Implicit Feedback Datasets", available at
 * [[http://dx.doi.org/10.1109/ICDM.2008.22]], adapted for the blocked approach used here.
 *
 * Essentially instead of finding the low-rank approximations to the rating matrix `R`,
 * this finds the approximations for a preference matrix `P` where the elements of `P` are 1 if
 * r > 0 and 0 if r <= 0. The ratings then act as 'confidence' values related to strength of
 * indicated user
 * preferences rather than explicit ratings given to items.
 */
class ALS private (
    private var numUserBlocks: Int,
    private var numProductBlocks: Int,
    private var rank: Int,
    private var iterations: Int,
    private var lambda: Double,
    private var implicitPrefs: Boolean,
    private var alpha: Double,
    private var seed: Long = System.nanoTime()
  ) extends Serializable with Logging {

  /**
   * Constructs an ALS instance with default parameters: {numBlocks: -1, rank: 10, iterations: 10,
   * lambda: 0.01, implicitPrefs: false, alpha: 1.0}.
   */
  def this() = this(-1, -1, 10, 10, 0.01, false, 1.0)

  /** If true, do alternating nonnegative least squares. */
  private var nonnegative = false

  /** storage level for user/product in/out links */
  private var intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  private var finalRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK

  /**
   * Set the number of blocks for both user blocks and product blocks to parallelize the computation
   * into; pass -1 for an auto-configured number of blocks. Default: -1.
   */
  def setBlocks(numBlocks: Int): this.type = {
    this.numUserBlocks = numBlocks
    this.numProductBlocks = numBlocks
    this
  }

  /**
   * Set the number of user blocks to parallelize the computation.
   */
  def setUserBlocks(numUserBlocks: Int): this.type = {
    this.numUserBlocks = numUserBlocks
    this
  }

  /**
   * Set the number of product blocks to parallelize the computation.
   */
  def setProductBlocks(numProductBlocks: Int): this.type = {
    this.numProductBlocks = numProductBlocks
    this
  }

  /** Set the rank of the feature matrices computed (number of features). Default: 10. */
  def setRank(rank: Int): this.type = {
    this.rank = rank
    this
  }

  /** Set the number of iterations to run. Default: 10. */
  def setIterations(iterations: Int): this.type = {
    this.iterations = iterations
    this
  }

  /** Set the regularization parameter, lambda. Default: 0.01. */
  def setLambda(lambda: Double): this.type = {
    this.lambda = lambda
    this
  }

  /** Sets whether to use implicit preference. Default: false. */
  def setImplicitPrefs(implicitPrefs: Boolean): this.type = {
    this.implicitPrefs = implicitPrefs
    this
  }

  /**
   * :: Experimental ::
   * Sets the constant used in computing confidence in implicit ALS. Default: 1.0.
   */
  @Experimental
  def setAlpha(alpha: Double): this.type = {
    this.alpha = alpha
    this
  }

  /** Sets a random seed to have deterministic results. */
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Set whether the least-squares problems solved at each iteration should have
   * nonnegativity constraints.
   */
  def setNonnegative(b: Boolean): this.type = {
    this.nonnegative = b
    this
  }

  /**
   * :: DeveloperApi ::
   * Sets storage level for intermediate RDDs (user/product in/out links). The default value is
   * `MEMORY_AND_DISK`. Users can change it to a serialized storage, e.g., `MEMORY_AND_DISK_SER` and
   * set `spark.rdd.compress` to `true` to reduce the space requirement, at the cost of speed.
   */
  @DeveloperApi
  def setIntermediateRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    require(storageLevel != StorageLevel.NONE,
      "ALS is not designed to run without persisting intermediate RDDs.")
    this.intermediateRDDStorageLevel = storageLevel
    this
  }

  /**
   * :: DeveloperApi ::
   * Sets storage level for final RDDs (user/product used in MatrixFactorizationModel). The default
   * value is `MEMORY_AND_DISK`. Users can change it to a serialized storage, e.g. 
   * `MEMORY_AND_DISK_SER` and set `spark.rdd.compress` to `true` to reduce the space requirement,
   * at the cost of speed.
   */
  @DeveloperApi
  def setFinalRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    this.finalRDDStorageLevel = storageLevel
    this
  }

  /**
   * Run ALS with the configured parameters on an input RDD of (user, product, rating) triples.
   * Returns a MatrixFactorizationModel with feature vectors for each user and product.
   */
  def run(ratings: RDD[Rating]): MatrixFactorizationModel = {
    val sc = ratings.context

    val numUserBlocks = if (this.numUserBlocks == -1) {
      math.max(sc.defaultParallelism, ratings.partitions.size / 2)
    } else {
      this.numUserBlocks
    }
    val numProductBlocks = if (this.numProductBlocks == -1) {
      math.max(sc.defaultParallelism, ratings.partitions.size / 2)
    } else {
      this.numProductBlocks
    }

    val (floatUserFactors, floatProdFactors) = NewALS.train[Int](
      ratings = ratings.map(r => NewALS.Rating(r.user, r.product, r.rating.toFloat)),
      rank = rank,
      numUserBlocks = numUserBlocks,
      numItemBlocks = numProductBlocks,
      maxIter = iterations,
      regParam = lambda,
      implicitPrefs = implicitPrefs,
      alpha = alpha,
      nonnegative = nonnegative,
      intermediateRDDStorageLevel = intermediateRDDStorageLevel,
      finalRDDStorageLevel = StorageLevel.NONE,
      seed = seed)

    val userFactors = floatUserFactors
      .mapValues(_.map(_.toDouble))
      .setName("users")
      .persist(finalRDDStorageLevel)
    val prodFactors = floatProdFactors
      .mapValues(_.map(_.toDouble))
      .setName("products")
      .persist(finalRDDStorageLevel)
    new MatrixFactorizationModel(rank, userFactors, prodFactors)
  }

  /**
   * Java-friendly version of [[ALS.run]].
   */
  def run(ratings: JavaRDD[Rating]): MatrixFactorizationModel = run(ratings.rdd)
}

/**
 * Top-level methods for calling Alternating Least Squares (ALS) matrix factorization.
 */
object ALS {
  /**
   * Train a matrix factorization model given an RDD of ratings given by users to some products,
   * in the form of (userID, productID, rating) pairs. We approximate the ratings matrix as the
   * product of two lower-rank matrices of a given rank (number of features). To solve for these
   * features, we run a given number of iterations of ALS. This is done using a level of
   * parallelism given by `blocks`.
   *
   * @param ratings    RDD of (userID, productID, rating) pairs
   * @param rank       number of features to use
   * @param iterations number of iterations of ALS (recommended: 10-20)
   * @param lambda     regularization factor (recommended: 0.01)
   * @param blocks     level of parallelism to split computation into
   * @param seed       random seed
   */
  def train(
      ratings: RDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int,
      seed: Long
    ): MatrixFactorizationModel = {
    new ALS(blocks, blocks, rank, iterations, lambda, false, 1.0, seed).run(ratings)
  }

  /**
   * Train a matrix factorization model given an RDD of ratings given by users to some products,
   * in the form of (userID, productID, rating) pairs. We approximate the ratings matrix as the
   * product of two lower-rank matrices of a given rank (number of features). To solve for these
   * features, we run a given number of iterations of ALS. This is done using a level of
   * parallelism given by `blocks`.
   *
   * @param ratings    RDD of (userID, productID, rating) pairs
   * @param rank       number of features to use
   * @param iterations number of iterations of ALS (recommended: 10-20)
   * @param lambda     regularization factor (recommended: 0.01)
   * @param blocks     level of parallelism to split computation into
   */
  def train(
      ratings: RDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int
    ): MatrixFactorizationModel = {
    new ALS(blocks, blocks, rank, iterations, lambda, false, 1.0).run(ratings)
  }

  /**
   * Train a matrix factorization model given an RDD of ratings given by users to some products,
   * in the form of (userID, productID, rating) pairs. We approximate the ratings matrix as the
   * product of two lower-rank matrices of a given rank (number of features). To solve for these
   * features, we run a given number of iterations of ALS. The level of parallelism is determined
   * automatically based on the number of partitions in `ratings`.
   *
   * @param ratings    RDD of (userID, productID, rating) pairs
   * @param rank       number of features to use
   * @param iterations number of iterations of ALS (recommended: 10-20)
   * @param lambda     regularization factor (recommended: 0.01)
   */
  def train(ratings: RDD[Rating], rank: Int, iterations: Int, lambda: Double)
    : MatrixFactorizationModel = {
    train(ratings, rank, iterations, lambda, -1)
  }

  /**
   * Train a matrix factorization model given an RDD of ratings given by users to some products,
   * in the form of (userID, productID, rating) pairs. We approximate the ratings matrix as the
   * product of two lower-rank matrices of a given rank (number of features). To solve for these
   * features, we run a given number of iterations of ALS. The level of parallelism is determined
   * automatically based on the number of partitions in `ratings`.
   *
   * @param ratings    RDD of (userID, productID, rating) pairs
   * @param rank       number of features to use
   * @param iterations number of iterations of ALS (recommended: 10-20)
   */
  def train(ratings: RDD[Rating], rank: Int, iterations: Int)
    : MatrixFactorizationModel = {
    train(ratings, rank, iterations, 0.01, -1)
  }

  /**
   * Train a matrix factorization model given an RDD of 'implicit preferences' given by users
   * to some products, in the form of (userID, productID, preference) pairs. We approximate the
   * ratings matrix as the product of two lower-rank matrices of a given rank (number of features).
   * To solve for these features, we run a given number of iterations of ALS. This is done using
   * a level of parallelism given by `blocks`.
   *
   * @param ratings    RDD of (userID, productID, rating) pairs
   * @param rank       number of features to use
   * @param iterations number of iterations of ALS (recommended: 10-20)
   * @param lambda     regularization factor (recommended: 0.01)
   * @param blocks     level of parallelism to split computation into
   * @param alpha      confidence parameter
   * @param seed       random seed
   */
  def trainImplicit(
      ratings: RDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int,
      alpha: Double,
      seed: Long
    ): MatrixFactorizationModel = {
    new ALS(blocks, blocks, rank, iterations, lambda, true, alpha, seed).run(ratings)
  }

  /**
   * Train a matrix factorization model given an RDD of 'implicit preferences' given by users
   * to some products, in the form of (userID, productID, preference) pairs. We approximate the
   * ratings matrix as the product of two lower-rank matrices of a given rank (number of features).
   * To solve for these features, we run a given number of iterations of ALS. This is done using
   * a level of parallelism given by `blocks`.
   *
   * @param ratings    RDD of (userID, productID, rating) pairs
   * @param rank       number of features to use
   * @param iterations number of iterations of ALS (recommended: 10-20)
   * @param lambda     regularization factor (recommended: 0.01)
   * @param blocks     level of parallelism to split computation into
   * @param alpha      confidence parameter
   */
  def trainImplicit(
      ratings: RDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int,
      alpha: Double
    ): MatrixFactorizationModel = {
    new ALS(blocks, blocks, rank, iterations, lambda, true, alpha).run(ratings)
  }

  /**
   * Train a matrix factorization model given an RDD of 'implicit preferences' given by users to
   * some products, in the form of (userID, productID, preference) pairs. We approximate the
   * ratings matrix as the product of two lower-rank matrices of a given rank (number of features).
   * To solve for these features, we run a given number of iterations of ALS. The level of
   * parallelism is determined automatically based on the number of partitions in `ratings`.
   *
   * @param ratings    RDD of (userID, productID, rating) pairs
   * @param rank       number of features to use
   * @param iterations number of iterations of ALS (recommended: 10-20)
   * @param lambda     regularization factor (recommended: 0.01)
   * @param alpha      confidence parameter
   */
  def trainImplicit(ratings: RDD[Rating], rank: Int, iterations: Int, lambda: Double, alpha: Double)
    : MatrixFactorizationModel = {
    trainImplicit(ratings, rank, iterations, lambda, -1, alpha)
  }

  /**
   * Train a matrix factorization model given an RDD of 'implicit preferences' ratings given by
   * users to some products, in the form of (userID, productID, rating) pairs. We approximate the
   * ratings matrix as the product of two lower-rank matrices of a given rank (number of features).
   * To solve for these features, we run a given number of iterations of ALS. The level of
   * parallelism is determined automatically based on the number of partitions in `ratings`.
   * Model parameters `alpha` and `lambda` are set to reasonable default values
   *
   * @param ratings    RDD of (userID, productID, rating) pairs
   * @param rank       number of features to use
   * @param iterations number of iterations of ALS (recommended: 10-20)
   */
  def trainImplicit(ratings: RDD[Rating], rank: Int, iterations: Int)
    : MatrixFactorizationModel = {
    trainImplicit(ratings, rank, iterations, 0.01, -1, 1.0)
  }

  /**
   * :: DeveloperApi ::
   * Statistics of a block in ALS computation.
   *
   * @param category type of this block, "user" or "product"
   * @param index index of this block
   * @param count number of users or products inside this block, the same as the number of
   *              least-squares problems to solve on this block in each iteration
   * @param numRatings total number of ratings inside this block, the same as the number of outer
   *                   products we need to make on this block in each iteration
   * @param numInLinks total number of incoming links, the same as the number of vectors to retrieve
   *                   before each iteration
   * @param numOutLinks total number of outgoing links, the same as the number of vectors to send
   *                    for the next iteration
   */
  @DeveloperApi
  case class BlockStats(
      category: String,
      index: Int,
      count: Long,
      numRatings: Long,
      numInLinks: Long,
      numOutLinks: Long)

  /**
   * :: DeveloperApi ::
   * Given an RDD of ratings, number of user blocks, and number of product blocks, computes the
   * statistics of each block in ALS computation. This is useful for estimating cost and diagnosing
   * load balance.
   *
   * @param ratings an RDD of ratings
   * @param numUserBlocks number of user blocks
   * @param numProductBlocks number of product blocks
   * @return statistics of user blocks and product blocks
   */
  @DeveloperApi
  def analyzeBlocks(
      ratings: RDD[Rating],
      numUserBlocks: Int,
      numProductBlocks: Int): Array[BlockStats] = {

    val userPartitioner = new ALSPartitioner(numUserBlocks)
    val productPartitioner = new ALSPartitioner(numProductBlocks)

    val ratingsByUserBlock = ratings.map { rating =>
      (userPartitioner.getPartition(rating.user), rating)
    }
    val ratingsByProductBlock = ratings.map { rating =>
      (productPartitioner.getPartition(rating.product),
        Rating(rating.product, rating.user, rating.rating))
    }

    val als = new ALS()
    val (userIn, userOut) =
      als.makeLinkRDDs(numUserBlocks, numProductBlocks, ratingsByUserBlock, userPartitioner)
    val (prodIn, prodOut) =
      als.makeLinkRDDs(numProductBlocks, numUserBlocks, ratingsByProductBlock, productPartitioner)

    def sendGrid(outLinks: RDD[(Int, OutLinkBlock)]): Map[(Int, Int), Long] = {
      outLinks.map { x =>
        val grid = new mutable.HashMap[(Int, Int), Long]()
        val uPartition = x._1
        x._2.shouldSend.foreach { ss =>
          ss.foreach { pPartition =>
            val pair = (uPartition, pPartition)
            grid.put(pair, grid.getOrElse(pair, 0L) + 1L)
          }
        }
        grid
      }.reduce { (grid1, grid2) =>
        grid2.foreach { x =>
          grid1.put(x._1, grid1.getOrElse(x._1, 0L) + x._2)
        }
        grid1
      }.toMap
    }

    val userSendGrid = sendGrid(userOut)
    val prodSendGrid = sendGrid(prodOut)

    val userInbound = new Array[Long](numUserBlocks)
    val prodInbound = new Array[Long](numProductBlocks)
    val userOutbound = new Array[Long](numUserBlocks)
    val prodOutbound = new Array[Long](numProductBlocks)

    for (u <- 0 until numUserBlocks; p <- 0 until numProductBlocks) {
      userOutbound(u) += userSendGrid.getOrElse((u, p), 0L)
      prodInbound(p) += userSendGrid.getOrElse((u, p), 0L)
      userInbound(u) += prodSendGrid.getOrElse((p, u), 0L)
      prodOutbound(p) += prodSendGrid.getOrElse((p, u), 0L)
    }

    val userCounts = userOut.mapValues(x => x.elementIds.length).collectAsMap()
    val prodCounts = prodOut.mapValues(x => x.elementIds.length).collectAsMap()

    val userRatings = countRatings(userIn)
    val prodRatings = countRatings(prodIn)

    val userStats = Array.tabulate(numUserBlocks)(
      u => BlockStats("user", u, userCounts(u), userRatings(u), userInbound(u), userOutbound(u)))
    val productStatus = Array.tabulate(numProductBlocks)(
      p => BlockStats("product", p, prodCounts(p), prodRatings(p), prodInbound(p), prodOutbound(p)))

    (userStats ++ productStatus).toArray
  }

  private def countRatings(inLinks: RDD[(Int, InLinkBlock)]): Map[Int, Long] = {
    inLinks.mapValues { ilb =>
      var numRatings = 0L
      ilb.ratingsForBlock.foreach { ar =>
        ar.foreach { p => numRatings += p._1.length }
      }
      numRatings
    }.collectAsMap().toMap
  }
}
