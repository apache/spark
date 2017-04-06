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

package org.apache.spark.ml.recommendation

import java.io.IOException
import java.util.Arrays

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag
import scala.util.hashing.byteswap64

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.hadoop.fs.Path

import org.apache.spark.{Dependency, Partitioner, ShuffleDependency, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

/**
 * Alternating Least Squares (ALS) matrix factorization.
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
 * product blocks that need that user's feature vector. This is achieved by pre-computing some
 * information about the ratings matrix to determine the "out-links" of each user (which blocks of
 * products it will contribute to) and "in-link" information for each product (which of the feature
 * vectors it receives from each user block it will depend on). This allows us to send only an
 * array of feature vectors between each user block and product block, and have the product block
 * find the users' ratings and update the products based on these messages.
 *
 * For implicit preference data, the algorithm used is based on
 * "Collaborative Filtering for Implicit Feedback Datasets", available at
 * http://dx.doi.org/10.1109/ICDM.2008.22, adapted for the blocked approach used here.
 *
 * Essentially instead of finding the low-rank approximations to the rating matrix `R`,
 * this finds the approximations for a preference matrix `P` where the elements of `P` are 1 if
 * r is greater than 0 and 0 if r is less than or equal to 0. The ratings then act as 'confidence'
 * values related to strength of indicated user
 * preferences rather than explicit ratings given to items.
 */
@Since("1.3.0")
class ALS(@Since("1.4.0") override val uid: String) extends Estimator[ALSModel] with ALSParams
  with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("als"))

  /** @group setParam */
  @Since("1.3.0")
  def setRank(value: Int): this.type = set(rank, value)

  /** @group setParam */
  @Since("1.3.0")
  def setNumUserBlocks(value: Int): this.type = set(numUserBlocks, value)

  /** @group setParam */
  @Since("1.3.0")
  def setNumItemBlocks(value: Int): this.type = set(numItemBlocks, value)

  /** @group setParam */
  @Since("1.3.0")
  def setImplicitPrefs(value: Boolean): this.type = set(implicitPrefs, value)

  /** @group setParam */
  @Since("1.3.0")
  def setAlpha(value: Double): this.type = set(alpha, value)

  /** @group setParam */
  @Since("1.3.0")
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.3.0")
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** @group setParam */
  @Since("1.3.0")
  def setNonnegative(value: Boolean): this.type = set(nonnegative, value)

  /** @group setParam */
  @Since("1.4.0")
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("1.3.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group expertSetParam */
  @Since("2.0.0")
  def setIntermediateStorageLevel(value: String): this.type = set(intermediateStorageLevel, value)

  /** @group expertSetParam */
  @Since("2.0.0")
  def setFinalStorageLevel(value: String): this.type = set(finalStorageLevel, value)

  /** @group expertSetParam */
  @Since("2.2.0")
  def setColdStartStrategy(value: String): this.type = set(coldStartStrategy, value)

  /**
   * Sets both numUserBlocks and numItemBlocks to the specific value.
   *
   * @group setParam
   */
  @Since("1.3.0")
  def setNumBlocks(value: Int): this.type = {
    setNumUserBlocks(value)
    setNumItemBlocks(value)
    this
  }

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): ALSModel = {
    transformSchema(dataset.schema)
    import dataset.sparkSession.implicits._

    val r = if ($(ratingCol) != "") col($(ratingCol)).cast(FloatType) else lit(1.0f)
    val ratings = dataset
      .select(checkedCast(col($(userCol))), checkedCast(col($(itemCol))), r)
      .rdd
      .map { row =>
        Rating(row.getInt(0), row.getInt(1), row.getFloat(2))
      }

    val instr = Instrumentation.create(this, ratings)
    instr.logParams(rank, numUserBlocks, numItemBlocks, implicitPrefs, alpha, userCol,
      itemCol, ratingCol, predictionCol, maxIter, regParam, nonnegative, checkpointInterval,
      seed, intermediateStorageLevel, finalStorageLevel)

    val (userFactors, itemFactors) = ALS.train(
      ratings,
      rank = $(rank),
      numUserBlocks = $(numUserBlocks),
      numItemBlocks = $(numItemBlocks),
      maxIter = $(maxIter),
      regParam = $(regParam),
      implicitPrefs = $(implicitPrefs),
      alpha = $(alpha),
      nonnegative = $(nonnegative),
      intermediateRDDStorageLevel = StorageLevel.fromString($(intermediateStorageLevel)),
      finalRDDStorageLevel = StorageLevel.fromString($(finalStorageLevel)),
      checkpointInterval = $(checkpointInterval),
      seed = $(seed)
    )

    val userDF = userFactors.toDF("id", "features")
    val itemDF = itemFactors.toDF("id", "features")
    val model = new ALSModel(uid, $(rank), userDF, itemDF).setParent(this)
    instr.logSuccess(model)
    copyValues(model)
  }

  @Since("1.3.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): ALS = defaultCopy(extra)
}


/**
 * :: DeveloperApi ::
 * An implementation of ALS that supports generic ID types, specialized for Int and Long. This is
 * exposed as a developer API for users who do need other ID types. But it is not recommended
 * because it increases the shuffle size and memory requirement during training. For simplicity,
 * users and items must have the same type. The number of distinct users/items should be smaller
 * than 2 billion.
 */
@DeveloperApi
object ALS extends DefaultParamsReadable[ALS] with Logging with Solvers {

  @Since("1.6.0")
  override def load(path: String): ALS = super.load(path)

  /**
   * :: DeveloperApi ::
   * Implementation of the ALS algorithm.
   */
  @DeveloperApi
  def train[ID: ClassTag]( // scalastyle:ignore
      ratings: RDD[Rating[ID]],
      rank: Int = 10,
      numUserBlocks: Int = 10,
      numItemBlocks: Int = 10,
      maxIter: Int = 10,
      regParam: Double = 0.1,
      implicitPrefs: Boolean = false,
      alpha: Double = 1.0,
      nonnegative: Boolean = false,
      intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
      finalRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
      checkpointInterval: Int = 10,
      seed: Long = 0L)(
      implicit ord: Ordering[ID]): (RDD[(ID, Array[Float])], RDD[(ID, Array[Float])]) = {
    require(!ratings.isEmpty(), s"No ratings available from $ratings")
    require(intermediateRDDStorageLevel != StorageLevel.NONE,
      "ALS is not designed to run without persisting intermediate RDDs.")
    val sc = ratings.sparkContext
    val ratingBlocks = RatingBlocks.create(
      ratings = ratings,
      numUserBlocks = numUserBlocks,
      numItemBlocks = numItemBlocks,
      storageLevel = intermediateRDDStorageLevel)
    val userLocalIndexEncoder = new LocalIndexEncoder(numUserBlocks)
    val itemLocalIndexEncoder = new LocalIndexEncoder(numItemBlocks)
    val solver = if (nonnegative) new NNLSSolver else new CholeskySolver
    val seedGen = new XORShiftRandom(seed)
    var userFactors = initialize(ratingBlocks.userIn, rank, seedGen.nextLong())
    var itemFactors = initialize(ratingBlocks.itemIn, rank, seedGen.nextLong())
    var previousCheckpointFile: Option[String] = None
    val shouldCheckpoint: Int => Boolean = (iter) =>
      sc.checkpointDir.isDefined && checkpointInterval != -1 && (iter % checkpointInterval == 0)
    val deletePreviousCheckpointFile: () => Unit = () =>
      previousCheckpointFile.foreach { file =>
        try {
          val checkpointFile = new Path(file)
          checkpointFile.getFileSystem(sc.hadoopConfiguration).delete(checkpointFile, true)
        } catch {
          case e: IOException =>
            logWarning(s"Cannot delete checkpoint file $file:", e)
        }
      }
    if (implicitPrefs) {
      for (iter <- 1 to maxIter) {
        userFactors.setName(s"userFactors-$iter").persist(intermediateRDDStorageLevel)
        val previousItemFactors = itemFactors
        itemFactors = computeFactors(userFactors, ratingBlocks.userOut, ratingBlocks.itemIn,
          rank, regParam, userLocalIndexEncoder, implicitPrefs, alpha, solver)
        previousItemFactors.unpersist()
        itemFactors.setName(s"itemFactors-$iter").persist(intermediateRDDStorageLevel)
        // TODO: Generalize PeriodicGraphCheckpointer and use it here.
        val deps = itemFactors.dependencies
        if (shouldCheckpoint(iter)) {
          itemFactors.checkpoint() // itemFactors gets materialized in computeFactors
        }
        val previousUserFactors = userFactors
        userFactors = computeFactors(itemFactors, ratingBlocks.itemOut, ratingBlocks.userIn,
          rank, regParam, itemLocalIndexEncoder, implicitPrefs, alpha, solver)
        if (shouldCheckpoint(iter)) {
          ALS.cleanShuffleDependencies(sc, deps)
          deletePreviousCheckpointFile()
          previousCheckpointFile = itemFactors.getCheckpointFile
        }
        previousUserFactors.unpersist()
      }
    } else {
      for (iter <- 0 until maxIter) {
        itemFactors = computeFactors(userFactors, ratingBlocks.userOut, ratingBlocks.itemIn,
          rank, regParam, userLocalIndexEncoder, solver = solver)
        if (shouldCheckpoint(iter)) {
          val deps = itemFactors.dependencies
          itemFactors.checkpoint()
          itemFactors.count() // checkpoint item factors and cut lineage
          ALS.cleanShuffleDependencies(sc, deps)
          deletePreviousCheckpointFile()
          previousCheckpointFile = itemFactors.getCheckpointFile
        }
        userFactors = computeFactors(itemFactors, ratingBlocks.itemOut, ratingBlocks.userIn,
          rank, regParam, itemLocalIndexEncoder, solver = solver)
      }
    }
    val userIdAndFactors = ratingBlocks.userIn
      .mapValues(_.srcIds)
      .join(userFactors)
      .mapPartitions({ items =>
        items.flatMap { case (_, (ids, factors)) =>
          ids.view.zip(factors)
        }
      // Preserve the partitioning because IDs are consistent with the partitioners in
      // ratingBlocks.userIn and userFactors.
      }, preservesPartitioning = true)
      .setName("userFactors")
      .persist(finalRDDStorageLevel)
    val itemIdAndFactors = ratingBlocks.itemIn
      .mapValues(_.srcIds)
      .join(itemFactors)
      .mapPartitions({ items =>
        items.flatMap { case (_, (ids, factors)) =>
          ids.view.zip(factors)
        }
      }, preservesPartitioning = true)
      .setName("itemFactors")
      .persist(finalRDDStorageLevel)
    if (finalRDDStorageLevel != StorageLevel.NONE) {
      userIdAndFactors.count()
      itemFactors.unpersist()
      itemIdAndFactors.count()
      ratingBlocks.userIn.unpersist()
      ratingBlocks.userOut.unpersist()
      ratingBlocks.itemIn.unpersist()
      ratingBlocks.itemOut.unpersist()
    }
    (userIdAndFactors, itemIdAndFactors)
  }

  /**
   * Initializes factors randomly given the in-link blocks.
   *
   * @param inBlocks in-link blocks
   * @param rank rank
   * @return initialized factor blocks
   */
  private[this] def initialize[ID](
      inBlocks: RDD[(Int, InBlock[ID])],
      rank: Int,
      seed: Long): RDD[(Int, FactorBlock)] = {
    // Choose a unit vector uniformly at random from the unit sphere, but from the
    // "first quadrant" where all elements are nonnegative. This can be done by choosing
    // elements distributed as Normal(0,1) and taking the absolute value, and then normalizing.
    // This appears to create factorizations that have a slightly better reconstruction
    // (<1%) compared picking elements uniformly at random in [0,1].
    inBlocks.map { case (srcBlockId, inBlock) =>
      val random = new XORShiftRandom(byteswap64(seed ^ srcBlockId))
      val factors = Array.fill(inBlock.srcIds.length) {
        val factor = Array.fill(rank)(random.nextGaussian().toFloat)
        val nrm = blas.snrm2(rank, factor, 1)
        blas.sscal(rank, 1.0f / nrm, factor, 1)
        factor
      }
      (srcBlockId, factors)
    }
  }

  /**
   * Compute dst factors by constructing and solving least square problems.
   *
   * @param srcFactorBlocks src factors
   * @param srcOutBlocks src out-blocks
   * @param dstInBlocks dst in-blocks
   * @param rank rank
   * @param regParam regularization constant
   * @param srcEncoder encoder for src local indices
   * @param implicitPrefs whether to use implicit preference
   * @param alpha the alpha constant in the implicit preference formulation
   * @param solver solver for least squares problems
   * @return dst factors
   */
  private[this] def computeFactors[ID](
      srcFactorBlocks: RDD[(Int, FactorBlock)],
      srcOutBlocks: RDD[(Int, OutBlock)],
      dstInBlocks: RDD[(Int, InBlock[ID])],
      rank: Int,
      regParam: Double,
      srcEncoder: LocalIndexEncoder,
      implicitPrefs: Boolean = false,
      alpha: Double = 1.0,
      solver: LeastSquaresNESolver): RDD[(Int, FactorBlock)] = {
    val numSrcBlocks = srcFactorBlocks.partitions.length
    val YtY = if (implicitPrefs) Some(computeYtY(srcFactorBlocks, rank)) else None
    val srcOut = srcOutBlocks.join(srcFactorBlocks).flatMap {
      case (srcBlockId, (srcOutBlock, srcFactors)) =>
        srcOutBlock.view.zipWithIndex.map { case (activeIndices, dstBlockId) =>
          (dstBlockId, (srcBlockId, activeIndices.map(idx => srcFactors(idx))))
        }
    }
    val merged = srcOut.groupByKey(new ALSPartitioner(dstInBlocks.partitions.length))
    dstInBlocks.join(merged).mapValues {
      case (InBlock(dstIds, srcPtrs, srcEncodedIndices, ratings), srcFactors) =>
        val sortedSrcFactors = new Array[FactorBlock](numSrcBlocks)
        srcFactors.foreach { case (srcBlockId, factors) =>
          sortedSrcFactors(srcBlockId) = factors
        }
        val dstFactors = new Array[Array[Float]](dstIds.length)
        var j = 0
        val ls = new NormalEquation(rank)
        while (j < dstIds.length) {
          ls.reset()
          if (implicitPrefs) {
            ls.merge(YtY.get)
          }
          var i = srcPtrs(j)
          var numExplicits = 0
          while (i < srcPtrs(j + 1)) {
            val encoded = srcEncodedIndices(i)
            val blockId = srcEncoder.blockId(encoded)
            val localIndex = srcEncoder.localIndex(encoded)
            val srcFactor = sortedSrcFactors(blockId)(localIndex)
            val rating = ratings(i)
            if (implicitPrefs) {
              // Extension to the original paper to handle b < 0. confidence is a function of |b|
              // instead so that it is never negative. c1 is confidence - 1.0.
              val c1 = alpha * math.abs(rating)
              // For rating <= 0, the corresponding preference is 0. So the term below is only added
              // for rating > 0. Because YtY is already added, we need to adjust the scaling here.
              if (rating > 0) {
                numExplicits += 1
                ls.add(srcFactor, (c1 + 1.0) / c1, c1)
              }
            } else {
              ls.add(srcFactor, rating)
              numExplicits += 1
            }
            i += 1
          }
          // Weight lambda by the number of explicit ratings based on the ALS-WR paper.
          dstFactors(j) = solver.solve(ls, numExplicits * regParam)
          j += 1
        }
        dstFactors
    }
  }

  /**
   * Computes the Gramian matrix of user or item factors, which is only used in implicit preference.
   * Caching of the input factors is handled in [[ALS#train]].
   */
  private[this] def computeYtY(factorBlocks: RDD[(Int, FactorBlock)], rank: Int): NormalEquation = {
    factorBlocks.values.aggregate(new NormalEquation(rank))(
      seqOp = (ne, factors) => {
        factors.foreach(ne.add(_, 0.0))
        ne
      },
      combOp = (ne1, ne2) => ne1.merge(ne2))
  }

  /**
   * Private function to clean up all of the shuffles files from the dependencies and their parents.
   */
  private[recommendation] def cleanShuffleDependencies[T](
      sc: SparkContext,
      deps: Seq[Dependency[_]],
      blocking: Boolean = false): Unit = {
    // If there is no reference tracking we skip clean up.
    sc.cleaner.foreach { cleaner =>
      /**
       * Clean the shuffles & all of its parents.
       */
      def cleanEagerly(dep: Dependency[_]): Unit = {
        if (dep.isInstanceOf[ShuffleDependency[_, _, _]]) {
          val shuffleId = dep.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
          cleaner.doCleanupShuffle(shuffleId, blocking)
        }
        val rdd = dep.rdd
        val rddDeps = rdd.dependencies
        if (rdd.getStorageLevel == StorageLevel.NONE && rddDeps != null) {
          rddDeps.foreach(cleanEagerly)
        }
      }
      deps.foreach(cleanEagerly)
    }
  }
}
