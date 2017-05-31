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

import java.{util => ju}
import java.io.IOException
import java.util.Locale

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Sorting, Try}
import scala.util.hashing.byteswap64

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._

import org.apache.spark.{Dependency, Partitioner, ShuffleDependency, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.CholeskyDecomposition
import org.apache.spark.mllib.optimization.NNLS
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{BoundedPriorityQueue, Utils}
import org.apache.spark.util.collection.{OpenHashMap, OpenHashSet, SortDataFormat, Sorter}
import org.apache.spark.util.random.XORShiftRandom

/**
 * Common params for ALS and ALSModel.
 */
private[recommendation] trait ALSModelParams extends Params with HasPredictionCol {
  /**
   * Param for the column name for user ids. Ids must be integers. Other
   * numeric types are supported for this column, but will be cast to integers as long as they
   * fall within the integer value range.
   * Default: "user"
   * @group param
   */
  val userCol = new Param[String](this, "userCol", "column name for user ids. Ids must be within " +
    "the integer value range.")

  /** @group getParam */
  def getUserCol: String = $(userCol)

  /**
   * Param for the column name for item ids. Ids must be integers. Other
   * numeric types are supported for this column, but will be cast to integers as long as they
   * fall within the integer value range.
   * Default: "item"
   * @group param
   */
  val itemCol = new Param[String](this, "itemCol", "column name for item ids. Ids must be within " +
    "the integer value range.")

  /** @group getParam */
  def getItemCol: String = $(itemCol)

  /**
   * Attempts to safely cast a user/item id to an Int. Throws an exception if the value is
   * out of integer range or contains a fractional part.
   */
  protected[recommendation] val checkedCast = udf { (n: Any) =>
    n match {
      case v: Int => v // Avoid unnecessary casting
      case v: Number =>
        val intV = v.intValue
        // Checks if number within Int range and has no fractional part.
        if (v.doubleValue == intV) {
          intV
        } else {
          throw new IllegalArgumentException(s"ALS only supports values in Integer range " +
            s"and without fractional part for columns ${$(userCol)} and ${$(itemCol)}. " +
            s"Value $n was either out of Integer range or contained a fractional part that " +
            s"could not be converted.")
        }
      case _ => throw new IllegalArgumentException(s"ALS only supports values in Integer range " +
        s"for columns ${$(userCol)} and ${$(itemCol)}. Value $n was not numeric.")
    }
  }

  /**
   * Param for strategy for dealing with unknown or new users/items at prediction time.
   * This may be useful in cross-validation or production scenarios, for handling user/item ids
   * the model has not seen in the training data.
   * Supported values:
   * - "nan":  predicted value for unknown ids will be NaN.
   * - "drop": rows in the input DataFrame containing unknown ids will be dropped from
   *           the output DataFrame containing predictions.
   * Default: "nan".
   * @group expertParam
   */
  val coldStartStrategy = new Param[String](this, "coldStartStrategy",
    "strategy for dealing with unknown or new users/items at prediction time. This may be " +
    "useful in cross-validation or production scenarios, for handling user/item ids the model " +
    "has not seen in the training data. Supported values: " +
    s"${ALSModel.supportedColdStartStrategies.mkString(",")}.",
    (s: String) =>
      ALSModel.supportedColdStartStrategies.contains(s.toLowerCase(Locale.ROOT)))

  /** @group expertGetParam */
  def getColdStartStrategy: String = $(coldStartStrategy).toLowerCase(Locale.ROOT)
}

/**
 * Common params for ALS.
 */
private[recommendation] trait ALSParams extends ALSModelParams with HasMaxIter with HasRegParam
  with HasPredictionCol with HasCheckpointInterval with HasSeed {

  /**
   * Param for rank of the matrix factorization (positive).
   * Default: 10
   * @group param
   */
  val rank = new IntParam(this, "rank", "rank of the factorization", ParamValidators.gtEq(1))

  /** @group getParam */
  def getRank: Int = $(rank)

  /**
   * Param for number of user blocks (positive).
   * Default: 10
   * @group param
   */
  val numUserBlocks = new IntParam(this, "numUserBlocks", "number of user blocks",
    ParamValidators.gtEq(1))

  /** @group getParam */
  def getNumUserBlocks: Int = $(numUserBlocks)

  /**
   * Param for number of item blocks (positive).
   * Default: 10
   * @group param
   */
  val numItemBlocks = new IntParam(this, "numItemBlocks", "number of item blocks",
      ParamValidators.gtEq(1))

  /** @group getParam */
  def getNumItemBlocks: Int = $(numItemBlocks)

  /**
   * Param to decide whether to use implicit preference.
   * Default: false
   * @group param
   */
  val implicitPrefs = new BooleanParam(this, "implicitPrefs", "whether to use implicit preference")

  /** @group getParam */
  def getImplicitPrefs: Boolean = $(implicitPrefs)

  /**
   * Param for the alpha parameter in the implicit preference formulation (nonnegative).
   * Default: 1.0
   * @group param
   */
  val alpha = new DoubleParam(this, "alpha", "alpha for implicit preference",
    ParamValidators.gtEq(0))

  /** @group getParam */
  def getAlpha: Double = $(alpha)

  /**
   * Param for the column name for ratings.
   * Default: "rating"
   * @group param
   */
  val ratingCol = new Param[String](this, "ratingCol", "column name for ratings")

  /** @group getParam */
  def getRatingCol: String = $(ratingCol)

  /**
   * Param for whether to apply nonnegativity constraints.
   * Default: false
   * @group param
   */
  val nonnegative = new BooleanParam(
    this, "nonnegative", "whether to use nonnegative constraint for least squares")

  /** @group getParam */
  def getNonnegative: Boolean = $(nonnegative)

  /**
   * Param for StorageLevel for intermediate datasets. Pass in a string representation of
   * `StorageLevel`. Cannot be "NONE".
   * Default: "MEMORY_AND_DISK".
   *
   * @group expertParam
   */
  val intermediateStorageLevel = new Param[String](this, "intermediateStorageLevel",
    "StorageLevel for intermediate datasets. Cannot be 'NONE'.",
    (s: String) => Try(StorageLevel.fromString(s)).isSuccess && s != "NONE")

  /** @group expertGetParam */
  def getIntermediateStorageLevel: String = $(intermediateStorageLevel)

  /**
   * Param for StorageLevel for ALS model factors. Pass in a string representation of
   * `StorageLevel`.
   * Default: "MEMORY_AND_DISK".
   *
   * @group expertParam
   */
  val finalStorageLevel = new Param[String](this, "finalStorageLevel",
    "StorageLevel for ALS model factors.",
    (s: String) => Try(StorageLevel.fromString(s)).isSuccess)

  /** @group expertGetParam */
  def getFinalStorageLevel: String = $(finalStorageLevel)

  setDefault(rank -> 10, maxIter -> 10, regParam -> 0.1, numUserBlocks -> 10, numItemBlocks -> 10,
    implicitPrefs -> false, alpha -> 1.0, userCol -> "user", itemCol -> "item",
    ratingCol -> "rating", nonnegative -> false, checkpointInterval -> 10,
    intermediateStorageLevel -> "MEMORY_AND_DISK", finalStorageLevel -> "MEMORY_AND_DISK",
    coldStartStrategy -> "nan")

  /**
   * Validates and transforms the input schema.
   *
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    // user and item will be cast to Int
    SchemaUtils.checkNumericType(schema, $(userCol))
    SchemaUtils.checkNumericType(schema, $(itemCol))
    // rating will be cast to Float
    SchemaUtils.checkNumericType(schema, $(ratingCol))
    SchemaUtils.appendColumn(schema, $(predictionCol), FloatType)
  }
}

/**
 * Model fitted by ALS.
 *
 * @param rank rank of the matrix factorization model
 * @param userFactors a DataFrame that stores user factors in two columns: `id` and `features`
 * @param itemFactors a DataFrame that stores item factors in two columns: `id` and `features`
 */
@Since("1.3.0")
class ALSModel private[ml] (
    @Since("1.4.0") override val uid: String,
    @Since("1.4.0") val rank: Int,
    @transient val userFactors: DataFrame,
    @transient val itemFactors: DataFrame)
  extends Model[ALSModel] with ALSModelParams with MLWritable {

  /** @group setParam */
  @Since("1.4.0")
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group expertSetParam */
  @Since("2.2.0")
  def setColdStartStrategy(value: String): this.type = set(coldStartStrategy, value)

  private val predict = udf { (featuresA: Seq[Float], featuresB: Seq[Float]) =>
    if (featuresA != null && featuresB != null) {
      // TODO(SPARK-19759): try dot-producting on Seqs or another non-converted type for
      // potential optimization.
      blas.sdot(rank, featuresA.toArray, 1, featuresB.toArray, 1)
    } else {
      Float.NaN
    }
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    // create a new column named map(predictionCol) by running the predict UDF.
    val predictions = dataset
      .join(userFactors,
        checkedCast(dataset($(userCol))) === userFactors("id"), "left")
      .join(itemFactors,
        checkedCast(dataset($(itemCol))) === itemFactors("id"), "left")
      .select(dataset("*"),
        predict(userFactors("features"), itemFactors("features")).as($(predictionCol)))
    getColdStartStrategy match {
      case ALSModel.Drop =>
        predictions.na.drop("all", Seq($(predictionCol)))
      case ALSModel.NaN =>
        predictions
    }
  }

  @Since("1.3.0")
  override def transformSchema(schema: StructType): StructType = {
    // user and item will be cast to Int
    SchemaUtils.checkNumericType(schema, $(userCol))
    SchemaUtils.checkNumericType(schema, $(itemCol))
    SchemaUtils.appendColumn(schema, $(predictionCol), FloatType)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): ALSModel = {
    val copied = new ALSModel(uid, rank, userFactors, itemFactors)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new ALSModel.ALSModelWriter(this)

  /**
   * Returns top `numItems` items recommended for each user, for all users.
   * @param numItems max number of recommendations for each user
   * @return a DataFrame of (userCol: Int, recommendations), where recommendations are
   *         stored as an array of (itemCol: Int, rating: Float) Rows.
   */
  @Since("2.2.0")
  def recommendForAllUsers(numItems: Int): DataFrame = {
    recommendForAll(userFactors, itemFactors, $(userCol), $(itemCol), numItems)
  }

  /**
   * Returns top `numUsers` users recommended for each item, for all items.
   * @param numUsers max number of recommendations for each item
   * @return a DataFrame of (itemCol: Int, recommendations), where recommendations are
   *         stored as an array of (userCol: Int, rating: Float) Rows.
   */
  @Since("2.2.0")
  def recommendForAllItems(numUsers: Int): DataFrame = {
    recommendForAll(itemFactors, userFactors, $(itemCol), $(userCol), numUsers)
  }

  /**
   * Makes recommendations for all users (or items).
   *
   * Note: the previous approach used for computing top-k recommendations
   * used a cross-join followed by predicting a score for each row of the joined dataset.
   * However, this results in exploding the size of intermediate data. While Spark SQL makes it
   * relatively efficient, the approach implemented here is significantly more efficient.
   *
   * This approach groups factors into blocks and computes the top-k elements per block,
   * using dot product and an efficient [[BoundedPriorityQueue]] (instead of gemm).
   * It then computes the global top-k by aggregating the per block top-k elements with
   * a [[TopByKeyAggregator]]. This significantly reduces the size of intermediate and shuffle data.
   * This is the DataFrame equivalent to the approach used in
   * [[org.apache.spark.mllib.recommendation.MatrixFactorizationModel]].
   *
   * @param srcFactors src factors for which to generate recommendations
   * @param dstFactors dst factors used to make recommendations
   * @param srcOutputColumn name of the column for the source ID in the output DataFrame
   * @param dstOutputColumn name of the column for the destination ID in the output DataFrame
   * @param num max number of recommendations for each record
   * @return a DataFrame of (srcOutputColumn: Int, recommendations), where recommendations are
   *         stored as an array of (dstOutputColumn: Int, rating: Float) Rows.
   */
  private def recommendForAll(
      srcFactors: DataFrame,
      dstFactors: DataFrame,
      srcOutputColumn: String,
      dstOutputColumn: String,
      num: Int): DataFrame = {
    import srcFactors.sparkSession.implicits._

    val srcFactorsBlocked = blockify(srcFactors.as[(Int, Array[Float])])
    val dstFactorsBlocked = blockify(dstFactors.as[(Int, Array[Float])])
    val ratings = srcFactorsBlocked.crossJoin(dstFactorsBlocked)
      .as[(Seq[(Int, Array[Float])], Seq[(Int, Array[Float])])]
      .flatMap { case (srcIter, dstIter) =>
        val m = srcIter.size
        val n = math.min(dstIter.size, num)
        val output = new Array[(Int, Int, Float)](m * n)
        var i = 0
        val pq = new BoundedPriorityQueue[(Int, Float)](num)(Ordering.by(_._2))
        srcIter.foreach { case (srcId, srcFactor) =>
          dstIter.foreach { case (dstId, dstFactor) =>
            // We use F2jBLAS which is faster than a call to native BLAS for vector dot product
            val score = BLAS.f2jBLAS.sdot(rank, srcFactor, 1, dstFactor, 1)
            pq += dstId -> score
          }
          pq.foreach { case (dstId, score) =>
            output(i) = (srcId, dstId, score)
            i += 1
          }
          pq.clear()
        }
        output.toSeq
      }
    // We'll force the IDs to be Int. Unfortunately this converts IDs to Int in the output.
    val topKAggregator = new TopByKeyAggregator[Int, Int, Float](num, Ordering.by(_._2))
    val recs = ratings.as[(Int, Int, Float)].groupByKey(_._1).agg(topKAggregator.toColumn)
      .toDF("id", "recommendations")

    val arrayType = ArrayType(
      new StructType()
        .add(dstOutputColumn, IntegerType)
        .add("rating", FloatType)
    )
    recs.select($"id".as(srcOutputColumn), $"recommendations".cast(arrayType))
  }

  /**
   * Blockifies factors to improve the efficiency of cross join
   * TODO: SPARK-20443 - expose blockSize as a param?
   */
  private def blockify(
      factors: Dataset[(Int, Array[Float])],
      blockSize: Int = 4096): Dataset[Seq[(Int, Array[Float])]] = {
    import factors.sparkSession.implicits._
    factors.mapPartitions(_.grouped(blockSize))
  }

}

@Since("1.6.0")
object ALSModel extends MLReadable[ALSModel] {

  private val NaN = "nan"
  private val Drop = "drop"
  private[recommendation] final val supportedColdStartStrategies = Array(NaN, Drop)

  @Since("1.6.0")
  override def read: MLReader[ALSModel] = new ALSModelReader

  @Since("1.6.0")
  override def load(path: String): ALSModel = super.load(path)

  private[ALSModel] class ALSModelWriter(instance: ALSModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val extraMetadata = "rank" -> instance.rank
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMetadata))
      val userPath = new Path(path, "userFactors").toString
      instance.userFactors.write.format("parquet").save(userPath)
      val itemPath = new Path(path, "itemFactors").toString
      instance.itemFactors.write.format("parquet").save(itemPath)
    }
  }

  private class ALSModelReader extends MLReader[ALSModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[ALSModel].getName

    override def load(path: String): ALSModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      implicit val format = DefaultFormats
      val rank = (metadata.metadata \ "rank").extract[Int]
      val userPath = new Path(path, "userFactors").toString
      val userFactors = sparkSession.read.format("parquet").load(userPath)
      val itemPath = new Path(path, "itemFactors").toString
      val itemFactors = sparkSession.read.format("parquet").load(itemPath)

      val model = new ALSModel(metadata.uid, rank, userFactors, itemFactors)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

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

  import org.apache.spark.ml.recommendation.ALS.Rating

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

    val (userFactors, itemFactors) = ALS.train(ratings, rank = $(rank),
      numUserBlocks = $(numUserBlocks), numItemBlocks = $(numItemBlocks),
      maxIter = $(maxIter), regParam = $(regParam), implicitPrefs = $(implicitPrefs),
      alpha = $(alpha), nonnegative = $(nonnegative),
      intermediateRDDStorageLevel = StorageLevel.fromString($(intermediateStorageLevel)),
      finalRDDStorageLevel = StorageLevel.fromString($(finalStorageLevel)),
      checkpointInterval = $(checkpointInterval), seed = $(seed))
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
object ALS extends DefaultParamsReadable[ALS] with Logging {

  /**
   * :: DeveloperApi ::
   * Rating class for better code readability.
   */
  @DeveloperApi
  case class Rating[@specialized(Int, Long) ID](user: ID, item: ID, rating: Float)

  @Since("1.6.0")
  override def load(path: String): ALS = super.load(path)

  /** Trait for least squares solvers applied to the normal equation. */
  private[recommendation] trait LeastSquaresNESolver extends Serializable {
    /** Solves a least squares problem with regularization (possibly with other constraints). */
    def solve(ne: NormalEquation, lambda: Double): Array[Float]
  }

  /** Cholesky solver for least square problems. */
  private[recommendation] class CholeskySolver extends LeastSquaresNESolver {

    /**
     * Solves a least squares problem with L2 regularization:
     *
     *   min norm(A x - b)^2^ + lambda * norm(x)^2^
     *
     * @param ne a [[NormalEquation]] instance that contains AtA, Atb, and n (number of instances)
     * @param lambda regularization constant
     * @return the solution x
     */
    override def solve(ne: NormalEquation, lambda: Double): Array[Float] = {
      val k = ne.k
      // Add scaled lambda to the diagonals of AtA.
      var i = 0
      var j = 2
      while (i < ne.triK) {
        ne.ata(i) += lambda
        i += j
        j += 1
      }
      CholeskyDecomposition.solve(ne.ata, ne.atb)
      val x = new Array[Float](k)
      i = 0
      while (i < k) {
        x(i) = ne.atb(i).toFloat
        i += 1
      }
      ne.reset()
      x
    }
  }

  /** NNLS solver. */
  private[recommendation] class NNLSSolver extends LeastSquaresNESolver {
    private var rank: Int = -1
    private var workspace: NNLS.Workspace = _
    private var ata: Array[Double] = _
    private var initialized: Boolean = false

    private def initialize(rank: Int): Unit = {
      if (!initialized) {
        this.rank = rank
        workspace = NNLS.createWorkspace(rank)
        ata = new Array[Double](rank * rank)
        initialized = true
      } else {
        require(this.rank == rank)
      }
    }

    /**
     * Solves a nonnegative least squares problem with L2 regularization:
     *
     *   min_x_  norm(A x - b)^2^ + lambda * n * norm(x)^2^
     *   subject to x >= 0
     */
    override def solve(ne: NormalEquation, lambda: Double): Array[Float] = {
      val rank = ne.k
      initialize(rank)
      fillAtA(ne.ata, lambda)
      val x = NNLS.solve(ata, ne.atb, workspace)
      ne.reset()
      x.map(x => x.toFloat)
    }

    /**
     * Given a triangular matrix in the order of fillXtX above, compute the full symmetric square
     * matrix that it represents, storing it into destMatrix.
     */
    private def fillAtA(triAtA: Array[Double], lambda: Double) {
      var i = 0
      var pos = 0
      var a = 0.0
      while (i < rank) {
        var j = 0
        while (j <= i) {
          a = triAtA(pos)
          ata(i * rank + j) = a
          ata(j * rank + i) = a
          pos += 1
          j += 1
        }
        ata(i * rank + i) += lambda
        i += 1
      }
    }
  }

  /**
   * Representing a normal equation to solve the following weighted least squares problem:
   *
   * minimize \sum,,i,, c,,i,, (a,,i,,^T^ x - d,,i,,)^2^ + lambda * x^T^ x.
   *
   * Its normal equation is given by
   *
   * \sum,,i,, c,,i,, (a,,i,, a,,i,,^T^ x - d,,i,, a,,i,,) + lambda * x = 0.
   *
   * Distributing and letting b,,i,, = c,,i,, * d,,i,,
   *
   * \sum,,i,, c,,i,, a,,i,, a,,i,,^T^ x - b,,i,, a,,i,, + lambda * x = 0.
   */
  private[recommendation] class NormalEquation(val k: Int) extends Serializable {

    /** Number of entries in the upper triangular part of a k-by-k matrix. */
    val triK = k * (k + 1) / 2
    /** A^T^ * A */
    val ata = new Array[Double](triK)
    /** A^T^ * b */
    val atb = new Array[Double](k)

    private val da = new Array[Double](k)
    private val upper = "U"

    private def copyToDouble(a: Array[Float]): Unit = {
      var i = 0
      while (i < k) {
        da(i) = a(i)
        i += 1
      }
    }

    /** Adds an observation. */
    def add(a: Array[Float], b: Double, c: Double = 1.0): this.type = {
      require(c >= 0.0)
      require(a.length == k)
      copyToDouble(a)
      blas.dspr(upper, k, c, da, 1, ata)
      if (b != 0.0) {
        blas.daxpy(k, b, da, 1, atb, 1)
      }
      this
    }

    /** Merges another normal equation object. */
    def merge(other: NormalEquation): this.type = {
      require(other.k == k)
      blas.daxpy(ata.length, 1.0, other.ata, 1, ata, 1)
      blas.daxpy(atb.length, 1.0, other.atb, 1, atb, 1)
      this
    }

    /** Resets everything to zero, which should be called after each solve. */
    def reset(): Unit = {
      ju.Arrays.fill(ata, 0.0)
      ju.Arrays.fill(atb, 0.0)
    }
  }

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
    val userPart = new ALSPartitioner(numUserBlocks)
    val itemPart = new ALSPartitioner(numItemBlocks)
    val userLocalIndexEncoder = new LocalIndexEncoder(userPart.numPartitions)
    val itemLocalIndexEncoder = new LocalIndexEncoder(itemPart.numPartitions)
    val solver = if (nonnegative) new NNLSSolver else new CholeskySolver
    val blockRatings = partitionRatings(ratings, userPart, itemPart)
      .persist(intermediateRDDStorageLevel)
    val (userInBlocks, userOutBlocks) =
      makeBlocks("user", blockRatings, userPart, itemPart, intermediateRDDStorageLevel)
    // materialize blockRatings and user blocks
    userOutBlocks.count()
    val swappedBlockRatings = blockRatings.map {
      case ((userBlockId, itemBlockId), RatingBlock(userIds, itemIds, localRatings)) =>
        ((itemBlockId, userBlockId), RatingBlock(itemIds, userIds, localRatings))
    }
    val (itemInBlocks, itemOutBlocks) =
      makeBlocks("item", swappedBlockRatings, itemPart, userPart, intermediateRDDStorageLevel)
    // materialize item blocks
    itemOutBlocks.count()
    val seedGen = new XORShiftRandom(seed)
    var userFactors = initialize(userInBlocks, rank, seedGen.nextLong())
    var itemFactors = initialize(itemInBlocks, rank, seedGen.nextLong())
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
        itemFactors = computeFactors(userFactors, userOutBlocks, itemInBlocks, rank, regParam,
          userLocalIndexEncoder, implicitPrefs, alpha, solver)
        previousItemFactors.unpersist()
        itemFactors.setName(s"itemFactors-$iter").persist(intermediateRDDStorageLevel)
        // TODO: Generalize PeriodicGraphCheckpointer and use it here.
        val deps = itemFactors.dependencies
        if (shouldCheckpoint(iter)) {
          itemFactors.checkpoint() // itemFactors gets materialized in computeFactors
        }
        val previousUserFactors = userFactors
        userFactors = computeFactors(itemFactors, itemOutBlocks, userInBlocks, rank, regParam,
          itemLocalIndexEncoder, implicitPrefs, alpha, solver)
        if (shouldCheckpoint(iter)) {
          ALS.cleanShuffleDependencies(sc, deps)
          deletePreviousCheckpointFile()
          previousCheckpointFile = itemFactors.getCheckpointFile
        }
        previousUserFactors.unpersist()
      }
    } else {
      for (iter <- 0 until maxIter) {
        itemFactors = computeFactors(userFactors, userOutBlocks, itemInBlocks, rank, regParam,
          userLocalIndexEncoder, solver = solver)
        if (shouldCheckpoint(iter)) {
          val deps = itemFactors.dependencies
          itemFactors.checkpoint()
          itemFactors.count() // checkpoint item factors and cut lineage
          ALS.cleanShuffleDependencies(sc, deps)
          deletePreviousCheckpointFile()
          previousCheckpointFile = itemFactors.getCheckpointFile
        }
        userFactors = computeFactors(itemFactors, itemOutBlocks, userInBlocks, rank, regParam,
          itemLocalIndexEncoder, solver = solver)
      }
    }
    val userIdAndFactors = userInBlocks
      .mapValues(_.srcIds)
      .join(userFactors)
      .mapPartitions({ items =>
        items.flatMap { case (_, (ids, factors)) =>
          ids.view.zip(factors)
        }
      // Preserve the partitioning because IDs are consistent with the partitioners in userInBlocks
      // and userFactors.
      }, preservesPartitioning = true)
      .setName("userFactors")
      .persist(finalRDDStorageLevel)
    val itemIdAndFactors = itemInBlocks
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
      userInBlocks.unpersist()
      userOutBlocks.unpersist()
      itemInBlocks.unpersist()
      itemOutBlocks.unpersist()
      blockRatings.unpersist()
    }
    (userIdAndFactors, itemIdAndFactors)
  }

  /**
   * Factor block that stores factors (Array[Float]) in an Array.
   */
  private type FactorBlock = Array[Array[Float]]

  /**
   * Out-link block that stores, for each dst (item/user) block, which src (user/item) factors to
   * send. For example, outLinkBlock(0) contains the local indices (not the original src IDs) of the
   * src factors in this block to send to dst block 0.
   */
  private type OutBlock = Array[Array[Int]]

  /**
   * In-link block for computing src (user/item) factors. This includes the original src IDs
   * of the elements within this block as well as encoded dst (item/user) indices and corresponding
   * ratings. The dst indices are in the form of (blockId, localIndex), which are not the original
   * dst IDs. To compute src factors, we expect receiving dst factors that match the dst indices.
   * For example, if we have an in-link record
   *
   * {srcId: 0, dstBlockId: 2, dstLocalIndex: 3, rating: 5.0},
   *
   * and assume that the dst factors are stored as dstFactors: Map[Int, Array[Array[Float]]], which
   * is a blockId to dst factors map, the corresponding dst factor of the record is dstFactor(2)(3).
   *
   * We use a CSC-like (compressed sparse column) format to store the in-link information. So we can
   * compute src factors one after another using only one normal equation instance.
   *
   * @param srcIds src ids (ordered)
   * @param dstPtrs dst pointers. Elements in range [dstPtrs(i), dstPtrs(i+1)) of dst indices and
   *                ratings are associated with srcIds(i).
   * @param dstEncodedIndices encoded dst indices
   * @param ratings ratings
   * @see [[LocalIndexEncoder]]
   */
  private[recommendation] case class InBlock[@specialized(Int, Long) ID: ClassTag](
      srcIds: Array[ID],
      dstPtrs: Array[Int],
      dstEncodedIndices: Array[Int],
      ratings: Array[Float]) {
    /** Size of the block. */
    def size: Int = ratings.length
    require(dstEncodedIndices.length == size)
    require(dstPtrs.length == srcIds.length + 1)
  }

  /**
   * Initializes factors randomly given the in-link blocks.
   *
   * @param inBlocks in-link blocks
   * @param rank rank
   * @return initialized factor blocks
   */
  private def initialize[ID](
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
   * A rating block that contains src IDs, dst IDs, and ratings, stored in primitive arrays.
   */
  private[recommendation] case class RatingBlock[@specialized(Int, Long) ID: ClassTag](
      srcIds: Array[ID],
      dstIds: Array[ID],
      ratings: Array[Float]) {
    /** Size of the block. */
    def size: Int = srcIds.length
    require(dstIds.length == srcIds.length)
    require(ratings.length == srcIds.length)
  }

  /**
   * Builder for [[RatingBlock]]. `mutable.ArrayBuilder` is used to avoid boxing/unboxing.
   */
  private[recommendation] class RatingBlockBuilder[@specialized(Int, Long) ID: ClassTag]
    extends Serializable {

    private val srcIds = mutable.ArrayBuilder.make[ID]
    private val dstIds = mutable.ArrayBuilder.make[ID]
    private val ratings = mutable.ArrayBuilder.make[Float]
    var size = 0

    /** Adds a rating. */
    def add(r: Rating[ID]): this.type = {
      size += 1
      srcIds += r.user
      dstIds += r.item
      ratings += r.rating
      this
    }

    /** Merges another [[RatingBlockBuilder]]. */
    def merge(other: RatingBlock[ID]): this.type = {
      size += other.srcIds.length
      srcIds ++= other.srcIds
      dstIds ++= other.dstIds
      ratings ++= other.ratings
      this
    }

    /** Builds a [[RatingBlock]]. */
    def build(): RatingBlock[ID] = {
      RatingBlock[ID](srcIds.result(), dstIds.result(), ratings.result())
    }
  }

  /**
   * Partitions raw ratings into blocks.
   *
   * @param ratings raw ratings
   * @param srcPart partitioner for src IDs
   * @param dstPart partitioner for dst IDs
   * @return an RDD of rating blocks in the form of ((srcBlockId, dstBlockId), ratingBlock)
   */
  private def partitionRatings[ID: ClassTag](
      ratings: RDD[Rating[ID]],
      srcPart: Partitioner,
      dstPart: Partitioner): RDD[((Int, Int), RatingBlock[ID])] = {

     /* The implementation produces the same result as the following but generates less objects.

     ratings.map { r =>
       ((srcPart.getPartition(r.user), dstPart.getPartition(r.item)), r)
     }.aggregateByKey(new RatingBlockBuilder)(
         seqOp = (b, r) => b.add(r),
         combOp = (b0, b1) => b0.merge(b1.build()))
       .mapValues(_.build())
     */

    val numPartitions = srcPart.numPartitions * dstPart.numPartitions
    ratings.mapPartitions { iter =>
      val builders = Array.fill(numPartitions)(new RatingBlockBuilder[ID])
      iter.flatMap { r =>
        val srcBlockId = srcPart.getPartition(r.user)
        val dstBlockId = dstPart.getPartition(r.item)
        val idx = srcBlockId + srcPart.numPartitions * dstBlockId
        val builder = builders(idx)
        builder.add(r)
        if (builder.size >= 2048) { // 2048 * (3 * 4) = 24k
          builders(idx) = new RatingBlockBuilder
          Iterator.single(((srcBlockId, dstBlockId), builder.build()))
        } else {
          Iterator.empty
        }
      } ++ {
        builders.view.zipWithIndex.filter(_._1.size > 0).map { case (block, idx) =>
          val srcBlockId = idx % srcPart.numPartitions
          val dstBlockId = idx / srcPart.numPartitions
          ((srcBlockId, dstBlockId), block.build())
        }
      }
    }.groupByKey().mapValues { blocks =>
      val builder = new RatingBlockBuilder[ID]
      blocks.foreach(builder.merge)
      builder.build()
    }.setName("ratingBlocks")
  }

  /**
   * Builder for uncompressed in-blocks of (srcId, dstEncodedIndex, rating) tuples.
   *
   * @param encoder encoder for dst indices
   */
  private[recommendation] class UncompressedInBlockBuilder[@specialized(Int, Long) ID: ClassTag](
      encoder: LocalIndexEncoder)(
      implicit ord: Ordering[ID]) {

    private val srcIds = mutable.ArrayBuilder.make[ID]
    private val dstEncodedIndices = mutable.ArrayBuilder.make[Int]
    private val ratings = mutable.ArrayBuilder.make[Float]

    /**
     * Adds a dst block of (srcId, dstLocalIndex, rating) tuples.
     *
     * @param dstBlockId dst block ID
     * @param srcIds original src IDs
     * @param dstLocalIndices dst local indices
     * @param ratings ratings
     */
    def add(
        dstBlockId: Int,
        srcIds: Array[ID],
        dstLocalIndices: Array[Int],
        ratings: Array[Float]): this.type = {
      val sz = srcIds.length
      require(dstLocalIndices.length == sz)
      require(ratings.length == sz)
      this.srcIds ++= srcIds
      this.ratings ++= ratings
      var j = 0
      while (j < sz) {
        this.dstEncodedIndices += encoder.encode(dstBlockId, dstLocalIndices(j))
        j += 1
      }
      this
    }

    /** Builds a [[UncompressedInBlock]]. */
    def build(): UncompressedInBlock[ID] = {
      new UncompressedInBlock(srcIds.result(), dstEncodedIndices.result(), ratings.result())
    }
  }

  /**
   * A block of (srcId, dstEncodedIndex, rating) tuples stored in primitive arrays.
   */
  private[recommendation] class UncompressedInBlock[@specialized(Int, Long) ID: ClassTag](
      val srcIds: Array[ID],
      val dstEncodedIndices: Array[Int],
      val ratings: Array[Float])(
      implicit ord: Ordering[ID]) {

    /** Size the of block. */
    def length: Int = srcIds.length

    /**
     * Compresses the block into an [[InBlock]]. The algorithm is the same as converting a
     * sparse matrix from coordinate list (COO) format into compressed sparse column (CSC) format.
     * Sorting is done using Spark's built-in Timsort to avoid generating too many objects.
     */
    def compress(): InBlock[ID] = {
      val sz = length
      assert(sz > 0, "Empty in-link block should not exist.")
      sort()
      val uniqueSrcIdsBuilder = mutable.ArrayBuilder.make[ID]
      val dstCountsBuilder = mutable.ArrayBuilder.make[Int]
      var preSrcId = srcIds(0)
      uniqueSrcIdsBuilder += preSrcId
      var curCount = 1
      var i = 1
      while (i < sz) {
        val srcId = srcIds(i)
        if (srcId != preSrcId) {
          uniqueSrcIdsBuilder += srcId
          dstCountsBuilder += curCount
          preSrcId = srcId
          curCount = 0
        }
        curCount += 1
        i += 1
      }
      dstCountsBuilder += curCount
      val uniqueSrcIds = uniqueSrcIdsBuilder.result()
      val numUniqueSrdIds = uniqueSrcIds.length
      val dstCounts = dstCountsBuilder.result()
      val dstPtrs = new Array[Int](numUniqueSrdIds + 1)
      var sum = 0
      i = 0
      while (i < numUniqueSrdIds) {
        sum += dstCounts(i)
        i += 1
        dstPtrs(i) = sum
      }
      InBlock(uniqueSrcIds, dstPtrs, dstEncodedIndices, ratings)
    }

    private def sort(): Unit = {
      val sz = length
      // Since there might be interleaved log messages, we insert a unique id for easy pairing.
      val sortId = Utils.random.nextInt()
      logDebug(s"Start sorting an uncompressed in-block of size $sz. (sortId = $sortId)")
      val start = System.nanoTime()
      val sorter = new Sorter(new UncompressedInBlockSort[ID])
      sorter.sort(this, 0, length, Ordering[KeyWrapper[ID]])
      val duration = (System.nanoTime() - start) / 1e9
      logDebug(s"Sorting took $duration seconds. (sortId = $sortId)")
    }
  }

  /**
   * A wrapper that holds a primitive key.
   *
   * @see [[UncompressedInBlockSort]]
   */
  private class KeyWrapper[@specialized(Int, Long) ID: ClassTag](
      implicit ord: Ordering[ID]) extends Ordered[KeyWrapper[ID]] {

    var key: ID = _

    override def compare(that: KeyWrapper[ID]): Int = {
      ord.compare(key, that.key)
    }

    def setKey(key: ID): this.type = {
      this.key = key
      this
    }
  }

  /**
   * [[SortDataFormat]] of [[UncompressedInBlock]] used by [[Sorter]].
   */
  private class UncompressedInBlockSort[@specialized(Int, Long) ID: ClassTag](
      implicit ord: Ordering[ID])
    extends SortDataFormat[KeyWrapper[ID], UncompressedInBlock[ID]] {

    override def newKey(): KeyWrapper[ID] = new KeyWrapper()

    override def getKey(
        data: UncompressedInBlock[ID],
        pos: Int,
        reuse: KeyWrapper[ID]): KeyWrapper[ID] = {
      if (reuse == null) {
        new KeyWrapper().setKey(data.srcIds(pos))
      } else {
        reuse.setKey(data.srcIds(pos))
      }
    }

    override def getKey(
        data: UncompressedInBlock[ID],
        pos: Int): KeyWrapper[ID] = {
      getKey(data, pos, null)
    }

    private def swapElements[@specialized(Int, Float) T](
        data: Array[T],
        pos0: Int,
        pos1: Int): Unit = {
      val tmp = data(pos0)
      data(pos0) = data(pos1)
      data(pos1) = tmp
    }

    override def swap(data: UncompressedInBlock[ID], pos0: Int, pos1: Int): Unit = {
      swapElements(data.srcIds, pos0, pos1)
      swapElements(data.dstEncodedIndices, pos0, pos1)
      swapElements(data.ratings, pos0, pos1)
    }

    override def copyRange(
        src: UncompressedInBlock[ID],
        srcPos: Int,
        dst: UncompressedInBlock[ID],
        dstPos: Int,
        length: Int): Unit = {
      System.arraycopy(src.srcIds, srcPos, dst.srcIds, dstPos, length)
      System.arraycopy(src.dstEncodedIndices, srcPos, dst.dstEncodedIndices, dstPos, length)
      System.arraycopy(src.ratings, srcPos, dst.ratings, dstPos, length)
    }

    override def allocate(length: Int): UncompressedInBlock[ID] = {
      new UncompressedInBlock(
        new Array[ID](length), new Array[Int](length), new Array[Float](length))
    }

    override def copyElement(
        src: UncompressedInBlock[ID],
        srcPos: Int,
        dst: UncompressedInBlock[ID],
        dstPos: Int): Unit = {
      dst.srcIds(dstPos) = src.srcIds(srcPos)
      dst.dstEncodedIndices(dstPos) = src.dstEncodedIndices(srcPos)
      dst.ratings(dstPos) = src.ratings(srcPos)
    }
  }

  /**
   * Creates in-blocks and out-blocks from rating blocks.
   *
   * @param prefix prefix for in/out-block names
   * @param ratingBlocks rating blocks
   * @param srcPart partitioner for src IDs
   * @param dstPart partitioner for dst IDs
   * @return (in-blocks, out-blocks)
   */
  private def makeBlocks[ID: ClassTag](
      prefix: String,
      ratingBlocks: RDD[((Int, Int), RatingBlock[ID])],
      srcPart: Partitioner,
      dstPart: Partitioner,
      storageLevel: StorageLevel)(
      implicit srcOrd: Ordering[ID]): (RDD[(Int, InBlock[ID])], RDD[(Int, OutBlock)]) = {
    val inBlocks = ratingBlocks.map {
      case ((srcBlockId, dstBlockId), RatingBlock(srcIds, dstIds, ratings)) =>
        // The implementation is a faster version of
        // val dstIdToLocalIndex = dstIds.toSet.toSeq.sorted.zipWithIndex.toMap
        val start = System.nanoTime()
        val dstIdSet = new OpenHashSet[ID](1 << 20)
        dstIds.foreach(dstIdSet.add)
        val sortedDstIds = new Array[ID](dstIdSet.size)
        var i = 0
        var pos = dstIdSet.nextPos(0)
        while (pos != -1) {
          sortedDstIds(i) = dstIdSet.getValue(pos)
          pos = dstIdSet.nextPos(pos + 1)
          i += 1
        }
        assert(i == dstIdSet.size)
        Sorting.quickSort(sortedDstIds)
        val dstIdToLocalIndex = new OpenHashMap[ID, Int](sortedDstIds.length)
        i = 0
        while (i < sortedDstIds.length) {
          dstIdToLocalIndex.update(sortedDstIds(i), i)
          i += 1
        }
        logDebug(
          "Converting to local indices took " + (System.nanoTime() - start) / 1e9 + " seconds.")
        val dstLocalIndices = dstIds.map(dstIdToLocalIndex.apply)
        (srcBlockId, (dstBlockId, srcIds, dstLocalIndices, ratings))
    }.groupByKey(new ALSPartitioner(srcPart.numPartitions))
      .mapValues { iter =>
        val builder =
          new UncompressedInBlockBuilder[ID](new LocalIndexEncoder(dstPart.numPartitions))
        iter.foreach { case (dstBlockId, srcIds, dstLocalIndices, ratings) =>
          builder.add(dstBlockId, srcIds, dstLocalIndices, ratings)
        }
        builder.build().compress()
      }.setName(prefix + "InBlocks")
      .persist(storageLevel)
    val outBlocks = inBlocks.mapValues { case InBlock(srcIds, dstPtrs, dstEncodedIndices, _) =>
      val encoder = new LocalIndexEncoder(dstPart.numPartitions)
      val activeIds = Array.fill(dstPart.numPartitions)(mutable.ArrayBuilder.make[Int])
      var i = 0
      val seen = new Array[Boolean](dstPart.numPartitions)
      while (i < srcIds.length) {
        var j = dstPtrs(i)
        ju.Arrays.fill(seen, false)
        while (j < dstPtrs(i + 1)) {
          val dstBlockId = encoder.blockId(dstEncodedIndices(j))
          if (!seen(dstBlockId)) {
            activeIds(dstBlockId) += i // add the local index in this out-block
            seen(dstBlockId) = true
          }
          j += 1
        }
        i += 1
      }
      activeIds.map { x =>
        x.result()
      }
    }.setName(prefix + "OutBlocks")
      .persist(storageLevel)
    (inBlocks, outBlocks)
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
  private def computeFactors[ID](
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
              // Extension to the original paper to handle rating < 0. confidence is a function
              // of |rating| instead so that it is never negative. c1 is confidence - 1.
              val c1 = alpha * math.abs(rating)
              // For rating <= 0, the corresponding preference is 0. So the second argument of add
              // is only there for rating > 0.
              if (rating > 0.0) {
                numExplicits += 1
              }
              ls.add(srcFactor, if (rating > 0.0) 1.0 + c1 else 0.0, c1)
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
  private def computeYtY(factorBlocks: RDD[(Int, FactorBlock)], rank: Int): NormalEquation = {
    factorBlocks.values.aggregate(new NormalEquation(rank))(
      seqOp = (ne, factors) => {
        factors.foreach(ne.add(_, 0.0))
        ne
      },
      combOp = (ne1, ne2) => ne1.merge(ne2))
  }

  /**
   * Encoder for storing (blockId, localIndex) into a single integer.
   *
   * We use the leading bits (including the sign bit) to store the block id and the rest to store
   * the local index. This is based on the assumption that users/items are approximately evenly
   * partitioned. With this assumption, we should be able to encode two billion distinct values.
   *
   * @param numBlocks number of blocks
   */
  private[recommendation] class LocalIndexEncoder(numBlocks: Int) extends Serializable {

    require(numBlocks > 0, s"numBlocks must be positive but found $numBlocks.")

    private[this] final val numLocalIndexBits =
      math.min(java.lang.Integer.numberOfLeadingZeros(numBlocks - 1), 31)
    private[this] final val localIndexMask = (1 << numLocalIndexBits) - 1

    /** Encodes a (blockId, localIndex) into a single integer. */
    def encode(blockId: Int, localIndex: Int): Int = {
      require(blockId < numBlocks)
      require((localIndex & ~localIndexMask) == 0)
      (blockId << numLocalIndexBits) | localIndex
    }

    /** Gets the block id from an encoded index. */
    @inline
    def blockId(encoded: Int): Int = {
      encoded >>> numLocalIndexBits
    }

    /** Gets the local index from an encoded index. */
    @inline
    def localIndex(encoded: Int): Int = {
      encoded & localIndexMask
    }
  }

  /**
   * Partitioner used by ALS. We require that getPartition is a projection. That is, for any key k,
   * we have getPartition(getPartition(k)) = getPartition(k). Since the default HashPartitioner
   * satisfies this requirement, we simply use a type alias here.
   */
  private[recommendation] type ALSPartitioner = org.apache.spark.HashPartitioner

  /**
   * Private function to clean up all of the shuffles files from the dependencies and their parents.
   */
  private[spark] def cleanShuffleDependencies[T](
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
