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

import java.util.{Locale, Random}

import scala.util.Try

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._

import org.apache.spark.Partitioner
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasBlockSize, _}
import org.apache.spark.ml.recommendation.logistic.LogisticFactorizationBase
import org.apache.spark.ml.recommendation.logistic.local.ItemData
import org.apache.spark.ml.recommendation.logistic.pair.{LongPair, LongPairMulti}
import org.apache.spark.ml.recommendation.logistic.pair.generator.BatchedGenerator
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DatasetUtils._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.{StorageLevel, StorageLevelMapper}


/**
 * Common params for LMF and LMFModel.
 */
private[recommendation] trait LMFModelParams extends Params with HasPredictionCol
  with HasBlockSize {
  /**
   * Param for the column name for user ids. Ids must be longs.
   * Default: "user"
   * @group param
   */
  val userCol = new Param[String](this, "userCol", "column name for user ids. Ids must be within " +
    "the long value range.")

  /** @group getParam */
  def getUserCol: String = $(userCol)

  /**
   * Param for the column name for item ids. Ids must be longs.
   * Default: "item"
   * @group param
   */
  val itemCol = new Param[String](this, "itemCol", "column name for item ids. Ids must be within " +
    "the long value range.")

  /** @group getParam */
  def getItemCol: String = $(itemCol)

  /**
   * Attempts to safely cast a user/item id to an Long. Throws an exception if the value is
   * out of integer range or contains a fractional part.
   */
  protected[recommendation] def checkLongs(dataset: Dataset[_], colName: String): Column = {
    dataset.schema(colName).dataType match {
      case LongType =>
        val column = dataset(colName)
        when(column.isNull, raise_error(lit(s"$colName Ids MUST NOT be Null")))
          .otherwise(column)

      case _: NumericType =>
        val column = dataset(colName)
        val casted = column.cast(LongType)
        // Checks if number within Long range and has no fractional part.
        when(column.isNull || column =!= casted,
          raise_error(concat(
            lit(s"LMF only supports non-Null values in Long range and " +
              s"without fractional part for column $colName, but got "), column)))
          .otherwise(casted)

      case other => throw new IllegalArgumentException(s"LMF only supports values in " +
        s"Long range for column $colName, but got type $other.")
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
      s"${LMFModel.supportedColdStartStrategies.mkString(",")}.",
    (s: String) =>
      LMFModel.supportedColdStartStrategies.contains(s.toLowerCase(Locale.ROOT)))

  /** @group expertGetParam */
  def getColdStartStrategy: String = $(coldStartStrategy).toLowerCase(Locale.ROOT)

  setDefault(blockSize -> 4096)
}

/**
 * Common params for LMF.
 */
private[recommendation] trait LMFParams extends LMFModelParams with HasMaxIter
  with HasRegParam with HasCheckpointInterval with HasSeed with HasStepSize
  with HasParallelism with HasFitIntercept with HasLabelCol with HasWeightCol {

  /**
   * Param for rank of the matrix factorization.
   * Default: 10
   * @group param
   */
  val rank = new IntParam(this, "rank", "rank of the factorization", ParamValidators.gtEq(1))

  /** @group getParam */
  def getRank: Int = $(rank)

  /**
   * Param to decide whether to use implicit preference.
   * Default: true
   * @group param
   */
  val implicitPrefs = new BooleanParam(this, "implicitPrefs", "whether to use implicit preference")

  /** @group getParam */
  def getImplicitPrefs: Boolean = $(implicitPrefs)

  /**
   * Param for the power parameter in the negative sampling formula.
   * Default: 0.0
   * @group param
   */
  val pow = new DoubleParam(this, "pow", "power for negative sampling (>= 0)",
    ParamValidators.gtEq(0))

  /** @group getParam */
  def getPow: Double = $(pow)

  /**
   * The minimum number of times a user must appear to be included in the lmf factorization.
   * Default: 1
   * @group param
   */
  final val minUserCount = new IntParam(this, "minUserCount", "the minimum number of times" +
    " a user must appear to be included in the lmf factorization (> 0)", ParamValidators.gt(0))

  /** @group getParam */
  def getMinUserCount: Int = $(minUserCount)

  /**
   * The minimum number of times a user must appear to be included in the lmf factorization.
   * Default: 1
   * @group param
   */
  final val minItemCount = new IntParam(this, "minItemCount", "the minimum number of times" +
    " a item must appear to be included in the lmf factorization (> 0)", ParamValidators.gt(0))

  /** @group getParam */
  def getMinItemCount: Int = $(minItemCount)

  /**
   * Param for the number of negative samples per positive sample (nonnegative).
   * Default: 10
   * @group param
   */
  val negative = new IntParam(this, "negative", "number of negative samples (> 0)",
    ParamValidators.gtEq(0))

  /** @group getParam */
  def getNegative: Int = $(negative)

  /**
   * Param for StorageLevel for intermediate datasets. Pass in a string representation of
   * `StorageLevel`. Cannot be "NONE".
   * Default: "MEMORY_AND_DISK".
   *
   * @group expertParam
   */
  val intermediateStorageLevel = new Param[String](this, "intermediateStorageLevel",
    "storageLevel for intermediate datasets. Cannot be 'NONE'.",
    (s: String) => Try(StorageLevel.fromString(s)).isSuccess && s != "NONE")

  /** @group expertGetParam */
  def getIntermediateStorageLevel: String = $(intermediateStorageLevel)

  /**
   * Param for StorageLevel for LMF model factors. Pass in a string representation of
   * `StorageLevel`.
   * Default: "MEMORY_AND_DISK".
   *
   * @group expertParam
   */
  val finalStorageLevel = new Param[String](this, "finalStorageLevel",
    "storageLevel for LMF model factors.",
    (s: String) => Try(StorageLevel.fromString(s)).isSuccess)

  /** @group expertGetParam */
  def getFinalStorageLevel: String = $(finalStorageLevel)

  /**
   * Param for stepSize decay (positive).
   * Default: None
   * @group param
   */
  val minStepSize: DoubleParam = new DoubleParam(this, "minStepSize",
    "minimum step size to be used for stepSize decay (> 0)", ParamValidators.gt(0))
  def getMinStepSize: Double = $(minStepSize)

  /**
   * Param for number partitions used for factorization (positive).
   * Default: 1
   * @group param
   */
  val numPartitions: IntParam = new IntParam(this, "numPartitions",
    "number partitions to be used to split the data (> 0)",
    ParamValidators.gt(0))
  def getNumPartitions: Int = $(numPartitions)

  /**
   * Param for path where the intermediate state will be saved.
   * Default: None
   * @group param
   */
  val checkpointPath: Param[String] = new Param[String](this, "checkpointPath",
    "path where the intermediate state will be saved")
  def getCheckpointPath: String = $(checkpointPath)

  /**
   * Param to decide whether to verbose loss values.
   * Default: false
   * @group param
   */
  val verbose: BooleanParam = new BooleanParam(this, "verbose",
    "whether to verbose loss values")
  def getVerbose: Boolean = $(verbose)

  setDefault(rank -> 10, implicitPrefs -> true, maxIter -> 10,
    negative -> 10, fitIntercept -> false,
    stepSize -> 0.025, pow -> 0.0, regParam -> 0.0,
    minUserCount -> 1, minItemCount -> 1, userCol -> "user",
    itemCol -> "item", maxIter -> 1, numPartitions -> 1, coldStartStrategy -> "nan",
    intermediateStorageLevel -> StorageLevelMapper.MEMORY_AND_DISK.name(),
    finalStorageLevel -> StorageLevelMapper.MEMORY_AND_DISK.name(),
    verbose -> false)

  /**
   * Validates and transforms the input schema.
   *
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    // user and item will be cast to Long
    SchemaUtils.checkNumericType(schema, $(userCol))
    SchemaUtils.checkNumericType(schema, $(itemCol))
    // label will be cast to Float
    get(labelCol).foreach(SchemaUtils.checkNumericType(schema, _))
    get(weightCol).foreach(SchemaUtils.checkNumericType(schema, _))
    SchemaUtils.appendColumn(schema, $(predictionCol), FloatType)
  }
}


/**
 * Model fitted by LMF.
 *
 * @param rank rank of the matrix factorization model
 * @param userFactors a DataFrame that stores user factors in three columns: `id`, `features`
 *                    and `intercept`
 * @param itemFactors a DataFrame that stores item factors in three columns: `id`, `features`
 *                    and `intercept`
 */
@Since("4.0.0")
class LMFModel private[ml] (
                             @Since("4.0.0") override val uid: String,
                             @Since("4.0.0") val rank: Int,
                             @transient val userFactors: DataFrame,
                             @transient val itemFactors: DataFrame)
  extends Model[LMFModel] with LMFModelParams with MLWritable {

  /** @group setParam */
  @Since("4.0.0")
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group expertSetParam */
  @Since("4.0.0")
  def setColdStartStrategy(value: String): this.type = set(coldStartStrategy, value)

  /**
   * Set block size for stacking input data in matrices.
   * Default is 4096.
   *
   * @group expertSetParam
   */
  @Since("4.0.0")
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  private val predict = udf { (featuresA: Seq[Float], interceptA: Float,
                               featuresB: Seq[Float], interceptB: Float) =>
    if (featuresA != null && featuresB != null) {
      var dotProduct = 0.0f
      var i = 0
      while (i < rank) {
        dotProduct += featuresA(i) * featuresB(i)
        i += 1
      }
      dotProduct += interceptA
      dotProduct += interceptB
      dotProduct
    } else {
      Float.NaN
    }
  }

  @Since("4.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    // create a new column named map(predictionCol) by running the predict UDF.
    val validatedUsers = checkLongs(dataset, $(userCol))
    val validatedItems = checkLongs(dataset, $(itemCol))

    val validatedInputAlias = Identifiable.randomUID("__lmf_validated_input")
    val itemFactorsAlias = Identifiable.randomUID("__lmf_item_factors")
    val userFactorsAlias = Identifiable.randomUID("__lmf_user_factors")

    val predictions = dataset
      .withColumns(Seq($(userCol), $(itemCol)), Seq(validatedUsers, validatedItems))
      .alias(validatedInputAlias)
      .join(userFactors.alias(userFactorsAlias),
        col(s"${validatedInputAlias}.${$(userCol)}") === col(s"${userFactorsAlias}.id"), "left")
      .join(itemFactors.alias(itemFactorsAlias),
        col(s"${validatedInputAlias}.${$(itemCol)}") === col(s"${itemFactorsAlias}.id"), "left")
      .select(col(s"${validatedInputAlias}.*"),
        predict(col(s"${userFactorsAlias}.features"), col(s"${userFactorsAlias}.intercept"),
          col(s"${itemFactorsAlias}.features"), col(s"${itemFactorsAlias}.intercept"))
          .alias($(predictionCol)))

    getColdStartStrategy match {
      case LMFModel.Drop =>
        predictions.na.drop("all", Seq($(predictionCol)))
      case LMFModel.NaN =>
        predictions
    }
  }

  @Since("4.0.0")
  override def transformSchema(schema: StructType): StructType = {
    // user and item will be cast to Long
    SchemaUtils.checkNumericType(schema, $(userCol))
    SchemaUtils.checkNumericType(schema, $(itemCol))
    SchemaUtils.appendColumn(schema, $(predictionCol), FloatType)
  }

  @Since("4.0.0")
  override def copy(extra: ParamMap): LMFModel = {
    val copied = new LMFModel(uid, rank, userFactors, itemFactors)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("4.0.0")
  override def write: MLWriter = new LMFModel.LMFModelWriter(this)

  @Since("4.0.0")
  override def toString: String = {
    s"LMFModel: uid=$uid, rank=$rank"
  }

  /**
   * Returns top `numItems` items recommended for each user, for all users.
   * @param numItems max number of recommendations for each user
   * @return a DataFrame of (userCol: Long, recommendations), where recommendations are
   *         stored as an array of (itemCol: Long, rating: Float) Rows.
   */
  @Since("4.0.0")
  def recommendForAllUsers(numItems: Int): DataFrame = {
    recommendForAll(rank, userFactors, itemFactors,
      $(userCol), $(itemCol), numItems, $(blockSize))
  }

  /**
   * Returns top `numItems` items recommended for each user id in the input data set. Note that if
   * there are duplicate ids in the input dataset, only one set of recommendations per unique id
   * will be returned.
   * @param dataset a Dataset containing a column of user ids. The column name must match `userCol`.
   * @param numItems max number of recommendations for each user.
   * @return a DataFrame of (userCol: Long, recommendations), where recommendations are
   *         stored as an array of (itemCol: Long, rating: Float) Rows.
   */
  @Since("4.0.0")
  def recommendForUserSubset(dataset: Dataset[_], numItems: Int): DataFrame = {
    val srcFactorSubset = getSourceFactorSubset(
      dataset, userFactors, $(userCol))
    recommendForAll(
      rank, srcFactorSubset, itemFactors, $(userCol), $(itemCol), numItems, $(blockSize))
  }

  /**
   * Returns top `numUsers` users recommended for each item, for all items.
   * @param numUsers max number of recommendations for each item
   * @return a DataFrame of (itemCol: Long, recommendations), where recommendations are
   *         stored as an array of (userCol: Long, rating: Float) Rows.
   */
  @Since("4.0.0")
  def recommendForAllItems(numUsers: Int): DataFrame = {
    recommendForAll(
      rank, itemFactors, userFactors, $(itemCol), $(userCol), numUsers, $(blockSize))
  }

  /**
   * Returns top `numUsers` users recommended for each item id in the input data set. Note that if
   * there are duplicate ids in the input dataset, only one set of recommendations per unique id
   * will be returned.
   * @param dataset a Dataset containing a column of item ids. The column name must match `itemCol`.
   * @param numUsers max number of recommendations for each item.
   * @return a DataFrame of (itemCol: Long, recommendations), where recommendations are
   *         stored as an array of (userCol: Long, rating: Float) Rows.
   */
  @Since("4.0.0")
  def recommendForItemSubset(dataset: Dataset[_], numUsers: Int): DataFrame = {
    val srcFactorSubset = getSourceFactorSubset(
      dataset, itemFactors, $(itemCol))
    recommendForAll(rank,
      srcFactorSubset, userFactors, $(itemCol), $(userCol), numUsers, $(blockSize))
  }
}

@Since("4.0.0")
object LMFModel extends MLReadable[LMFModel] {

  private val NaN = "nan"
  private val Drop = "drop"
  private[recommendation] final val supportedColdStartStrategies = Array(NaN, Drop)

  @Since("4.0.0")
  override def read: MLReader[LMFModel] = new LMFModelReader

  @Since("4.0.0")
  override def load(path: String): LMFModel = super.load(path)

  private[LMFModel] class LMFModelWriter(instance: LMFModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val extraMetadata = "rank" -> instance.rank
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession, Some(extraMetadata))
      val userPath = new Path(path, "userFactors").toString
      instance.userFactors.write.format("parquet").save(userPath)
      val itemPath = new Path(path, "itemFactors").toString
      instance.itemFactors.write.format("parquet").save(itemPath)
    }
  }

  private class LMFModelReader extends MLReader[LMFModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[LMFModel].getName

    override def load(path: String): LMFModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      implicit val format = DefaultFormats
      val rank = (metadata.metadata \ "rank").extract[Int]
      val userPath = new Path(path, "userFactors").toString
      val userFactors = sparkSession.read.format("parquet").load(userPath)
      val itemPath = new Path(path, "itemFactors").toString
      val itemFactors = sparkSession.read.format("parquet").load(itemPath)

      val model = new LMFModel(metadata.uid, rank, userFactors, itemFactors)

      metadata.getAndSetParams(model)
      model
    }
  }
}

@Since("4.0.0")
class LMF(@Since("4.0.0") override val uid: String) extends Estimator[LMFModel] with LMFParams
  with DefaultParamsWritable {
  
  @Since("4.0.0")
  def this() = this(Identifiable.randomUID("lmf"))

  /** @group setParam */
  @Since("4.0.0")
  def setRank(value: Int): this.type = set(rank, value)

  /** @group setParam */
  @Since("4.0.0")
  def setNegative(value: Int): this.type = set(negative, value)

  /** @group setParam */
  @Since("4.0.0")
  def setImplicitPrefs(value: Boolean): this.type = set(implicitPrefs, value)

  /** @group setParam */
  @Since("4.0.0")
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("4.0.0")
  def setStepSize(value: Double): this.type = set(stepSize, value)

  /** @group setParam */
  @Since("4.0.0")
  def setMinStepSize(value: Double): this.type = set(minStepSize, value)

  /** @group setParam */
  @Since("4.0.0")
  def setParallelism(value: Int): this.type = set(parallelism, value)

  /** @group setParam */
  @Since("4.0.0")
  def setNumPartitions(value: Int): this.type = set(numPartitions, value)

  /** @group setParam */
  @Since("4.0.0")
  def setPow(value: Double): this.type = set(pow, value)

  /** @group setParam */
  @Since("4.0.0")
  def setMinUserCount(value: Int): this.type = set(minUserCount, value)

  /** @group setParam */
  @Since("4.0.0")
  def setMinItemCount(value: Int): this.type = set(minItemCount, value)

  /** @group setParam */
  @Since("4.0.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  /** @group setParam */
  @Since("4.0.0")
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** @group setParam */
  @Since("4.0.0")
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("4.0.0")
  def setCheckpointPath(value: String): this.type = set(checkpointPath, value)

  /** @group setParam */
  @Since("4.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("4.0.0")
  def setVerbose(value: Boolean): this.type = set(verbose, value)

  /** @group expertSetParam */
  @Since("4.0.0")
  def setIntermediateStorageLevel(value: String): this.type = set(intermediateStorageLevel, value)

  /** @group expertSetParam */
  @Since("4.0.0")
  def setFinalStorageLevel(value: String): this.type = set(finalStorageLevel, value)

  /** @group expertSetParam */
  @Since("4.0.0")
  def setColdStartStrategy(value: String): this.type = set(coldStartStrategy, value)

  /**
   * Set block size for stacking input data in matrices.
   * Default is 4096.
   *
   * @group expertSetParam
   */
  @Since("4.0.0")
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  @Since("4.0.0")
  override def fit(dataset: Dataset[_]): LMFModel = instrumented { instr =>
    transformSchema(dataset.schema)
    import dataset.sparkSession.implicits._

    val validatedUsers = checkLongs(dataset, $(userCol))
    val validatedItems = checkLongs(dataset, $(itemCol))
    val validatedLabels = get(labelCol).fold{
      if ($(implicitPrefs)) {
        lit(LongPair.EMPTY)
      } else {
        throw new IllegalArgumentException(s"The labelCol must be set with " +
          s"implicitPrefs disabled.")
      }
    } {label =>
      if ($(implicitPrefs)) {
        throw new IllegalArgumentException(s"LMF does not support the labelCol " +
          s"in implicitPrefs mode. All labels are treated as positives.")
      } else {
        checkClassificationLabels(label, Some(2)).cast(FloatType)
      }
    }

    val validatedWeights = get(weightCol).fold(lit(LongPair.EMPTY))(
      checkNonNegativeWeights)

    val numExecutors = Try(dataset.sparkSession.sparkContext
      .getConf.get("spark.executor.instances").toInt).getOrElse($(numPartitions))
    val numCores = Try(dataset.sparkSession.sparkContext
      .getConf.get("spark.executor.cores").toInt).getOrElse(1)

    val ratings = dataset
      .select(validatedUsers, validatedItems, validatedLabels, validatedWeights)
      .rdd
      .map { case Row(u: Long, i: Long, l: Float, w: Float) => (u, i, l, w) }
      .repartition(numExecutors * numCores / $(parallelism))
      .persist(StorageLevel.fromString($(intermediateStorageLevel)))

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, rank, negative, maxIter, stepSize, minStepSize,
      parallelism, numPartitions, pow, minUserCount, minItemCount, regParam,
      fitIntercept, implicitPrefs, intermediateStorageLevel, finalStorageLevel,
      checkpointPath, checkpointInterval, verbose, blockSize)

    val result = new LMF.Backend($(rank), $(negative), $(maxIter), $(stepSize),
      get(minStepSize), $(parallelism), $(numPartitions),
      $(pow), $(minUserCount), $(minItemCount), $(regParam), $(fitIntercept),
      $(implicitPrefs), get(labelCol).isDefined, get(weightCol).isDefined,
      $(seed), StorageLevel.fromString($(intermediateStorageLevel)),
      StorageLevel.fromString($(finalStorageLevel)),
      get(checkpointPath), get(checkpointInterval).getOrElse(-1), $(verbose)).train(ratings)

    ratings.unpersist()

    val userDF = result.filter(_.t == ItemData.TYPE_LEFT).map{entry =>
      (entry.id, entry.f.slice(0, $(rank)), if ($(fitIntercept)) entry.f($(rank)) else 0f)
    }.toDF("id", "features", "intercept")

    val itemDF = result.filter(_.t == ItemData.TYPE_RIGHT).map{entry =>
      (entry.id, entry.f.slice(0, $(rank)), if ($(fitIntercept)) entry.f($(rank)) else 0f)
    }.toDF("id", "features", "intercept")

    val model = new LMFModel(uid, $(rank), userDF, itemDF).setBlockSize($(blockSize))
      .setParent(this)
    copyValues(model)
  }

  @Since("4.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("4.0.0")
  override def copy(extra: ParamMap): LMF = defaultCopy(extra)
}

object LMF extends DefaultParamsReadable[LMF] with Logging {

  @Since("4.0.0")
  override def load(path: String): LMF = super.load(path)

  private[recommendation] class Backend(
                          dotVectorSize: Int,
                          negative: Int,
                          numIterations: Int,
                          learningRate: Double,
                          minLearningRate: Option[Double],
                          numThread: Int,
                          numPartitions: Int,
                          pow: Double,
                          minUserCount: Int,
                          minItemCount: Int,
                          lambda: Double,
                          useBias: Boolean,
                          implicitPrefs: Boolean,
                          withLabel: Boolean,
                          withWeight: Boolean,
                          seed: Long,
                          intermediateRDDStorageLevel: StorageLevel,
                          finalRDDStorageLevel: StorageLevel,
                          checkpointPath: Option[String],
                          checkpointInterval: Int,
                          verbose: Boolean
                ) extends LogisticFactorizationBase[(Long, Long, Float, Float)](
                          dotVectorSize,
                          negative,
                          numIterations,
                          learningRate,
                          minLearningRate,
                          numThread,
                          numPartitions,
                          pow,
                          lambda,
                          useBias,
                          implicitPrefs,
                          seed: Long,
                          intermediateRDDStorageLevel,
                          finalRDDStorageLevel,
                          checkpointPath,
                          checkpointInterval,
                          verbose
  ) {

    override protected def gamma: Float = 1f / negative

    override protected def pairs(data: RDD[(Long, Long, Float, Float)],
                                 partitioner1: Partitioner,
                                 partitioner2: Partitioner,
                                 seed: Long): RDD[LongPairMulti] = {
      data.mapPartitions(it => BatchedGenerator(it
        .filter(e => partitioner1.getPartition(e._1) == partitioner2.getPartition(e._2))
        .map(e => LongPair(partitioner1.getPartition(e._1), e._1, e._2, e._3, e._4)),
        partitioner1.numPartitions, withLabel, withWeight))
    }

    override protected def initialize(data: RDD[(Long, Long, Float, Float)]): RDD[ItemData] = {
      data.flatMap(e => Array((ItemData.TYPE_LEFT, e._1) -> 1L, (ItemData.TYPE_RIGHT, e._2) -> 1L))
        .reduceByKey(_ + _)
        .filter(e => if (e._1._1 == ItemData.TYPE_LEFT) {
          e._2 >= minUserCount
        } else {
          e._2 >= minItemCount
        }).mapPartitions { it =>
          val rnd = new Random()
          it.map { case ((t, w), n) =>
            rnd.setSeed(w ^ seed)
            new ItemData(t, w, n,
              logistic.local.Optimizer.initEmbedding(dotVectorSize, useBias, rnd))
          }
        }
    }
  }
}
