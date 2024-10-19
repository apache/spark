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

import java.util.Random

import scala.util.Try

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._

import org.apache.spark.Partitioner
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.recommendation.logfac.LogisticFactorizationBase
import org.apache.spark.ml.recommendation.logfac.local.{ItemData, Optimizer}
import org.apache.spark.ml.recommendation.logfac.pair.LongPairMulti
import org.apache.spark.ml.recommendation.logfac.pair.generator.BatchedGenerator
import org.apache.spark.ml.recommendation.logfac.pair.generator.w2v.{Item2VecGenerator, SamplingMode, WindowGenerator}
import org.apache.spark.ml.recommendation.logfac.pair.generator.w2v.SamplingMode.SamplingMode
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.{StorageLevel, StorageLevelMapper}


/**
 * Common params for Item2Vec and Item2VecModel.
 */
private[recommendation] trait Item2VecModelParams extends Params with HasPredictionCol
  with HasBlockSize {

  /**
   * Param for the column name for context item ids.
   * Default: "context"
   * @group param
   */
  val contextCol = new Param[String](this, "contextCol", "column name for context item ids.")

  /** @group getParam */
  final def getContextCol: String = $(contextCol)

  /**
   * Param for the column name for item ids.
   * Default: "item"
   * @group param
   */
  val itemCol = new Param[String](this, "itemCol", "column name for item ids.")

  /** @group getParam */
  final def getItemCol: String = $(itemCol)

  /**
   * Attempts to safely cast a context/item id to an Long. Throws an exception if the value is
   * out of long range or contains a fractional part.
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

  protected[recommendation] def checkArrayLongs(dataset: Dataset[_], colName: String): Column = {
    dataset.schema(colName).dataType match {
      case ArrayType(LongType, false) =>
        val column = dataset(colName)
        when(column.isNull, raise_error(lit(s"$colName Ids MUST NOT be Null")))
          .otherwise(column)
      case other => throw new IllegalArgumentException(s"Item2Vec only supports Array[Long] " +
        s"inputs for column $colName, but got type $other.")
    }
  }

  setDefault(blockSize -> 4096, contextCol -> "context", itemCol -> "item")
}

/**
 * Common params for Item2Vec.
 */
private[recommendation] trait Item2VecParams extends Item2VecModelParams
  with HasInputCol with HasMaxIter with HasCheckpointInterval
  with HasSeed with HasStepSize with HasParallelism with HasFitIntercept {

  /**
   * Param for dimension of the item embeddings.
   * Default: 10
   * @group param
   */
  val rank = new IntParam(this, "rank", "the dimension of embeddings", ParamValidators.gtEq(1))

  /** @group getParam */
  def getRank: Int = $(rank)

  /**
   * The minimum number of times a context must appear to be included in the Item2Vec factorization.
   * Default: 1
   * @group param
   */
  final val minCount = new IntParam(this, "minCount", "the minimum number of times" +
    " a item must appear to be included in the Item2Vec factorization (> 0)", ParamValidators.gt(0))

  /** @group getParam */
  def getMinCount: Int = $(minCount)

  /**
   * Param for the number of negative samples per positive sample (> 0).
   * Default: 10
   * @group param
   */
  val negative = new IntParam(this, "negative", "number of negative samples (> 0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def getNegative: Int = $(negative)

  /**
   * Param for the number of negative samples per positive sample (> 0).
   * Default: 10
   * @group param
   */
  val windowSize = new IntParam(this, "windowSize",
    "size of the window in WINDOW mode and number of samples in ITEM2VEC mode (> 0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def getWindowSize: Int = $(windowSize)

  /**
   * Param for number partitions used for factorization (positive).
   * Default: 1
   * @group param
   */
  val numPartitions: IntParam = new IntParam(this, "numPartitions",
    "number partitions to be used to split the data (> 0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def getNumPartitions: Int = $(numPartitions)

  /**
   * Param for path where the intermediate state will be saved.
   * Default: None
   * @group param
   */
  val checkpointPath: Param[String] = new Param[String](this, "checkpointPath",
    "path where the intermediate state will be saved")

  /** @group getParam */
  def getCheckpointPath: String = $(checkpointPath)

  /**
   * Param for factors regularization parameter (&gt;= 0).
   * Default: 0
   * @group expertParam
   */
  final val regParam: DoubleParam = new DoubleParam(this, "regParam",
    "regularization parameter for embeddings (>= 0)", ParamValidators.gtEq(0))

  /** @group getParam */
  final def getRegParam: Double = $(regParam)

  /**
   * Param for sampling mode (WINDOW or ITEM2VEC).
   * Default: ITEM2VEC
   * @group expertParam
   */
  final val samplingMode: Param[String] = new Param[String](this, "samplingMode",
    "samplingMode (WINDOW or ITEM2VEC)", ParamValidators.inArray(Array("WINDOW", "ITEM2VEC")))

  /** @group getParam */
  final def getSamplingModel: String = $(samplingMode)

  /**
   * Param for the for skewness of the negative sampling distribution.
   * The probability of sampling item as negative is c_i^pow / sum(c_j^pow for all j).
   *
   * Default: 0.0
   * @group expertParam
   */
  val pow = new DoubleParam(this, "pow", "power for negative sampling (>= 0)",
    ParamValidators.gtEq(0))

  /** @group expertGetParam */
  def getPow: Double = $(pow)


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
   * Param for StorageLevel for Item2Vec model factors. Pass in a string representation of
   * `StorageLevel`.
   * Default: "MEMORY_AND_DISK".
   *
   * @group expertParam
   */
  val finalStorageLevel = new Param[String](this, "finalStorageLevel",
    "storageLevel for Item2Vec model factors.",
    (s: String) => Try(StorageLevel.fromString(s)).isSuccess)

  /** @group expertGetParam */
  def getFinalStorageLevel: String = $(finalStorageLevel)

  /**
   * Param to verbose loss.
   * Default: false
   * @group expertParam
   */
  val verbose: BooleanParam = new BooleanParam(this, "verbose",
    "verbose loss")

  /** @group expertGetParam */
  def getVerbose: Boolean = $(verbose)

  setDefault(rank -> 10, maxIter -> 10, windowSize -> 10,
    negative -> 10, fitIntercept -> false, samplingMode -> "ITEM2VEC",
    stepSize -> 0.025, pow -> 0.0, regParam -> 0.0, maxIter -> 1, numPartitions -> 1,
    minCount -> 1, inputCol -> "input",
    intermediateStorageLevel -> StorageLevelMapper.MEMORY_AND_DISK.name(),
    finalStorageLevel -> StorageLevelMapper.MEMORY_AND_DISK.name(),
    verbose -> false)
}


/**
 * Model fitted by Item2Vec.
 *
 * @param rank rank of the matrix factorization model
 * @param contextFactors a DataFrame that stores context factors in three columns: `id`, `features`
 *                    and `intercept`
 * @param itemFactors a DataFrame that stores item factors in three columns: `id`, `features`
 *                    and `intercept`
 */
@Since("4.0.0")
class Item2VecModel private[ml] (
                             @Since("4.0.0") override val uid: String,
                             @Since("4.0.0") val rank: Int,
                             @transient val contextFactors: DataFrame,
                             @transient val itemFactors: DataFrame)
  extends Model[Item2VecModel] with Item2VecModelParams with MLWritable {
  
  /** @group setParam */
  @Since("4.0.0")
  def setContextCol(value: String): this.type = set(contextCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

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
    val validatedContexts = checkLongs(dataset, $(contextCol))
    val validatedItems = checkLongs(dataset, $(itemCol))

    val validatedInputAlias = Identifiable.randomUID("__Item2Vec_validated_input")
    val itemFactorsAlias = Identifiable.randomUID("__Item2Vec_item_factors")
    val contextFactorsAlias = Identifiable.randomUID("__Item2Vec_context_factors")
    val predictions = dataset
      .withColumns(Seq($(contextCol), $(itemCol)), Seq(validatedContexts, validatedItems))
      .alias(validatedInputAlias)
      .join(contextFactors.alias(contextFactorsAlias),
        col(s"${validatedInputAlias}.${$(contextCol)}") ===
          col(s"${contextFactorsAlias}.id"), "left")
      .join(itemFactors.alias(itemFactorsAlias),
        col(s"${validatedInputAlias}.${$(itemCol)}") === col(s"${itemFactorsAlias}.id"), "left")
      .select(col(s"${validatedInputAlias}.*"),
        predict(col(s"${contextFactorsAlias}.features"), col(s"${contextFactorsAlias}.intercept"),
          col(s"${itemFactorsAlias}.features"), col(s"${itemFactorsAlias}.intercept"))
          .alias($(predictionCol)))
    predictions
  }

  @Since("4.0.0")
  override def transformSchema(schema: StructType): StructType = {
    // context and item will be cast to Long
    SchemaUtils.checkNumericType(schema, $(contextCol))
    SchemaUtils.checkNumericType(schema, $(itemCol))
    SchemaUtils.appendColumn(schema, $(predictionCol), FloatType)
  }

  @Since("4.0.0")
  override def copy(extra: ParamMap): Item2VecModel = {
    val copied = new Item2VecModel(uid, rank, contextFactors, itemFactors)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("4.0.0")
  override def write: MLWriter = new Item2VecModel.Item2VecModelWriter(this)

  @Since("4.0.0")
  override def toString: String = {
    s"Item2VecModel: uid=$uid, rank=$rank"
  }

  /**
   * Returns top `numItems` items recommended for each context, for all contexts.
   * @param numItems max number of recommendations for each context
   * @return a DataFrame of (contextCol: Long, recommendations), where recommendations are
   *         stored as an array of (itemCol: Long, rating: Float) Rows.
   */
  @Since("4.0.0")
  def recommendForAllContexts(numItems: Int): DataFrame = {
    recommendForAll(rank, contextFactors, itemFactors,
      $(contextCol), $(itemCol), numItems, $(blockSize))
  }

  /**
   * Returns top `numItems` items recommended for each context id in the input data set.
   * Note that if there are duplicate ids in the input dataset, only one set of
   * recommendations per unique id will be returned.
   * @param dataset a Dataset containing a column of context ids. The column name
   *                must match `contextCol`.
   * @param numItems max number of recommendations for each context.
   * @return a DataFrame of (contextCol: Long, recommendations), where recommendations are
   *         stored as an array of (itemCol: Long, rating: Float) Rows.
   */
  @Since("4.0.0")
  def recommendForContextSubset(dataset: Dataset[_], numItems: Int): DataFrame = {
    val srcFactorSubset = getSourceFactorSubset(
      dataset, contextFactors, $(contextCol))
    recommendForAll(
      rank, srcFactorSubset, itemFactors, $(contextCol), $(itemCol), numItems, $(blockSize))
  }

  /**
   * Returns top `numContexts` contexts recommended for each item, for all items.
   * @param numContexts max number of recommendations for each item
   * @return a DataFrame of (itemCol: Long, recommendations), where recommendations are
   *         stored as an array of (contextCol: Long, rating: Float) Rows.
   */
  @Since("4.0.0")
  def recommendForAllItems(numContexts: Int): DataFrame = {
    recommendForAll(
      rank, itemFactors, contextFactors, $(itemCol), $(contextCol), numContexts, $(blockSize))
  }

  /**
   * Returns top `numContexts` contexts recommended for each item id in the input data set.
   * Note that if there are duplicate ids in the input dataset, only one set of
   * recommendations per unique id will be returned.
   * @param dataset a Dataset containing a column of item ids. The column name must match `itemCol`.
   * @param numContexts max number of recommendations for each item.
   * @return a DataFrame of (itemCol: Long, recommendations), where recommendations are
   *         stored as an array of (contextCol: Long, rating: Float) Rows.
   */
  @Since("4.0.0")
  def recommendForItemSubset(dataset: Dataset[_], numContexts: Int): DataFrame = {
    val srcFactorSubset = getSourceFactorSubset(
      dataset, itemFactors, $(itemCol))
    recommendForAll(rank,
      srcFactorSubset, contextFactors, $(itemCol), $(contextCol), numContexts, $(blockSize))
  }
}

@Since("4.0.0")
object Item2VecModel extends MLReadable[Item2VecModel] {

  @Since("4.0.0")
  override def read: MLReader[Item2VecModel] = new Item2VecModelReader

  @Since("4.0.0")
  override def load(path: String): Item2VecModel = super.load(path)

  private[Item2VecModel] class Item2VecModelWriter(instance: Item2VecModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val extraMetadata = "rank" -> instance.rank
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession, Some(extraMetadata))
      val contextPath = new Path(path, "contextFactors").toString
      instance.contextFactors.write.format("parquet").save(contextPath)
      val itemPath = new Path(path, "itemFactors").toString
      instance.itemFactors.write.format("parquet").save(itemPath)
    }
  }

  private class Item2VecModelReader extends MLReader[Item2VecModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[Item2VecModel].getName

    override def load(path: String): Item2VecModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      implicit val format = DefaultFormats
      val rank = (metadata.metadata \ "rank").extract[Int]
      val contextPath = new Path(path, "contextFactors").toString
      val contextFactors = sparkSession.read.format("parquet").load(contextPath)
      val itemPath = new Path(path, "itemFactors").toString
      val itemFactors = sparkSession.read.format("parquet").load(itemPath)

      val model = new Item2VecModel(metadata.uid, rank, contextFactors, itemFactors)

      metadata.getAndSetParams(model)
      model
    }
  }
}


@Since("4.0.0")
class Item2Vec(@Since("4.0.0") override val uid: String)
  extends Estimator[Item2VecModel] with Item2VecParams
  with DefaultParamsWritable {

  @Since("4.0.0")
  def this() = this(Identifiable.randomUID("Item2Vec"))

  /** @group setParam */
  @Since("4.0.0")
  def setRank(value: Int): this.type = set(rank, value)

  /** @group setParam */
  @Since("4.0.0")
  def setNegative(value: Int): this.type = set(negative, value)

  /** @group setParam */
  @Since("4.0.0")
  def setWindowSize(value: Int): this.type = set(windowSize, value)

  /** @group setParam */
  @Since("4.0.0")
  def setSamplingModeSize(value: String): this.type = set(samplingMode, value)

  /** @group setParam */
  @Since("4.0.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setContextCol(value: String): this.type = set(contextCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setItemCol(value: String): this.type = set(itemCol, value)

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
  def setParallelism(value: Int): this.type = set(parallelism, value)

  /** @group setParam */
  @Since("4.0.0")
  def setNumPartitions(value: Int): this.type = set(numPartitions, value)

  /** @group setParam */
  @Since("4.0.0")
  def setMinCount(value: Int): this.type = set(minCount, value)

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

  /** @group expertSetParam */
  @Since("4.0.0")
  def setPow(value: Double): this.type = set(pow, value)

  /** @group expertSetParam */
  @Since("4.0.0")
  def setVerbose(value: Boolean): this.type = set(verbose, value)

  /** @group expertSetParam */
  @Since("4.0.0")
  def setIntermediateStorageLevel(value: String): this.type = set(intermediateStorageLevel, value)

  /** @group expertSetParam */
  @Since("4.0.0")
  def setFinalStorageLevel(value: String): this.type = set(finalStorageLevel, value)

  /**
   * Set block size for stacking input data in matrices.
   * Default is 4096.
   *
   * @group expertSetParam
   */
  @Since("4.0.0")
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  @Since("4.0.0")
  override def fit(dataset: Dataset[_]): Item2VecModel = instrumented { instr =>
    import dataset.sparkSession.implicits._

    val validatedInputs = checkArrayLongs(dataset, $(inputCol))
    if (get(checkpointInterval).isDefined ^ get(checkpointPath).isDefined) {
      throw new IllegalArgumentException(s"checkpointPath and checkpointInterval" +
        s"must be set together.")
    }

    val numExecutors = Try(dataset.sparkSession.sparkContext
      .getConf.get("spark.executor.instances").toInt).getOrElse($(numPartitions))
    val numCores = Try(dataset.sparkSession.sparkContext
      .getConf.get("spark.executor.cores").toInt).getOrElse(1)

    val ratings = dataset
      .select(validatedInputs)
      .rdd
      .map { case Row(seq: Array[Long]) => seq}
      .repartition(numExecutors * numCores / $(parallelism))
      .persist(StorageLevel.fromString($(intermediateStorageLevel)))

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, rank, negative, maxIter, stepSize, parallelism,
      numPartitions, pow, minCount, regParam, fitIntercept, intermediateStorageLevel,
      finalStorageLevel, checkpointPath, checkpointInterval, verbose, blockSize)

    val result = new Item2Vec.Backend($(rank), $(windowSize), $(negative), $(maxIter),
      $(stepSize), $(parallelism), $(numPartitions), $(pow),
      $(minCount), $(regParam), SamplingMode.withName($(samplingMode)), $(fitIntercept),
      $(seed), StorageLevel.fromString($(intermediateStorageLevel)),
      StorageLevel.fromString($(finalStorageLevel)),
      get(checkpointPath), get(checkpointInterval).getOrElse(-1), $(verbose)).train(ratings)

    ratings.unpersist()

    val contextDF = result.filter(_.t == ItemData.TYPE_LEFT).map{entry =>
      (entry.id, entry.f.slice(0, $(rank)), if ($(fitIntercept)) entry.f($(rank)) else 0f)
    }.toDF("id", "features", "intercept")

    val itemDF = result.filter(_.t == ItemData.TYPE_RIGHT).map{entry =>
      (entry.id, entry.f.slice(0, $(rank)), if ($(fitIntercept)) entry.f($(rank)) else 0f)
    }.toDF("id", "features", "intercept")

    val model = new Item2VecModel(uid, $(rank), contextDF, itemDF)
      .setBlockSize($(blockSize))
      .setContextCol($(contextCol))
      .setItemCol($(itemCol))
      .setPredictionCol($(predictionCol))
      .setParent(this)
    copyValues(model)
  }

  @Since("4.0.0")
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  @Since("4.0.0")
  override def copy(extra: ParamMap): Item2Vec = defaultCopy(extra)
}

object Item2Vec extends DefaultParamsReadable[Item2Vec] with Logging {

  @Since("4.0.0")
  override def load(path: String): Item2Vec = super.load(path)

  private[recommendation] class Backend(
                                         dotVectorSize: Int,
                                         windowSize: Int,
                                         negative: Int,
                                         numIterations: Int,
                                         learningRate: Double,
                                         numThread: Int,
                                         numPartitions: Int,
                                         pow: Double,
                                         minCount: Int,
                                         lambda: Double,
                                         samplingMode: SamplingMode,
                                         useBias: Boolean,
                                         seed: Long,
                                         intermediateRDDStorageLevel: StorageLevel,
                                         finalRDDStorageLevel: StorageLevel,
                                         checkpointPath: Option[String],
                                         checkpointInterval: Int,
                                         verbose: Boolean
                               ) extends LogisticFactorizationBase[Array[Long]](
                                         dotVectorSize,
                                         negative,
                                         numIterations,
                                         learningRate,
                                         numThread,
                                         numPartitions,
                                         pow,
                                         lambda,
                                         lambda,
                                         useBias,
                                         false,
                                         seed: Long,
                                         intermediateRDDStorageLevel,
                                         finalRDDStorageLevel,
                                         checkpointPath,
                                         checkpointInterval,
                                         verbose
                                       ) {

    override protected def gamma: Double = 1.0

    override protected def pairs(sent: RDD[Array[Long]],
                                 partitioner1: Partitioner,
                                 partitioner2: Partitioner,
                                 seed: Long): RDD[LongPairMulti] = {
      sent.mapPartitionsWithIndex({ case (idx, it) =>
        BatchedGenerator({
          if (samplingMode == SamplingMode.ITEM2VEC) {
            new Item2VecGenerator(it, windowSize, partitioner1, partitioner2,
              seed * partitioner1.numPartitions + idx)
          } else if (samplingMode == SamplingMode.WINDOW) {
            new WindowGenerator(it, windowSize, partitioner1, partitioner2)
          } else {
            assert(false)
            null
          }
        }, partitioner1.numPartitions, false, false)
      })
    }

    override protected def initialize(sent: RDD[Array[Long]]): RDD[ItemData] = {
      sent.flatMap(identity(_)).map(_ -> 1L)
        .reduceByKey(_ + _).filter(_._2 >= minCount)
        .mapPartitions { it =>
          val rnd = new Random()
          it.flatMap { case (w, n) =>
            rnd.setSeed(w.hashCode)
            Iterator(new ItemData(ItemData.TYPE_LEFT, w, n,
              Optimizer.initEmbedding(dotVectorSize, useBias, rnd)),
              new ItemData(ItemData.TYPE_RIGHT, w, n,
                Optimizer.initEmbedding(dotVectorSize, useBias, rnd)))
          }
        }
    }
  }
}
