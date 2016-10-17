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

package org.apache.spark.ml.feature

import scala.util.Random

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Params for [[LSH]].
 */
@Since("2.1.0")
private[ml] trait LSHParams extends HasInputCol with HasOutputCol {
  /**
   * Param for the dimension of LSH OR-amplification.
   *
   * In this implementation, we use LSH OR-amplification to reduce the false negative rate. The
   * higher the dimension is, the lower the false negative rate.
   * @group param
   */
  @Since("2.1.0")
  final val outputDim: IntParam = new IntParam(this, "outputDim", "output dimension, where" +
    "increasing dimensionality lowers the false negative rate, and decreasing dimensionality" +
    " improves the running performance", ParamValidators.gt(0))

  /** @group getParam */
  @Since("2.1.0")
  final def getOutputDim: Int = $(outputDim)

  setDefault(outputDim -> 1, outputCol -> "lshFeatures")

  /**
   * Transform the Schema for LSH
   * @param schema The schema of the input dataset without [[outputCol]]
   * @return A derived schema with [[outputCol]] added
   */
  @Since("2.1.0")
  protected[this] final def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }
}

/**
 * Model produced by [[LSH]].
 */
@Since("2.1.0")
private[ml] abstract class LSHModel[T <: LSHModel[T]] extends Model[T] with LSHParams {
  self: T =>

  @Since("2.1.0")
  override def copy(extra: ParamMap): T = defaultCopy(extra)

  /**
   * The hash function of LSH, mapping a predefined KeyType to a Vector
   * @return The mapping of LSH function.
   */
  @Since("2.1.0")
  protected[this] val hashFunction: Vector => Vector

  /**
   * Calculate the distance between two different keys using the distance metric corresponding
   * to the hashFunction
   * @param x One input vector in the metric space
   * @param y One input vector in the metric space
   * @return The distance between x and y
   */
  @Since("2.1.0")
  protected[ml] def keyDistance(x: Vector, y: Vector): Double

  /**
   * Calculate the distance between two different hash Vectors.
   *
   * @param x One of the hash vector
   * @param y Another hash vector
   * @return The distance between hash vectors x and y
   */
  @Since("2.1.0")
  protected[ml] def hashDistance(x: Vector, y: Vector): Double

  @Since("2.1.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val transformUDF = udf(hashFunction, new VectorUDT)
    dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
  }

  @Since("2.1.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  /**
   * Given a large dataset and an item, approximately find at most k items which have the closest
   * distance to the item. If the [[outputCol]] is missing, the method will transform the data; if
   * the [[outputCol]] exists, it will use the [[outputCol]]. This allows caching of the
   * transformed data when necessary.
   *
   * This method implements two ways of fetching k nearest neighbors:
   *  - Single Probing: Fast, return at most k elements (Probing only one buckets)
   *  - Multiple Probing: Slow, return exact k elements (Probing multiple buckets close to the key)
   *
   * @param dataset the dataset to search for nearest neighbors of the key
   * @param key Feature vector representing the item to search for
   * @param numNearestNeighbors The maximum number of nearest neighbors
   * @param singleProbing True for using Single Probing; false for multiple probing
   * @param distCol Output column for storing the distance between each result record and the key
   * @return A dataset containing at most k items closest to the key. A distCol is added to show
   *         the distance between each record and the key.
   */
  @Since("2.1.0")
  def approxNearestNeighbors(
      dataset: Dataset[_],
      key: Vector,
      numNearestNeighbors: Int,
      singleProbing: Boolean,
      distCol: String): Dataset[_] = {
    require(numNearestNeighbors > 0, "The number of nearest neighbors cannot be less than 1")
    // Get Hash Value of the key
    val keyHash = hashFunction(key)
    val modelDataset: DataFrame = if (!dataset.columns.contains($(outputCol))) {
        transform(dataset)
      } else {
        dataset.toDF()
      }

    // In the origin dataset, find the hash value that is closest to the key
    val hashDistUDF = udf((x: Vector) => hashDistance(x, keyHash), DataTypes.DoubleType)
    val hashDistCol = hashDistUDF(col($(outputCol)))

    val modelSubset = if (singleProbing) {
      modelDataset.filter(hashDistCol === 0.0)
    } else {
      // Compute threshold to get exact k elements.
      val modelDatasetSortedByHash = modelDataset.sort(hashDistCol).limit(numNearestNeighbors)
      val thresholdDataset = modelDatasetSortedByHash.select(max(hashDistCol))
      val hashThreshold = thresholdDataset.take(1).head.getDouble(0)

      // Filter the dataset where the hash value is less than the threshold.
      modelDataset.filter(hashDistCol <= hashThreshold)
    }

    // Get the top k nearest neighbor by their distance to the key
    val keyDistUDF = udf((x: Vector) => keyDistance(x, key), DataTypes.DoubleType)
    val modelSubsetWithDistCol = modelSubset.withColumn(distCol, keyDistUDF(col($(inputCol))))
    modelSubsetWithDistCol.sort(distCol).limit(numNearestNeighbors)
  }

  /**
   * Overloaded method for approxNearestNeighbors. Use Single Probing as default way to search
   * nearest neighbors and "distCol" as default distCol.
   */
  @Since("2.1.0")
  def approxNearestNeighbors(
      dataset: Dataset[_],
      key: Vector,
      numNearestNeighbors: Int): Dataset[_] = {
    approxNearestNeighbors(dataset, key, numNearestNeighbors, true, "distCol")
  }

  /**
   * Preprocess step for approximate similarity join. Transform and explode the [[outputCol]] to
   * two explodeCols: entry and value. "entry" is the index in hash vector, and "value" is the
   * value of corresponding value of the index in the vector.
   *
   * @param dataset The dataset to transform and explode.
   * @param explodeCols The alias for the exploded columns, must be a seq of two strings.
   * @return A dataset containing idCol, inputCol and explodeCols
   */
  @Since("2.1.0")
  private[this] def processDataset(
      dataset: Dataset[_],
      inputName: String,
      explodeCols: Seq[String]): Dataset[_] = {
    require(explodeCols.size == 2, "explodeCols must be two strings.")
    val vectorToMap = udf((x: Vector) => x.asBreeze.iterator.toMap,
      MapType(DataTypes.IntegerType, DataTypes.DoubleType))
    val modelDataset: DataFrame = if (!dataset.columns.contains($(outputCol))) {
      transform(dataset)
    } else {
      dataset.toDF()
    }
    modelDataset.select(
      struct(col("*")).as(inputName),
      explode(vectorToMap(col($(outputCol)))).as(explodeCols))
  }

  /**
   * Recreate a column using the same column name but different attribute id. Used in approximate
   * similarity join.
   * @param dataset The dataset where a column need to recreate
   * @param colName The name of the column to recreate
   * @param tmpColName A temporary column name which does not conflict with existing columns
   * @return
   */
  @Since("2.1.0")
  private[this] def recreateCol(
      dataset: Dataset[_],
      colName: String,
      tmpColName: String): Dataset[_] = {
    dataset
      .withColumnRenamed(colName, tmpColName)
      .withColumn(colName, col(tmpColName))
      .drop(tmpColName)
  }

  /**
   * Join two dataset to approximately find all pairs of records whose distance are smaller than
   * the threshold. If the [[outputCol]] is missing, the method will transform the data; if the
   * [[outputCol]] exists, it will use the [[outputCol]]. This allows caching of the transformed
   * data when necessary.
   *
   * @param datasetA One of the datasets to join
   * @param datasetB Another dataset to join
   * @param threshold The threshold for the distance of record pairs
   * @param distCol Output column for storing the distance between each result record and the key
   * @param leftColName The alias of all columns of datasetA in the output Dataset
   * @param rightColName The alias of all columns of datasetB in the output Dataset
   * @return A joined dataset containing pairs of records. A distCol is added to show the distance
   *         between each pair of records.
   */
  @Since("2.1.0")
  def approxSimilarityJoin(
      datasetA: Dataset[_],
      datasetB: Dataset[_],
      threshold: Double,
      distCol: String,
      leftColName: String,
      rightColName: String): Dataset[_] = {

    val explodeCols = Seq("entry", "hashValue")
    val explodedA = processDataset(datasetA, leftColName, explodeCols)

    // If this is a self join, we need to recreate the inputCol of datasetB to avoid ambiguity.
    // TODO: Remove recreateCol logic once SPARK-17154 is resolved.
    val explodedB = if (datasetA != datasetB) {
      processDataset(datasetB, rightColName, explodeCols)
    } else {
      val recreatedB = recreateCol(datasetB, $(inputCol), s"${$(inputCol)}#${Random.nextString(5)}")
      processDataset(recreatedB, rightColName, explodeCols)
    }

    // Do a hash join on where the exploded hash values are equal.
    val joinedDataset = explodedA.join(explodedB, explodeCols)
      .drop(explodeCols: _*).distinct()

    // Add a new column to store the distance of the two records.
    val distUDF = udf((x: Vector, y: Vector) => keyDistance(x, y), DataTypes.DoubleType)
    val joinedDatasetWithDist = joinedDataset.select(col("*"),
      distUDF(col(s"$leftColName.${$(inputCol)}"), col(s"$rightColName.${$(inputCol)}")).as(distCol)
    )

    // Filter the joined datasets where the distance are smaller than the threshold.
    joinedDatasetWithDist.filter(col(distCol) < threshold)
  }

  /**
   * Overloaded method for approxSimilarityJoin. Use "distCol" as default distCol, "leftCol" as
   * default leftCol, rightCol as default rightCol
   */
  @Since("2.1.0")
  def approxSimilarityJoin(
      datasetA: Dataset[_],
      datasetB: Dataset[_],
      threshold: Double): Dataset[_] = {
    approxSimilarityJoin(datasetA, datasetB, threshold, "distCol", "leftCol", "rightCol")
  }
}

/**
 * Locality Sensitive Hashing for different metrics space. Support basic transformation with a new
 * hash column, approximate nearest neighbor search with a dataset and a key, and approximate
 * similarity join of two datasets.
 *
 * This LSH class implements OR-amplification: more than 1 hash functions can be chosen, and each
 * input vector are hashed by all hash functions. Two input vectors are defined to be in the same
 * bucket as long as ANY one of the hash value matches.
 *
 * References:
 * (1) Gionis, Aristides, Piotr Indyk, and Rajeev Motwani. "Similarity search in high dimensions
 * via hashing." VLDB 7 Sep. 1999: 518-529.
 * (2) Wang, Jingdong et al. "Hashing for similarity search: A survey." arXiv preprint
 * arXiv:1408.2927 (2014).
 */
@Since("2.1.0")
private[ml] abstract class LSH[T <: LSHModel[T]] extends Estimator[T] with LSHParams {
  self: Estimator[T] =>

  /** @group setParam */
  @Since("2.1.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("2.1.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("2.1.0")
  def setOutputDim(value: Int): this.type = set(outputDim, value)

  /**
   * Validate and create a new instance of concrete LSHModel. Because different LSHModel may have
   * different initial setting, developer needs to define how their LSHModel is created instead of
   * using reflection in this abstract class.
   * @param inputDim The dimension of the input dataset
   * @return A new LSHModel instance without any params
   */
  @Since("2.1.0")
  protected[this] def createRawLSHModel(inputDim: Int): T

  @Since("2.1.0")
  override def copy(extra: ParamMap): Estimator[T] = defaultCopy(extra)

  @Since("2.1.0")
  override def fit(dataset: Dataset[_]): T = {
    transformSchema(dataset.schema, logging = true)
    val inputDim = dataset.select(col($(inputCol))).head().get(0).asInstanceOf[Vector].size
    val model = createRawLSHModel(inputDim).setParent(this)
    copyValues(model)
  }
}
