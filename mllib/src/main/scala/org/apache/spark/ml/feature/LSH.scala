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

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.{IntParam, ParamValidators}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Params for [[LSH]].
 */
private[ml] trait LSHParams extends HasInputCol with HasOutputCol {
  /**
   * Param for the number of hash tables used in LSH OR-amplification.
   *
   * LSH OR-amplification can be used to reduce the false negative rate. Higher values for this
   * param lead to a reduced false negative rate, at the expense of added computational complexity.
   * @group param
   */
  final val numHashTables: IntParam = new IntParam(this, "numHashTables", "number of hash " +
    "tables, where increasing number of hash tables lowers the false negative rate, and " +
    "decreasing it improves the running performance", ParamValidators.gt(0))

  /** @group getParam */
  final def getNumHashTables: Int = $(numHashTables)

  setDefault(numHashTables -> 1)

  /**
   * Transform the Schema for LSH
   * @param schema The schema of the input dataset without [[outputCol]].
   * @return A derived schema with [[outputCol]] added.
   */
  protected[this] final def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.appendColumn(schema, $(outputCol), DataTypes.createArrayType(new VectorUDT))
  }
}

/**
 * Model produced by [[LSH]].
 */
private[ml] abstract class LSHModel[T <: LSHModel[T]]
  extends Model[T] with LSHParams with MLWritable {
  self: T =>

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * The hash function of LSH, mapping an input feature vector to multiple hash vectors.
   * @return The mapping of LSH function.
   */
  protected[ml] def hashFunction(elems: Vector): Array[Vector]

  /**
   * Calculate the distance between two different keys using the distance metric corresponding
   * to the hashFunction.
   * @param x One input vector in the metric space.
   * @param y One input vector in the metric space.
   * @return The distance between x and y.
   */
  protected[ml] def keyDistance(x: Vector, y: Vector): Double

  /**
   * Calculate the distance between two different hash Vectors.
   *
   * @param x One of the hash vector.
   * @param y Another hash vector.
   * @return The distance between hash vectors x and y.
   */
  protected[ml] def hashDistance(x: Seq[Vector], y: Seq[Vector]): Double

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val transformUDF = udf(hashFunction(_: Vector), DataTypes.createArrayType(new VectorUDT))
    dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  // TODO: Fix the MultiProbe NN Search in SPARK-18454
  private[feature] def approxNearestNeighbors(
      dataset: Dataset[_],
      key: Vector,
      numNearestNeighbors: Int,
      singleProbe: Boolean,
      distCol: String): Dataset[_] = {
    require(numNearestNeighbors > 0, "The number of nearest neighbors cannot be less than 1")
    // Get Hash Value of the key
    val keyHash = hashFunction(key)
    val modelDataset: DataFrame = if (!dataset.columns.contains($(outputCol))) {
        transform(dataset)
      } else {
        dataset.toDF()
      }

    val modelSubset = if (singleProbe) {
      def sameBucket(x: Seq[Vector], y: Seq[Vector]): Boolean = {
        x.zip(y).exists(tuple => tuple._1 == tuple._2)
      }

      // In the origin dataset, find the hash value that hash the same bucket with the key
      val sameBucketWithKeyUDF = udf((x: Seq[Vector]) =>
        sameBucket(x, keyHash), DataTypes.BooleanType)

      modelDataset.filter(sameBucketWithKeyUDF(col($(outputCol))))
    } else {
      // In the origin dataset, find the hash value that is closest to the key
      // Limit the use of hashDist since it's controversial
      val hashDistUDF = udf((x: Seq[Vector]) => hashDistance(x, keyHash), DataTypes.DoubleType)
      val hashDistCol = hashDistUDF(col($(outputCol)))

      // Compute threshold to get exact k elements.
      // TODO: SPARK-18409: Use approxQuantile to get the threshold
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
   * Given a large dataset and an item, approximately find at most k items which have the closest
   * distance to the item. If the [[outputCol]] is missing, the method will transform the data; if
   * the [[outputCol]] exists, it will use the [[outputCol]]. This allows caching of the
   * transformed data when necessary.
   *
   * @note This method is experimental and will likely change behavior in the next release.
   *
   * @param dataset The dataset to search for nearest neighbors of the key.
   * @param key Feature vector representing the item to search for.
   * @param numNearestNeighbors The maximum number of nearest neighbors.
   * @param distCol Output column for storing the distance between each result row and the key.
   * @return A dataset containing at most k items closest to the key. A column "distCol" is added
   *         to show the distance between each row and the key.
   */
  def approxNearestNeighbors(
    dataset: Dataset[_],
    key: Vector,
    numNearestNeighbors: Int,
    distCol: String): Dataset[_] = {
    approxNearestNeighbors(dataset, key, numNearestNeighbors, true, distCol)
  }

  /**
   * Overloaded method for approxNearestNeighbors. Use "distCol" as default distCol.
   */
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
   * @return A dataset containing idCol, inputCol and explodeCols.
   */
  private[this] def processDataset(
      dataset: Dataset[_],
      inputName: String,
      explodeCols: Seq[String]): Dataset[_] = {
    require(explodeCols.size == 2, "explodeCols must be two strings.")
    val modelDataset: DataFrame = if (!dataset.columns.contains($(outputCol))) {
      transform(dataset)
    } else {
      dataset.toDF()
    }
    modelDataset.select(
      struct(col("*")).as(inputName), posexplode(col($(outputCol))).as(explodeCols))
  }

  /**
   * Recreate a column using the same column name but different attribute id. Used in approximate
   * similarity join.
   * @param dataset The dataset where a column need to recreate.
   * @param colName The name of the column to recreate.
   * @param tmpColName A temporary column name which does not conflict with existing columns.
   * @return
   */
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
   * Join two datasets to approximately find all pairs of rows whose distance are smaller than
   * the threshold. If the [[outputCol]] is missing, the method will transform the data; if the
   * [[outputCol]] exists, it will use the [[outputCol]]. This allows caching of the transformed
   * data when necessary.
   *
   * @param datasetA One of the datasets to join.
   * @param datasetB Another dataset to join.
   * @param threshold The threshold for the distance of row pairs.
   * @param distCol Output column for storing the distance between each pair of rows.
   * @return A joined dataset containing pairs of rows. The original rows are in columns
   *         "datasetA" and "datasetB", and a column "distCol" is added to show the distance
   *         between each pair.
   */
  def approxSimilarityJoin(
      datasetA: Dataset[_],
      datasetB: Dataset[_],
      threshold: Double,
      distCol: String): Dataset[_] = {

    val leftColName = "datasetA"
    val rightColName = "datasetB"
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

    // Add a new column to store the distance of the two rows.
    val distUDF = udf((x: Vector, y: Vector) => keyDistance(x, y), DataTypes.DoubleType)
    val joinedDatasetWithDist = joinedDataset.select(col("*"),
      distUDF(col(s"$leftColName.${$(inputCol)}"), col(s"$rightColName.${$(inputCol)}")).as(distCol)
    )

    // Filter the joined datasets where the distance are smaller than the threshold.
    joinedDatasetWithDist.filter(col(distCol) < threshold)
  }

  /**
   * Overloaded method for approxSimilarityJoin. Use "distCol" as default distCol.
   */
  def approxSimilarityJoin(
      datasetA: Dataset[_],
      datasetB: Dataset[_],
      threshold: Double): Dataset[_] = {
    approxSimilarityJoin(datasetA, datasetB, threshold, "distCol")
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
private[ml] abstract class LSH[T <: LSHModel[T]]
  extends Estimator[T] with LSHParams with DefaultParamsWritable {
  self: Estimator[T] =>

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setNumHashTables(value: Int): this.type = set(numHashTables, value)

  /**
   * Validate and create a new instance of concrete LSHModel. Because different LSHModel may have
   * different initial setting, developer needs to define how their LSHModel is created instead of
   * using reflection in this abstract class.
   * @param inputDim The dimension of the input dataset
   * @return A new LSHModel instance without any params
   */
  protected[this] def createRawLSHModel(inputDim: Int): T

  override def fit(dataset: Dataset[_]): T = {
    transformSchema(dataset.schema, logging = true)
    val inputDim = dataset.select(col($(inputCol))).head().get(0).asInstanceOf[Vector].size
    val model = createRawLSHModel(inputDim).setParent(this)
    copyValues(model)
  }
}
