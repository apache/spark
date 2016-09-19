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

package org.apache.spark.ml.lsh

import scala.util.Random

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Params for [[LSH]].
 */
private[ml] trait LSHParams extends HasInputCol with HasOutputCol {
  /**
   * Param for output dimension.
   *
   * @group param
   */
  final val outputDim: IntParam = new IntParam(this, "outputDim", "output dimension",
    ParamValidators.gt(0))

  /** @group getParam */
  final def getOutputDim: Int = $(outputDim)

  setDefault(outputDim -> 1)

  setDefault(outputCol -> "lsh_output")

  /**
   * Transform the Schema for LSH
   * @param schema The schema of the input dataset without outputCol
   * @return A derived schema with outputCol added
   */
  final def transformLSHSchema(schema: StructType): StructType = {
    val outputFields = schema.fields :+
      StructField($(outputCol), new VectorUDT, nullable = false)
    StructType(outputFields)
  }
}

/**
 * Model produced by [[LSH]].
 */
abstract class LSHModel[KeyType, T <: LSHModel[KeyType, T]] private[ml]
  extends Model[T] with LSHParams {
  override def copy(extra: ParamMap): T = defaultCopy(extra)
  /**
   * :: DeveloperApi ::
   *
   * The hash function of LSH, mapping a predefined KeyType to a Vector
   * @return The mapping of LSH function.
   */
  protected[this] val hashFunction: KeyType => Vector

  /**
   * :: DeveloperApi ::
   *
   * Calculate the distance between two different keys using the distance metric corresponding
   * to the hashFunction
   * @param x One of the point in the metric space
   * @param y Another the point in the metric space
   * @return The distance between x and y in double
   */
  protected[ml] def keyDistance(x: KeyType, y: KeyType): Double

  /**
   * :: DeveloperApi ::
   *
   * Calculate the distance between two different hash Vectors. By default, the distance is the
   * minimum distance of two hash values in any dimension.
   *
   * @param x One of the hash vector
   * @param y Another hash vector
   * @return The distance between hash vectors x and y in double
   */
  protected[ml] def hashDistance(x: Vector, y: Vector): Double = {
    (x.asBreeze - y.asBreeze).toArray.map(math.abs).min
  }

  /**
   * Transforms the input dataset.
   */
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val transformUDF = udf(hashFunction, new VectorUDT)
    dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
  }

  /**
   * :: DeveloperApi ::
   *
   * Check transform validity and derive the output schema from the input schema.
   *
   * Typical implementation should first conduct verification on schema change and parameter
   * validity, including complex parameter interaction checks.
   */
  override def transformSchema(schema: StructType): StructType = {
    transformLSHSchema(schema)
  }

  /**
   * Given a large dataset and an item, approximately find at most k items which have the closest
   * distance to the item.
   * @param key The key to hash for the item
   * @param k The maximum number of items closest to the key
   * @param distCol The column to store the distance between pairs
   * @return A dataset containing at most k items closest to the key. A distCol is added to show
   *         the distance between each record and the key.
   */
  def approxNearestNeighbors(dataset: Dataset[_], key: KeyType, k: Int = 1,
                             distCol: String = "distance"): Dataset[_] = {
    if (k < 1) {
      throw new Exception(s"Invalid number of nearest neighbors $k")
    }
    // Get Hash Value of the key v
    val keyHash = hashFunction(key)
    val modelDataset = transform(dataset)

    // In the origin dataset, find the hash value u that is closest to v
    val hashDistUDF = udf((x: Vector) => hashDistance(x, keyHash), DataTypes.DoubleType)
    val nearestHashDataset = modelDataset.select(min(hashDistUDF(col($(outputCol)))))
    val nearestHashValue = nearestHashDataset.collect()(0)(0).asInstanceOf[Double]

    // Filter the dataset where the hash value equals to u
    val modelSubset = modelDataset.filter(hashDistUDF(col($(outputCol))) === nearestHashValue)

    // Get the top k nearest neighbor by their distance to the key
    val keyDistUDF = udf((x: KeyType) => keyDistance(x, key), DataTypes.DoubleType)
    val modelSubsetWithDistCol = modelSubset.withColumn(distCol, keyDistUDF(col($(inputCol))))
    modelSubsetWithDistCol.sort(distCol).limit(k)
  }

  /**
   * Preprocess step for approximate similarity join. Transform and explode the outputCol to
   * explodeCols.
   * @param dataset The dataset to transform and explode.
   * @param explodeCols The alias for the exploded columns, must be a seq of two strings.
   * @return A dataset containing idCol, inputCol and explodeCols
   */
  private[this] def processDataset(dataset: Dataset[_], explodeCols: Seq[String]): Dataset[_] = {
    if (explodeCols.size != 2) {
      throw new Exception("explodeCols must be two strings.")
    }
    val vectorToMap: UserDefinedFunction = udf((x: Vector) => x.asBreeze.iterator.toMap,
      MapType(DataTypes.IntegerType, DataTypes.DoubleType))
    transform(dataset)
      .select(col("*"), explode(vectorToMap(col($(outputCol)))).as(explodeCols))
  }

  /**
   * Recreate a column using the same column name but different attribute id. Used in approximate
   * similarity join.
   * @param dataset The dataset where a column need to recreate
   * @param colName The name of the column to recreate
   * @param tmpColName A temporary column name which does not conflict with existing columns
   * @return
   */
  private[this] def recreateCol(dataset: Dataset[_], colName: String,
                                tmpColName: String): Dataset[_] = {
    dataset
      .withColumnRenamed(colName, tmpColName)
      .withColumn(colName, col(tmpColName))
      .drop(tmpColName)
  }

  /**
   * Join two dataset to approximately find all pairs of records whose distance are smaller
   * than the threshold.
   * @param datasetA One of the datasets to join
   * @param datasetB Another dataset to join
   * @param threshold The threshold for the distance of record pairs
   * @param distCol The column to store the distance between pairs
   * @return A joined dataset containing pairs of records. A distCol is added to show the distance
   *         between each pair of records.
   */
  def approxSimilarityJoin(datasetA: Dataset[_], datasetB: Dataset[_], threshold: Double,
                           distCol: String = "distance"): Dataset[_] = {

    val explodeCols = Seq("lsh#entry", "lsh#hashValue")
    val explodedA = processDataset(datasetA, explodeCols)

    // If this is a self join, we need to recreate the inputCol of datasetB to avoid ambiguity.
    val explodedB = if (datasetA != datasetB) {
      processDataset(datasetB, explodeCols)
    } else {
      val recreatedB = recreateCol(datasetB, $(inputCol), s"${$(inputCol)}#${Random.nextString(5)}")
      processDataset(recreatedB, explodeCols)
    }

    // Do a hash join on where the exploded hash values are equal.
    val joinedDataset = explodedA.join(explodedB, explodeCols)
      .drop(explodeCols: _*)

    // Add a new column to store the distance of the two records.
    val distUDF = udf((x: KeyType, y: KeyType) => keyDistance(x, y), DataTypes.DoubleType)
    val joinedDatasetWithDist = joinedDataset.select(col("*"),
      distUDF(explodedA($(inputCol)), explodedB($(inputCol))).as(distCol)
    )

    // Filter the joined datasets where the distance are smaller than the threshold.
    joinedDatasetWithDist.distinct().filter(col(distCol) < threshold)
  }
}

abstract class LSH[KeyType, T <: LSHModel[KeyType, T]] extends Estimator[T] with LSHParams {
  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setOutputDim(value: Int): this.type = set(outputDim, value)

  /**
   * :: DeveloperApi ::
   *
   * Validate and create a new instance of concrete LSHModel. Because different LSHModel may have
   * different initial setting, developer needs to define how their LSHModel is created instead of
   * using reflection in this abstract class.
   * @param inputDim the input dimension of input dataset
   * @return A new LSHModel instance without any params
   */
  protected[this] def createRawLSHModel(inputDim: Int): T

  override def copy(extra: ParamMap): Estimator[T] = defaultCopy(extra)

  /**
   * Fits a model to the input data.
   */
  override def fit(dataset: Dataset[_]): T = {
    val inputDim = dataset.select(col($(inputCol))).head().get(0).asInstanceOf[Vector].size
    val model = createRawLSHModel(inputDim).setParent(this)
    copyValues(model)
  }

  /**
   * :: DeveloperApi ::
   *
   * Check transform validity and derive the output schema from the input schema.
   *
   * Typical implementation should first conduct verification on schema change and parameter
   * validity, including complex parameter interaction checks.
   */
  override def transformSchema(schema: StructType): StructType = {
    transformLSHSchema(schema)
  }
}
