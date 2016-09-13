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

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

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

  /**
   * Param for distance column name.
   *
   * @group param
   */
  final val distCol: Param[String] = new Param[String](this, "distCol", "distance column name")

  /** @group getParam */
  final def getOutputDim: Int = $(outputDim)

  /** @group getParam */
  final def getDistCol: String = $(distCol)

  setDefault(outputDim -> 1)

  setDefault(outputCol -> "lsh_output")

  setDefault(distCol -> "lsh_distance")

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

  protected var modelDataset: DataFrame = null

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
  protected[this] def keyDistance(x: KeyType, y: KeyType): Double

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
  protected[this] def hashDistance(x: Vector, y: Vector): Double = {
    (x.asBreeze - y.asBreeze).toArray.map(math.abs).min
  }

  /**
   * Transforms the input dataset.
   */
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val transformUDF = udf(hashFunction, new VectorUDT)
    modelDataset = dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
    modelDataset
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
   * Get the dataset inside the model. This is used in approximate similarity join or when user
   * wants to run their own algorithm on the LSH dataset.
   * @return The dataset inside the model
   */
  def getModelDataset: Dataset[_] = modelDataset

  /**
   * Given a large dataset and an item, approximately find at most k items which have the closest
   * distance to the item.
   * @param key The key to hash for the item
   * @param k The maximum number of items closest to the key
   * @return A dataset containing at most k items closest to the key.
   */
  def approxNearestNeighbors(key: KeyType, k: Int = 1): Dataset[_] = {
    if (k < 1) {
      throw new Exception(s"Invalid number of nearest neighbors $k")
    }
    // Get Hash Value of the key v
    val keyHash = hashFunction(key)

    // In the origin dataset, find the hash value u that is closest to v
    val hashDistUDF = udf((x: Vector) => hashDistance(x, keyHash), DataTypes.DoubleType)
    val nearestHashDataset = modelDataset.select(min(hashDistUDF(col($(outputCol)))))
    val nearestHashValue = nearestHashDataset.collect()(0)(0).asInstanceOf[Double]

    // Filter the dataset where the hash value equals to u
    val modelSubset = modelDataset.filter(hashDistUDF(col($(outputCol))) === nearestHashValue)

    // Get the top k nearest neighbor by their distance to the key
    val keyDistUDF = udf((x: KeyType) => keyDistance(x, key), DataTypes.DoubleType)
    val modelSubsetWithDistCol = modelSubset.withColumn($(distCol), keyDistUDF(col($(inputCol))))
    modelSubsetWithDistCol.sort($(distCol)).limit(k)
  }
}

abstract class LSH[KeyType, T <: LSHModel[KeyType, T]] extends Estimator[T] with LSHParams {
  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setOutputDim(value: Int): this.type = set(outputDim, value)

  /** @group setParam */
  def setDistCol(value: String): this.type = set(distCol, value)

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
    model.transform(dataset)
    model
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
