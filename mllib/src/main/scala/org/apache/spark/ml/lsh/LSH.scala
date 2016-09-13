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
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

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

  protected var modelDataset: DataFrame = null

  /**
   * :: DeveloperApi ::
   *
   * The hash function of LSH, mapping a predefined KeyType to a Vector
   * @return The mapping of LSH function.
   */
  protected[this] val hashFunction: KeyType => Vector


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
