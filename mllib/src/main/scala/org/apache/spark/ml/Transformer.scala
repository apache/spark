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

package org.apache.spark.ml

import scala.annotation.varargs
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Abstract class for transformers that transform one dataset into another.
 */
abstract class Transformer extends PipelineStage {

  /**
   * Transforms the dataset with optional parameters
   * @param dataset input dataset
   * @param firstParamPair the first param pair, overwrite embedded params
   * @param otherParamPairs other param pairs, overwrite embedded params
   * @return transformed dataset
   */
  @Since("2.0.0")
  @varargs
  def transform(
      dataset: Dataset[_],
      firstParamPair: ParamPair[_],
      otherParamPairs: ParamPair[_]*): DataFrame = {
    val map = new ParamMap()
      .put(firstParamPair)
      .put(otherParamPairs: _*)
    transform(dataset, map)
  }

  /**
   * Transforms the dataset with provided parameter map as additional parameters.
   * @param dataset input dataset
   * @param paramMap additional parameters, overwrite embedded params
   * @return transformed dataset
   */
  @Since("2.0.0")
  def transform(dataset: Dataset[_], paramMap: ParamMap): DataFrame = {
    this.copy(paramMap).transform(dataset)
  }

  /**
   * Transforms the input dataset.
   */
  @Since("2.0.0")
  def transform(dataset: Dataset[_]): DataFrame

  override def copy(extra: ParamMap): Transformer
}

/**
 * Abstract class for transformers that take one input column, apply transformation, and output the
 * result as a new column.
 */
abstract class UnaryTransformer[IN: TypeTag, OUT: TypeTag, T <: UnaryTransformer[IN, OUT, T]]
  extends Transformer with HasInputCol with HasOutputCol with Logging {

  /** @group setParam */
  def setInputCol(value: String): T = set(inputCol, value).asInstanceOf[T]

  /** @group setParam */
  def setOutputCol(value: String): T = set(outputCol, value).asInstanceOf[T]

  /**
   * Creates the transform function using the given param map. The input param map already takes
   * account of the embedded param map. So the param values should be determined solely by the input
   * param map.
   */
  protected def createTransformFunc: IN => OUT

  /**
   * Returns the data type of the output column.
   */
  protected def outputDataType: DataType

  /**
   * Validates the input type. Throw an exception if it is invalid.
   */
  protected def validateInputType(inputType: DataType): Unit = {}

  override def transformSchema(schema: StructType): StructType = {
    val inputType = SchemaUtils.getSchemaFieldType(schema, $(inputCol))
    validateInputType(inputType)
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), outputDataType, nullable = false)
    StructType(outputFields)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val transformUDF = udf(this.createTransformFunc)
    dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  override def copy(extra: ParamMap): T = defaultCopy(extra)
}
