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

import org.apache.spark.Logging
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.param._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.api.java.JavaSchemaRDD
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.expressions.ScalaUdf
import org.apache.spark.sql.catalyst.types._

/**
 * :: AlphaComponent ::
 * Abstract class for transformers that transform one dataset into another.
 */
@AlphaComponent
abstract class Transformer extends PipelineStage with Params {

  /**
   * Transforms the dataset with optional parameters
   * @param dataset input dataset
   * @param paramPairs optional list of param pairs, overwrite embedded params
   * @return transformed dataset
   */
  @varargs
  def transform(dataset: SchemaRDD, paramPairs: ParamPair[_]*): SchemaRDD = {
    val map = new ParamMap()
    paramPairs.foreach(map.put(_))
    transform(dataset, map)
  }

  /**
   * Transforms the dataset with provided parameter map as additional parameters.
   * @param dataset input dataset
   * @param paramMap additional parameters, overwrite embedded params
   * @return transformed dataset
   */
  def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD

  // Java-friendly versions of transform.

  /**
   * Transforms the dataset with optional parameters.
   * @param dataset input datset
   * @param paramPairs optional list of param pairs, overwrite embedded params
   * @return transformed dataset
   */
  @varargs
  def transform(dataset: JavaSchemaRDD, paramPairs: ParamPair[_]*): JavaSchemaRDD = {
    transform(dataset.schemaRDD, paramPairs: _*).toJavaSchemaRDD
  }

  /**
   * Transforms the dataset with provided parameter map as additional parameters.
   * @param dataset input dataset
   * @param paramMap additional parameters, overwrite embedded params
   * @return transformed dataset
   */
  def transform(dataset: JavaSchemaRDD, paramMap: ParamMap): JavaSchemaRDD = {
    transform(dataset.schemaRDD, paramMap).toJavaSchemaRDD
  }
}

/**
 * Abstract class for transformers that take one input column, apply transformation, and output the
 * result as a new column.
 */
private[ml] abstract class UnaryTransformer[IN, OUT, T <: UnaryTransformer[IN, OUT, T]]
  extends Transformer with HasInputCol with HasOutputCol with Logging {

  def setInputCol(value: String): T = set(inputCol, value).asInstanceOf[T]
  def setOutputCol(value: String): T = set(outputCol, value).asInstanceOf[T]

  /**
   * Creates the transform function using the given param map. The input param map already takes
   * account of the embedded param map. So the param values should be determined solely by the input
   * param map.
   */
  protected def createTransformFunc(paramMap: ParamMap): IN => OUT

  /**
   * Returns the data type of the output column.
   */
  protected def outputDataType: DataType

  /**
   * Validates the input type. Throw an exception if it is invalid.
   */
  protected def validateInputType(inputType: DataType): Unit = {}

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = this.paramMap ++ paramMap
    val inputType = schema(map(inputCol)).dataType
    validateInputType(inputType)
    if (schema.fieldNames.contains(map(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${map(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField(map(outputCol), outputDataType, !outputDataType.isPrimitive)
    StructType(outputFields)
  }

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    transformSchema(dataset.schema, paramMap, logging = true)
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    val udf = ScalaUdf(this.createTransformFunc(map), outputDataType, Seq(map(inputCol).attr))
    dataset.select(Star(None), udf as map(outputCol))
  }
}
