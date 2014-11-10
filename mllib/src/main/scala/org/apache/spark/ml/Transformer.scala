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

import org.apache.spark.ml.param._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.api.java.JavaSchemaRDD
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.dsl._

/**
 * Abstract class for transformers that transform one dataset into another.
 */
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
   * Transforms the dataset with provided parameter map.
   * @param dataset input dataset
   * @param paramMap parameters
   * @return transformed dataset
   */
  def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD

  // Java-friendly versions of transform.

  @varargs
  def transform(dataset: JavaSchemaRDD, paramPairs: ParamPair[_]*): JavaSchemaRDD = {
    transform(dataset.schemaRDD, paramPairs: _*).toJavaSchemaRDD
  }

  def transform(dataset: JavaSchemaRDD, paramMap: ParamMap): JavaSchemaRDD = {
    transform(dataset.schemaRDD, paramMap).toJavaSchemaRDD
  }
}

/**
 * Abstract class for transformers that take one input column, apply transformation, and output the
 * result as a new column.
 */
abstract class UnaryTransformer[IN, OUT: TypeTag, SELF <: UnaryTransformer[IN, OUT, SELF]]
    extends Transformer with HasInputCol with HasOutputCol {

  def setInputCol(value: String): SELF = { set(inputCol, value); this.asInstanceOf[SELF] }
  def setOutputCol(value: String): SELF = { set(outputCol, value); this.asInstanceOf[SELF] }

  /**
   * Creates the transform function using the given param map. The input param map already takes
   * account of the embedded param map. So the param values should be determined solely by the input
   * param map.
   */
  protected def createTransformFunc(paramMap: ParamMap): IN => OUT

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    val udf: IN => OUT = this.createTransformFunc(map)
    dataset.select(Star(None), udf.call(map(inputCol).attr) as map(outputCol))
  }
}
