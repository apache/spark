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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param.{ParamPair, Params, ParamMap, Param}
import org.apache.spark.sql.types.StructType
import org.apache.spark.mllib.feature
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{DenseVector, Matrix, Vector, VectorUDT}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * :: Experimental ::
 *
 * Keeps parameter for the Random Projection
 */
@Experimental
trait RPParams  extends Params {
  var intrinsicDimension: Param[Double] = new Param(this, "dimension", "dimension to project into")
  var inputColumn: Param[String] = new Param(this, "inputColumn", "name of input column")
  var outputColumn: Param[String] = new Param(this, "outputColumn", "name of output column")
  def setInputColumn(value: String): RPParams = set(ParamPair(inputColumn, value))
  def setOutputColumn(value: String): RPParams = set(ParamPair(outputColumn, value))
  def setIntrinsicDimension(value: Double): RPParams = set(ParamPair(intrinsicDimension, value))
  def validateParams(map: ParamMap): Unit = {
    require(map.contains(intrinsicDimension), "please specify the intrinsic dimension")
    require(map.contains(inputColumn), "please specify input column")
    require(map.contains(outputColumn), "please specify output column")
  }

  /**
   *
   * @param schema
   * @param paramMap
   * @return
   */
  def validateAndTransformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    val inputfeature = schema(map(inputColumn)).dataType
    val outputFields = schema.fields :+ StructField(map(outputColumn), inputfeature, false)
    StructType(outputFields)
  }
}

/**
 * :: Experimental ::
 *
 * The model that actually performs the reduction
 */
@Experimental
class RPModel(override val parent: Estimator[RPModel],
              val fittingParamMap: ParamMap,
              randomProjectionMatrix: Matrix,
              rp: feature.RandomProjection,
              origDimension: Int) extends Model[RPModel] with RPParams {



  /**
   * prepare schema for the new DataFrame
   * @param schema
   * @return
   */
  def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, extractParamMap())
  }

  /**
   *
   * @param dataSet
   * @return
   */
  override def transform(dataSet: DataFrame): DataFrame = {
    val map = extractParamMap()
    validateParams(map)
    dataSet.withColumn(map(outputColumn),
      callUDF((row: Any) => applyRP(row),
        dataSet.schema(map(inputColumn)).dataType,
        dataSet(map(inputColumn))))
  }


  def applyRP(row: Any): Vector = {
    val vec: Vector = row match {
      case (row: Vector) => row
      case (row: GenericRow) => row.getAs[Vector](0)
    }
    randomProjectionMatrix.transpose.multiply(new DenseVector(vec.toArray))
  }


  /**
   * tranform the DataFrame into matrix to be able to perform some math on it
   * @param dataset
   * @param paramMap
   * @return
   */
  def createMatrix(dataset: DataFrame, paramMap: ParamMap): RowMatrix = {
    val map = extractParamMap(paramMap) ++ fittingParamMap
    val mat: RowMatrix = new RowMatrix(dataset.select(map(inputColumn)).map {
      case (row: Vector) => row
      case (row: GenericRow) => row.getAs[Vector](0)
    })
    mat
  }

  override val uid: String = _
}

/**
 * :: Experimental ::
 *
 * Estimates the model based on input-data
 */
@Experimental
class RandomProjection extends Estimator[RPModel] with RPParams {

  /**
   * fit model to data
   * @param dataset
   * @return
   */
  override def fit(dataset: DataFrame): RPModel = {
    val map = extractParamMap()
    validateParams(map)

    val inputColumnValue = map(inputColumn)
    require(dataset.schema.fieldNames.contains(inputColumnValue),
            s"input column '${inputColumnValue}' does not exist")

    val dimension = map(intrinsicDimension)
    val origDimension = getOriginalDimension(dataset, map)
    val rp = new feature.RandomProjection(dimension.toInt)
    val matrix = rp.computeRPMatrix(dataset.sqlContext.sparkContext, origDimension)
    val model = new RPModel(this, map, matrix.toLocalMatrix(), rp, origDimension)
    model.setInputColumn(map(inputColumn))
    model.setOutputColumn(map(outputColumn))
    model.setIntrinsicDimension(map(intrinsicDimension))
    model
  }

  /**
   * reason about the original dimension as it is needed for the RP
   * @param dataSet
   * @param params
   * @return
   */
  def getOriginalDimension(dataSet: DataFrame, params: ParamMap = extractParamMap()): Int = {
    val col = params(inputColumn)
    dataSet.select(col).first() match {
      case (row: Vector) => row.length
      case (row: GenericRow) => row.getAs[Vector](0).size
    }
  }

  /**
   *
   * @param schema
   * @return
   */
  def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, extractParamMap)
  }

  override val uid: String = _
}
