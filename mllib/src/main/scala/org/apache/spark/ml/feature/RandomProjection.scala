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
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
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
trait RPParams  extends Params with HasInputCol with HasOutputCol{
  var intrinsicDimensionParam: DoubleParam = new DoubleParam(this,
                                                            "intrinsicDimension",
                                                            "dimension to project into")

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setIntrinsicDimension(value: Double): RPParams = {
    set(ParamPair(intrinsicDimensionParam, value))
  }

  def validateParams(map: ParamMap): Unit = {
    getInputCol
    require(map.contains(intrinsicDimensionParam), "please specify the intrinsic dimension")
    require(map.contains(inputCol), "please specify input column")
    require(map.contains(outputCol), "please specify output column")
  }

  /** @group getParam */
  def getIntrinsicDimension: Double = $(intrinsicDimensionParam)

  /**
   *
   * @param schema
   * @param paramMap
   * @return
   */
  def validateAndTransformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    val inputfeature = schema(map(inputCol)).dataType
    val outputFields = schema.fields :+ StructField(map(outputCol), inputfeature, false)
    StructType(outputFields)
  }
}

/**
 * :: Experimental ::
 *
 * The model that actually performs the reduction
 */
@Experimental
class RPModel(override val uid: String,
              val parentModel: Estimator[RPModel],
              val fittingParamMap: ParamMap,
              randomProjectionMatrix: Matrix,
              rp: feature.RandomProjection,
              origDimension: Int) extends Model[RPModel] with RPParams {

  parent = parentModel

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
    dataSet.withColumn(map(outputCol),
      callUDF((row: Any) => applyRP(row),
        dataSet.schema(map(inputCol)).dataType,
        dataSet(map(inputCol))))
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
    val mat: RowMatrix = new RowMatrix(dataset.select(map(inputCol)).map {
      case (row: Vector) => row
      case (row: GenericRow) => row.getAs[Vector](0)
    })
    mat
  }

}

/**
 * :: Experimental ::
 *
 * Estimates the model based on input-data
 */
@Experimental
class RandomProjection(override val uid: String)
  extends Estimator[RPModel] with RPParams {

  def this() = this(Identifiable.randomUID("random_projection"))

  /**
   * fit model to data
   * @param dataset
   * @return
   */
  override def fit(dataset: DataFrame): RPModel = {
    val map = extractParamMap()
    validateParams(map)

    val inputColumnValue = map(inputCol)
    require(dataset.schema.fieldNames.contains(inputColumnValue),
            s"input column '${inputColumnValue}' does not exist")

    val dimension = getIntrinsicDimension
    val origDimension = getOriginalDimension(dataset, map)
    val rp = new feature.RandomProjection(dimension.toInt)
    val matrix = rp.computeRPMatrix(dataset.sqlContext.sparkContext, origDimension)
    val model = new RPModel(uid, this, map, matrix.toLocalMatrix(), rp, origDimension)
    model.setInputCol(map(inputCol))
    model.setOutputCol(map(outputCol))
    model.setIntrinsicDimension(dimension)
    model
  }

  /**
   * reason about the original dimension as it is needed for the RP
   * @param dataSet
   * @param params
   * @return
   */
  def getOriginalDimension(dataSet: DataFrame, params: ParamMap = extractParamMap()): Int = {
    val col = params(inputCol)
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
}
