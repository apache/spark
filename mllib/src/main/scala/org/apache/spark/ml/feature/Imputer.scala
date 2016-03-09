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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
  * Params for [[Imputer]] and [[ImputerModel]].
  */
private[feature] trait ImputerParams extends Params with HasInputCol with HasOutputCol {

  /**
    * The imputation strategy.
    * If "mean", then replace missing values using the mean along the axis.
    * If "median", then replace missing values using the median along the axis.
    * If "most", then replace missing using the most frequent value along the axis.
    * Default: mean
    *
    * @group param
    */
  val strategy: Param[String] = new Param(this, "strategy", "strategy for imputation. " +
    "If mean, then replace missing values using the mean along the axis." +
    "If median, then replace missing values using the median along the axis." +
    "If most, then replace missing using the most frequent value along the axis.")

  /** @group getParam */
  def getStrategy: String = $(strategy)

  /**
    * The placeholder for the missing values. All occurrences of missingvalues will be imputed.
    * Default: Double.NaN
    *
    * @group param
    */
  val missingValue: DoubleParam = new DoubleParam(this, "missingValue",
    "The placeholder for the missing values. All occurrences of missingvalues will be imputed")

  /** @group getParam */
  def getMissingValue: Double = $(missingValue)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    validateParams()
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT] || inputType.isInstanceOf[DoubleType],
      s"Input column ${$(inputCol)} must of type Vector or Double")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

  override def validateParams(): Unit = {
    require(Seq("mean", "median", "most").contains($(strategy)),
      s"${$(strategy)} is not supported. Options are mean, median and most")
  }
}

/**
 * :: Experimental ::
 * Imputation estimator for completing missing values, either using the mean, the median or
 * the most frequent value of the column in which the missing values are located. This class
 * also allows for different missing values encodings.
 *
 */
@Experimental
class Imputer @Since("2.0.0")(override val uid: String)
  extends Estimator[ImputerModel] with ImputerParams with DefaultParamsWritable {

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("imputer"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setStrategy(value: String): this.type = set(strategy, value)

  /** @group setParam */
  def setMissingValue(value: Double): this.type = set(missingValue, value)

  setDefault(strategy -> "mean", missingValue -> Double.NaN)

  override def fit(dataset: DataFrame): ImputerModel = {
    val alternate = dataset.select($(inputCol)).schema.fields(0).dataType match {
      case DoubleType =>
        val colStatistics = getColStatistics(dataset, $(inputCol))
        Vectors.dense(Array(colStatistics))
      case _: VectorUDT =>
        val vl = dataset.first().getAs[Vector]($(inputCol)).size
        val statisticsArray = new Array[Double](vl)
        (0 until vl).foreach(i => {
          val getI = udf((v: Vector) => v(i))
          val tempColName = $(inputCol) + i
          val tempData = dataset.where(s"${$(inputCol)} is not null")
            .select($(inputCol)).withColumn(tempColName, getI(col($(inputCol))))
          statisticsArray(i) = getColStatistics(tempData, tempColName)
        })
        Vectors.dense(statisticsArray)
    }
    copyValues(new ImputerModel(uid, alternate).setParent(this))
  }

  private def getColStatistics(dataset: DataFrame, colName: String): Double = {
    val missValue = $(missingValue) match {
      case Double.NaN => "NaN"
      case _ => $(missingValue).toString
    }
    val colStatistics = $(strategy) match {
      case "mean" =>
        dataset.where(s"$colName != '$missValue'").selectExpr(s"avg($colName)").first().getDouble(0)
      case "median" =>
        // TODO: optimize the sort with quick-select or Percentile(Hive) if required
        val rddDouble = dataset.select(colName).where(s"$colName != $missValue").rdd
          .map(_.getDouble(0))
        rddDouble.sortBy(d => d).zipWithIndex().map {
          case (v, idx) => (idx, v)
        }.lookup(rddDouble.count()/2).head
      case "most" =>
        val input = dataset.where(s"$colName != $missValue").select(colName).rdd
          .map(_.getDouble(0))
        val most = input.map(d => (d, 1)).reduceByKey(_ + _).sortBy(-_._2).first()._1
        most
    }
    colStatistics
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): Imputer = {
    val copied = new Imputer(uid)
    copyValues(copied, extra)
  }
}

/**
  * :: Experimental ::
  * Model fitted by [[Imputer]].
  *
  * @param alternate statistics value for each original column during fitting
  */
@Experimental
class ImputerModel private[ml] (
    override val uid: String,
    val alternate: Vector)
  extends Model[ImputerModel] with ImputerParams with MLWritable {

  import ImputerModel._

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  private def matchMissingValue(value: Double): Boolean = {
    val miss = $(missingValue)
    value == miss || (value.isNaN && miss.isNaN)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    dataset.select($(inputCol)).schema.fields(0).dataType match {
      case DoubleType =>
        val impute = udf { (d: Double) =>
          if (matchMissingValue(d)) alternate(0) else d
        }
        dataset.withColumn($(outputCol), impute(col($(inputCol))))
      case _: VectorUDT =>
        val impute = udf { (vector: Vector) =>
          if (vector == null) {
            alternate
          }
          else {
            val vCopy = vector.copy
            vCopy match {
              case d: DenseVector =>
                var iter = 0
                while(iter < d.size) {
                  if (matchMissingValue(vCopy(iter))) {
                    d.values(iter) = alternate(iter)
                  }

                  iter += 1
                }
              case s: SparseVector =>
                var iter = 0
                while(iter < s.values.size) {
                  if (matchMissingValue(s.values(iter))) {
                    s.values(iter) = alternate(s.indices(iter))
                  }
                  iter += 1
                }
            }
            vCopy
          }
        }
        dataset.withColumn($(outputCol), impute(col($(inputCol))))
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): ImputerModel = {
    val copied = new ImputerModel(uid, alternate)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("2.0.0")
  override def write: MLWriter = new ImputerModelWriter(this)
}


@Since("2.0.0")
object ImputerModel extends MLReadable[ImputerModel] {

  private[ImputerModel]
  class ImputerModelWriter(instance: ImputerModel) extends MLWriter {

    private case class Data(alternate: Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = new Data(instance.alternate)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class ImputerReader extends MLReader[ImputerModel] {

    private val className = classOf[ImputerModel].getName

    override def load(path: String): ImputerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val Row(alternate: Vector) = sqlContext.read.parquet(dataPath)
        .select("alternate")
        .head()
      val model = new ImputerModel(metadata.uid, alternate)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("2.0.0")
  override def read: MLReader[ImputerModel] = new ImputerReader

  @Since("2.0.0")
  override def load(path: String): ImputerModel = super.load(path)
}
