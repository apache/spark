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

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.NumericType

/**
 * Params for [[Imputer]] and [[ImputerModel]].
 */
private[feature] trait ImputerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * The imputation strategy.
   * If "mean", then replace missing values using the mean value of the feature.
   * If "median", then replace missing values using the median value of the feature.
   * If "most", then replace missing using the most frequent value of the feature.
   * Default: mean
   *
   * @group param
   */
  val strategy: Param[String] = new Param(this, "strategy", "strategy for imputation. " +
    "If mean, then replace missing values using the mean value of the feature." +
    "If median, then replace missing values using the median value of the feature." +
    "If most, then replace missing using the most frequent value of the feature.",
    ParamValidators.inArray[String](Imputer.supportedStrategyNames.toArray))

  /** @group getParam */
  def getStrategy: String = $(strategy)

  /**
   * The placeholder for the missing values. All occurrences of missingValue will be imputed.
   * Default: Double.NaN
   *
   * @group param
   */
  val missingValue: DoubleParam = new DoubleParam(this, "missingValue",
    "The placeholder for the missing values. All occurrences of missingValue will be imputed")

  /** @group getParam */
  def getMissingValue: Double = $(missingValue)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[NumericType],
      s"Input column ${$(inputCol)} must be of NumericType")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+
      StructField($(outputCol), inputType, schema($(inputCol)).nullable)
    StructType(outputFields)
  }

}

/**
 * :: Experimental ::
 * Imputation estimator for completing missing values, either using the mean("mean"), the
 * median("median") or the most frequent value("most") of the column in which the missing
 * values are located.
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

  /**
   * Imputation strategy. Available options are ["mean", "median" and "most"].
   * @group setParam
   */
  def setStrategy(value: String): this.type = set(strategy, value)

  /** @group setParam */
  def setMissingValue(value: Double): this.type = set(missingValue, value)

  setDefault(strategy -> "mean", missingValue -> Double.NaN)

  override def fit(dataset: Dataset[_]): ImputerModel = {
    val ic = col($(inputCol))
    val filtered = dataset.select(ic.cast(DoubleType))
      .filter(ic.isNotNull && !ic.isNaN && ic =!= $(missingValue))
    val surrogate = $(strategy) match {
      case "mean" => filtered.select(avg($(inputCol))).first().getDouble(0)
      case "median" => filtered.stat.approxQuantile($(inputCol), Array(0.5), 0.001)(0)
      case "most" => filtered.rdd.map(r => r.getDouble(0)).map(d => (d, 1)).reduceByKey(_ + _)
        .sortBy(-_._2).first()._1
    }
    copyValues(new ImputerModel(uid, surrogate).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): Imputer = {
    val copied = new Imputer(uid)
    copyValues(copied, extra)
  }
}

@Since("2.0.0")
object Imputer extends DefaultParamsReadable[Imputer] {

  /** Set of strategy names that Imputer currently supports. */
  private[ml] val supportedStrategyNames = Set("mean", "median", "most")

  @Since("2.0.0")
  override def load(path: String): Imputer = super.load(path)
}

/**
 * :: Experimental ::
 * Model fitted by [[Imputer]].
 *
 * @param surrogate statistics value for each feature during fitting
 */
@Experimental
class ImputerModel private[ml](
    override val uid: String,
    val surrogate: Double)
  extends Model[ImputerModel] with ImputerParams with MLWritable {

  import ImputerModel._

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputType = dataset.select($(inputCol)).schema.fields(0).dataType
    inputType match {
      case _: NumericType =>
        val ic = col($(inputCol)).cast(DoubleType)
        dataset.withColumn($(outputCol), when(ic.isNull, surrogate)
          .when(ic.isNaN, surrogate)
          .when(ic === $(missingValue), surrogate)
          .otherwise(ic)
          .cast(inputType))
      case _ => throw new SparkException("imputer supports numeric type only")
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): ImputerModel = {
    val copied = new ImputerModel(uid, surrogate)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("2.0.0")
  override def write: MLWriter = new ImputerModelWriter(this)
}


@Since("2.0.0")
object ImputerModel extends MLReadable[ImputerModel] {

  private[ImputerModel]
  class ImputerModelWriter(instance: ImputerModel) extends MLWriter {

    private case class Data(surrogate: Double)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = new Data(instance.surrogate)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class ImputerReader extends MLReader[ImputerModel] {

    private val className = classOf[ImputerModel].getName

    override def load(path: String): ImputerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val Row(surrogate: Double) = sqlContext.read.parquet(dataPath)
        .select("surrogate")
        .head()
      val model = new ImputerModel(metadata.uid, surrogate)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("2.0.0")
  override def read: MLReader[ImputerModel] = new ImputerReader

  @Since("2.0.0")
  override def load(path: String): ImputerModel = super.load(path)
}
