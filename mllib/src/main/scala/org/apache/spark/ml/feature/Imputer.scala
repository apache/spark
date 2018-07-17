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
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCols}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Params for [[Imputer]] and [[ImputerModel]].
 */
private[feature] trait ImputerParams extends Params with HasInputCols with HasOutputCols {

  /**
   * The imputation strategy. Currently only "mean" and "median" are supported.
   * If "mean", then replace missing values using the mean value of the feature.
   * If "median", then replace missing values using the approximate median value of the feature.
   * Default: mean
   *
   * @group param
   */
  final val strategy: Param[String] = new Param(this, "strategy", s"strategy for imputation. " +
    s"If ${Imputer.mean}, then replace missing values using the mean value of the feature. " +
    s"If ${Imputer.median}, then replace missing values using the median value of the feature.",
    ParamValidators.inArray[String](Array(Imputer.mean, Imputer.median)))

  /** @group getParam */
  def getStrategy: String = $(strategy)

  /**
   * The placeholder for the missing values. All occurrences of missingValue will be imputed.
   * Note that null values are always treated as missing.
   * Default: Double.NaN
   *
   * @group param
   */
  final val missingValue: DoubleParam = new DoubleParam(this, "missingValue",
    "The placeholder for the missing values. All occurrences of missingValue will be imputed")

  /** @group getParam */
  def getMissingValue: Double = $(missingValue)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require($(inputCols).length == $(inputCols).distinct.length, s"inputCols contains" +
      s" duplicates: (${$(inputCols).mkString(", ")})")
    require($(outputCols).length == $(outputCols).distinct.length, s"outputCols contains" +
      s" duplicates: (${$(outputCols).mkString(", ")})")
    require($(inputCols).length == $(outputCols).length, s"inputCols(${$(inputCols).length})" +
      s" and outputCols(${$(outputCols).length}) should have the same length")
    val outputFields = $(inputCols).zip($(outputCols)).map { case (inputCol, outputCol) =>
      val inputField = schema(inputCol)
      SchemaUtils.checkColumnTypes(schema, inputCol, Seq(DoubleType, FloatType))
      StructField(outputCol, inputField.dataType, inputField.nullable)
    }
    StructType(schema ++ outputFields)
  }
}

/**
 * :: Experimental ::
 * Imputation estimator for completing missing values, either using the mean or the median
 * of the columns in which the missing values are located. The input columns should be of
 * DoubleType or FloatType. Currently Imputer does not support categorical features
 * (SPARK-15041) and possibly creates incorrect values for a categorical feature.
 *
 * Note that the mean/median value is computed after filtering out missing values.
 * All Null values in the input columns are treated as missing, and so are also imputed. For
 * computing median, DataFrameStatFunctions.approxQuantile is used with a relative error of 0.001.
 */
@Experimental
@Since("2.2.0")
class Imputer @Since("2.2.0") (@Since("2.2.0") override val uid: String)
  extends Estimator[ImputerModel] with ImputerParams with DefaultParamsWritable {

  @Since("2.2.0")
  def this() = this(Identifiable.randomUID("imputer"))

  /** @group setParam */
  @Since("2.2.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("2.2.0")
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  /**
   * Imputation strategy. Available options are ["mean", "median"].
   * @group setParam
   */
  @Since("2.2.0")
  def setStrategy(value: String): this.type = set(strategy, value)

  /** @group setParam */
  @Since("2.2.0")
  def setMissingValue(value: Double): this.type = set(missingValue, value)

  setDefault(strategy -> Imputer.mean, missingValue -> Double.NaN)

  override def fit(dataset: Dataset[_]): ImputerModel = {
    transformSchema(dataset.schema, logging = true)
    val spark = dataset.sparkSession

    val cols = $(inputCols).map { inputCol =>
      when(col(inputCol).equalTo($(missingValue)), null)
        .when(col(inputCol).isNaN, null)
        .otherwise(col(inputCol))
        .cast("double")
        .as(inputCol)
    }

    val results = $(strategy) match {
      case Imputer.mean =>
        // Function avg will ignore null automatically.
        // For a column only containing null, avg will return null.
        val row = dataset.select(cols.map(avg): _*).head()
        Array.range(0, $(inputCols).length).map { i =>
          if (row.isNullAt(i)) {
            Double.NaN
          } else {
            row.getDouble(i)
          }
        }

      case Imputer.median =>
        // Function approxQuantile will ignore null automatically.
        // For a column only containing null, approxQuantile will return an empty array.
        dataset.select(cols: _*).stat.approxQuantile($(inputCols), Array(0.5), 0.001)
          .map { array =>
            if (array.isEmpty) {
              Double.NaN
            } else {
              array.head
            }
          }
    }

    val emptyCols = $(inputCols).zip(results).filter(_._2.isNaN).map(_._1)
    if (emptyCols.nonEmpty) {
      throw new SparkException(s"surrogate cannot be computed. " +
        s"All the values in ${emptyCols.mkString(",")} are Null, Nan or " +
        s"missingValue(${$(missingValue)})")
    }

    val rows = spark.sparkContext.parallelize(Seq(Row.fromSeq(results)))
    val schema = StructType($(inputCols).map(col => StructField(col, DoubleType, nullable = false)))
    val surrogateDF = spark.createDataFrame(rows, schema)
    copyValues(new ImputerModel(uid, surrogateDF).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): Imputer = defaultCopy(extra)
}

@Since("2.2.0")
object Imputer extends DefaultParamsReadable[Imputer] {

  /** strategy names that Imputer currently supports. */
  private[feature] val mean = "mean"
  private[feature] val median = "median"

  @Since("2.2.0")
  override def load(path: String): Imputer = super.load(path)
}

/**
 * :: Experimental ::
 * Model fitted by [[Imputer]].
 *
 * @param surrogateDF a DataFrame containing inputCols and their corresponding surrogates,
 *                    which are used to replace the missing values in the input DataFrame.
 */
@Experimental
@Since("2.2.0")
class ImputerModel private[ml] (
    @Since("2.2.0") override val uid: String,
    @Since("2.2.0") val surrogateDF: DataFrame)
  extends Model[ImputerModel] with ImputerParams with MLWritable {

  import ImputerModel._

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val surrogates = surrogateDF.select($(inputCols).map(col): _*).head().toSeq

    val newCols = $(inputCols).zip($(outputCols)).zip(surrogates).map {
      case ((inputCol, outputCol), surrogate) =>
        val inputType = dataset.schema(inputCol).dataType
        val ic = col(inputCol)
        when(ic.isNull, surrogate)
          .when(ic === $(missingValue), surrogate)
          .otherwise(ic)
          .cast(inputType)
    }
    dataset.withColumns($(outputCols), newCols).toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): ImputerModel = {
    val copied = new ImputerModel(uid, surrogateDF)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("2.2.0")
  override def write: MLWriter = new ImputerModelWriter(this)
}


@Since("2.2.0")
object ImputerModel extends MLReadable[ImputerModel] {

  private[ImputerModel] class ImputerModelWriter(instance: ImputerModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.surrogateDF.repartition(1).write.parquet(dataPath)
    }
  }

  private class ImputerReader extends MLReader[ImputerModel] {

    private val className = classOf[ImputerModel].getName

    override def load(path: String): ImputerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val surrogateDF = sqlContext.read.parquet(dataPath)
      val model = new ImputerModel(metadata.uid, surrogateDF)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("2.2.0")
  override def read: MLReader[ImputerModel] = new ImputerReader

  @Since("2.2.0")
  override def load(path: String): ImputerModel = super.load(path)
}
