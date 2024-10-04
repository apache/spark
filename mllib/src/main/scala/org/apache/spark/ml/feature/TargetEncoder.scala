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

import scala.collection.immutable.ArraySeq

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Private trait for params and common methods for TargetEncoder and TargetEncoderModel */
private[ml] trait TargetEncoderBase extends Params with HasLabelCol
  with HasInputCol with HasInputCols with HasOutputCol with HasOutputCols with HasHandleInvalid {

  /**
   * Param for how to handle invalid data during transform().
   * Options are 'keep' (invalid data presented as an extra categorical feature) or
   * 'error' (throw an error).
   * Note that this Param is only used during transform; during fitting, invalid data
   * will result in an error.
   * Default: "error"
   * @group param
   */
  @Since("4.0.0")
  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "How to handle invalid data during transform(). " +
    "Options are 'keep' (invalid data presented as an extra categorical feature) " +
    "or error (throw an error). Note that this Param is only used during transform; " +
    "during fitting, invalid data will result in an error.",
    ParamValidators.inArray(TargetEncoder.supportedHandleInvalids))

  setDefault(handleInvalid -> TargetEncoder.ERROR_INVALID)

  @Since("4.0.0")
  val targetType: Param[String] = new Param[String](this, "targetType",
    "How to handle invalid data during transform(). " +
      "Options are 'keep' (invalid data presented as an extra categorical feature) " +
      "or error (throw an error). Note that this Param is only used during transform; " +
      "during fitting, invalid data will result in an error.",
    ParamValidators.inArray(TargetEncoder.supportedTargetTypes))

  setDefault(targetType -> TargetEncoder.TARGET_BINARY)

  final def getTargetType: String = $(targetType)

  @Since("4.0.0")
  val smoothing: DoubleParam = new DoubleParam(this, "smoothing",
    "lower bound of the output feature range",
    ParamValidators.gtEq(0.0))

  setDefault(smoothing -> 0.0)

  final def getSmoothing: Double = $(smoothing)

  private[feature] lazy val inputFeatures = if (isSet(inputCol)) Array($(inputCol))
                                            else if (isSet(inputCols)) $(inputCols)
                                            else Array.empty[String]

  private[feature] lazy val outputFeatures = if (isSet(outputCol)) Array($(outputCol))
                                    else if (isSet(outputCols)) $(outputCols)
                                    else inputFeatures.map{field: String => s"${field}_indexed"}

  private[feature] def validateSchema(schema: StructType,
                                      fitting: Boolean): StructType = {

    require(inputFeatures.length > 0,
      s"At least one input column must be specified.")

    require(inputFeatures.length == outputFeatures.length,
      s"The number of input columns ${inputFeatures.length} must be the same as the number of " +
        s"output columns ${outputFeatures.length}.")

    val features = if (fitting) inputFeatures :+ $(labelCol)
                  else inputFeatures

    features.foreach {
      feature => {
        try {
          val field = schema(feature)
          if (field.dataType != DoubleType) {
            throw new SparkException(s"Data type for column ${feature} is ${field.dataType}" +
              s", but ${DoubleType.typeName} is required.")
          }
        } catch {
          case e: IllegalArgumentException =>
            throw new SparkException(s"No column named ${feature} found on dataset.")
        }
      }
    }
    schema
  }

}

/**
 * Target Encoding maps a column of categorical indices into a numerical feature derived
 * from the target.
 *
 * When `handleInvalid` is configured to 'keep', previously unseen values of a feature
 * are mapped to the dataset overall statistics.
 *
 * When 'targetType' is configured to 'binary', categories are encoded as the conditional
 * probability of the target given that category (bin counting).
 * When 'targetType' is configured to 'continuous', categories are encoded as the average
 * of the target given that category (mean encoding)
 *
 * Parameter 'smoothing' controls how in-category stats and overall stats are weighted.
 *
 * @note When encoding multi-column by using `inputCols` and `outputCols` params, input/output cols
 * come in pairs, specified by the order in the arrays, and each pair is treated independently.
 *
 * @see `StringIndexer` for converting categorical values into category indices
 */
@Since("4.0.0")
class TargetEncoder @Since("4.0.0") (@Since("4.0.0") override val uid: String)
    extends Estimator[TargetEncoderModel] with TargetEncoderBase with DefaultParamsWritable {

  @Since("4.0.0")
  def this() = this(Identifiable.randomUID("TargetEncoder"))

  /** @group setParam */
  @Since("4.0.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("4.0.0")
  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */
  @Since("4.0.0")
  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  /** @group setParam */
  @Since("4.0.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /** @group setParam */
  @Since("4.0.0")
  def setTargetType(value: String): this.type = set(targetType, value)

  /** @group setParam */
  @Since("4.0.0")
  def setSmoothing(value: Double): this.type = set(smoothing, value)

  @Since("4.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema, fitting = true)
  }

  @Since("4.0.0")
  override def fit(dataset: Dataset[_]): TargetEncoderModel = {
    validateSchema(dataset.schema, fitting = true)

    val stats = dataset
      .select(ArraySeq.unsafeWrapArray(
        (inputFeatures :+ $(labelCol)).map(col)): _*)
      .rdd
      .treeAggregate(
        Array.fill(inputFeatures.length) {
          Map.empty[Double, (Double, Double)]
        })(
        (agg, row: Row) => {
          val label = row.getDouble(inputFeatures.length)
          Range(0, inputFeatures.length).map {
            feature => try {
              val value = row.getDouble(feature)
              if (value < 0.0 || value != value.toInt) throw new SparkException(
                  s"Values from column ${inputFeatures(feature)} must be indices, but got $value.")
              val counter = agg(feature).getOrElse(value, (0.0, 0.0))
              val globalCounter = agg(feature).getOrElse(TargetEncoder.UNSEEN_CATEGORY, (0.0, 0.0))
              $(targetType) match {
                case TargetEncoder.TARGET_BINARY =>
                  if (label == 1.0) agg(feature) +
                    (value -> (1 + counter._1, 1 + counter._2)) +
                    (TargetEncoder.UNSEEN_CATEGORY -> (1 + globalCounter._1, 1 + globalCounter._2))
                  else if (label == 0.0) agg(feature) +
                    (value -> (1 + counter._1, counter._2)) +
                    (TargetEncoder.UNSEEN_CATEGORY -> (1 + globalCounter._1, globalCounter._2))
                  else throw new SparkException(
                    s"Values from column ${getLabelCol} must be binary (0,1) but got $label.")
                case TargetEncoder.TARGET_CONTINUOUS => agg(feature) +
                  (value -> (1 + counter._1,
                    counter._2 + ((label - counter._2) / (1 + counter._1)))) +
                  (TargetEncoder.UNSEEN_CATEGORY -> (1 + globalCounter._1,
                    globalCounter._2 + ((label - globalCounter._2) / (1 + globalCounter._1))))
              }
            } catch {
            case e: SparkRuntimeException =>
              if (e.getErrorClass == "ROW_VALUE_IS_NULL") {
                throw new SparkException(s"Null value found in feature ${inputFeatures(feature)}." +
                  s" See Imputer estimator for completing missing values.")
              } else throw e
            }
          }.toArray
        },
        (agg1, agg2) => Range(0, inputFeatures.length)
          .map {
            feature => {
              val values = agg1(feature).keySet ++ agg2(feature).keySet
              values.map(value =>
                value -> {
                  val stat1 = agg1(feature).getOrElse(value, (0.0, 0.0))
                  val stat2 = agg2(feature).getOrElse(value, (0.0, 0.0))
                  $(targetType) match {
                    case TargetEncoder.TARGET_BINARY => (stat1._1 + stat2._1, stat1._2 + stat2._2)
                    case TargetEncoder.TARGET_CONTINUOUS => (stat1._1 + stat2._1,
                      ((stat1._1 * stat1._2) + (stat2._1 * stat2._2)) / (stat1._1 + stat2._1))
                      }
                }).toMap
            }
          }.toArray)

    val encodings: Map[String, Map[Double, Double]] = stats.zipWithIndex.map {
      case (stat, idx) =>
        val global = stat.get(TargetEncoder.UNSEEN_CATEGORY).get
        inputFeatures(idx) -> stat.map {
          case (feature, value) => feature -> {
            val weight = value._1 / (value._1 + $(smoothing))
            $(targetType) match {
              case TargetEncoder.TARGET_BINARY =>
                weight * (value._2 / value._1) + (1 - weight) * (global._2 / global._1)
              case TargetEncoder.TARGET_CONTINUOUS =>
                weight * value._2 + (1 - weight) * global._2
            }
          }
        }
    }.toMap

    val model = new TargetEncoderModel(uid, encodings).setParent(this)
    copyValues(model)
  }

  @Since("4.0.0")
  override def copy(extra: ParamMap): TargetEncoder = defaultCopy(extra)
}

@Since("4.0.0")
object TargetEncoder extends DefaultParamsReadable[TargetEncoder] {

  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val supportedHandleInvalids: Array[String] = Array(KEEP_INVALID, ERROR_INVALID)

  private[feature] val TARGET_BINARY: String = "binary"
  private[feature] val TARGET_CONTINUOUS: String = "continuous"
  private[feature] val supportedTargetTypes: Array[String] = Array(TARGET_BINARY, TARGET_CONTINUOUS)

  private[feature] val UNSEEN_CATEGORY: Double = -1

  @Since("4.0.0")
  override def load(path: String): TargetEncoder = super.load(path)
}

/**
 * @param encodings  Original number of categories for each feature being encoded.
 *                       The array contains one value for each input column, in order.
 */
@Since("4.0.0")
class TargetEncoderModel private[ml] (
    @Since("4.0.0") override val uid: String,
    @Since("4.0.0") val encodings: Map[String, Map[Double, Double]])
  extends Model[TargetEncoderModel] with TargetEncoderBase with MLWritable {

  @Since("4.0.0")
  override def transformSchema(schema: StructType): StructType = {
    inputFeatures.zip(outputFeatures)
      .foldLeft(validateSchema(schema, fitting = false)) {
        case (newSchema, fieldName) =>
          val field = schema(fieldName._1)
          newSchema.add(StructField(fieldName._2, field.dataType, field.nullable))
      }
  }

  @Since("4.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    validateSchema(dataset.schema, fitting = false)

    val apply_encodings: Map[Double, Double] => (Column => Column) =
      (mappings: Map[Double, Double]) => {
        (col: Column) => {
          val first :: rest = mappings.toList.sortWith{
            (a, b) => (b._1 == TargetEncoder.UNSEEN_CATEGORY) ||
              ((a._1 != TargetEncoder.UNSEEN_CATEGORY) && (a._1 < b._1))
          }
          rest
            .foldLeft(when(col === first._1, first._2))(
              (new_col: Column, encoding) =>
                if (encoding._1 != TargetEncoder.UNSEEN_CATEGORY) {
                  new_col.when(col === encoding._1, encoding._2)
                } else {
                  new_col.otherwise(
                    if ($(handleInvalid) == TargetEncoder.KEEP_INVALID) encoding._2
                    else raise_error(concat(
                      lit("Unseen value "), col,
                      lit(s" in feature ${col.toString}. To handle unseen values, " +
                        s"set Param handleInvalid to ${TargetEncoder.KEEP_INVALID}."))))
                })
        }
      }

    dataset.withColumns(
      inputFeatures.zip(outputFeatures).map {
        feature =>
          feature._2 -> (encodings.get(feature._1) match {
            case Some(dict: Map[Double, Double]) =>
              apply_encodings(dict)(col(feature._1))
                  .as(feature._2, NominalAttribute.defaultAttr
                    .withName(feature._2)
                    .withNumValues(dict.size)
                    .withValues(dict.values.toSet.toArray.map(_.toString)).toMetadata())
            case None =>
              throw new SparkException(s"No encodings found for ${feature._1}.")
              col(feature._1)
          })
        }.toMap)
  }


  @Since("4.0.0")
  override def copy(extra: ParamMap): TargetEncoderModel = {
    val copied = new TargetEncoderModel(uid, encodings)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("4.0.0")
  override def write: MLWriter = new TargetEncoderModel.TargetEncoderModelWriter(this)

  @Since("4.0.0")
  override def toString: String = {
    s"TargetEncoderModel: uid=$uid, " +
      s" handleInvalid=${$(handleInvalid)}, targetType=${$(targetType)}, " +
      s"numInputCols=${inputFeatures.length}, numOutputCols=${outputFeatures.length}, " +
      s"smoothing=${$(smoothing)}"
  }

}

@Since("4.0.0")
object TargetEncoderModel extends MLReadable[TargetEncoderModel] {

  private[TargetEncoderModel]
  class TargetEncoderModelWriter(instance: TargetEncoderModel) extends MLWriter {

    private case class Data(encodings: Map[String, Map[Double, Double]])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val data = Data(instance.encodings)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).write.parquet(dataPath)
    }
  }

  private class TargetEncoderModelReader extends MLReader[TargetEncoderModel] {

    private val className = classOf[TargetEncoderModel].getName

    override def load(path: String): TargetEncoderModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("encodings")
        .head()
      val encodings = data.getAs[Map[String, Map[Double, Double]]](0)
      val model = new TargetEncoderModel(metadata.uid, encodings)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("4.0.0")
  override def read: MLReader[TargetEncoderModel] = new TargetEncoderModelReader

  @Since("4.0.0")
  override def load(path: String): TargetEncoderModel = super.load(path)
}

