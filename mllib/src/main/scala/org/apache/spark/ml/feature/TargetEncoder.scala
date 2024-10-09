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
          if (!field.dataType.isInstanceOf[NumericType]) {
            throw new SparkException(s"Data type for column ${feature} is ${field.dataType}" +
              s", but a subclass of ${NumericType} is required.")
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

    val feature_types = inputFeatures.map{
      feature => dataset.schema(feature).dataType
    }
    val label_type = dataset.schema($(labelCol)).dataType

    val stats = dataset
      .select((inputFeatures :+ $(labelCol)).map(col).toIndexedSeq: _*)
      .rdd.treeAggregate(
        Array.fill(inputFeatures.length) {
          Map.empty[Option[Double], (Double, Double)]
        })(
        (agg, row: Row) => if (!row.isNullAt(inputFeatures.length)) {
          val label = label_type match {
            case ByteType => row.getByte(inputFeatures.length).toDouble
            case ShortType => row.getShort(inputFeatures.length).toDouble
            case IntegerType => row.getInt(inputFeatures.length).toDouble
            case LongType => row.getLong(inputFeatures.length).toDouble
            case DoubleType => row.getDouble(inputFeatures.length)
          }
          inputFeatures.indices.map {
            feature => {
              val category: Option[Double] = {
                if (row.isNullAt(feature)) None // null category
                else {
                  val value: Double = feature_types(feature) match {
                    case ByteType => row.getByte(feature).toDouble
                    case ShortType => row.getShort(feature).toDouble
                    case IntegerType => row.getInt(feature).toDouble
                    case LongType => row.getLong(feature).toDouble
                    case DoubleType => row.getDouble(feature)
                  }
                  if (value < 0.0 || value != value.toInt) throw new SparkException(
                    s"Values from column ${inputFeatures(feature)} must be indices, " +
                      s"but got $value.")
                  else Some(value)
                }
              }
              val (class_count, class_stat) = agg(feature).getOrElse(category, (0.0, 0.0))
              val (global_count, global_stat) =
                agg(feature).getOrElse(TargetEncoder.UNSEEN_CATEGORY, (0.0, 0.0))
              $(targetType) match {
                case TargetEncoder.TARGET_BINARY => // counting
                  if (label == 1.0) {
                    agg(feature) +
                      (category -> (1 + class_count, 1 + class_stat)) +
                      (TargetEncoder.UNSEEN_CATEGORY -> (1 + global_count, 1 + global_stat))
                  } else if (label == 0.0) {
                    agg(feature) +
                      (category -> (1 + class_count, class_stat)) +
                      (TargetEncoder.UNSEEN_CATEGORY -> (1 + global_count, global_stat))
                  } else throw new SparkException(
                    s"Values from column ${getLabelCol} must be binary (0,1) but got $label.")
                case TargetEncoder.TARGET_CONTINUOUS => // incremental mean
                  agg(feature) +
                    (category -> (1 + class_count,
                      class_stat + ((label - class_stat) / (1 + class_count)))) +
                    (TargetEncoder.UNSEEN_CATEGORY -> (1 + global_count,
                      global_stat + ((label - global_stat) / (1 + global_count))))
              }
            }
          }.toArray
        } else agg,  // ignore null-labeled observations
        (agg1, agg2) => inputFeatures.indices.map {
          feature => {
            val categories = agg1(feature).keySet ++ agg2(feature).keySet
            categories.map(category =>
              category -> {
                val (counter1, stat1) = agg1(feature).getOrElse(category, (0.0, 0.0))
                val (counter2, stat2) = agg2(feature).getOrElse(category, (0.0, 0.0))
                $(targetType) match {
                  case TargetEncoder.TARGET_BINARY => (counter1 + counter2, stat1 + stat2)
                  case TargetEncoder.TARGET_CONTINUOUS => (counter1 + counter2,
                    ((counter1 * stat1) + (counter2 * stat2)) / (counter1 + counter2))
                }
              }).toMap
          }
        }.toArray)

    // encodings: Map[feature, Map[Some(category), encoding]]
    val encodings: Map[String, Map[Option[Double], Double]] =
      inputFeatures.zip(stats).map {
        case (feature, stat) =>
          val (global_count, global_stat) = stat.get(TargetEncoder.UNSEEN_CATEGORY).get
          feature -> stat.map {
            case (cat, (class_count, class_stat)) => cat -> {
              val weight = class_count / (class_count + $(smoothing))
              $(targetType) match {
                case TargetEncoder.TARGET_BINARY =>
                  weight * (class_stat/ class_count) + (1 - weight) * (global_stat / global_count)
                case TargetEncoder.TARGET_CONTINUOUS =>
                  weight * class_stat + (1 - weight) * global_stat
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

  // handleInvalid parameter values
  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val supportedHandleInvalids: Array[String] = Array(KEEP_INVALID, ERROR_INVALID)

  // targetType parameter values
  private[feature] val TARGET_BINARY: String = "binary"
  private[feature] val TARGET_CONTINUOUS: String = "continuous"
  private[feature] val supportedTargetTypes: Array[String] = Array(TARGET_BINARY, TARGET_CONTINUOUS)

  private[feature] val UNSEEN_CATEGORY: Option[Double] = Some(-1)

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
     @Since("4.0.0") val encodings: Map[String, Map[Option[Double], Double]])
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

    // builds a column-to-column function from a map of encodings
    val apply_encodings: Map[Option[Double], Double] => (Column => Column) =
      (mappings: Map[Option[Double], Double]) => {
        (col: Column) => {
          val nullWhen = when(col.isNull,
            mappings.get(None) match {
              case Some(code) => lit(code)
              case None => if ($(handleInvalid) == TargetEncoder.KEEP_INVALID) {
                lit(mappings.get(TargetEncoder.UNSEEN_CATEGORY).get)
              } else raise_error(lit(
                s"Unseen null value in feature ${col.toString}. To handle unseen values, " +
                  s"set Param handleInvalid to ${TargetEncoder.KEEP_INVALID}."))
            })
          val ordered_mappings = (mappings - None).toList.sortWith {
            (a, b) => (b._1 == TargetEncoder.UNSEEN_CATEGORY) ||
              ((a._1 != TargetEncoder.UNSEEN_CATEGORY) && (a._1.get < b._1.get))
          }
          ordered_mappings
            .foldLeft(nullWhen)(
              (new_col: Column, mapping) => {
                val (Some(original), encoded) = mapping
                if (original != TargetEncoder.UNSEEN_CATEGORY.get) {
                  new_col.when(col === original, lit(encoded))
                } else { // unseen category
                  new_col.otherwise(
                    if ($(handleInvalid) == TargetEncoder.KEEP_INVALID) lit(encoded)
                    else raise_error(concat(
                      lit("Unseen value "), col,
                      lit(s" in feature ${col.toString}. To handle unseen values, " +
                        s"set Param handleInvalid to ${TargetEncoder.KEEP_INVALID}."))))
                }
              })
        }
      }

    dataset.withColumns(
      inputFeatures.zip(outputFeatures).map {
        feature =>
          feature._2 -> (encodings.get(feature._1) match {
            case Some(dict) =>
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

    private case class Data(encodings: Map[String, Map[Option[Double], Double]])

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
      val encodings = data.getAs[Map[String, Map[Option[Double], Double]]](0)
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

