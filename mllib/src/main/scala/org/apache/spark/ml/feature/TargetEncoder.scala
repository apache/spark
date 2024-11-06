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
      "or 'error' (throw an error). Note that this Param is only used during transform; " +
      "during fitting, invalid data will result in an error.",
    ParamValidators.inArray(TargetEncoder.supportedHandleInvalids))

  setDefault(handleInvalid -> TargetEncoder.ERROR_INVALID)

  @Since("4.0.0")
  val targetType: Param[String] = new Param[String](this, "targetType",
    "Type of label considered during fit(). " +
      "Options are 'binary' and 'continuous'. When 'binary', estimates are calculated as " +
      "conditional probability of the target given each category. When 'continuous', " +
      "estimates are calculated as the average of the target given each category" +
      "Note that this Param is only used during fitting.",
    ParamValidators.inArray(TargetEncoder.supportedTargetTypes))

  setDefault(targetType -> TargetEncoder.TARGET_BINARY)

  final def getTargetType: String = $(targetType)

  @Since("4.0.0")
  val smoothing: DoubleParam = new DoubleParam(this, "smoothing",
    "Smoothing factor for encodings. Smoothing blends in-class estimates with overall estimates " +
      "according to the relative size of the particular class on the whole dataset, reducing the " +
      "risk of overfitting due to unreliable estimates",
    ParamValidators.gtEq(0.0))

  setDefault(smoothing -> 0.0)

  final def getSmoothing: Double = $(smoothing)

  private[feature] lazy val inputFeatures =
    if (isSet(inputCol)) {
      Array($(inputCol))
    } else if (isSet(inputCols)) {
      $(inputCols)
    } else {
      Array.empty[String]
    }

  private[feature] lazy val outputFeatures =
    if (isSet(outputCol)) {
      Array($(outputCol))
    } else if (isSet(outputCols)) {
      $(outputCols)
    } else {
      inputFeatures.map{field: String => s"${field}_indexed"}
    }

  private[feature] def validateSchema(schema: StructType, fitting: Boolean): StructType = {

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

    // stats: Array[Map[category, (counter,stat)]]
    val stats = dataset
      .select((inputFeatures :+ $(labelCol)).map(col(_).cast(DoubleType)).toIndexedSeq: _*)
      .rdd.treeAggregate(
        Array.fill(inputFeatures.length) {
          Map.empty[Double, (Double, Double)]
        })(

        (agg, row: Row) => if (!row.isNullAt(inputFeatures.length)) {
          val label = row.getDouble(inputFeatures.length)
          if (!label.equals(Double.NaN)) {
            inputFeatures.indices.map {
              feature => {
                val category: Double = {
                  if (row.isNullAt(feature)) TargetEncoder.NULL_CATEGORY // null category
                  else {
                    val value = row.getDouble(feature)
                    if (value < 0.0 || value != value.toInt) throw new SparkException(
                      s"Values from column ${inputFeatures(feature)} must be indices, " +
                        s"but got $value.")
                    else value // non-null category
                  }
                }
                val (class_count, class_stat) = agg(feature).getOrElse(category, (0.0, 0.0))
                val (global_count, global_stat) =
                  agg(feature).getOrElse(TargetEncoder.UNSEEN_CATEGORY, (0.0, 0.0))
                $(targetType) match {
                  case TargetEncoder.TARGET_BINARY => // counting
                    if (label == 1.0) {
                      // positive => increment both counters for current & unseen categories
                      agg(feature) +
                        (category -> (1 + class_count, 1 + class_stat)) +
                        (TargetEncoder.UNSEEN_CATEGORY -> (1 + global_count, 1 + global_stat))
                    } else if (label == 0.0) {
                      // negative => increment only global counter for current & unseen categories
                      agg(feature) +
                        (category -> (1 + class_count, class_stat)) +
                        (TargetEncoder.UNSEEN_CATEGORY -> (1 + global_count, global_stat))
                    } else throw new SparkException(
                      s"Values from column ${getLabelCol} must be binary (0,1) but got $label.")
                  case TargetEncoder.TARGET_CONTINUOUS => // incremental mean
                    // increment counter and iterate on mean for current & unseen categories
                    agg(feature) +
                      (category -> (1 + class_count,
                        class_stat + ((label - class_stat) / (1 + class_count)))) +
                      (TargetEncoder.UNSEEN_CATEGORY -> (1 + global_count,
                        global_stat + ((label - global_stat) / (1 + global_count))))
                }
              }
            }.toArray
          } else agg  // ignore NaN-labeled observations
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



    val model = new TargetEncoderModel(uid, stats).setParent(this)
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

  private[feature] val UNSEEN_CATEGORY: Double = Int.MaxValue
  private[feature] val NULL_CATEGORY: Double = -1

  @Since("4.0.0")
  override def load(path: String): TargetEncoder = super.load(path)
}

/**
 * @param stats  Array of statistics for each input feature.
 *               Array( Map( category, (counter, stat) ) )
 */
@Since("4.0.0")
class TargetEncoderModel private[ml] (
                     @Since("4.0.0") override val uid: String,
                     @Since("4.0.0") val stats: Array[Map[Double, (Double, Double)]])
  extends Model[TargetEncoderModel] with TargetEncoderBase with MLWritable {

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
  def setSmoothing(value: Double): this.type = set(smoothing, value)

  @Since("4.0.0")
  override def transformSchema(schema: StructType): StructType = {
    if (outputFeatures.length == stats.length) {
      outputFeatures.filter(_ != null)
        .foldLeft(validateSchema(schema, fitting = false)) {
          case (newSchema, outputField) =>
            newSchema.add(StructField(outputField, DoubleType, nullable = false))
        }
    } else throw new SparkException("The number of features does not match the number of " +
      s"encodings in the model (${stats.length}). " +
      s"Found ${outputFeatures.length} features)")
  }

  @Since("4.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)

    // encodings: Array[Map[category, encoding]]
    val encodings: Array[Map[Double, Double]] =
      stats.map {
        stat =>
          val (global_count, global_stat) = stat.get(TargetEncoder.UNSEEN_CATEGORY).get
          stat.map {
            case (cat, (class_count, class_stat)) => cat -> {
              val weight = class_count / (class_count + $(smoothing)) // smoothing weight
              $(targetType) match {
                case TargetEncoder.TARGET_BINARY =>
                  // calculate conditional probabilities and blend
                  weight * (class_stat/ class_count) + (1 - weight) * (global_stat / global_count)
                case TargetEncoder.TARGET_CONTINUOUS =>
                  // blend means
                  weight * class_stat + (1 - weight) * global_stat
              }
            }
          }
      }

    // builds a column-to-column function from a map of encodings
    val apply_encodings: Map[Double, Double] => (Column => Column) =
      (mappings: Map[Double, Double]) => {
        (col: Column) => {
          val nullWhen = when(col.isNull,
            mappings.get(TargetEncoder.NULL_CATEGORY) match {
              case Some(code) => lit(code)
              case None => if ($(handleInvalid) == TargetEncoder.KEEP_INVALID) {
                lit(mappings.get(TargetEncoder.UNSEEN_CATEGORY).get)
              } else raise_error(lit(
                s"Unseen null value in feature ${col.toString}. To handle unseen values, " +
                  s"set Param handleInvalid to ${TargetEncoder.KEEP_INVALID}."))
            })
          val ordered_mappings = (mappings - TargetEncoder.NULL_CATEGORY).toList.sortWith {
            (a, b) =>
              (b._1 == TargetEncoder.UNSEEN_CATEGORY) ||
                ((a._1 != TargetEncoder.UNSEEN_CATEGORY) && (a._1 < b._1))
          }
          ordered_mappings
            .foldLeft(nullWhen)(
              (new_col: Column, mapping) => {
                val (original, encoded) = mapping
                if (original != TargetEncoder.UNSEEN_CATEGORY) {
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
      inputFeatures.zip(outputFeatures).zip(encodings)
        .map {
          case ((featureIn, featureOut), mapping) =>
            featureOut ->
              apply_encodings(mapping)(col(featureIn))
                .as(featureOut, NominalAttribute.defaultAttr
                  .withName(featureOut)
                  .withNumValues(mapping.values.toSet.size)
                  .withValues(mapping.values.toSet.toArray.map(_.toString)).toMetadata())
        }.toMap)

  }

  @Since("4.0.0")
  override def copy(extra: ParamMap): TargetEncoderModel = {
    val copied = new TargetEncoderModel(uid, stats)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("4.0.0")
  override def write: MLWriter = new TargetEncoderModel.TargetEncoderModelWriter(this)

  @Since("4.0.0")
  override def toString: String = {
    s"TargetEncoderModel: uid=$uid, " +
      s"handleInvalid=${$(handleInvalid)}, targetType=${$(targetType)}, " +
      s"numInputCols=${inputFeatures.length}, numOutputCols=${outputFeatures.length}, " +
      s"smoothing=${$(smoothing)}"
  }

}

@Since("4.0.0")
object TargetEncoderModel extends MLReadable[TargetEncoderModel] {

  private[TargetEncoderModel]
  class TargetEncoderModelWriter(instance: TargetEncoderModel) extends MLWriter {

    private case class Data(stats: Array[Map[Double, (Double, Double)]])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val data = Data(instance.stats)
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
      val stats = data.getAs[Array[Map[Double, (Double, Double)]]](0)
      val model = new TargetEncoderModel(metadata.uid, stats)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("4.0.0")
  override def read: MLReader[TargetEncoderModel] = new TargetEncoderModelReader

  @Since("4.0.0")
  override def load(path: String): TargetEncoderModel = super.load(path)
}

