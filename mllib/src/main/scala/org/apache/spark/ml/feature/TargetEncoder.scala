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

  private[feature] def inputFeatures: Array[String] =
    if (isSet(inputCol)) {
      Array($(inputCol))
    } else if (isSet(inputCols)) {
      $(inputCols)
    } else {
      Array.empty[String]
    }

  private[feature] def outputFeatures: Array[String] =
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

  private def extractLabel(name: String, targetType: String): Column = {
    val c = col(name).cast(DoubleType)
    targetType match {
      case TargetEncoder.TARGET_BINARY =>
        when(c === 0 || c === 1, c)
          .when(c.isNull || c.isNaN, c)
          .otherwise(raise_error(
            concat(lit("Labels for TARGET_BINARY must be {0, 1}, but got "), c)))

      case TargetEncoder.TARGET_CONTINUOUS => c
    }
  }

  private def extractValue(name: String): Column = {
    val c = col(name).cast(DoubleType)
    when(c >= 0 && c === c.cast(IntegerType), c)
      .when(c.isNull, lit(TargetEncoder.NULL_CATEGORY))
      .when(c.isNaN, raise_error(lit("Values MUST NOT be NaN")))
      .otherwise(raise_error(
        concat(lit("Values MUST be non-negative integers, but got "), c)))
  }

  @Since("4.0.0")
  override def fit(dataset: Dataset[_]): TargetEncoderModel = {
    validateSchema(dataset.schema, fitting = true)
    val numFeatures = inputFeatures.length

    // Append the unseen category, for global stats computation
    val arrayCol = array(
      (inputFeatures.map(v => extractValue(v)) :+ lit(TargetEncoder.UNSEEN_CATEGORY))
        .toIndexedSeq: _*)

    val checked = dataset
      .select(extractLabel($(labelCol), $(targetType)).as("label"), arrayCol.as("array"))
      .where(!col("label").isNaN && !col("label").isNull)
      .select(col("label"), posexplode(col("array")).as(Seq("index", "value")))

    val statCol = $(targetType) match {
      case TargetEncoder.TARGET_BINARY => count_if(col("label") === 1)
      case TargetEncoder.TARGET_CONTINUOUS => avg(col("label"))
    }
    val aggregated = checked
      .groupBy("index", "value")
      .agg(count(lit(1)).cast(DoubleType).as("count"), statCol.cast(DoubleType).as("stat"))

    // stats: Array[Map[category, (count, stat)]]
    val stats = Array.fill(numFeatures)(collection.mutable.Map.empty[Double, (Double, Double)])
    aggregated.select("index", "value", "count", "stat").collect()
      .foreach { case Row(index: Int, value: Double, count: Double, stat: Double) =>
        if (index < numFeatures) {
          // Assign the per-category stats to the corresponding feature
          stats(index).update(value, (count, stat))
        } else {
          // Assign the global stats to all features
          assert(value == TargetEncoder.UNSEEN_CATEGORY)
          stats.foreach { s => s.update(value, (count, stat)) }
        }
      }

    val model = new TargetEncoderModel(uid, stats.map(_.toMap)).setParent(this)
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
    @Since("4.0.0") private[ml] val stats: Array[Map[Double, (Double, Double)]])
  extends Model[TargetEncoderModel] with TargetEncoderBase with MLWritable {

  // For ml connect only
  private[ml] def this() = this("", Array.empty)

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

    val newCols = inputFeatures.zip(outputFeatures).zip(encodings).map {
      case ((featureIn, featureOut), mapping) =>
        val unseenErrMsg = s"Unseen value %s in feature $featureIn. " +
          s"To handle unseen values, set Param handleInvalid to ${TargetEncoder.KEEP_INVALID}."
        val unseenErrCol = raise_error(printf(lit(unseenErrMsg), col(featureIn).cast(StringType)))

        val fillUnseenCol = $(handleInvalid) match {
          case TargetEncoder.KEEP_INVALID => lit(mapping(TargetEncoder.UNSEEN_CATEGORY))
          case _ => unseenErrCol
        }
        val fillNullCol = mapping.get(TargetEncoder.NULL_CATEGORY) match {
          case Some(code) => lit(code)
          case _ => fillUnseenCol
        }
        val filteredMapping = mapping.filter { case (k, _) =>
          k != TargetEncoder.UNSEEN_CATEGORY && k != TargetEncoder.NULL_CATEGORY
        }

        val castedCol = col(featureIn).cast(DoubleType)
        val targetCol = try_element_at(typedlit(filteredMapping), castedCol)
        when(castedCol.isNull, fillNullCol)
          .when(!targetCol.isNull, targetCol)
          .otherwise(fillUnseenCol)
          .as(featureOut, NominalAttribute.defaultAttr
            .withName(featureOut)
            .withNumValues(mapping.values.toSet.size)
            .withValues(mapping.values.toSet.toArray.map(_.toString)).toMetadata())
    }
    dataset.withColumns(outputFeatures.toIndexedSeq, newCols.toIndexedSeq)
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
  private[ml] case class Data(
    index: Int, categories: Array[Double],
    counts: Array[Double], stats: Array[Double])

  private[TargetEncoderModel]
  class TargetEncoderModelWriter(instance: TargetEncoderModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val datum = instance.stats.iterator.zipWithIndex.map { case (stat, index) =>
        val (_categories, _countsAndStats) = stat.toSeq.unzip
        val (_counts, _stats) = _countsAndStats.unzip
        Data(index, _categories.toArray, _counts.toArray, _stats.toArray)
      }.toSeq
      val dataPath = new Path(path, "data").toString
      ReadWriteUtils.saveArray[Data](dataPath, datum.toArray, sparkSession)
    }
  }

  private class TargetEncoderModelReader extends MLReader[TargetEncoderModel] {

    private val className = classOf[TargetEncoderModel].getName

    override def load(path: String): TargetEncoderModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString

      val datum = ReadWriteUtils.loadArray[Data](dataPath, sparkSession)
      val stats = datum.map { data =>
        (data.index, data.categories.zip(data.counts.zip(data.stats)).toMap)
      }.sortBy(_._1).map(_._2)

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

