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

import java.io.{DataInputStream, DataOutputStream}

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.NumericAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.SizeEstimator

/** Private trait for params and common methods for FrequencyEncoder and FrequencyEncoderModel */
private[ml] trait FrequencyEncoderBase extends Params
  with HasInputCol with HasInputCols with HasOutputCol with HasOutputCols with HasHandleInvalid {

  /**
   * Param for how to handle invalid data during transform().
   * Options are 'keep' (unseen categories encoded as zero, since a category absent from the
   * training data was observed zero times) or 'error' (throw an error).
   * Note that this Param is only used during transform; during fitting, invalid data
   * will result in an error.
   * Default: "error"
   * @group param
   */
  @Since("4.4.0")
  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "How to handle invalid data during transform(). " +
      "Options are 'keep' (unseen categories are encoded as zero) " +
      "or 'error' (throw an error). Note that this Param is only used during transform; " +
      "during fitting, invalid data will result in an error.",
    ParamValidators.inArray(FrequencyEncoder.supportedHandleInvalids))

  setDefault(handleInvalid -> FrequencyEncoder.ERROR_INVALID)

  /**
   * Param for whether to encode categories as a proportion of the training rows rather than
   * as a raw count. Proportions are comparable across datasets of different sizes, which is
   * usually what is wanted when the encoding feeds a model; raw counts preserve the absolute
   * scale.
   * Default: true
   * @group param
   */
  @Since("4.4.0")
  val normalize: BooleanParam = new BooleanParam(this, "normalize",
    "Whether to encode categories as a proportion of the training rows (true) or as a raw " +
      "count (false). Note that this Param is only used during fitting.")

  setDefault(normalize -> true)

  /** @group getParam */
  @Since("4.4.0")
  final def getNormalize: Boolean = $(normalize)

  private[feature] def inputFeatures: Array[String] =
    if (isSet(inputCol)) {
      Array($(inputCol))
    } else if (isSet(inputCols)) {
      $(inputCols)
    } else {
      Array.empty[String]
    }

  // No derived default such as `input_encoded`. Deriving one from the input name leaves
  // `getOutputCol` reporting HasOutputCol's default while `transform` produces something else,
  // so a caller passing the getter's answer downstream gets a column that does not exist.
  // Falling through to `$(outputCol)` keeps the getters truthful.
  private[feature] def outputFeatures: Array[String] =
    if (isSet(outputCol)) {
      Array($(outputCol))
    } else if (isSet(outputCols)) {
      $(outputCols)
    } else if (isSet(inputCol)) {
      Array($(outputCol))
    } else {
      Array.empty[String]
    }

  // One definition of the output schema, shared by the estimator and the model so they cannot
  // disagree. SchemaUtils.appendColumn rejects a name that already exists, which gives output
  // collisions a single consistent answer instead of a schema that claims a duplicate field
  // while the transformed frame quietly replaces one.
  private[feature] def outputSchema(schema: StructType): StructType = {
    validateSchema(schema)
    outputFeatures.foldLeft(schema) { (acc, name) =>
      SchemaUtils.appendColumn(acc, StructField(name, DoubleType, nullable = false,
        NumericAttribute.defaultAttr.withName(name).toMetadata()))
    }
  }

  private[feature] def validateSchema(schema: StructType): StructType = {

    require(inputFeatures.length > 0,
      s"At least one input column must be specified.")

    require(inputFeatures.length == outputFeatures.length,
      s"The number of input columns ${inputFeatures.length} must be the same as the number of " +
        s"output columns ${outputFeatures.length}.")

    inputFeatures.foreach { feature =>
      try {
        val field = schema(feature)
        if (!field.dataType.isInstanceOf[NumericType]) {
          throw new SparkException(s"Data type for column ${feature} is ${field.dataType}" +
            s", but a subclass of ${NumericType} is required.")
        }
      } catch {
        case _: IllegalArgumentException =>
          throw new SparkException(s"No column named ${feature} found on dataset.")
      }
    }
    schema
  }
}

/**
 * Frequency Encoding maps a column of categorical indices to how often each category occurs
 * in the training data, either as a proportion of the training rows or as a raw count.
 *
 * Unlike `TargetEncoder` it needs no label, so it is available for unsupervised pipelines, and
 * and where `OneHotEncoder` represents a feature as a vector with one dimension per category,
 * this reduces it to a single scalar, trading the identity of a category for how common it is
 * and so keeping the feature space flat however many distinct values there are.
 *
 * Categories that occur equally often in the training data receive the same encoding. That is
 * inherent to the technique rather than a limitation of this implementation: the encoding carries
 * how common a category is and nothing else.
 *
 * When `handleInvalid` is configured to 'keep', categories not seen during fitting are encoded
 * as zero, which is the frequency actually observed for them.
 *
 * @note When encoding multi-column by using `inputCols` and `outputCols` params, input/output cols
 * come in pairs, specified by the order in the arrays, and each pair is treated independently.
 *
 * @see `StringIndexer` for converting categorical values into category indices
 * @see `TargetEncoder` for encoding categories against a label
 */
@Since("4.4.0")
class FrequencyEncoder @Since("4.4.0") (@Since("4.4.0") override val uid: String)
  extends Estimator[FrequencyEncoderModel] with FrequencyEncoderBase with DefaultParamsWritable {

  @Since("4.4.0")
  def this() = this(Identifiable.randomUID("frequencyEncoder"))

  /** @group setParam */
  @Since("4.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("4.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("4.4.0")
  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */
  @Since("4.4.0")
  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  /** @group setParam */
  @Since("4.4.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /** @group setParam */
  @Since("4.4.0")
  def setNormalize(value: Boolean): this.type = set(normalize, value)

  @Since("4.4.0")
  override def transformSchema(schema: StructType): StructType = outputSchema(schema)

  private def extractValue(name: String): Column = {
    val c = col(name).cast(DoubleType)
    val notAnIndex = raise_error(
      concat(lit(s"Values from column $name MUST be non-negative integers, but got "), c))
    val outOfRange = raise_error(
      concat(lit(s"FrequencyEncoder only supports up to ${FrequencyEncoder.MAX_INDEX} " +
        "indices, but got "), c))
    // The order of these branches is load bearing. Categories are carried as Double map keys,
    // and a Double represents integers exactly only up to 2^53, so a value above that would
    // alias a different one and merge two distinct categories without saying so. The range is
    // therefore checked before anything narrows, which also makes the integrality test below
    // safe: by the time it runs the value is known to fit. `OneHotEncoder` caps category
    // indices the same way, and `StringIndexer` is the documented way to produce them.
    when(c.isNull, lit(FrequencyEncoder.NULL_CATEGORY))
      .when(c.isNaN, raise_error(lit(s"Values from column $name MUST NOT be NaN")))
      .when(c < 0, notAnIndex)
      .when(c > FrequencyEncoder.MAX_INDEX, outOfRange)
      // Integrality is tested on the source column, not on `c`. Casting to double first would
      // hide a fractional decimal: decimal(38,18) 1.000000000000000001 becomes exactly 1.0 and
      // would then pass as category 1, quietly merging with real 1s. By this branch the
      // magnitude is known to be within range, so casting to long here cannot overflow.
      .when(col(name) =!= col(name).cast(LongType), notAnIndex)
      .otherwise(c)
  }

  @Since("4.4.0")
  override def fit(dataset: Dataset[_]): FrequencyEncoderModel = {
    // transformSchema rather than validateSchema, so an output column that collides with an
    // existing one is refused here rather than after the work of fitting is already done.
    transformSchema(dataset.schema, logging = true)
    val numFeatures = inputFeatures.length

    // One array plus one posexplode, so a single groupBy aggregates every input column rather
    // than one shuffle per column.
    val arrayCol = array(inputFeatures.map(v => extractValue(v)).toIndexedSeq: _*)

    val aggregated = dataset
      .select(posexplode(arrayCol).as(Seq("index", "value")))
      .groupBy("index", "value")
      .agg(count(lit(1)).cast(DoubleType).as("count"))

    // counts: Array[Map[category, count]]
    val counts = Array.fill(numFeatures)(collection.mutable.Map.empty[Double, Double])
    aggregated.select("index", "value", "count").collect()
      .foreach { case Row(index: Int, value: Double, occurrences: Double) =>
        counts(index).update(value, occurrences)
      }

    // Every row contributes exactly one value per feature, so the per-feature totals are already
    // in hand and normalizing needs no second pass over the data.
    val encodings = counts.map { featureCounts =>
      val total = featureCounts.values.sum
      if ($(normalize) && total > 0) {
        featureCounts.map { case (category, n) => category -> n / total }.toMap
      } else {
        featureCounts.toMap
      }
    }

    val model = new FrequencyEncoderModel(uid, encodings).setParent(this)
    copyValues(model)
  }

  @Since("4.4.0")
  override def copy(extra: ParamMap): FrequencyEncoder = defaultCopy(extra)
}

@Since("4.4.0")
object FrequencyEncoder extends DefaultParamsReadable[FrequencyEncoder] {

  // handleInvalid parameter values
  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val supportedHandleInvalids: Array[String] = Array(KEEP_INVALID, ERROR_INVALID)

  private[feature] val NULL_CATEGORY: Double = -1

  // Category indices are carried as Double map keys, which are exact only to 2^53. Capping at
  // Int.MaxValue keeps every accepted index exactly representable, so two distinct inputs can
  // never collapse onto one key. This is the same ceiling OneHotEncoder applies to indices.
  private[feature] val MAX_INDEX: Int = Int.MaxValue

  @Since("4.4.0")
  override def load(path: String): FrequencyEncoder = super.load(path)
}

/**
 * The `normalize` param is carried but not acted on here: it decides how `fit` computed these
 * encodings, and the model keeps it so that save and load round-trip it and `toString` can report
 * how the values were derived. Changing it on a fitted model does not recompute anything, which is
 * why no setter is offered for it.
 *
 * @param encodings  Array of encodings for each input feature.
 *                   Array( Map( category, frequency ) )
 */
@Since("4.4.0")
class FrequencyEncoderModel private[ml] (
    @Since("4.4.0") override val uid: String,
    @Since("4.4.0") private[ml] val encodings: Array[Map[Double, Double]])
  extends Model[FrequencyEncoderModel] with FrequencyEncoderBase with MLWritable {

  // For ml connect only
  private[ml] def this() = this("", Array.empty)

  private[spark] override def estimatedSize: Long = {
    var size = estimateMatadataSize
    // encodings: Array[Map[Double, Double]]
    size += SizeEstimator.estimate(encodings)
    size
  }

  /** @group setParam */
  @Since("4.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("4.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("4.4.0")
  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */
  @Since("4.4.0")
  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  /** @group setParam */
  @Since("4.4.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  @Since("4.4.0")
  override def transformSchema(schema: StructType): StructType = {
    if (outputFeatures.length != encodings.length) {
      throw new SparkException("The number of features does not match the number of " +
        s"encodings in the model (${encodings.length}). " +
        s"found ${outputFeatures.length} output columns.")
    }
    outputSchema(schema)
  }

  @Since("4.4.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)

    val newCols = inputFeatures.zip(outputFeatures).zip(encodings).map {
      case ((featureIn, featureOut), mapping) =>
        val unseenErrMsg = s"Unseen value %s in feature $featureIn. " +
          s"To handle unseen values, set Param handleInvalid to ${FrequencyEncoder.KEEP_INVALID}."
        val unseenErrCol = raise_error(printf(lit(unseenErrMsg), col(featureIn).cast(StringType)))

        // A category absent from the training data was observed zero times, so that is what it
        // encodes to.
        val fillUnseenCol = $(handleInvalid) match {
          case FrequencyEncoder.KEEP_INVALID => lit(0.0)
          case _ => unseenErrCol
        }
        val fillNullCol = mapping.get(FrequencyEncoder.NULL_CATEGORY) match {
          case Some(code) => lit(code)
          case _ => fillUnseenCol
        }
        val filteredMapping = mapping.filter { case (k, _) => k != FrequencyEncoder.NULL_CATEGORY }

        val castedCol = col(featureIn).cast(DoubleType)
        // A feature can reach here with nothing but nulls in training, leaving no non-null
        // categories to look up. Handling that explicitly is clearer than relying on an empty
        // map literal returning null from try_element_at, which is what it does today.
        val encodedCol = if (filteredMapping.isEmpty) {
          when(castedCol.isNull, fillNullCol).otherwise(fillUnseenCol)
        } else {
          val targetCol = try_element_at(typedlit(filteredMapping), castedCol)
          when(castedCol.isNull, fillNullCol)
            .when(!targetCol.isNull, targetCol)
            .otherwise(fillUnseenCol)
        }

        // Numeric, not nominal: an encoded frequency is a continuous quantity, and its ordering
        // and magnitude are the whole point of the encoding.
        encodedCol.as(featureOut, NumericAttribute.defaultAttr.withName(featureOut).toMetadata())
    }
    dataset.withColumns(outputFeatures.toIndexedSeq, newCols.toIndexedSeq)
  }

  @Since("4.4.0")
  override def copy(extra: ParamMap): FrequencyEncoderModel = {
    val copied = new FrequencyEncoderModel(uid, encodings)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("4.4.0")
  override def write: MLWriter = new FrequencyEncoderModel.FrequencyEncoderModelWriter(this)

  @Since("4.4.0")
  override def toString: String = {
    s"FrequencyEncoderModel: uid=$uid, " +
      s"handleInvalid=${$(handleInvalid)}, normalize=${$(normalize)}, " +
      s"numInputCols=${inputFeatures.length}, numOutputCols=${outputFeatures.length}"
  }
}

@Since("4.4.0")
object FrequencyEncoderModel extends MLReadable[FrequencyEncoderModel] {
  private[ml] case class Data(index: Int, categories: Array[Double], frequencies: Array[Double])

  private[ml] def serializeData(data: Data, dos: DataOutputStream): Unit = {
    import ReadWriteUtils._
    dos.writeInt(data.index)
    serializeDoubleArray(data.categories, dos)
    serializeDoubleArray(data.frequencies, dos)
  }

  private[ml] def deserializeData(dis: DataInputStream): Data = {
    import ReadWriteUtils._
    val index = dis.readInt()
    val categories = deserializeDoubleArray(dis)
    val frequencies = deserializeDoubleArray(dis)
    Data(index, categories, frequencies)
  }

  private[FrequencyEncoderModel]
  class FrequencyEncoderModelWriter(instance: FrequencyEncoderModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val datum = instance.encodings.iterator.zipWithIndex.map { case (mapping, index) =>
        val (categories, frequencies) = mapping.toSeq.unzip
        Data(index, categories.toArray, frequencies.toArray)
      }.toSeq
      val dataPath = new Path(path, "data").toString
      ReadWriteUtils.saveArray[Data](dataPath, datum.toArray, sparkSession, serializeData)
    }
  }

  private class FrequencyEncoderModelReader extends MLReader[FrequencyEncoderModel] {

    private val className = classOf[FrequencyEncoderModel].getName

    override def load(path: String): FrequencyEncoderModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString

      val datum = ReadWriteUtils.loadArray[Data](dataPath, sparkSession, deserializeData)
      // Sorted by index: the encodings are positional, matched to inputCols by their order, so
      // reading them back in an arbitrary order would silently encode the wrong columns.
      val encodings = datum.map { data =>
        (data.index, data.categories.zip(data.frequencies).toMap)
      }.sortBy(_._1).map(_._2)

      val model = new FrequencyEncoderModel(metadata.uid, encodings)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("4.4.0")
  override def read: MLReader[FrequencyEncoderModel] = new FrequencyEncoderModelReader

  @Since("4.4.0")
  override def load(path: String): FrequencyEncoderModel = super.load(path)
}
