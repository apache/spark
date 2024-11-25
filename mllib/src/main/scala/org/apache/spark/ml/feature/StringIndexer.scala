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

import org.apache.spark.{SparkException, SparkIllegalArgumentException}
import org.apache.spark.annotation.Since
import org.apache.spark.internal.{LogKeys, MDC}
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.VersionUtils.majorMinorVersion
import org.apache.spark.util.collection.OpenHashMap

/**
 * Base trait for [[StringIndexer]] and [[StringIndexerModel]].
 */
private[feature] trait StringIndexerBase extends Params with HasHandleInvalid with HasInputCol
  with HasOutputCol with HasInputCols with HasOutputCols {

  /**
   * Param for how to handle invalid data (unseen labels or NULL values).
   * Options are 'skip' (filter out rows with invalid data),
   * 'error' (throw an error), or 'keep' (put invalid data in a special additional
   * bucket, at index numLabels).
   * Default: "error"
   * @group param
   */
  @Since("1.6.0")
  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "How to handle invalid data (unseen labels or NULL values). " +
    "Options are 'skip' (filter out rows with invalid data), error (throw an error), " +
    "or 'keep' (put invalid data in a special additional bucket, at index numLabels).",
    ParamValidators.inArray(StringIndexer.supportedHandleInvalids))

  /**
   * Param for how to order labels of string column. The first label after ordering is assigned
   * an index of 0.
   * Options are:
   *   - 'frequencyDesc': descending order by label frequency (most frequent label assigned 0)
   *   - 'frequencyAsc': ascending order by label frequency (least frequent label assigned 0)
   *   - 'alphabetDesc': descending alphabetical order
   *   - 'alphabetAsc': ascending alphabetical order
   * Default is 'frequencyDesc'.
   *
   * Note: In case of equal frequency when under frequencyDesc/Asc, the strings are further sorted
   *       alphabetically.
   *
   * @group param
   */
  @Since("2.3.0")
  final val stringOrderType: Param[String] = new Param(this, "stringOrderType",
    "How to order labels of string column. " +
    "The first label after ordering is assigned an index of 0. " +
    s"Supported options: ${StringIndexer.supportedStringOrderType.mkString(", ")}.",
    ParamValidators.inArray(StringIndexer.supportedStringOrderType))

  setDefault(handleInvalid -> StringIndexer.ERROR_INVALID,
    stringOrderType -> StringIndexer.frequencyDesc)

  /** @group getParam */
  @Since("2.3.0")
  def getStringOrderType: String = $(stringOrderType)

  /** Returns the input and output column names corresponding in pair. */
  private[feature] def getInOutCols(): (Array[String], Array[String]) = {
    ParamValidators.checkSingleVsMultiColumnParams(this, Seq(outputCol), Seq(outputCols))

    if (isSet(inputCol)) {
      (Array($(inputCol)), Array($(outputCol)))
    } else {
      require($(inputCols).length == $(outputCols).length,
        "The number of input columns does not match output columns")
      ($(inputCols), $(outputCols))
    }
  }

  private def validateAndTransformField(
      schema: StructType,
      inputColName: String,
      inputDataType: DataType,
      outputColName: String): StructField = {
    require(inputDataType == StringType || inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be either string type or numeric type, " +
        s"but got $inputDataType.")
    require(schema.fields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    NominalAttribute.defaultAttr.withName(outputColName).toStructField()
  }

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(
      schema: StructType,
      skipNonExistsCol: Boolean = false): StructType = {
    val (inputColNames, outputColNames) = getInOutCols()

    require(outputColNames.distinct.length == outputColNames.length,
      s"Output columns should not be duplicate.")

    val outputFields = inputColNames.zip(outputColNames).flatMap {
      case (inputColName, outputColName) =>
        try {
          val dtype = SchemaUtils.getSchemaFieldType(schema, inputColName)
          Some(
            validateAndTransformField(schema, inputColName, dtype, outputColName)
          )
        } catch {
          case e: SparkIllegalArgumentException if e.getCondition == "FIELD_NOT_FOUND" =>
            if (skipNonExistsCol) {
              None
            } else {
              throw new SparkException(s"Input column $inputColName does not exist.")
            }
        }
    }
    StructType(schema.fields ++ outputFields)
  }
}

/**
 * A label indexer that maps string column(s) of labels to ML column(s) of label indices.
 * If the input columns are numeric, we cast them to string and index the string values.
 * The indices are in [0, numLabels). By default, this is ordered by label frequencies
 * so the most frequent label gets index 0. The ordering behavior is controlled by
 * setting `stringOrderType`.
 *
 * @see `IndexToString` for the inverse transformation
 */
@Since("1.4.0")
class StringIndexer @Since("1.4.0") (
    @Since("1.4.0") override val uid: String) extends Estimator[StringIndexerModel]
  with StringIndexerBase with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("strIdx"))

  /** @group setParam */
  @Since("1.6.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /** @group setParam */
  @Since("2.3.0")
  def setStringOrderType(value: String): this.type = set(stringOrderType, value)

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("3.0.0")
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  /**
   * Gets columns from dataset. If a column is not string type, we replace NaN values
   * with null. Columns are casted to string type.
   */
  private def getSelectedCols(dataset: Dataset[_], inputCols: Seq[String]): Seq[Column] = {
    inputCols.map { colName =>
      val col = dataset.col(colName)
      // We don't count for NaN values. Because `StringIndexerAggregator` only processes strings,
      // we replace NaNs with null in advance.
      val fpTypes = Seq(DoubleType, FloatType).map(_.catalogString)
      when(typeof(col).isin(fpTypes: _*) && isnan(col), lit(null))
        .otherwise(col)
        .cast(StringType)
    }
  }

  private def sortByFreq(dataset: Dataset[_], ascending: Boolean): Array[Array[String]] = {
    val (inputCols, _) = getInOutCols()
    val selectedCols = getSelectedCols(dataset, inputCols.toImmutableArraySeq)
    val numCols = inputCols.length

    // In the case of equal frequency, always sorts strings by alphabet (ascending).
    val countCol = if (ascending) count(lit(1)) else negate(count(lit(1)))

    val result = Array.fill(numCols)(Array.empty[String])
    dataset.select(posexplode(array(selectedCols: _*)).as(Seq("index", "value")))
      .where(col("value").isNotNull)
      .groupBy("index", "value")
      .agg(countCol.as("count"))
      .groupBy("index")
      .agg(sort_array(collect_list(struct("count", "value"))).getField("value"))
      .collect()
      .foreach(r => result(r.getInt(0)) = r.getSeq[String](1).toArray)
    result
  }

  private def sortByAlphabet(dataset: Dataset[_], ascending: Boolean): Array[Array[String]] = {
    val (inputCols, _) = getInOutCols()
    val selectedCols = getSelectedCols(dataset, inputCols.toImmutableArraySeq)
    val numCols = inputCols.length

    val result = Array.fill(numCols)(Array.empty[String])
    dataset.select(posexplode(array(selectedCols: _*)).as(Seq("index", "value")))
      .where(col("value").isNotNull)
      .groupBy("index")
      .agg(sort_array(collect_set("value"), ascending))
      .collect()
      .foreach(r => result(r.getInt(0)) = r.getSeq[String](1).toArray)
    result
  }

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): StringIndexerModel = {
    transformSchema(dataset.schema, logging = true)

    // In case of equal frequency when frequencyDesc/Asc, the strings are further sorted
    // alphabetically.
    val labelsArray = $(stringOrderType) match {
      case StringIndexer.frequencyDesc => sortByFreq(dataset, ascending = false)
      case StringIndexer.frequencyAsc => sortByFreq(dataset, ascending = true)
      case StringIndexer.alphabetDesc => sortByAlphabet(dataset, ascending = false)
      case StringIndexer.alphabetAsc => sortByAlphabet(dataset, ascending = true)
     }
    copyValues(new StringIndexerModel(uid, labelsArray).setParent(this))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): StringIndexer = defaultCopy(extra)
}

@Since("1.6.0")
object StringIndexer extends DefaultParamsReadable[StringIndexer] {
  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val supportedHandleInvalids: Array[String] =
    Array(SKIP_INVALID, ERROR_INVALID, KEEP_INVALID)
  private[feature] val frequencyDesc: String = "frequencyDesc"
  private[feature] val frequencyAsc: String = "frequencyAsc"
  private[feature] val alphabetDesc: String = "alphabetDesc"
  private[feature] val alphabetAsc: String = "alphabetAsc"
  private[feature] val supportedStringOrderType: Array[String] =
    Array(frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc)

  @Since("1.6.0")
  override def load(path: String): StringIndexer = super.load(path)
}

/**
 * Model fitted by [[StringIndexer]].
 *
 * @param labelsArray Array of ordered list of labels, corresponding to indices to be assigned
 *                    for each input column.
 *
 * @note During transformation, if any input column does not exist,
 * `StringIndexerModel.transform` would skip the input column.
 * If all input columns do not exist, it returns the input dataset unmodified.
 * This is a temporary fix for the case when target labels do not exist during prediction.
 */
@Since("1.4.0")
class StringIndexerModel (
    @Since("1.4.0") override val uid: String,
    @Since("3.0.0") val labelsArray: Array[Array[String]])
  extends Model[StringIndexerModel] with StringIndexerBase with MLWritable {

  import StringIndexerModel._

  @Since("1.5.0")
  def this(uid: String, labels: Array[String]) = this(uid, Array(labels))

  @Since("1.5.0")
  def this(labels: Array[String]) = this(Identifiable.randomUID("strIdx"), Array(labels))

  @Since("3.0.0")
  def this(labelsArray: Array[Array[String]]) = this(Identifiable.randomUID("strIdx"), labelsArray)

  @deprecated("`labels` is deprecated and will be removed in 3.1.0. Use `labelsArray` " +
    "instead.", "3.0.0")
  @Since("1.5.0")
  def labels: Array[String] = {
    require(labelsArray.length == 1, "This StringIndexerModel is fit on multiple columns. " +
      "Call `labelsArray` instead.")
    labelsArray(0)
  }

  // Prepares the maps for string values to corresponding index values.
  private val labelsToIndexArray: Array[OpenHashMap[String, Double]] = {
    for (labels <- labelsArray) yield {
      val n = labels.length
      val map = new OpenHashMap[String, Double](n)
      labels.zipWithIndex.foreach { case (label, idx) =>
        map.update(label, idx)
      }
      map
    }
  }

  /** @group setParam */
  @Since("1.6.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("3.0.0")
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  // This filters out any null values and also the input labels which are not in
  // the dataset used for fitting.
  private def filterInvalidData(dataset: Dataset[_], inputColNames: Seq[String]): Dataset[_] = {
    val conditions: Seq[Column] = inputColNames.indices.map { i =>
      val inputColName = inputColNames(i)
      val labelToIndex = labelsToIndexArray(i)
      // We have this additional lookup at `labelToIndex` when `handleInvalid` is set to
      // `StringIndexer.SKIP_INVALID`. Another idea is to do this lookup natively by SQL
      // expression, however, lookup for a key in a map is not efficient in SparkSQL now.
      // See `ElementAt` and `GetMapValue` expressions. If SQL's map lookup is improved,
      // we can consider to change this.
      val filter = udf { label: String =>
        labelToIndex.contains(label)
      }
      filter(dataset(inputColName))
    }

    dataset.na.drop(inputColNames.filter(dataset.schema.fieldNames.contains(_)))
      .where(conditions.reduce(_ and _))
  }

  private def getIndexer(labels: Seq[String], labelToIndex: OpenHashMap[String, Double]) = {
    val keepInvalid = (getHandleInvalid == StringIndexer.KEEP_INVALID)

    udf { label: String =>
      if (label == null) {
        if (keepInvalid) {
          labels.length
        } else {
          throw new SparkException("StringIndexer encountered NULL value. To handle or skip " +
            "NULLS, try setting StringIndexer.handleInvalid.")
        }
      } else {
        if (labelToIndex.contains(label)) {
          labelToIndex(label)
        } else if (keepInvalid) {
          labels.length
        } else {
          throw new SparkException(s"Unseen label: $label. To handle unseen labels, " +
            s"set Param handleInvalid to ${StringIndexer.KEEP_INVALID}.")
        }
      }
    }.asNondeterministic()
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val (inputColNames, outputColNames) = getInOutCols()
    val outputColumns = new Array[Column](outputColNames.length)

    // Skips invalid rows if `handleInvalid` is set to `StringIndexer.SKIP_INVALID`.
    val filteredDataset = if (getHandleInvalid == StringIndexer.SKIP_INVALID) {
      filterInvalidData(dataset, inputColNames.toImmutableArraySeq)
    } else {
      dataset
    }

    for (i <- outputColNames.indices) {
      val inputColName = inputColNames(i)
      val outputColName = outputColNames(i)
      val labelToIndex = labelsToIndexArray(i)
      val labels = labelsArray(i)

      try {
        dataset.col(inputColName)
        val filteredLabels = getHandleInvalid match {
          case StringIndexer.KEEP_INVALID => labels :+ "__unknown"
          case _ => labels
        }
        val metadata = NominalAttribute.defaultAttr
          .withName(outputColName)
          .withValues(filteredLabels)
          .toMetadata()

        val indexer = getIndexer(labels.toImmutableArraySeq, labelToIndex)

        outputColumns(i) = indexer(dataset(inputColName).cast(StringType))
          .as(outputColName, metadata)
      } catch {
        case _: AnalysisException =>
          logWarning(log"Input column ${MDC(LogKeys.COLUMN_NAME, inputColName)} does not exist " +
            log"during transformation. Skip StringIndexerModel for this column.")
          outputColNames(i) = null
      }
    }
    val filteredOutputColNames = outputColNames.filter(_ != null)
    val filteredOutputColumns = outputColumns.filter(_ != null)

    require(filteredOutputColNames.length == filteredOutputColumns.length)
    if (filteredOutputColNames.length > 0) {
      filteredDataset.withColumns(
        filteredOutputColNames.toImmutableArraySeq, filteredOutputColumns.toImmutableArraySeq)
    } else {
      filteredDataset.toDF()
    }
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, skipNonExistsCol = true)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): StringIndexerModel = {
    val copied = new StringIndexerModel(uid, labelsArray)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: StringIndexModelWriter = new StringIndexModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"StringIndexerModel: uid=$uid, handleInvalid=${$(handleInvalid)}" +
      get(stringOrderType).map(t => s", stringOrderType=$t").getOrElse("") +
      get(inputCols).map(c => s", numInputCols=${c.length}").getOrElse("") +
      get(outputCols).map(c => s", numOutputCols=${c.length}").getOrElse("")
  }
}

@Since("1.6.0")
object StringIndexerModel extends MLReadable[StringIndexerModel] {

  private[StringIndexerModel]
  class StringIndexModelWriter(instance: StringIndexerModel) extends MLWriter {

    private case class Data(labelsArray: Array[Array[String]])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val data = Data(instance.labelsArray)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).write.parquet(dataPath)
    }
  }

  private class StringIndexerModelReader extends MLReader[StringIndexerModel] {

    private val className = classOf[StringIndexerModel].getName

    override def load(path: String): StringIndexerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString

      // We support loading old `StringIndexerModel` saved by previous Spark versions.
      // Previous model has `labels`, but new model has `labelsArray`.
      val (majorVersion, minorVersion) = majorMinorVersion(metadata.sparkVersion)
      val labelsArray = if (majorVersion < 3) {
        // Spark 2.4 and before.
        val data = sparkSession.read.parquet(dataPath)
          .select("labels")
          .head()
        val labels = data.getAs[Seq[String]](0).toArray
        Array(labels)
      } else {
        // After Spark 3.0.
        val data = sparkSession.read.parquet(dataPath)
          .select("labelsArray")
          .head()
        data.getSeq[scala.collection.Seq[String]](0).map(_.toArray).toArray
      }
      val model = new StringIndexerModel(metadata.uid, labelsArray)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[StringIndexerModel] = new StringIndexerModelReader

  @Since("1.6.0")
  override def load(path: String): StringIndexerModel = super.load(path)
}

/**
 * A `Transformer` that maps a column of indices back to a new column of corresponding
 * string values.
 * The index-string mapping is either from the ML attributes of the input column,
 * or from user-supplied labels (which take precedence over ML attributes).
 *
 * @see `StringIndexer` for converting strings into indices
 */
@Since("1.5.0")
class IndexToString @Since("2.2.0") (@Since("1.5.0") override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  @Since("1.5.0")
  def this() =
    this(Identifiable.randomUID("idxToStr"))

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setLabels(value: Array[String]): this.type = set(labels, value)

  /**
   * Optional param for array of labels specifying index-string mapping.
   *
   * Default: Not specified, in which case [[inputCol]] metadata is used for labels.
   * @group param
   */
  @Since("1.5.0")
  final val labels: StringArrayParam = new StringArrayParam(this, "labels",
    "Optional array of labels specifying index-string mapping." +
      " If not provided or if empty, then metadata from inputCol is used instead.")

  /** @group getParam */
  @Since("1.5.0")
  final def getLabels: Array[String] = $(labels)

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = SchemaUtils.getSchemaFieldType(schema, inputColName)
    require(inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be a numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val outputFields = inputFields :+ StructField($(outputCol), StringType)
    StructType(outputFields)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val inputColSchema = SchemaUtils.getSchemaField(dataset.schema, $(inputCol))
    // If the labels array is empty use column metadata
    val values = if (!isDefined(labels) || $(labels).isEmpty) {
      Attribute.fromStructField(inputColSchema)
        .asInstanceOf[NominalAttribute].values.get
    } else {
      $(labels)
    }
    val indexer = udf { index: Double =>
      val idx = index.toInt
      if (0 <= idx && idx < values.length) {
        values(idx)
      } else {
        throw new SparkException(s"Unseen index: $index ??")
      }
    }
    val outputColName = $(outputCol)
    dataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(DoubleType)).as(outputColName))
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): IndexToString = {
    defaultCopy(extra)
  }
}

@Since("1.6.0")
object IndexToString extends DefaultParamsReadable[IndexToString] {

  @Since("1.6.0")
  override def load(path: String): IndexToString = super.load(path)
}
