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

import org.apache.spark.SparkException
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

import scala.collection.mutable

/**
 * Base trait for [[StringIndexer]] and [[StringIndexerModel]].
 */
private[feature] trait StringIndexerBase extends Params with HasInputCol with HasOutputCol
    with HasHandleInvalid with HasInputCols with HasOutputCols {

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val outputColNames = $(outputCols)
    val inputDataTypes = inputColNames.map(name => schema(name).dataType)
    inputDataTypes.foreach {
      case _: NumericType | StringType =>
      case other =>
        throw new IllegalArgumentException("The input columns must be either string type " +
          s"or numeric type, but got $other.")
    }
    val originalFields = schema.fields
    val originalColNames = originalFields.map(_.name)
    val intersect = outputColNames.toSet.intersect(originalColNames.toSet)
    if (intersect.nonEmpty) {
      throw new IllegalArgumentException(s"Output column ${intersect.mkString("[", ",", "]")} " +
        "already exists.")
    }
    val attrs = $(outputCols).map { x => NominalAttribute.defaultAttr.withName(x) }
    val outputFields = Array.concat(originalFields, attrs.map(_.toStructField()))
    StructType(outputFields)
  }

  override def validateParams(): Unit = {
    if (isSet(inputCols) && isSet(inputCol)) {
      require($(inputCols).contains($(inputCol)), "StringIndexer found inconsistent values " +
        s"for inputCol and inputCols. Param inputCol is set with $inputCol which is not " +
        s"included by inputCols $inputCols")
    }
    if (isSet(outputCols) && isSet(outputCol)) {
      require($(outputCols).contains($(outputCol)), "StringIndexer found inconsistent values " +
        s"for outputCol and outputCols. Param outputCol is set with $outputCol which is not " +
        s"included by outputCols $outputCols")
    }
    require($(inputCols).length == $(outputCols).length, "StringIndexer inputCols' length " +
      s"${$(inputCols).length} is not equal with outputCols' length ${$(outputCols).length}")
  }
}

/**
 * :: Experimental ::
 * A label indexer that maps a string column of labels to an ML column of label indices.
 * If the input column is numeric, we cast it to string and index the string values.
 * The indices are in [0, numLabels), ordered by label frequencies.
 * So the most frequent label gets index 0.
 *
 * @see [[IndexToString]] for the inverse transformation
 */
@Experimental
class StringIndexer(override val uid: String) extends Estimator[StringIndexerModel]
  with StringIndexerBase {

  def this() = this(Identifiable.randomUID("strIdx"))

  /** @group setParam */
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  setDefault(handleInvalid, "error")

  /** @group setParam */
  def setInputCol(value: String): this.type = {
    set(inputCol, value)
    if (!isSet(inputCols)) {
      set(inputCols, Array(value))
    }
    this
  }

  /** @group setParam */
  def setOutputCol(value: String): this.type = {
    set(outputCol, value)
    if (!isSet(outputCols)) {
      set(outputCols, Array(value))
    }
    this
  }

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  override def fit(dataset: DataFrame): StringIndexerModel = {
    val data = dataset.select($(inputCols).map(col(_).cast(StringType)) : _*)
    val counts = data.rdd.treeAggregate(new Aggregator)(_.add(_), _.merge(_)).distinctArray
    val labels = counts.map(_.toSeq.sortBy(-_._2).map(_._1).toArray)
    copyValues(new StringIndexerModel(uid, labels).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): StringIndexer = defaultCopy(extra)
}

private[feature] class Aggregator extends Serializable {

  var initialized: Boolean = false
  var k: Int = _
  var distinctArray: Array[mutable.HashMap[String, Long]] = _

  private def init(k: Int): Unit = {
    this.k = k
    distinctArray = new Array[mutable.HashMap[String, Long]](k)
    (0 until k).foreach { x =>
      distinctArray(x) = new mutable.HashMap[String, Long]
    }
    initialized = true
  }

  def add(r: Row): this.type = {
    if (!initialized) {
      init(r.size)
    }
    (0 until k).foreach { x =>
      val current = r.getString(x)
      val count: Long = distinctArray(x).getOrElse(current, 0L)
      distinctArray(x).put(current, count + 1)
    }
    this
  }

  def merge(other: Aggregator): Aggregator = {
    (0 until k).foreach { x =>
      other.distinctArray(x).foreach {
        case (key, value) =>
          val count: Long = this.distinctArray(x).getOrElse(key, 0L)
          this.distinctArray(x).put(key, count + value)
      }
    }
    this
  }
}

/**
 * :: Experimental ::
 * Model fitted by [[StringIndexer]].
 *
 * NOTE: During transformation, if the input column does not exist,
 * [[StringIndexerModel.transform]] would return the input dataset unmodified.
 * This is a temporary fix for the case when target labels do not exist during prediction.
 *
 * @param labels  Ordered list of labels, corresponding to indices to be assigned.
 */
@Experimental
class StringIndexerModel (
    override val uid: String,
    val labels: Array[Array[String]]) extends Model[StringIndexerModel] with StringIndexerBase {

  def this(labels: Array[Array[String]]) = this(Identifiable.randomUID("strIdx"), labels)

  private val labelToIndex: Array[OpenHashMap[String, Double]] = {
    val k = labels.length
    val mapArray = new Array[OpenHashMap[String, Double]](k)
    (0 until k).foreach { x =>
      val n = labels(x).length
      mapArray(x) = new OpenHashMap[String, Double](k)
      var i = 0
      while (i < n) {
        mapArray(x).update(labels(x)(i), i)
        i += 1
      }
    }
    mapArray
  }

  /** @group setParam */
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  setDefault(handleInvalid, "error")

  /** @group setParam */
  def setInputCol(value: String): this.type = {
    set(inputCol, value)
    if (!isSet(inputCols)) {
      set(inputCols, Array(value))
    }
    this
  }

  /** @group setParam */
  def setOutputCol(value: String): this.type = {
    set(outputCol, value)
    if (!isSet(outputCols)) {
      set(outputCols, Array(value))
    }
    this
  }

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  override def transform(dataset: DataFrame): DataFrame = {
    val notExists = $(inputCols).filter(!dataset.schema.fieldNames.contains(_))
    if (notExists.length > 0) {
      logInfo(s"Input columns ${notExists.mkString("[", ",", "]")} do not exist " +
        "during transformation. Skip StringIndexerModel.")
      return dataset
    }

    val k = $(inputCols).length

    // If we are skipping invalid records, filter them out.
    val filteredDataset = (getHandleInvalid) match {
      case "skip" => {
        (0 until k).foldLeft[DataFrame](dataset) {
          case (df, x) => {
            val filterer = udf { label: String =>
              labelToIndex(x).contains(label)
            }
            dataset.where(filterer(dataset($(inputCols)(x))))
          }
        }
      }
      case _ => dataset
    }

    val transformedDataset = (0 until k).foldLeft[DataFrame](filteredDataset) {
      case (df, x) => {
        val indexer = udf { label: String =>
          if (labelToIndex(x).contains(label)) {
            labelToIndex(x)(label)
          } else {
            throw new SparkException(s"Unseen label: $label.")
          }
        }

        val inputCol = $(inputCols)(x)
        val outputCol = $(outputCols)(x)
        val metadata = NominalAttribute.defaultAttr.withName(inputCol)
          .withValues(labels(x)).toMetadata()

        df.withColumn(outputCol, indexer(col($(inputCols)(x))).as(outputCol, metadata))
      }
    }

    transformedDataset
  }

  override def transformSchema(schema: StructType): StructType = {
    if ($(inputCols).filter(!schema.fieldNames.contains(_)).isEmpty) {
      validateAndTransformSchema(schema)
    } else {
      // If not all the input columns exist during transformation, we skip StringIndexerModel.
      schema
    }
  }

  override def copy(extra: ParamMap): StringIndexerModel = {
    val copied = new StringIndexerModel(uid, labels)
    copyValues(copied, extra).setParent(parent)
  }
}

/**
 * :: Experimental ::
 * A [[Transformer]] that maps a column of indices back to a new column of corresponding
 * string values.
 * The index-string mapping is either from the ML attributes of the input column,
 * or from user-supplied labels (which take precedence over ML attributes).
 *
 * @see [[StringIndexer]] for converting strings into indices
 */
@Experimental
class IndexToString private[ml] (
  override val uid: String) extends Transformer
    with HasInputCol with HasOutputCol {

  def this() =
    this(Identifiable.randomUID("idxToStr"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setLabels(value: Array[String]): this.type = set(labels, value)

  /**
   * Optional param for array of labels specifying index-string mapping.
   *
   * Default: Empty array, in which case [[inputCol]] metadata is used for labels.
   * @group param
   */
  final val labels: StringArrayParam = new StringArrayParam(this, "labels",
    "Optional array of labels specifying index-string mapping." +
      " If not provided or if empty, then metadata from inputCol is used instead.")
  setDefault(labels, Array.empty[String])

  /** @group getParam */
  final def getLabels: Array[String] = $(labels)

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
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

  override def transform(dataset: DataFrame): DataFrame = {
    val inputColSchema = dataset.schema($(inputCol))
    // If the labels array is empty use column metadata
    val values = if ($(labels).isEmpty) {
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

  override def copy(extra: ParamMap): IndexToString = {
    defaultCopy(extra)
  }
}
