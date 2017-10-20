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
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCols, HasOutputCols}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, NumericType, StructField, StructType}

/** Private trait for params for OneHotEncoderEstimator and OneHotEncoderModel */
private[ml] trait OneHotEncoderParams extends Params with HasHandleInvalid
    with HasInputCols with HasOutputCols {

  /**
   * Param for how to handle invalid data.
   * Options are 'skip' (filter out rows with invalid data) or 'error' (throw an error).
   * Default: "error"
   * @group param
   */
  @Since("2.3.0")
  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "How to handle invalid data " +
    "Options are 'skip' (filter out rows with invalid data) or error (throw an error).",
    ParamValidators.inArray(OneHotEncoderEstimator.supportedHandleInvalids))

  setDefault(handleInvalid, OneHotEncoderEstimator.ERROR_INVALID)

  /**
   * Whether to drop the last category in the encoded vector (default: true)
   * @group param
   */
  @Since("2.3.0")
  final val dropLast: BooleanParam =
    new BooleanParam(this, "dropLast", "whether to drop the last category")
  setDefault(dropLast -> true)

  /** @group getParam */
  @Since("2.3.0")
  def getDropLast: Boolean = $(dropLast)
}

/**
 * A one-hot encoder that maps a column of category indices to a column of binary vectors, with
 * at most a single one-value per row that indicates the input category index.
 * For example with 5 categories, an input value of 2.0 would map to an output vector of
 * `[0.0, 0.0, 1.0, 0.0]`.
 * The last category is not included by default (configurable via `dropLast`),
 * because it makes the vector entries sum up to one, and hence linearly dependent.
 * So an input value of 4.0 maps to `[0.0, 0.0, 0.0, 0.0]`.
 *
 * @note This is different from scikit-learn's OneHotEncoder, which keeps all categories.
 * The output vectors are sparse.
 *
 * @see `StringIndexer` for converting categorical values into category indices
 */
@Since("2.3.0")
class OneHotEncoderEstimator @Since("2.3.0") (@Since("2.3.0") override val uid: String)
    extends Estimator[OneHotEncoderModel] with OneHotEncoderParams with DefaultParamsWritable {

  @Since("2.3.0")
  def this() = this(Identifiable.randomUID("oneHotEncoder"))

  /** @group setParam */
  @Since("2.3.0")
  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */
  @Since("2.3.0")
  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  /** @group setParam */
  @Since("2.3.0")
  def setDropLast(value: Boolean): this.type = set(dropLast, value)

  /** @group setParam */
  @Since("2.3.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  @Since("2.3.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val outputColNames = $(outputCols)
    val inputFields = schema.fields

    require(inputColNames.length == outputColNames.length,
      s"The number of input columns ${inputColNames.length} must be the same as the number of " +
        s"output columns ${outputColNames.length}.")

    val outputFields = inputColNames.zip(outputColNames).map { case (inputColName, outputColName) =>

      require(schema(inputColName).dataType.isInstanceOf[NumericType],
        s"Input column must be of type NumericType but got ${schema(inputColName).dataType}")
      require(!inputFields.exists(_.name == outputColName),
        s"Output column $outputColName already exists.")

      OneHotEncoderCommon.transformOutputColumnSchema(
        schema(inputColName), $(dropLast), outputColName)
    }
    StructType(inputFields ++ outputFields)
  }

  @Since("2.3.0")
  override def fit(dataset: Dataset[_]): OneHotEncoderModel = {
    val transformedSchema = transformSchema(dataset.schema)
    val categorySizes = new Array[Int]($(outputCols).length)

    val columnToScanIndices = $(outputCols).zipWithIndex.flatMap { case (outputColName, idx) =>
      val numOfAttrs = AttributeGroup.fromStructField(
        transformedSchema(outputColName)).size
      if (numOfAttrs < 0) {
        Some(idx)
      } else {
        categorySizes(idx) = numOfAttrs
        None
      }
    }

    // Some input columns don't have attributes or their attributes don't have necessary info.
    // We need to scan the data to get the number of values for each column.
    if (columnToScanIndices.length > 0) {
      val inputColNames = columnToScanIndices.map($(inputCols)(_))
      val outputColNames = columnToScanIndices.map($(outputCols)(_))
      val attrGroups = OneHotEncoderCommon.getOutputAttrGroupFromData(
        dataset, $(dropLast), inputColNames, outputColNames)
      attrGroups.zip(columnToScanIndices).foreach { case (attrGroup, idx) =>
        categorySizes(idx) = attrGroup.size
      }
    }

    val model = new OneHotEncoderModel(uid, categorySizes).setParent(this)
    copyValues(model)
  }

  @Since("2.3.0")
  override def copy(extra: ParamMap): OneHotEncoderEstimator = defaultCopy(extra)
}

@Since("2.3.0")
object OneHotEncoderEstimator extends DefaultParamsReadable[OneHotEncoderEstimator] {

  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val supportedHandleInvalids: Array[String] = Array(SKIP_INVALID, ERROR_INVALID)

  @Since("2.3.0")
  override def load(path: String): OneHotEncoderEstimator = super.load(path)
}

@Since("2.3.0")
class OneHotEncoderModel private[ml] (
    @Since("2.3.0") override val uid: String,
    @Since("2.3.0") val categorySizes: Array[Int])
  extends Model[OneHotEncoderModel] with OneHotEncoderParams with MLWritable {

  import OneHotEncoderModel._

  private def encoders: Array[UserDefinedFunction] = {
    val oneValue = Array(1.0)
    val emptyValues = Array.empty[Double]
    val emptyIndices = Array.empty[Int]
    val dropLast = getDropLast
    val handleInvalid = getHandleInvalid

    categorySizes.map { size =>
      udf { label: Double =>
        if (label < size) {
          Vectors.sparse(size, Array(label.toInt), oneValue)
        } else if (label == size && dropLast) {
          Vectors.sparse(size, emptyIndices, emptyValues)
        } else {
          if (handleInvalid == OneHotEncoderEstimator.ERROR_INVALID) {
            throw new SparkException(s"Unseen value: $label. To handle unseen values, " +
              s"set Param handleInvalid to ${OneHotEncoderEstimator.SKIP_INVALID}.")
          } else {
            Vectors.sparse(size, emptyIndices, emptyValues)
          }
        }
      }
    }
  }

  /** @group setParam */
  @Since("2.3.0")
  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */
  @Since("2.3.0")
  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  /** @group setParam */
  @Since("2.3.0")
  def setDropLast(value: Boolean): this.type = set(dropLast, value)

  /** @group setParam */
  @Since("2.3.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  @Since("2.3.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val outputColNames = $(outputCols)
    val inputFields = schema.fields

    require(inputColNames.length == outputColNames.length,
      s"The number of input columns ${inputColNames.length} must be the same as the number of " +
        s"output columns ${outputColNames.length}.")

    require(inputColNames.length == categorySizes.length,
      s"The number of input columns ${inputColNames.length} must be the same as the number of " +
        s"features ${categorySizes.length} during fitting.")

    val inputOutputPairs = inputColNames.zip(outputColNames)
    val outputFields = inputOutputPairs.map { case (inputColName, outputColName) =>

      require(schema(inputColName).dataType.isInstanceOf[NumericType],
        s"Input column must be of type NumericType but got ${schema(inputColName).dataType}")
      require(!inputFields.exists(_.name == outputColName),
        s"Output column $outputColName already exists.")

      OneHotEncoderCommon.transformOutputColumnSchema(
        schema(inputColName), $(dropLast), outputColName)
    }
    verifyNumOfValues(StructType(inputFields ++ outputFields))
  }

  private def verifyNumOfValues(schema: StructType): StructType = {
    $(outputCols).zipWithIndex.foreach { case (outputColName, idx) =>
      val inputColName = $(inputCols)(idx)
      val attrGroup = AttributeGroup.fromStructField(schema(outputColName))

      // If the input metadata specifies number of category,
      // compare with expected category number.
      if (attrGroup.attributes.nonEmpty) {
        require(attrGroup.size == categorySizes(idx), "OneHotEncoderModel expected " +
          s"${categorySizes(idx)} categorical values for input column ${inputColName}, but " +
            s"the input column had metadata specifying ${attrGroup.size} values.")
      }
    }
    schema
  }

  @Since("2.3.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    if (getDropLast && getHandleInvalid == OneHotEncoderEstimator.SKIP_INVALID) {
      throw new IllegalArgumentException("When Param handleInvalid is set to " +
        s"${OneHotEncoderEstimator.SKIP_INVALID}, Param dropLast can't be true, " +
        "because last category and invalid values will conflict in encoded vector.")
    }

    val transformedSchema = transformSchema(dataset.schema, logging = true)

    val encodedColumns = encoders.zipWithIndex.map { case (encoder, idx) =>
      val inputColName = $(inputCols)(idx)
      val outputColName = $(outputCols)(idx)

      val outputAttrGroupFromSchema =
        AttributeGroup.fromStructField(transformedSchema(outputColName))

      val metadata = if (outputAttrGroupFromSchema.size < 0) {
        OneHotEncoderCommon.createAttrGroupForAttrNames(outputColName, false,
          categorySizes(idx)).toMetadata()
      } else {
        outputAttrGroupFromSchema.toMetadata()
      }

      encoder(col(inputColName).cast(DoubleType)).as(outputColName, metadata)
    }
    val allCols = Seq(col("*")) ++ encodedColumns
    dataset.select(allCols: _*)
  }

  @Since("2.3.0")
  override def copy(extra: ParamMap): OneHotEncoderModel = {
    val copied = new OneHotEncoderModel(uid, categorySizes)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("2.3.0")
  override def write: MLWriter = new OneHotEncoderModelWriter(this)
}

@Since("2.3.0")
object OneHotEncoderModel extends MLReadable[OneHotEncoderModel] {

  private[OneHotEncoderModel]
  class OneHotEncoderModelWriter(instance: OneHotEncoderModel) extends MLWriter {

    private case class Data(categorySizes: Array[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.categorySizes)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class OneHotEncoderModelReader extends MLReader[OneHotEncoderModel] {

    private val className = classOf[OneHotEncoderModel].getName

    override def load(path: String): OneHotEncoderModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("categorySizes")
        .head()
      val categorySizes = data.getAs[Seq[Int]](0).toArray
      val model = new OneHotEncoderModel(metadata.uid, categorySizes)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("2.3.0")
  override def read: MLReader[OneHotEncoderModel] = new OneHotEncoderModelReader

  @Since("2.3.0")
  override def load(path: String): OneHotEncoderModel = super.load(path)
}

/**
 * Provides some helper methods used by both `OneHotEncoder` and `OneHotEncoderEstimator`.
 */
private[feature] object OneHotEncoderCommon {

  private def genOutputAttrNames(
      inputCol: StructField,
      outputColName: String): Option[Array[String]] = {
    val inputAttr = Attribute.fromStructField(inputCol)
    inputAttr match {
      case nominal: NominalAttribute =>
        if (nominal.values.isDefined) {
          nominal.values
        } else if (nominal.numValues.isDefined) {
          nominal.numValues.map(n => Array.tabulate(n)(_.toString))
        } else {
          None
        }
      case binary: BinaryAttribute =>
        if (binary.values.isDefined) {
          binary.values
        } else {
          Some(Array.tabulate(2)(_.toString))
        }
      case _: NumericAttribute =>
        throw new RuntimeException(
          s"The input column ${inputCol.name} cannot be numeric.")
      case _ =>
        None // optimistic about unknown attributes
    }
  }

  /** Creates an `AttributeGroup` filled by the `BinaryAttribute` named as required. */
  private def genOutputAttrGroup(
      outputAttrNames: Option[Array[String]],
      outputColName: String): AttributeGroup = {
    outputAttrNames.map { attrNames =>
      val attrs: Array[Attribute] = attrNames.map { name =>
        BinaryAttribute.defaultAttr.withName(name)
      }
      new AttributeGroup(outputColName, attrs)
    }.getOrElse{
      new AttributeGroup(outputColName)
    }
  }

  /**
   * Prepares the `StructField` with proper metadata for `OneHotEncoder`'s output column.
   */
  def transformOutputColumnSchema(
      inputCol: StructField,
      dropLast: Boolean,
      outputColName: String): StructField = {
    val outputAttrNames = genOutputAttrNames(inputCol, outputColName)
    val filteredOutputAttrNames = outputAttrNames.map { names =>
      if (dropLast) {
        require(names.length > 1,
          s"The input column ${inputCol.name} should have at least two distinct values.")
        names.dropRight(1)
      } else {
        names
      }
    }

    genOutputAttrGroup(filteredOutputAttrNames, outputColName).toStructField()
  }

  /**
   * This method is called when we want to generate `AttributeGroup` from actual data for
   * one-hot encoder.
   */
  def getOutputAttrGroupFromData(
      dataset: Dataset[_],
      dropLast: Boolean,
      inputColNames: Seq[String],
      outputColNames: Seq[String]): Seq[AttributeGroup] = {
    // The RDD approach has advantage of early-stop if any values are invalid. It seems that
    // DataFrame ops don't have equivalent functions.
    val columns = inputColNames.map { inputColName =>
      col(inputColName).cast(DoubleType)
    }
    val numOfColumns = columns.length

    val numAttrsArray = dataset.select(columns: _*).rdd.map { row =>
      val array = new Array[Double](numOfColumns)
      (0 until numOfColumns).foreach(idx => array(idx) = row.getDouble(idx))
      array
    }.treeAggregate(new Array[Double](numOfColumns))(
      (maxValues, curValues) => {
        (0 until numOfColumns).map { idx =>
          val x = curValues(idx)
          assert(x <= Int.MaxValue,
            s"OneHotEncoder only supports up to ${Int.MaxValue} indices, but got $x.")
          assert(x >= 0.0 && x == x.toInt,
            s"Values from column ${inputColNames(idx)} must be indices, but got $x.")
          math.max(maxValues(idx), x)
        }.toArray
      },
      (m0, m1) => {
        (0 until numOfColumns).map(idx => math.max(m0(idx), m1(idx))).toArray
      }
    ).map(_.toInt + 1)

    outputColNames.zip(numAttrsArray).map { case (outputColName, numAttrs) =>
      createAttrGroupForAttrNames(outputColName, dropLast, numAttrs)
    }
  }

  /** Creates an `AttributeGroup` with the required number of `BinaryAttribute`. */
  def createAttrGroupForAttrNames(
      outputColName: String,
      dropLast: Boolean,
      numAttrs: Int): AttributeGroup = {
    val outputAttrNames = Array.tabulate(numAttrs)(_.toString)
    val filtered = if (dropLast) outputAttrNames.dropRight(1) else outputAttrNames
    genOutputAttrGroup(Some(filtered), outputColName)
  }
}
