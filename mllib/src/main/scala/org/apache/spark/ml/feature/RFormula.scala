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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model, Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasHandleInvalid, HasLabelCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._

/**
 * Base trait for [[RFormula]] and [[RFormulaModel]].
 */
private[feature] trait RFormulaBase extends HasFeaturesCol with HasLabelCol with HasHandleInvalid {

  /**
   * R formula parameter. The formula is provided in string form.
   * @group param
   */
  @Since("1.5.0")
  val formula: Param[String] = new Param(this, "formula", "R model formula")

  /** @group getParam */
  @Since("1.5.0")
  def getFormula: String = $(formula)

  /**
   * Force to index label whether it is numeric or string type.
   * Usually we index label only when it is string type.
   * If the formula was used by classification algorithms,
   * we can force to index label even it is numeric type by setting this param with true.
   * Default: false.
   * @group param
   */
  @Since("2.1.0")
  val forceIndexLabel: BooleanParam = new BooleanParam(this, "forceIndexLabel",
    "Force to index label whether it is numeric or string")
  setDefault(forceIndexLabel -> false)

  /** @group getParam */
  @Since("2.1.0")
  def getForceIndexLabel: Boolean = $(forceIndexLabel)

  /**
   * Param for how to handle invalid data (unseen or NULL values) in features and label column
   * of string type. Options are 'skip' (filter out rows with invalid data),
   * 'error' (throw an error), or 'keep' (put invalid data in a special additional
   * bucket, at index numLabels).
   * Default: "error"
   * @group param
   */
  @Since("2.3.0")
  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "How to handle invalid data (unseen or NULL values) in features and label column of string " +
    "type. Options are 'skip' (filter out rows with invalid data), error (throw an error), " +
    "or 'keep' (put invalid data in a special additional bucket, at index numLabels).",
    ParamValidators.inArray(StringIndexer.supportedHandleInvalids))
  setDefault(handleInvalid, StringIndexer.ERROR_INVALID)

  /**
   * Param for how to order categories of a string FEATURE column used by `StringIndexer`.
   * The last category after ordering is dropped when encoding strings.
   * Supported options: 'frequencyDesc', 'frequencyAsc', 'alphabetDesc', 'alphabetAsc'.
   * The default value is 'frequencyDesc'. When the ordering is set to 'alphabetDesc', `RFormula`
   * drops the same category as R when encoding strings.
   *
   * The options are explained using an example `'b', 'a', 'b', 'a', 'c', 'b'`:
   * {{{
   * +-----------------+---------------------------------------+----------------------------------+
   * |      Option     | Category mapped to 0 by StringIndexer |  Category dropped by RFormula    |
   * +-----------------+---------------------------------------+----------------------------------+
   * | 'frequencyDesc' | most frequent category ('b')          | least frequent category ('c')    |
   * | 'frequencyAsc'  | least frequent category ('c')         | most frequent category ('b')     |
   * | 'alphabetDesc'  | last alphabetical category ('c')      | first alphabetical category ('a')|
   * | 'alphabetAsc'   | first alphabetical category ('a')     | last alphabetical category ('c') |
   * +-----------------+---------------------------------------+----------------------------------+
   * }}}
   * Note that this ordering option is NOT used for the label column. When the label column is
   * indexed, it uses the default descending frequency ordering in `StringIndexer`.
   *
   * @group param
   */
  @Since("2.3.0")
  final val stringIndexerOrderType: Param[String] = new Param(this, "stringIndexerOrderType",
    "How to order categories of a string FEATURE column used by StringIndexer. " +
    "The last category after ordering is dropped when encoding strings. " +
    s"Supported options: ${StringIndexer.supportedStringOrderType.mkString(", ")}. " +
    "The default value is 'frequencyDesc'. When the ordering is set to 'alphabetDesc', " +
    "RFormula drops the same category as R when encoding strings.",
    ParamValidators.inArray(StringIndexer.supportedStringOrderType))
  setDefault(stringIndexerOrderType, StringIndexer.frequencyDesc)

  /** @group getParam */
  @Since("2.3.0")
  def getStringIndexerOrderType: String = $(stringIndexerOrderType)

  protected def hasLabelCol(schema: StructType): Boolean = {
    schema.map(_.name).contains($(labelCol))
  }
}

/**
 * :: Experimental ::
 * Implements the transforms required for fitting a dataset against an R model formula. Currently
 * we support a limited subset of the R operators, including '~', '.', ':', '+', and '-'. Also see
 * the R formula docs here: http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html
 *
 * The basic operators are:
 *  - `~` separate target and terms
 *  - `+` concat terms, "+ 0" means removing intercept
 *  - `-` remove a term, "- 1" means removing intercept
 *  - `:` interaction (multiplication for numeric values, or binarized categorical values)
 *  - `.` all columns except target
 *
 * Suppose `a` and `b` are double columns, we use the following simple examples
 * to illustrate the effect of `RFormula`:
 *  - `y ~ a + b` means model `y ~ w0 + w1 * a + w2 * b` where `w0` is the intercept and `w1, w2`
 * are coefficients.
 *  - `y ~ a + b + a:b - 1` means model `y ~ w1 * a + w2 * b + w3 * a * b` where `w1, w2, w3`
 * are coefficients.
 *
 * RFormula produces a vector column of features and a double or string column of label.
 * Like when formulas are used in R for linear regression, string input columns will be one-hot
 * encoded, and numeric columns will be cast to doubles.
 * If the label column is of type string, it will be first transformed to double with
 * `StringIndexer`. If the label column does not exist in the DataFrame, the output label column
 * will be created from the specified response variable in the formula.
 */
@Experimental
@Since("1.5.0")
class RFormula @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends Estimator[RFormulaModel] with RFormulaBase with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("rFormula"))

  /**
   * Sets the formula to use for this transformer. Must be called before use.
   * @group setParam
   * @param value an R formula in string form (e.g. "y ~ x + z")
   */
  @Since("1.5.0")
  def setFormula(value: String): this.type = set(formula, value)

  /** @group setParam */
  @Since("2.3.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /** @group setParam */
  @Since("1.5.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("2.1.0")
  def setForceIndexLabel(value: Boolean): this.type = set(forceIndexLabel, value)

  /** @group setParam */
  @Since("2.3.0")
  def setStringIndexerOrderType(value: String): this.type = set(stringIndexerOrderType, value)

  /** Whether the formula specifies fitting an intercept. */
  private[ml] def hasIntercept: Boolean = {
    require(isDefined(formula), "Formula must be defined first.")
    RFormulaParser.parse($(formula)).hasIntercept
  }

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): RFormulaModel = {
    transformSchema(dataset.schema, logging = true)
    require(isDefined(formula), "Formula must be defined first.")
    val parsedFormula = RFormulaParser.parse($(formula))
    val resolvedFormula = parsedFormula.resolve(dataset.schema)
    val encoderStages = ArrayBuffer[PipelineStage]()
    val oneHotEncodeColumns = ArrayBuffer[(String, String)]()

    val prefixesToRewrite = mutable.Map[String, String]()
    val tempColumns = ArrayBuffer[String]()
    def tmpColumn(category: String): String = {
      val col = Identifiable.randomUID(category)
      tempColumns += col
      col
    }

    // First we index each string column referenced by the input terms.
    val indexed: Map[String, String] = resolvedFormula.terms.flatten.distinct.map { term =>
      dataset.schema(term).dataType match {
        case _: StringType =>
          val indexCol = tmpColumn("stridx")
          encoderStages += new StringIndexer()
            .setInputCol(term)
            .setOutputCol(indexCol)
            .setStringOrderType($(stringIndexerOrderType))
            .setHandleInvalid($(handleInvalid))
          prefixesToRewrite(indexCol + "_") = term + "_"
          (term, indexCol)
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(dataset.schema(term))
          val size = if (group.size < 0) {
            dataset.select(term).first().getAs[Vector](0).size
          } else {
            group.size
          }
          encoderStages += new VectorSizeHint(uid)
            .setHandleInvalid("optimistic")
            .setInputCol(term)
            .setSize(size)
          (term, term)
        case _ =>
          (term, term)
      }
    }.toMap

    // Then we handle one-hot encoding and interactions between terms.
    var keepReferenceCategory = false
    val encodedTerms = resolvedFormula.terms.map {
      case Seq(term) if dataset.schema(term).dataType == StringType =>
        val encodedCol = tmpColumn("onehot")
        // Formula w/o intercept, one of the categories in the first category feature is
        // being used as reference category, we will not drop any category for that feature.
        if (!hasIntercept && !keepReferenceCategory) {
          encoderStages += new OneHotEncoderEstimator(uid)
            .setInputCols(Array(indexed(term)))
            .setOutputCols(Array(encodedCol))
            .setDropLast(false)
          keepReferenceCategory = true
        } else {
          oneHotEncodeColumns += indexed(term) -> encodedCol
        }
        prefixesToRewrite(encodedCol + "_") = term + "_"
        encodedCol
      case Seq(term) =>
        term
      case terms =>
        val interactionCol = tmpColumn("interaction")
        encoderStages += new Interaction()
          .setInputCols(terms.map(indexed).toArray)
          .setOutputCol(interactionCol)
        prefixesToRewrite(interactionCol + "_") = ""
        interactionCol
    }

    if (oneHotEncodeColumns.nonEmpty) {
      val (inputCols, outputCols) = oneHotEncodeColumns.toArray.unzip
      encoderStages += new OneHotEncoderEstimator(uid)
        .setInputCols(inputCols)
        .setOutputCols(outputCols)
        .setDropLast(true)
    }

    encoderStages += new VectorAssembler(uid)
      .setInputCols(encodedTerms.toArray)
      .setOutputCol($(featuresCol))
      .setHandleInvalid($(handleInvalid))
    encoderStages += new VectorAttributeRewriter($(featuresCol), prefixesToRewrite.toMap)
    encoderStages += new ColumnPruner(tempColumns.toSet)

    if ((dataset.schema.fieldNames.contains(resolvedFormula.label) &&
      dataset.schema(resolvedFormula.label).dataType == StringType) || $(forceIndexLabel)) {
      encoderStages += new StringIndexer()
        .setInputCol(resolvedFormula.label)
        .setOutputCol($(labelCol))
        .setHandleInvalid($(handleInvalid))
    }

    val pipelineModel = new Pipeline(uid).setStages(encoderStages.toArray).fit(dataset)
    copyValues(new RFormulaModel(uid, resolvedFormula, pipelineModel).setParent(this))
  }

  @Since("1.5.0")
  // optimistic schema; does not contain any ML attributes
  override def transformSchema(schema: StructType): StructType = {
    require(!hasLabelCol(schema) || !$(forceIndexLabel),
      "If label column already exists, forceIndexLabel can not be set with true.")
    if (hasLabelCol(schema)) {
      StructType(schema.fields :+ StructField($(featuresCol), new VectorUDT, true))
    } else {
      StructType(schema.fields :+ StructField($(featuresCol), new VectorUDT, true) :+
        StructField($(labelCol), DoubleType, true))
    }
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): RFormula = defaultCopy(extra)

  @Since("2.0.0")
  override def toString: String = s"RFormula(${get(formula).getOrElse("")}) (uid=$uid)"
}

@Since("2.0.0")
object RFormula extends DefaultParamsReadable[RFormula] {

  @Since("2.0.0")
  override def load(path: String): RFormula = super.load(path)
}

/**
 * :: Experimental ::
 * Model fitted by [[RFormula]]. Fitting is required to determine the factor levels of
 * formula terms.
 *
 * @param resolvedFormula the fitted R formula.
 * @param pipelineModel the fitted feature model, including factor to index mappings.
 */
@Experimental
@Since("1.5.0")
class RFormulaModel private[feature](
    @Since("1.5.0") override val uid: String,
    private[ml] val resolvedFormula: ResolvedRFormula,
    private[ml] val pipelineModel: PipelineModel)
  extends Model[RFormulaModel] with RFormulaBase with MLWritable {

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    checkCanTransform(dataset.schema)
    transformLabel(pipelineModel.transform(dataset))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    checkCanTransform(schema)
    val withFeatures = pipelineModel.transformSchema(schema)
    if (resolvedFormula.label.isEmpty || hasLabelCol(withFeatures)) {
      withFeatures
    } else if (schema.exists(_.name == resolvedFormula.label)) {
      val nullable = schema(resolvedFormula.label).dataType match {
        case _: NumericType | BooleanType => false
        case _ => true
      }
      StructType(withFeatures.fields :+ StructField($(labelCol), DoubleType, nullable))
    } else {
      // Ignore the label field. This is a hack so that this transformer can also work on test
      // datasets in a Pipeline.
      withFeatures
    }
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): RFormulaModel = {
    val copied = new RFormulaModel(uid, resolvedFormula, pipelineModel).setParent(parent)
    copyValues(copied, extra)
  }

  @Since("2.0.0")
  override def toString: String = s"RFormulaModel($resolvedFormula) (uid=$uid)"

  private def transformLabel(dataset: Dataset[_]): DataFrame = {
    val labelName = resolvedFormula.label
    if (labelName.isEmpty || hasLabelCol(dataset.schema)) {
      dataset.toDF
    } else if (dataset.schema.exists(_.name == labelName)) {
      dataset.schema(labelName).dataType match {
        case _: NumericType | BooleanType =>
          dataset.withColumn($(labelCol), dataset(labelName).cast(DoubleType))
        case other =>
          throw new IllegalArgumentException("Unsupported type for label: " + other)
      }
    } else {
      // Ignore the label field. This is a hack so that this transformer can also work on test
      // datasets in a Pipeline.
      dataset.toDF
    }
  }

  private def checkCanTransform(schema: StructType) {
    val columnNames = schema.map(_.name)
    require(!columnNames.contains($(featuresCol)), "Features column already exists.")
    require(
      !columnNames.contains($(labelCol)) || schema($(labelCol)).dataType.isInstanceOf[NumericType],
      s"Label column already exists and is not of type ${NumericType.simpleString}.")
  }

  @Since("2.0.0")
  override def write: MLWriter = new RFormulaModel.RFormulaModelWriter(this)
}

@Since("2.0.0")
object RFormulaModel extends MLReadable[RFormulaModel] {

  @Since("2.0.0")
  override def read: MLReader[RFormulaModel] = new RFormulaModelReader

  @Since("2.0.0")
  override def load(path: String): RFormulaModel = super.load(path)

  /** [[MLWriter]] instance for [[RFormulaModel]] */
  private[RFormulaModel] class RFormulaModelWriter(instance: RFormulaModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: resolvedFormula
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(instance.resolvedFormula))
        .repartition(1).write.parquet(dataPath)
      // Save pipeline model
      val pmPath = new Path(path, "pipelineModel").toString
      instance.pipelineModel.save(pmPath)
    }
  }

  private class RFormulaModelReader extends MLReader[RFormulaModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[RFormulaModel].getName

    override def load(path: String): RFormulaModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("label", "terms", "hasIntercept").head()
      val label = data.getString(0)
      val terms = data.getAs[Seq[Seq[String]]](1)
      val hasIntercept = data.getBoolean(2)
      val resolvedRFormula = ResolvedRFormula(label, terms, hasIntercept)

      val pmPath = new Path(path, "pipelineModel").toString
      val pipelineModel = PipelineModel.load(pmPath)

      val model = new RFormulaModel(metadata.uid, resolvedRFormula, pipelineModel)

      metadata.getAndSetParams(model)
      model
    }
  }
}

/**
 * Utility transformer for removing temporary columns from a DataFrame.
 * TODO(ekl) make this a public transformer
 */
private class ColumnPruner(override val uid: String, val columnsToPrune: Set[String])
  extends Transformer with MLWritable {

  def this(columnsToPrune: Set[String]) =
    this(Identifiable.randomUID("columnPruner"), columnsToPrune)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val columnsToKeep = dataset.columns.filter(!columnsToPrune.contains(_))
    dataset.select(columnsToKeep.map(dataset.col): _*)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields.filter(col => !columnsToPrune.contains(col.name)))
  }

  override def copy(extra: ParamMap): ColumnPruner = defaultCopy(extra)

  override def write: MLWriter = new ColumnPruner.ColumnPrunerWriter(this)
}

private object ColumnPruner extends MLReadable[ColumnPruner] {

  override def read: MLReader[ColumnPruner] = new ColumnPrunerReader

  override def load(path: String): ColumnPruner = super.load(path)

  /** [[MLWriter]] instance for [[ColumnPruner]] */
  private[ColumnPruner] class ColumnPrunerWriter(instance: ColumnPruner) extends MLWriter {

    private case class Data(columnsToPrune: Seq[String])

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: columnsToPrune
      val data = Data(instance.columnsToPrune.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class ColumnPrunerReader extends MLReader[ColumnPruner] {

    /** Checked against metadata when loading model */
    private val className = classOf[ColumnPruner].getName

    override def load(path: String): ColumnPruner = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("columnsToPrune").head()
      val columnsToPrune = data.getAs[Seq[String]](0).toSet
      val pruner = new ColumnPruner(metadata.uid, columnsToPrune)

      metadata.getAndSetParams(pruner)
      pruner
    }
  }
}

/**
 * Utility transformer that rewrites Vector attribute names via prefix replacement. For example,
 * it can rewrite attribute names starting with 'foo_' to start with 'bar_' instead.
 *
 * @param vectorCol name of the vector column to rewrite.
 * @param prefixesToRewrite the map of string prefixes to their replacement values. Each attribute
 *                          name defined in vectorCol will be checked against the keys of this
 *                          map. When a key prefixes a name, the matching prefix will be replaced
 *                          by the value in the map.
 */
private class VectorAttributeRewriter(
    override val uid: String,
    val vectorCol: String,
    val prefixesToRewrite: Map[String, String])
  extends Transformer with MLWritable {

  def this(vectorCol: String, prefixesToRewrite: Map[String, String]) =
    this(Identifiable.randomUID("vectorAttrRewriter"), vectorCol, prefixesToRewrite)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val metadata = {
      val group = AttributeGroup.fromStructField(dataset.schema(vectorCol))
      val attrs = group.attributes.get.map { attr =>
        if (attr.name.isDefined) {
          val name = prefixesToRewrite.foldLeft(attr.name.get) { case (curName, (from, to)) =>
            curName.replace(from, to)
          }
          attr.withName(name)
        } else {
          attr
        }
      }
      new AttributeGroup(vectorCol, attrs).toMetadata()
    }
    val otherCols = dataset.columns.filter(_ != vectorCol).map(dataset.col)
    val rewrittenCol = dataset.col(vectorCol).as(vectorCol, metadata)
    dataset.select(otherCols :+ rewrittenCol : _*)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      schema.fields.filter(_.name != vectorCol) ++
      schema.fields.filter(_.name == vectorCol))
  }

  override def copy(extra: ParamMap): VectorAttributeRewriter = defaultCopy(extra)

  override def write: MLWriter = new VectorAttributeRewriter.VectorAttributeRewriterWriter(this)
}

private object VectorAttributeRewriter extends MLReadable[VectorAttributeRewriter] {

  override def read: MLReader[VectorAttributeRewriter] = new VectorAttributeRewriterReader

  override def load(path: String): VectorAttributeRewriter = super.load(path)

  /** [[MLWriter]] instance for [[VectorAttributeRewriter]] */
  private[VectorAttributeRewriter]
  class VectorAttributeRewriterWriter(instance: VectorAttributeRewriter) extends MLWriter {

    private case class Data(vectorCol: String, prefixesToRewrite: Map[String, String])

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: vectorCol, prefixesToRewrite
      val data = Data(instance.vectorCol, instance.prefixesToRewrite)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class VectorAttributeRewriterReader extends MLReader[VectorAttributeRewriter] {

    /** Checked against metadata when loading model */
    private val className = classOf[VectorAttributeRewriter].getName

    override def load(path: String): VectorAttributeRewriter = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("vectorCol", "prefixesToRewrite").head()
      val vectorCol = data.getString(0)
      val prefixesToRewrite = data.getAs[Map[String, String]](1)
      val rewriter = new VectorAttributeRewriter(metadata.uid, vectorCol, prefixesToRewrite)

      metadata.getAndSetParams(rewriter)
      rewriter
    }
  }
}
