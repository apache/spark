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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.{Estimator, Model, Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
 * Base trait for [[RFormula]] and [[RFormulaModel]].
 */
private[feature] trait RFormulaBase extends HasFeaturesCol with HasLabelCol {

  protected def hasLabelCol(schema: StructType): Boolean = {
    schema.map(_.name).contains($(labelCol))
  }
}

/**
 * :: Experimental ::
 * Implements the transforms required for fitting a dataset against an R model formula. Currently
 * we support a limited subset of the R operators, including '~', '.', ':', '+', and '-'. Also see
 * the R formula docs here: http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html
 */
@Experimental
class RFormula(override val uid: String) extends Estimator[RFormulaModel] with RFormulaBase {

  def this() = this(Identifiable.randomUID("rFormula"))

  /**
   * R formula parameter. The formula is provided in string form.
   * @group param
   */
  val formula: Param[String] = new Param(this, "formula", "R model formula")

  /**
   * Sets the formula to use for this transformer. Must be called before use.
   * @group setParam
   * @param value an R formula in string form (e.g. "y ~ x + z")
   */
  def setFormula(value: String): this.type = set(formula, value)

  /** @group getParam */
  def getFormula: String = $(formula)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** Whether the formula specifies fitting an intercept. */
  private[ml] def hasIntercept: Boolean = {
    require(isDefined(formula), "Formula must be defined first.")
    RFormulaParser.parse($(formula)).hasIntercept
  }

  override def fit(dataset: DataFrame): RFormulaModel = {
    require(isDefined(formula), "Formula must be defined first.")
    val parsedFormula = RFormulaParser.parse($(formula))
    val resolvedFormula = parsedFormula.resolve(dataset.schema)
    val encoderStages = ArrayBuffer[PipelineStage]()

    val prefixesToRewrite = mutable.Map[String, String]()
    val tempColumns = ArrayBuffer[String]()
    def tmpColumn(category: String): String = {
      val col = Identifiable.randomUID(category)
      tempColumns += col
      col
    }

    // First we index each string column referenced by the input terms.
    val indexed: Map[String, String] = resolvedFormula.terms.flatten.distinct.map { term =>
      dataset.schema(term) match {
        case column if column.dataType == StringType =>
          val indexCol = tmpColumn("stridx")
          encoderStages += new StringIndexer()
            .setInputCol(term)
            .setOutputCol(indexCol)
          (term, indexCol)
        case _ =>
          (term, term)
      }
    }.toMap

    // Then we handle one-hot encoding and interactions between terms.
    val encodedTerms = resolvedFormula.terms.map {
      case Seq(term) if dataset.schema(term).dataType == StringType =>
        val encodedCol = tmpColumn("onehot")
        encoderStages += new OneHotEncoder()
          .setInputCol(indexed(term))
          .setOutputCol(encodedCol)
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

    encoderStages += new VectorAssembler(uid)
      .setInputCols(encodedTerms.toArray)
      .setOutputCol($(featuresCol))
    encoderStages += new VectorAttributeRewriter($(featuresCol), prefixesToRewrite.toMap)
    encoderStages += new ColumnPruner(tempColumns.toSet)

    if (dataset.schema.fieldNames.contains(resolvedFormula.label) &&
      dataset.schema(resolvedFormula.label).dataType == StringType) {
      encoderStages += new StringIndexer()
        .setInputCol(resolvedFormula.label)
        .setOutputCol($(labelCol))
    }

    val pipelineModel = new Pipeline(uid).setStages(encoderStages.toArray).fit(dataset)
    copyValues(new RFormulaModel(uid, resolvedFormula, pipelineModel).setParent(this))
  }

  // optimistic schema; does not contain any ML attributes
  override def transformSchema(schema: StructType): StructType = {
    if (hasLabelCol(schema)) {
      StructType(schema.fields :+ StructField($(featuresCol), new VectorUDT, true))
    } else {
      StructType(schema.fields :+ StructField($(featuresCol), new VectorUDT, true) :+
        StructField($(labelCol), DoubleType, true))
    }
  }

  override def copy(extra: ParamMap): RFormula = defaultCopy(extra)

  override def toString: String = s"RFormula(${get(formula)}) (uid=$uid)"
}

/**
 * :: Experimental ::
 * A fitted RFormula. Fitting is required to determine the factor levels of formula terms.
 * @param resolvedFormula the fitted R formula.
 * @param pipelineModel the fitted feature model, including factor to index mappings.
 */
@Experimental
class RFormulaModel private[feature](
    override val uid: String,
    resolvedFormula: ResolvedRFormula,
    pipelineModel: PipelineModel)
  extends Model[RFormulaModel] with RFormulaBase {

  override def transform(dataset: DataFrame): DataFrame = {
    checkCanTransform(dataset.schema)
    transformLabel(pipelineModel.transform(dataset))
  }

  override def transformSchema(schema: StructType): StructType = {
    checkCanTransform(schema)
    val withFeatures = pipelineModel.transformSchema(schema)
    if (hasLabelCol(withFeatures)) {
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

  override def copy(extra: ParamMap): RFormulaModel = copyValues(
    new RFormulaModel(uid, resolvedFormula, pipelineModel))

  override def toString: String = s"RFormulaModel(${resolvedFormula}) (uid=$uid)"

  private def transformLabel(dataset: DataFrame): DataFrame = {
    val labelName = resolvedFormula.label
    if (hasLabelCol(dataset.schema)) {
      dataset
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
      dataset
    }
  }

  private def checkCanTransform(schema: StructType) {
    val columnNames = schema.map(_.name)
    require(!columnNames.contains($(featuresCol)), "Features column already exists.")
    require(
      !columnNames.contains($(labelCol)) || schema($(labelCol)).dataType == DoubleType,
      "Label column already exists and is not of type DoubleType.")
  }
}

/**
 * Utility transformer for removing temporary columns from a DataFrame.
 * TODO(ekl) make this a public transformer
 */
private class ColumnPruner(columnsToPrune: Set[String]) extends Transformer {
  override val uid = Identifiable.randomUID("columnPruner")

  override def transform(dataset: DataFrame): DataFrame = {
    val columnsToKeep = dataset.columns.filter(!columnsToPrune.contains(_))
    dataset.select(columnsToKeep.map(dataset.col) : _*)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields.filter(col => !columnsToPrune.contains(col.name)))
  }

  override def copy(extra: ParamMap): ColumnPruner = defaultCopy(extra)
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
    vectorCol: String,
    prefixesToRewrite: Map[String, String])
  extends Transformer {

  override val uid = Identifiable.randomUID("vectorAttrRewriter")

  override def transform(dataset: DataFrame): DataFrame = {
    val metadata = {
      val group = AttributeGroup.fromStructField(dataset.schema(vectorCol))
      val attrs = group.attributes.get.map { attr =>
        if (attr.name.isDefined) {
          val name = attr.name.get
          val replacement = prefixesToRewrite.filter { case (k, _) => name.startsWith(k) }
          if (replacement.nonEmpty) {
            val (k, v) = replacement.headOption.get
            attr.withName(v + name.stripPrefix(k))
          } else {
            attr
          }
        } else {
          attr
        }
      }
      new AttributeGroup(vectorCol, attrs).toMetadata()
    }
    val otherCols = dataset.columns.filter(_ != vectorCol).map(dataset.col)
    val rewrittenCol = dataset.col(vectorCol).as(vectorCol, metadata)
    dataset.select((otherCols :+ rewrittenCol): _*)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      schema.fields.filter(_.name != vectorCol) ++
      schema.fields.filter(_.name == vectorCol))
  }

  override def copy(extra: ParamMap): VectorAttributeRewriter = defaultCopy(extra)
}
