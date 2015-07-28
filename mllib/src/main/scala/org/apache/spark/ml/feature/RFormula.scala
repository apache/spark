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

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.{Estimator, Model, Transformer, Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Base trait for [[RFormula]] and [[RFormulaModel]].
 */
private[feature] trait RFormulaBase extends HasFeaturesCol with HasLabelCol {
  /** @group getParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group getParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  protected def hasLabelCol(schema: StructType): Boolean = {
    schema.map(_.name).contains($(labelCol))
  }
}

/**
 * :: Experimental ::
 * Implements the transforms required for fitting a dataset against an R model formula. Currently
 * we support a limited subset of the R operators, including '~' and '+'. Also see the R formula
 * docs here: http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html
 */
@Experimental
class RFormula(override val uid: String) extends Estimator[RFormulaModel] with RFormulaBase {

  def this() = this(Identifiable.randomUID("rFormula"))

  /**
   * R formula parameter. The formula is provided in string form.
   * @group param
   */
  val formula: Param[String] = new Param(this, "formula", "R model formula")

  private var parsedFormula: Option[ParsedRFormula] = None

  /**
   * Sets the formula to use for this transformer. Must be called before use.
   * @group setParam
   * @param value an R formula in string form (e.g. "y ~ x + z")
   */
  def setFormula(value: String): this.type = {
    parsedFormula = Some(RFormulaParser.parse(value))
    set(formula, value)
    this
  }

  /** @group getParam */
  def getFormula: String = $(formula)

  /** Whether the formula specifies fitting an intercept. */
  private[ml] def hasIntercept: Boolean = {
    require(parsedFormula.isDefined, "Must call setFormula() first.")
    parsedFormula.get.hasIntercept
  }

  override def fit(dataset: DataFrame): RFormulaModel = {
    require(parsedFormula.isDefined, "Must call setFormula() first.")
    val resolvedFormula = parsedFormula.get.resolve(dataset.schema)
    // StringType terms and terms representing interactions need to be encoded before assembly.
    // TODO(ekl) add support for feature interactions
    val encoderStages = ArrayBuffer[PipelineStage]()
    val tempColumns = ArrayBuffer[String]()
    val encodedTerms = resolvedFormula.terms.map { term =>
      dataset.schema(term) match {
        case column if column.dataType == StringType =>
          val indexCol = term + "_idx_" + uid
          val encodedCol = term + "_onehot_" + uid
          encoderStages += new StringIndexer().setInputCol(term).setOutputCol(indexCol)
          encoderStages += new OneHotEncoder().setInputCol(indexCol).setOutputCol(encodedCol)
          tempColumns += indexCol
          tempColumns += encodedCol
          encodedCol
        case _ =>
          term
      }
    }
    encoderStages += new VectorAssembler(uid)
      .setInputCols(encodedTerms.toArray)
      .setOutputCol($(featuresCol))
    encoderStages += new ColumnPruner(tempColumns.toSet)
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

  override def toString: String = s"RFormula(${get(formula)})"
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
    if (hasLabelCol(schema)) {
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

  override def toString: String = s"RFormulaModel(${resolvedFormula})"

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
