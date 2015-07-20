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

import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.{Estimator, Model, Transformer}
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

  override def fit(dataset: DataFrame): RFormulaModel = {
    require(parsedFormula.isDefined, "Must call setFormula() first.")
    copyValues(new RFormulaModel(uid, parsedFormula.get).setParent(this))
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
 * A fitted RFormula. Fitting is required to determine the factor levels of formula terms.
 */
private[feature] class RFormulaModel(
    override val uid: String,
    parsedFormula: ParsedRFormula)
  extends Model[RFormulaModel] with RFormulaBase {

  override def transform(dataset: DataFrame): DataFrame = {
    checkCanTransform(dataset.schema)
    transformLabel(transformFeatures.transform(dataset))
  }

  override def transformSchema(schema: StructType): StructType = {
    checkCanTransform(schema)
    val withFeatures = transformFeatures.transformSchema(schema)
    if (hasLabelCol(schema)) {
      withFeatures
    } else {
      val nullable = schema(parsedFormula.label).dataType match {
        case _: NumericType | BooleanType => false
        case _ => true
      }
      StructType(withFeatures.fields :+ StructField($(labelCol), DoubleType, nullable))
    }
  }

  override def copy(extra: ParamMap): RFormulaModel = copyValues(
    new RFormulaModel(uid, parsedFormula))

  private def transformLabel(dataset: DataFrame): DataFrame = {
    if (hasLabelCol(dataset.schema)) {
      dataset
    } else {
      val labelName = parsedFormula.label
      dataset.schema(labelName).dataType match {
        case _: NumericType | BooleanType =>
          dataset.withColumn($(labelCol), dataset(labelName).cast(DoubleType))
        case other =>
          throw new IllegalArgumentException("Unsupported type for label: " + other)
      }
    }
  }

  private def checkCanTransform(schema: StructType) {
    val columnNames = schema.map(_.name)
    require(!columnNames.contains($(featuresCol)), "Features column already exists.")
    require(
      !columnNames.contains($(labelCol)) || schema($(labelCol)).dataType == DoubleType,
      "Label column already exists and is not of type DoubleType.")
  }

  private def transformFeatures: Transformer = {
    // TODO(ekl) add support for non-numeric features and feature interactions
    new VectorAssembler(uid)
      .setInputCols(parsedFormula.terms.toArray)
      .setOutputCol($(featuresCol))
  }
}

/**
 * Represents a parsed R formula.
 */
private[ml] case class ParsedRFormula(label: String, terms: Seq[String])

/**
 * Limited implementation of R formula parsing. Currently supports: '~', '+'.
 */
private[ml] object RFormulaParser extends RegexParsers {
  def term: Parser[String] = "([a-zA-Z]|\\.[a-zA-Z_])[a-zA-Z0-9._]*".r

  def expr: Parser[List[String]] = term ~ rep("+" ~> term) ^^ { case a ~ list => a :: list }

  def formula: Parser[ParsedRFormula] =
    (term ~ "~" ~ expr) ^^ { case r ~ "~" ~ t => ParsedRFormula(r, t) }

  def parse(value: String): ParsedRFormula = parseAll(formula, value) match {
    case Success(result, _) => result
    case failure: NoSuccess => throw new IllegalArgumentException(
      "Could not parse formula: " + value)
  }
}
