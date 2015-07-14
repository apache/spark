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
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * :: Experimental ::
 * Implements the transforms required for fitting a dataset against a R model formula.
 */
@Experimental
private[spark] class RModelFormula(override val uid: String)
  extends Transformer with HasFeaturesCol with HasLabelCol {

  def this() = this(Identifiable.randomUID("rModelFormula"))

  val formula: Param[String] = new Param(this, "formula", "R model formula")
  protected var parsedFormula: Option[RFormula] = None

  /**
   * Sets the formula to use for this transformer. Must be called before use.
   * @param value a R formula in string form (e.g. "y ~ x + z")
   */
  def setFormula(value: String): this.type = {
    parsedFormula = Some(RFormulaParser.parse(value))
    set(formula, value)
    this
  }

  override def transformSchema(schema: StructType): StructType = {
    require(parsedFormula.isDefined, "Must call setFormula() first.")
    val withFeatures = featureTransformer.transformSchema(schema)
    val nullable = schema(parsedFormula.get.response).dataType match {
      case _: NumericType | BooleanType => false
      case _ => true
    }
    StructType(withFeatures.fields :+ StructField($(labelCol), DoubleType, nullable))
  }

  override def transform(dataset: DataFrame): DataFrame = {
    require(parsedFormula.isDefined, "Must call setFormula() first.")
    transformLabel(featureTransformer.transform(dataset))
  }

  override def copy(extra: ParamMap): RModelFormula = defaultCopy(extra)

  override def toString: String = s"RModelFormula(${get(formula)})"

  protected def transformLabel(dataset: DataFrame): DataFrame = {
    val responseName = parsedFormula.get.response
    dataset.schema(responseName).dataType match {
      case _: NumericType | BooleanType =>
        dataset.select(
          col("*"),
          dataset(responseName).cast(DoubleType).as($(labelCol)))
      case StringType =>
        new StringIndexer(uid)
          .setInputCol(responseName)
          .setOutputCol($(labelCol))
          .fit(dataset)
          .transform(dataset)
      case other =>
        throw new IllegalArgumentException("Unsupported type for response: " + other)
    }
  }

  protected def featureTransformer: Transformer = {
    // TODO(ekl) add support for non-numeric features and feature interactions
    new VectorAssembler(uid)
      .setInputCols(parsedFormula.get.terms.toArray)
      .setOutputCol($(featuresCol))
  }
}

/**
 * :: Experimental ::
 * Represents a parsed R formula.
 */
private[ml] case class RFormula(response: String, terms: Seq[String])

/**
 * :: Experimental ::
 * Limited implementation of R formula parsing. Currently supports: '~', '+'.
 */
private[ml] object RFormulaParser extends RegexParsers {
  def term: Parser[String] = "([a-zA-Z]|\\.[a-zA-Z_])[a-zA-Z0-9._]*".r

  def expr: Parser[List[String]] = term ~ rep("+" ~> term) ^^ { case a ~ list => a :: list }

  def formula: Parser[RFormula] = (term ~ "~" ~ expr) ^^ { case r ~ "~" ~ t => RFormula(r, t) }

  def parse(value: String): RFormula = parseAll(formula, value) match {
    case Success(result, _) => result
    case failure: NoSuccess => throw new IllegalArgumentException(
      "Could not parse formula: " + value)
  }
}
