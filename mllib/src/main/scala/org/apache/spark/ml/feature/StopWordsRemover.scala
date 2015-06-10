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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{BooleanParam, Param}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, ArrayType, StructType}
import org.apache.spark.sql.functions.{col, udf}

/**
 * :: Experimental ::
 * stop words list
 */
@Experimental
object StopWords{
  val EnglishSet = ("a an and are as at be by for from has he in is it its of on that the to " +
    "was were will with").split("\\s").toSet
}

/**
 * :: Experimental ::
 * A feature transformer that filters out stop words from input
 * [[http://en.wikipedia.org/wiki/Stop_words]]
 */
@Experimental
class StopWordsRemover(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("stopWords"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * the stop words set to be filtered out
   * @group param
   */
  val stopWords: Param[Set[String]] = new Param(this, "stopWords", "stop words")

  /** @group setParam */
  def setStopWords(value: Set[String]): this.type = set(stopWords, value)

  /** @group getParam */
  def getStopWords: Set[String] = getOrDefault(stopWords)

  /**
   * whether to do a case sensitive comparison over the stop words
   * @group param
   */
  val caseSensitive: BooleanParam = new BooleanParam(this, "caseSensitive",
    "whether to do case-sensitive filter")

  /** @group setParam */
  def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)

  /** @group getParam */
  def getCaseSensitive: Boolean = getOrDefault(caseSensitive)

  setDefault(stopWords -> StopWords.EnglishSet, caseSensitive -> false)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val t = udf { terms: Seq[String] =>
      if ($(caseSensitive)) {
        terms.filterNot(s => s != null && $(stopWords).contains(s))
      }
      else {
        val lowerStopWords = $(stopWords).map(_.toLowerCase)
        terms.filterNot(s => s != null && lowerStopWords.contains(s.toLowerCase))
      }
    }
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType, but got $inputType.")
    val outputFields = schema.fields :+
      StructField($(outputCol), inputType, schema($(inputCol)).nullable)
    StructType(outputFields)
  }
}
