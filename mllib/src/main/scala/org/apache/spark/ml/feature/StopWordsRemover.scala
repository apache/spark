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

import java.util.Locale

import org.apache.spark.annotation.Since
import org.apache.spark.internal.{LogKeys, MDC}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol, HasOutputCols}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.util.ArrayImplicits._

/**
 * A feature transformer that filters out stop words from input.
 *
 * Since 3.0.0, `StopWordsRemover` can filter out multiple columns at once by setting the
 * `inputCols` parameter. Note that when both the `inputCol` and `inputCols` parameters are set,
 * an Exception will be thrown.
 *
 * @note null values from input array are preserved unless adding null to stopWords
 * explicitly.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Stop_words">Stop words (Wikipedia)</a>
 */
@Since("1.5.0")
class StopWordsRemover @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with HasInputCols with HasOutputCols
    with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("stopWords"))

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("3.0.0")
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  /**
   * The words to be filtered out.
   * Default: English stop words
   * @see `StopWordsRemover.loadDefaultStopWords()`
   * @group param
   */
  @Since("1.5.0")
  val stopWords: StringArrayParam =
    new StringArrayParam(this, "stopWords", "the words to be filtered out")

  /** @group setParam */
  @Since("1.5.0")
  def setStopWords(value: Array[String]): this.type = set(stopWords, value)

  /** @group getParam */
  @Since("1.5.0")
  def getStopWords: Array[String] = $(stopWords)

  /**
   * Whether to do a case sensitive comparison over the stop words.
   * Default: false
   * @group param
   */
  @Since("1.5.0")
  val caseSensitive: BooleanParam = new BooleanParam(this, "caseSensitive",
    "whether to do a case-sensitive comparison over the stop words")

  /** @group setParam */
  @Since("1.5.0")
  def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)

  /** @group getParam */
  @Since("1.5.0")
  def getCaseSensitive: Boolean = $(caseSensitive)

  /**
   * Locale of the input for case insensitive matching. Ignored when [[caseSensitive]]
   * is true.
   * Default: the string of default locale (`Locale.getDefault`), or `Locale.US` if default locale
   * is not in available locales in JVM.
   * @group param
   */
  @Since("2.4.0")
  val locale: Param[String] = new Param[String](this, "locale",
    "Locale of the input for case insensitive matching. Ignored when caseSensitive is true.",
    ParamValidators.inArray[String](Locale.getAvailableLocales.map(_.toString)))

  /** @group setParam */
  @Since("2.4.0")
  def setLocale(value: String): this.type = set(locale, value)

  /** @group getParam */
  @Since("2.4.0")
  def getLocale: String = $(locale)

  /**
   * Returns system default locale, or `Locale.US` if the default locale is not in available locales
   * in JVM.
   */
  private val getDefaultOrUS: Locale = {
    if (Locale.getAvailableLocales.contains(Locale.getDefault)) {
      Locale.getDefault
    } else {
      logWarning(log"Default locale set was [${MDC(LogKeys.LOCALE, Locale.getDefault)}]; " +
        log"however, it was not found in available locales in JVM, falling back to en_US locale. " +
        log"Set param `locale` in order to respect another locale.")
      Locale.US
    }
  }

  /** Returns the input and output column names corresponding in pair. */
  private[feature] def getInOutCols(): (Array[String], Array[String]) = {
    if (isSet(inputCol)) {
      (Array($(inputCol)), Array($(outputCol)))
    } else {
      ($(inputCols), $(outputCols))
    }
  }

  setDefault(stopWords -> StopWordsRemover.loadDefaultStopWords("english"),
    caseSensitive -> false, locale -> getDefaultOrUS.toString)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val t = if ($(caseSensitive)) {
      val stopWordsSet = $(stopWords).toSet
      udf { terms: Seq[String] =>
        terms.filter(s => !stopWordsSet.contains(s))
      }
    } else {
      val lc = new Locale($(locale))
      // scalastyle:off caselocale
      val toLower = (s: String) => if (s != null) s.toLowerCase(lc) else s
      // scalastyle:on caselocale
      val lowerStopWords = $(stopWords).map(toLower(_)).toSet
      udf { terms: Seq[String] =>
        terms.filter(s => !lowerStopWords.contains(toLower(s)))
      }
    }

    val (inputColNames, outputColNames) = getInOutCols()
    val outputCols = inputColNames.map { inputColName =>
      t(col(inputColName))
    }
    val outputMetadata = outputColNames.map(outputSchema(_).metadata)
    dataset.withColumns(
      outputColNames.toImmutableArraySeq,
      outputCols.toImmutableArraySeq,
      outputMetadata.toImmutableArraySeq)
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    ParamValidators.checkSingleVsMultiColumnParams(this, Seq(outputCol),
      Seq(outputCols))

    if (isSet(inputCols)) {
      require(getInputCols.length == getOutputCols.length,
        s"StopWordsRemover $this has mismatched Params " +
          s"for multi-column transform. Params ($inputCols, $outputCols) should have " +
          "equal lengths, but they have different lengths: " +
          s"(${getInputCols.length}, ${getOutputCols.length}).")
    }

    val (inputColNames, outputColNames) = getInOutCols()

    val newCols = inputColNames.zip(outputColNames).map { case (inputColName, outputColName) =>
       require(!schema.fieldNames.contains(outputColName),
        s"Output Column $outputColName already exists.")
      val inputType = SchemaUtils.getSchemaFieldType(schema, inputColName)
      require(DataTypeUtils.sameType(inputType, ArrayType(StringType)), "Input type must be " +
        s"${ArrayType(StringType).catalogString} but got ${inputType.catalogString}.")
      StructField(
        outputColName, inputType, SchemaUtils.getSchemaField(schema, inputColName).nullable
      )
    }
    StructType(schema.fields ++ newCols)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): StopWordsRemover = defaultCopy(extra)

  @Since("3.0.0")
  override def toString: String = {
    s"StopWordsRemover: uid=$uid, numStopWords=${$(stopWords).length}, locale=${$(locale)}, " +
      s"caseSensitive=${$(caseSensitive)}"
  }
}

@Since("1.6.0")
object StopWordsRemover extends DefaultParamsReadable[StopWordsRemover] {

  private[feature]
  val supportedLanguages = Set("danish", "dutch", "english", "finnish", "french", "german",
    "hungarian", "italian", "norwegian", "portuguese", "russian", "spanish", "swedish", "turkish")

  @Since("1.6.0")
  override def load(path: String): StopWordsRemover = super.load(path)

  /**
   * Loads the default stop words for the given language.
   * Supported languages: danish, dutch, english, finnish, french, german, hungarian,
   * italian, norwegian, portuguese, russian, spanish, swedish, turkish
   * @see <a href="http://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/">
   * here</a>
   */
  @Since("2.0.0")
  def loadDefaultStopWords(language: String): Array[String] = {
    require(supportedLanguages.contains(language),
      s"$language is not in the supported language list: ${supportedLanguages.mkString(", ")}.")
    val is = getClass.getResourceAsStream(s"/org/apache/spark/ml/feature/stopwords/$language.txt")
    scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toArray
  }
}
