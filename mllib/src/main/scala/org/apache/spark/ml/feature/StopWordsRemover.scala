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
import org.apache.spark.ml.param.{BooleanParam, ParamMap, StringArrayParam}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

/**
 * stop words list
 */
private object StopWords {

  /**
   * Use the same default stopwords list as scikit-learn.
   * The original list can be found from "Glasgow Information Retrieval Group"
   * [[http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words]]
   */
  val EnglishStopWords = Array( "a", "about", "above", "across", "after", "afterwards", "again",
    "against", "all", "almost", "alone", "along", "already", "also", "although", "always",
    "am", "among", "amongst", "amoungst", "amount", "an", "and", "another",
    "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are",
    "around", "as", "at", "back", "be", "became", "because", "become",
    "becomes", "becoming", "been", "before", "beforehand", "behind", "being",
    "below", "beside", "besides", "between", "beyond", "bill", "both",
    "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "con",
    "could", "couldnt", "cry", "de", "describe", "detail", "do", "done",
    "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else",
    "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone",
    "everything", "everywhere", "except", "few", "fifteen", "fify", "fill",
    "find", "fire", "first", "five", "for", "former", "formerly", "forty",
    "found", "four", "from", "front", "full", "further", "get", "give", "go",
    "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter",
    "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his",
    "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed",
    "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter",
    "latterly", "least", "less", "ltd", "made", "many", "may", "me",
    "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly",
    "move", "much", "must", "my", "myself", "name", "namely", "neither",
    "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone",
    "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on",
    "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our",
    "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps",
    "please", "put", "rather", "re", "same", "see", "seem", "seemed",
    "seeming", "seems", "serious", "several", "she", "should", "show", "side",
    "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone",
    "something", "sometime", "sometimes", "somewhere", "still", "such",
    "system", "take", "ten", "than", "that", "the", "their", "them",
    "themselves", "then", "thence", "there", "thereafter", "thereby",
    "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
    "third", "this", "those", "though", "three", "through", "throughout",
    "thru", "thus", "to", "together", "too", "top", "toward", "towards",
    "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
    "very", "via", "was", "we", "well", "were", "what", "whatever", "when",
    "whence", "whenever", "where", "whereafter", "whereas", "whereby",
    "wherein", "whereupon", "wherever", "whether", "which", "while", "whither",
    "who", "whoever", "whole", "whom", "whose", "why", "will", "with",
    "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves")
}

/**
 * :: Experimental ::
 * A feature transformer that filters out stop words from input.
 * Note: null values from input array are preserved unless adding null to stopWords explicitly.
 * @see [[http://en.wikipedia.org/wiki/Stop_words]]
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
  val stopWords: StringArrayParam = new StringArrayParam(this, "stopWords", "stop words")

  /** @group setParam */
  def setStopWords(value: Array[String]): this.type = set(stopWords, value)

  /** @group getParam */
  def getStopWords: Array[String] = $(stopWords)

  /**
   * whether to do a case sensitive comparison over the stop words
   * @group param
   */
  val caseSensitive: BooleanParam = new BooleanParam(this, "caseSensitive",
    "whether to do case-sensitive comparison during filtering")

  /** @group setParam */
  def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)

  /** @group getParam */
  def getCaseSensitive: Boolean = $(caseSensitive)

  setDefault(stopWords -> StopWords.EnglishStopWords, caseSensitive -> false)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val t = if ($(caseSensitive)) {
        val stopWordsSet = $(stopWords).toSet
        udf { terms: Seq[String] =>
          terms.filter(s => !stopWordsSet.contains(s))
        }
      } else {
        val toLower = (s: String) => if (s != null) s.toLowerCase else s
        val lowerStopWords = $(stopWords).map(toLower(_)).toSet
        udf { terms: Seq[String] =>
          terms.filter(s => !lowerStopWords.contains(toLower(s)))
        }
    }

    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(StringType)),
      s"Input type must be ArrayType(StringType) but got $inputType.")
    val outputFields = schema.fields :+
      StructField($(outputCol), inputType, schema($(inputCol)).nullable)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): StopWordsRemover = defaultCopy(extra)
}
