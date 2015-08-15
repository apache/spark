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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

/**
 * Params for [[CountVectorizer]] and [[CountVectorizerModel]].
 */
private[feature] trait CountVectorizerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * Max size of the vocabulary.
   * CountVectorizer will build a vocabulary that only considers the top
   * vocabSize terms ordered by term frequency across the corpus.
   *
   * Default: 2^18^
   * @group param
   */
  val vocabSize: IntParam =
    new IntParam(this, "vocabSize", "max size of the vocabulary", ParamValidators.gt(0))

  /** @group getParam */
  def getVocabSize: Int = $(vocabSize)

  /**
   * The minimum number of different documents a token must appear in to be included in the
   * vocabulary.
   *
   * Default: 1
   * @group param
   */
  val minDocFreq: IntParam = new IntParam(this, "minDocFreq",
    "The minimum number of documents a token must appear in to be included in the vocabulary"
    , ParamValidators.gtEq(1))

  /** @group getParam */
  def getMinDocFreq: Int = $(minDocFreq)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new ArrayType(StringType, true))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  /**
   * Filter to ignore scarce words in a document. For each document, terms with
   * frequency (count) less than the given threshold are ignored.
   * Note that the parameter is only used in transform of [[CountVectorizerModel]] and does not
   * affect fitting.
   * Default: 1
   * @group param
   */
  val minTermFreq: IntParam = new IntParam(this, "minTermFreq",
    "minimum frequency (count) filter used to neglect scarce words (>= 1). For each document, " +
      "terms with count less than the given threshold are ignored.", ParamValidators.gtEq(1))

  setDefault(minTermFreq -> 1)

  /** @group getParam */
  def getMinTermFreq: Int = $(minTermFreq)
}

/**
 * :: Experimental ::
 * Extracts a vocabulary from document collections and generates a [[CountVectorizerModel]].
 */
@Experimental
class CountVectorizer(override val uid: String)
  extends Estimator[CountVectorizerModel] with CountVectorizerParams {

  def this() = this(Identifiable.randomUID("cntVec"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setVocabSize(value: Int): this.type = set(vocabSize, value)

  /** @group setParam */
  def setMinDocFreq(value: Int): this.type = set(minDocFreq, value)

  setDefault(vocabSize -> (1 << 18), minDocFreq -> 1)

  override def fit(dataset: DataFrame): CountVectorizerModel = {
    transformSchema(dataset.schema, logging = true)
    val minDf = $(minDocFreq)
    val vocSize = $(vocabSize)
    val input = dataset.select($(inputCol)).map(_.getAs[Seq[String]](0))
    val wordCounts: RDD[(String, Long)] = input
      .flatMap { case (tokens) =>
        tokens.foldLeft(Map.empty[String, Long])(
          (count, word) => count + (word -> (count.getOrElse(word, 0L) + 1)))
        .map { case (word, count) => (word, (count, 1)) }}
      .reduceByKey { case ((wc1, df1), (wc2, df2)) => (wc1 + wc2, df1 + df2) }
      .filter { case (word, (wc, df)) => df >= minDf }
      .map { case (word, (count, dfCount)) => (word, count) }
      .cache()
    val fullVocabSize = wordCounts.count()
    val vocab: Array[String] = {
      val tmpSortedWC: Array[(String, Long)] = if (fullVocabSize <= vocSize) {
        // Use all terms
        wordCounts.collect().sortBy(-_._2)
      } else {
        // Sort terms to select vocab
        wordCounts.sortBy(_._2, ascending = false).take(vocSize)
      }
      tmpSortedWC.map(_._1)
    }

    require(vocab.length > 0,
      "The vocabulary size should be > 0. Lower minDocFreq as necessary.")
    copyValues(new CountVectorizerModel(uid, vocab).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): CountVectorizer = defaultCopy(extra)
}

/**
 * :: Experimental ::
 * Converts a text document to a sparse vector of token counts.
 * @param vocabulary An Array over terms. Only the terms in the vocabulary will be counted.
 */
@Experimental
class CountVectorizerModel(override val uid: String, val vocabulary: Array[String])
  extends Model[CountVectorizerModel] with CountVectorizerParams {

  def this(vocabulary: Array[String]) = {
    this(Identifiable.randomUID("cntVecModel"), vocabulary)
    set(vocabSize, vocabulary.length)
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setMinTermFreq(value: Int): this.type = set(minTermFreq, value)

  override def transform(dataset: DataFrame): DataFrame = {
    val dict = vocabulary.zipWithIndex.toMap
    val dictBr = dataset.sqlContext.sparkContext.broadcast(dict)
    val minTF = $(minTermFreq)
    val vectorizer = udf { (document: Seq[String]) =>
      val termCounts = mutable.HashMap.empty[Int, Double]
      document.foreach { term =>
        dictBr.value.get(term) match {
          case Some(index) => termCounts.put(index, termCounts.getOrElse(index, 0.0) + 1.0)
          case None => // ignore terms not in the vocabulary
        }
      }
      Vectors.sparse(dict.size, termCounts.filter(_._2 >= minTF).toSeq)
    }
    dataset.withColumn($(outputCol), vectorizer(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): CountVectorizerModel = {
    val copied = new CountVectorizerModel(uid, vocabulary)
    copyValues(copied, extra)
  }
}
