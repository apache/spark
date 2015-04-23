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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Params for [[Word2Vec]] and [[Word2VecModel]].
 */
private[feature] trait Word2VecBase extends Params
  with HasInputCol with HasOutputCol with HasMaxIter with HasStepSize {

  /**
   * The dimension of the code that you want to transform from words.
   */
  final val vectorSize = new IntParam(
    this, "vectorSize", "the dimension of codes after transforming from words")

  setDefault(vectorSize -> 100)

  /** @group getParam */
  def getVectorSize: Int = getOrDefault(vectorSize)

  /**
   * Number of partitions for sentences of words.
   */
  final val numPartitions = new IntParam(
    this, "numPartitions", "number of partitions for sentences of words")

  setDefault(numPartitions -> 1)

  /** @group getParam */
  def getNumPartitions: Int = getOrDefault(numPartitions)

  /**
   * A random seed to random an initial vector.
   */
  final val seed = new LongParam(this, "seed", "a random seed to random an initial vector")

  setDefault(seed -> 42L)

  /** @group getParam */
  def getSeed: Long = getOrDefault(seed)

  /**
   * The minimum number of times a token must appear to be included in the word2vec model's
   * vocabulary.
   */
  final val minCount = new IntParam(this, "minCount", "the minimum number of times a token must " +
    "appear to be included in the word2vec model's vocabulary")

  setDefault(minCount -> 5)

  /** @group getParam */
  def getMinCount: Int = getOrDefault(minCount)

  setDefault(stepSize -> 0.025)
  setDefault(maxIter -> 1)

  /**
   * Validate and transform the input schema.
   */
  protected def validateAndTransformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    SchemaUtils.checkColumnType(schema, map(inputCol), new ArrayType(StringType, true))
    SchemaUtils.appendColumn(schema, map(outputCol), new VectorUDT)
  }
}

/**
 * :: AlphaComponent ::
 * Word2Vec trains a model of `Map(String, Vector)`, i.e. transforms a word into a code for further
 * natural language processing or machine learning process.
 */
@AlphaComponent
final class Word2Vec extends Estimator[Word2VecModel] with Word2VecBase {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setVectorSize(value: Int): this.type = set(vectorSize, value)

  /** @group setParam */
  def setStepSize(value: Double): this.type = set(stepSize, value)

  /** @group setParam */
  def setNumPartitions(value: Int): this.type = set(numPartitions, value)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  def setMinCount(value: Int): this.type = set(minCount, value)

  override def fit(dataset: DataFrame, paramMap: ParamMap): Word2VecModel = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val input = dataset.select(map(inputCol)).map { case Row(v: Seq[String]) => v }
    val wordVectors = new feature.Word2Vec()
      .setLearningRate(map(stepSize))
      .setMinCount(map(minCount))
      .setNumIterations(map(maxIter))
      .setNumPartitions(map(numPartitions))
      .setSeed(map(seed))
      .setVectorSize(map(vectorSize))
      .fit(input)
    val model = new Word2VecModel(this, map, wordVectors)
    Params.inheritValues(map, this, model)
    model
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap)
  }
}

/**
 * :: AlphaComponent ::
 * Model fitted by [[Word2Vec]].
 */
@AlphaComponent
class Word2VecModel private[ml] (
    override val parent: Word2Vec,
    override val fittingParamMap: ParamMap,
    wordVectors: feature.Word2VecModel)
  extends Model[Word2VecModel] with Word2VecBase {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Transform a sentence column to a vector column to represent the whole sentence. The transform
   * is performed by averaging all word vectors it contains.
   */
  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val bWordVectors = dataset.sqlContext.sparkContext.broadcast(wordVectors)
    val word2Vec = udf { v: Seq[String] =>
      if (v.size == 0) {
        Vectors.zeros(map(vectorSize))
      } else {
        Vectors.dense(
          v.map(bWordVectors.value.getVectors).foldLeft(Array.fill[Double](map(vectorSize))(0)) {
            (cum, vec) => cum.zip(vec).map(x => x._1 + x._2)
          }.map(_ / v.size)
        )
      }
    }
    dataset.withColumn(map(outputCol), word2Vec(col(map(inputCol))))
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap)
  }
}
