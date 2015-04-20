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
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.HasInputCol
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.param._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Params for [[StandardScaler]] and [[StandardScalerModel]].
 */
private[feature] trait Word2VecParams extends Params with HasInputCol with HasMaxIter with HasLearningRate {

  /**
   * The dimension of the code that you want to transform from words.
   */
  val vectorSize = new IntParam(this, "vectorSize", "", Some(100))

  /** @group getParam */
  def getVectorSize: Int = get(vectorSize)

  /**
   * Number of partitions
   */
  val numPartitions = new IntParam(this, "numPartitions", "", Some(1))

  /** @group getParam */
  def getNumPartitions: Int = get(numPartitions)

  /**
   * The random seed
   */
  val seed = new LongParam(this, "seed", "", Some(Utils.random.nextLong()))

  /** @group getParam */
  def getSeed: Long = get(seed)

  /**
   * The minimum count of words that can be kept in training set.
   */
  val minCount = new IntParam(this, "minCount", "", Some(5))

  /** @group getParam */
  def getMinCount: Int = get(minCount)

  /**
   * The column name of the output column - synonyms.
   */
  val synonymsCol = new Param[String](this, "synonymsCol", "Synonyms column name")

  /** @group getParam */
  def getSynonymsCol: String = get(synonymsCol)

  /**
   * The column name of the output column - code.
   */
  val codeCol = new Param[String](this, "codeCol", "Code column name")

  /** @group getParam */
  def getCodeCol: String = get(codeCol)

  /**
   * The number of synonyms that you want to have.
   */
  val numSynonyms = new IntParam(this, "numSynonyms", "number of synonyms to find", Some(0))

  /** @group getParam */
  def getNumSynonyms: Int = get(numSynonyms)
}

/**
 * :: AlphaComponent ::
 * Standardizes features by removing the mean and scaling to unit variance using column summary
 * statistics on the samples in the training set.
 */
@AlphaComponent
class Word2Vec extends Estimator[Word2VecModel] with Word2VecParams {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setVectorSize(value: Int) = set(vectorSize, value)

  /** @group setParam */
  def setLearningRate(value: Double) = set(learningRate, value)

  /** @group setParam */
  def setNumPartitions(value: Int) = set(numPartitions, value)

  /** @group setParam */
  def setMaxIter(value: Int) = set(maxIter, value)

  /** @group setParam */
  def setSeed(value: Long) = set(seed, value)

  /** @group setParam */
  def setMinCount(value: Int) = set(minCount, value)

  type S <: Iterable[String]

  override def fit(dataset: DataFrame, paramMap: ParamMap): Word2VecModel = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = this.paramMap ++ paramMap
    val input = dataset.select(map(inputCol)).map { case Row(v: Seq[String]) => v }
    val wordVectors = new feature.Word2Vec()
      .setLearningRate(map(learningRate))
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
    val map = this.paramMap ++ paramMap
    val inputType = schema(map(inputCol)).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"Input column ${map(inputCol)} must be a Iterable[String] column")
    schema
  }
}

/**
 * :: AlphaComponent ::
 * Model fitted by [[StandardScaler]].
 */
@AlphaComponent
class Word2VecModel private[ml] (
    override val parent: Word2Vec,
    override val fittingParamMap: ParamMap,
    wordVectors: feature.Word2VecModel)
  extends Model[Word2VecModel] with Word2VecParams {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setSynonymsCol(value: String): this.type = set(synonymsCol, value)

  /** @group setParam */
  def setNumSynonyms(value: Int): this.type = set(numSynonyms, value)

  /** @group setParam */
  def setCodeCol(value: String): this.type = set(codeCol, value)

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = this.paramMap ++ paramMap

    var tmpData = dataset
    var numColsOutput = 0

    if (map(codeCol) != "") {
      val word2vec: String => Vector = (word) => wordVectors.transform(word)
      tmpData = tmpData.withColumn(map(codeCol),
        callUDF(word2vec, new VectorUDT, col(map(inputCol))))
      numColsOutput += 1
    }

    if (map(synonymsCol) != "" & map(numSynonyms) > 0) {
      val findSynonyms = udf { (word: String) =>
        wordVectors.findSynonyms(word, map(numSynonyms)).toMap : Map[String, Double]
      }
      tmpData = tmpData.withColumn(map(synonymsCol), findSynonyms(col(map(inputCol))))
      numColsOutput += 1
    }

    if (numColsOutput == 0) {
      this.logWarning(s"$uid: Word2VecModel.transform() was called as NOOP" +
        s" since no output columns were set.")
    }

    tmpData
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = this.paramMap ++ paramMap

    val inputType = schema(map(inputCol)).dataType
    require(inputType.isInstanceOf[StringType],
      s"Input column ${map(inputCol)} must be a string column")

    var outputFields = schema.fields

    if (map(codeCol) != "") {
      require(!schema.fieldNames.contains(map(codeCol)),
        s"Output column ${map(codeCol)} already exists.")
      outputFields = outputFields :+ StructField(map(codeCol), new VectorUDT, false)
    }

    if (map(synonymsCol) != "") {
      require(!schema.fieldNames.contains(map(synonymsCol)),
        s"Output column ${map(synonymsCol)} already exists.")
      require(map(numSynonyms) > 0,
        s"Number of synonyms should larger than 0")
      outputFields = outputFields :+
        StructField(map(synonymsCol), MapType(StringType, DoubleType), false)
    }

    StructType(outputFields)
  }
}
