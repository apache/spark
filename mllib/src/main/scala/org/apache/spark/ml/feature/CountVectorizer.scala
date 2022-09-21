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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.linalg.{Vectors, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{OpenHashMap, Utils}

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
   * Specifies the minimum number of different documents a term must appear in to be included
   * in the vocabulary.
   * If this is an integer greater than or equal to 1, this specifies the number of documents
   * the term must appear in; if this is a double in [0,1), then this specifies the fraction of
   * documents.
   *
   * Default: 1.0
   * @group param
   */
  val minDF: DoubleParam = new DoubleParam(this, "minDF", "Specifies the minimum number of" +
    " different documents a term must appear in to be included in the vocabulary." +
    " If this is an integer >= 1, this specifies the number of documents the term must" +
    " appear in; if this is a double in [0,1), then this specifies the fraction of documents.",
    ParamValidators.gtEq(0.0))

  /** @group getParam */
  def getMinDF: Double = $(minDF)

  /**
   * Specifies the maximum number of different documents a term could appear in to be included
   * in the vocabulary. A term that appears more than the threshold will be ignored. If this is an
   * integer greater than or equal to 1, this specifies the maximum number of documents the term
   * could appear in; if this is a double in [0,1), then this specifies the maximum fraction of
   * documents the term could appear in.
   *
   * Default: (2^63^) - 1
   * @group param
   */
  val maxDF: DoubleParam = new DoubleParam(this, "maxDF", "Specifies the maximum number of" +
    " different documents a term could appear in to be included in the vocabulary." +
    " A term that appears more than the threshold will be ignored. If this is an integer >= 1," +
    " this specifies the maximum number of documents the term could appear in;" +
    " if this is a double in [0,1), then this specifies the maximum fraction of" +
    " documents the term could appear in.",
    ParamValidators.gtEq(0.0))

  /** @group getParam */
  def getMaxDF: Double = $(maxDF)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val typeCandidates = List(new ArrayType(StringType, true), new ArrayType(StringType, false))
    SchemaUtils.checkColumnTypes(schema, $(inputCol), typeCandidates)
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  /**
   * Filter to ignore rare words in a document. For each document, terms with
   * frequency/count less than the given threshold are ignored.
   * If this is an integer greater than or equal to 1, then this specifies a count (of times the
   * term must appear in the document);
   * if this is a double in [0,1), then this specifies a fraction (out of the document's token
   * count).
   *
   * Note that the parameter is only used in transform of [[CountVectorizerModel]] and does not
   * affect fitting.
   *
   * Default: 1.0
   * @group param
   */
  val minTF: DoubleParam = new DoubleParam(this, "minTF", "Filter to ignore rare words in" +
    " a document. For each document, terms with frequency/count less than the given threshold are" +
    " ignored. If this is an integer >= 1, then this specifies a count (of times the term must" +
    " appear in the document); if this is a double in [0,1), then this specifies a fraction (out" +
    " of the document's token count). Note that the parameter is only used in transform of" +
    " CountVectorizerModel and does not affect fitting.", ParamValidators.gtEq(0.0))

  /** @group getParam */
  def getMinTF: Double = $(minTF)

  /**
   * Binary toggle to control the output vector values.
   * If True, all nonzero counts (after minTF filter applied) are set to 1. This is useful for
   * discrete probabilistic models that model binary events rather than integer counts.
   * Default: false
   * @group param
   */
  val binary: BooleanParam =
    new BooleanParam(this, "binary", "If True, all non zero counts are set to 1.")

  /** @group getParam */
  def getBinary: Boolean = $(binary)

  setDefault(vocabSize -> (1 << 18),
    minDF -> 1.0,
    maxDF -> Long.MaxValue,
    minTF -> 1.0,
    binary -> false)
}

/**
 * Extracts a vocabulary from document collections and generates a [[CountVectorizerModel]].
 */
@Since("1.5.0")
class CountVectorizer @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends Estimator[CountVectorizerModel] with CountVectorizerParams with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("cntVec"))

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setVocabSize(value: Int): this.type = set(vocabSize, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMinDF(value: Double): this.type = set(minDF, value)

  /** @group setParam */
  @Since("2.4.0")
  def setMaxDF(value: Double): this.type = set(maxDF, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMinTF(value: Double): this.type = set(minTF, value)

  /** @group setParam */
  @Since("2.0.0")
  def setBinary(value: Boolean): this.type = set(binary, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): CountVectorizerModel = {
    transformSchema(dataset.schema, logging = true)
    if (($(minDF) >= 1.0 && $(maxDF) >= 1.0) || ($(minDF) < 1.0 && $(maxDF) < 1.0)) {
      require($(maxDF) >= $(minDF), "maxDF must be >= minDF.")
    }

    val vocSize = $(vocabSize)
    val input = dataset.select($(inputCol)).rdd.map(_.getSeq[String](0))
    val countingRequired = $(minDF) < 1.0 || $(maxDF) < 1.0
    val maybeInputSize = if (countingRequired) {
      if (dataset.storageLevel == StorageLevel.NONE) {
        input.persist(StorageLevel.MEMORY_AND_DISK)
      }
      Some(input.count)
    } else {
      None
    }
    val minDf = if ($(minDF) >= 1.0) {
      $(minDF)
    } else {
      $(minDF) * maybeInputSize.get
    }
    val maxDf = if ($(maxDF) >= 1.0) {
      $(maxDF)
    } else {
      $(maxDF) * maybeInputSize.get
    }
    require(maxDf >= minDf, "maxDF must be >= minDF.")
    val allWordCounts = input.flatMap { tokens =>
      val wc = new OpenHashMap[String, Long]
      tokens.foreach { w =>
        wc.changeValue(w, 1L, _ + 1L)
      }
      wc.map { case (word, count) => (word, (count, 1)) }
    }.reduceByKey { (wcdf1, wcdf2) =>
      (wcdf1._1 + wcdf2._1, wcdf1._2 + wcdf2._2)
    }

    val filteringRequired = isSet(minDF) || isSet(maxDF)
    val maybeFilteredWordCounts = if (filteringRequired) {
      allWordCounts.filter { case (_, (_, df)) => df >= minDf && df <= maxDf }
    } else {
      allWordCounts
    }

    val wordCounts = maybeFilteredWordCounts
      .map { case (word, (count, _)) => (word, count) }
      .persist(StorageLevel.MEMORY_AND_DISK)

    val fullVocabSize = wordCounts.count()

    val vocab = wordCounts
      .top(math.min(fullVocabSize, vocSize).toInt)(Ordering.by(_._2))
      .map(_._1)

    if (input.getStorageLevel != StorageLevel.NONE) {
      input.unpersist()
    }
    wordCounts.unpersist()

    if (vocab.isEmpty) {
      this.logWarning("The vocabulary size is empty. " +
        "If this was unexpected, you may wish to lower minDF (or) increase maxDF.")
    }
    copyValues(new CountVectorizerModel(uid, vocab).setParent(this))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): CountVectorizer = defaultCopy(extra)
}

@Since("1.6.0")
object CountVectorizer extends DefaultParamsReadable[CountVectorizer] {

  @Since("1.6.0")
  override def load(path: String): CountVectorizer = super.load(path)
}

/**
 * Converts a text document to a sparse vector of token counts.
 * @param vocabulary An Array over terms. Only the terms in the vocabulary will be counted.
 */
@Since("1.5.0")
class CountVectorizerModel(
    @Since("1.5.0") override val uid: String,
    @Since("1.5.0") val vocabulary: Array[String])
  extends Model[CountVectorizerModel] with CountVectorizerParams with MLWritable {

  import CountVectorizerModel._

  @Since("1.5.0")
  def this(vocabulary: Array[String]) = {
    this(Identifiable.randomUID("cntVecModel"), vocabulary)
    set(vocabSize, vocabulary.length)
  }

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMinTF(value: Double): this.type = set(minTF, value)

  /** @group setParam */
  @Since("2.0.0")
  def setBinary(value: Boolean): this.type = set(binary, value)

  /** Dictionary created from [[vocabulary]] and its indices, broadcast once for [[transform()]] */
  private var broadcastDict: Option[Broadcast[Map[String, Int]]] = None

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)
    if (broadcastDict.isEmpty) {
      val dict = Utils.toMapWithIndex(vocabulary)
      broadcastDict = Some(dataset.sparkSession.sparkContext.broadcast(dict))
    }
    val dictBr = broadcastDict.get
    val minTf = $(minTF)
    val vectorizer = udf { document: Seq[String] =>
      val termCounts = new OpenHashMap[Int, Double]
      var tokenCount = 0L
      document.foreach { term =>
        dictBr.value.get(term) match {
          case Some(index) => termCounts.changeValue(index, 1.0, _ + 1.0)
          case None => // ignore terms not in the vocabulary
        }
        tokenCount += 1
      }
      val effectiveMinTF = if (minTf >= 1.0) minTf else tokenCount * minTf
      val effectiveCounts = if ($(binary)) {
        termCounts.filter(_._2 >= effectiveMinTF).map(p => (p._1, 1.0)).toSeq
      } else {
        termCounts.filter(_._2 >= effectiveMinTF).toSeq
      }

      Vectors.sparse(dictBr.value.size, effectiveCounts)
    }
    dataset.withColumn($(outputCol), vectorizer(col($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(outputCol).nonEmpty) {
      val attrs: Array[Attribute] = vocabulary.map(_ => new NumericAttribute)
      val field = new AttributeGroup($(outputCol), attrs).toStructField()
      outputSchema = SchemaUtils.updateField(outputSchema, field)
    }
    outputSchema
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): CountVectorizerModel = {
    val copied = new CountVectorizerModel(uid, vocabulary).setParent(parent)
    copyValues(copied, extra)
  }

  @Since("1.6.0")
  override def write: MLWriter = new CountVectorizerModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"CountVectorizerModel: uid=$uid, vocabularySize=${vocabulary.length}"
  }
}

@Since("1.6.0")
object CountVectorizerModel extends MLReadable[CountVectorizerModel] {

  private[CountVectorizerModel]
  class CountVectorizerModelWriter(instance: CountVectorizerModel) extends MLWriter {

    private case class Data(vocabulary: Seq[String])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.vocabulary)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class CountVectorizerModelReader extends MLReader[CountVectorizerModel] {

    private val className = classOf[CountVectorizerModel].getName

    override def load(path: String): CountVectorizerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("vocabulary")
        .head()
      val vocabulary = data.getAs[Seq[String]](0).toArray
      val model = new CountVectorizerModel(metadata.uid, vocabulary)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[CountVectorizerModel] = new CountVectorizerModelReader

  @Since("1.6.0")
  override def load(path: String): CountVectorizerModel = super.load(path)
}
