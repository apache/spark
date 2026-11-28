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

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.{HashPartitioner, SparkException}
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.functions.{array_to_vector, vector_to_array}
import org.apache.spark.ml.linalg.{BLAS, SQLDataTypes, Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Params for [[Hash2Vec]] and [[Hash2VecModel]].
 */
private[feature] trait Hash2VecBase
    extends Params
    with HasInputCol
    with HasOutputCol
    with HasSeed {

  /**
   * The number of hashed dimensions of a token vector. The output vectors have one extra element
   * when `normalization` is "l2WithMagnitude". Default: 512
   * @group param
   */
  final val vectorSize = new IntParam(
    this,
    "vectorSize",
    "the number of hashed context dimensions (> 0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def getVectorSize: Int = $(vectorSize)

  /**
   * The size of the context window: the tokens at distance 1 to `windowSize` on each side of a
   * token are its context. Default: 5
   * @group param
   */
  final val windowSize = new IntParam(
    this,
    "windowSize",
    "the window size (context tokens from [-window, window]) (> 0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def getWindowSize: Int = $(windowSize)

  /**
   * The number of partitions of the reduce side, where the sparse contexts are summed into one
   * dense vector per token. Each reduce task holds about (number of distinct tokens /
   * numPartitions) * vectorSize doubles, so a large vocabulary needs a larger value. It is also
   * the number of partitions of the fitted vectors. Default: the number of partitions of the
   * input
   * @group param
   */
  final val numPartitions = new IntParam(
    this,
    "numPartitions",
    "number of partitions of the token vectors (> 0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def getNumPartitions: Int = $(numPartitions)

  /**
   * How the weight of a context token decays with its distance d:
   *   - "constant": 1, so all the tokens in the context window have the same weight;
   *   - "linear": \( (windowSize + 1 - d) / windowSize \);
   *   - "harmonic": \( 1 / d \);
   *   - "gaussian": \( e^{-d^2 / (2 \cdot decayScale^2)} \);
   *   - "exponential": \( e^{-d / decayScale} \).
   *
   * Default: "gaussian"
   * @group param
   */
  final val decayFunction = new Param[String](
    this,
    "decayFunction",
    "how a context token's weight decays with its distance d. Supported: " +
      "constant (1), gaussian (exp(-d^2 / (2 * decayScale^2))), " +
      "exponential (exp(-d / decayScale)), linear ((windowSize + 1 - d) / windowSize), " +
      "harmonic (1 / d)",
    ParamValidators.inArray[String](Hash2Vec.supportedDecayFunctions))

  /** @group getParam */
  def getDecayFunction: String = $(decayFunction)

  /**
   * The distance scale of the "gaussian" and "exponential" decay functions: a larger value makes
   * the weight decay more slowly. Other decay functions ignore it. Default: 2.0
   * @group param
   */
  final val decayScale = new DoubleParam(
    this,
    "decayScale",
    "the scale of the gaussian and exponential decay functions (> 0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def getDecayScale: Double = $(decayScale)

  /**
   * How the output vectors are normalized:
   *   - "none": the raw sums;
   *   - "l2": unit L2 norm, which erases the magnitude of the vector;
   *   - "l2WithMagnitude": "l2" plus one extra element \( \log(norm + 1) / \sqrt{vectorSize} \).
   *
   * The magnitude grows with the number of occurrences of the token in the corpus, and
   * "l2WithMagnitude" keeps this information. Default: "l2"
   * @group param
   */
  final val normalization = new Param[String](
    this,
    "normalization",
    "how token vectors are normalized. Supported: none, l2, " +
      "l2WithMagnitude (l2, plus one extra element log(norm + 1) / sqrt(vectorSize))",
    ParamValidators.inArray[String](Hash2Vec.supportedNormalizations))

  /** @group getParam */
  def getNormalization: String = $(normalization)

  /**
   * The storage level of the fitted vectors. With "NONE", the vectors are recomputed from the
   * input every time they are used. Default: "MEMORY_AND_DISK"
   * @group expertParam
   */
  final val finalStorageLevel = new Param[String](
    this,
    "finalStorageLevel",
    "StorageLevel for the fitted token vectors",
    (s: String) => Try(StorageLevel.fromString(s)).isSuccess)

  /** @group expertGetParam */
  def getFinalStorageLevel: String = $(finalStorageLevel)

  setDefault(
    vectorSize -> 512,
    windowSize -> 5,
    decayFunction -> "gaussian",
    decayScale -> 2.0,
    normalization -> "l2",
    finalStorageLevel -> "MEMORY_AND_DISK")

  /** The size of the output vectors, which includes the optional magnitude element. */
  protected def outputVectorSize: Int =
    $(vectorSize) + (if ($(normalization) == Hash2Vec.L2WithMagnitude) 1 else 0)

  /**
   * Validate and transform the input schema.
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val typeCandidates = Seq(StringType, ByteType, ShortType, IntegerType, LongType)
      .flatMap(t => Seq(ArrayType(t, containsNull = true), ArrayType(t, containsNull = false)))
    SchemaUtils.checkColumnTypes(schema, $(inputCol), typeCandidates)
    SchemaUtils.appendColumn(schema, $(outputCol), SQLDataTypes.VectorType)
  }
}

/**
 * :: Experimental :: An implementation of the Hash2Vec algorithm. This approach is based on
 * feature hashing (a kind of random projection) and has no learning phase. The whole algorithm is
 * one pass over the corpus plus one aggregation of the resulting vectors. Compared to Word2Vec,
 * Hash2Vec trades embedding quality for scalability. The algorithm is stateless and does not need
 * a broadcast vocabulary or a trained weight matrix. Because of this, it scales horizontally with
 * both the corpus size and the vocabulary size, while the Word2Vec implementation in Spark ML
 * scales horizontally only with the corpus size.
 *
 * Choose this algorithm over Word2Vec when the vocabulary is big. With the default vector size
 * (100), Word2Vec has a hard limit of about 21M words, and at this size it broadcasts two weight
 * matrices of about 8.6GB each. A more realistic limit is around 10M words. Beyond 10M words,
 * choose Hash2Vec; below it, Word2Vec is better because its embeddings have better quality.
 *
 * Hash2Vec is also a good choice when speed matters: on big vocabularies and corpora it is much
 * faster than Word2Vec. If you need a baseline or an MVP, Hash2Vec may be a good choice too.
 *
 * On embedding quality: on typical ML tasks such as classification, Word2Vec gives around 20%
 * better results, according to the authors of Hash2Vec and independent validations.
 *
 * The input column can contain arrays of strings or of integral numbers. Mapping string tokens to
 * integer ids before fitting reduces memory pressure and increases performance. Fitting fails on
 * null sequences and on null tokens.
 *
 * Reference: Argerich, Luis, Joaquin Torre Zaffaroni, and Matias J. Cano. "Hash2vec, feature
 * hashing for word embeddings." arXiv preprint arXiv:1608.08940 (2016).
 *
 * @see
 *   <a href="https://arxiv.org/abs/1608.08940">Hash2vec, feature hashing for word embeddings</a>
 */
@Since("4.5.0")
@Experimental
final class Hash2Vec @Since("4.5.0") (@Since("4.5.0") override val uid: String)
    extends Estimator[Hash2VecModel]
    with Hash2VecBase
    with DefaultParamsWritable {

  @Since("4.5.0")
  def this() = this(Identifiable.randomUID("h2v"))

  /** @group setParam */
  @Since("4.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("4.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("4.5.0")
  def setVectorSize(value: Int): this.type = set(vectorSize, value)

  /** @group setParam */
  @Since("4.5.0")
  def setWindowSize(value: Int): this.type = set(windowSize, value)

  /** @group setParam */
  @Since("4.5.0")
  def setNumPartitions(value: Int): this.type = set(numPartitions, value)

  /** @group setParam */
  @Since("4.5.0")
  def setDecayFunction(value: String): this.type = set(decayFunction, value)

  /** @group setParam */
  @Since("4.5.0")
  def setDecayScale(value: Double): this.type = set(decayScale, value)

  /** @group setParam */
  @Since("4.5.0")
  def setNormalization(value: String): this.type = set(normalization, value)

  /** @group expertSetParam */
  @Since("4.5.0")
  def setFinalStorageLevel(value: String): this.type = set(finalStorageLevel, value)

  /** @group setParam */
  @Since("4.5.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("4.5.0")
  override def fit(dataset: Dataset[_]): Hash2VecModel = instrumented { instr =>
    transformSchema(dataset.schema, logging = true)
    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, params.toImmutableArraySeq: _*)

    val isString =
      SchemaUtils.getSchemaFieldType(dataset.schema, $(inputCol)) match {
        case ArrayType(StringType, _) => true
        case _ => false
      }
    val wordType = if (isString) StringType else LongType
    val input = dataset.select(col($(inputCol)).cast(ArrayType(wordType))).queryExecution.toRdd

    val size = $(vectorSize)
    val weights = Hash2Vec.decayWeights($(decayFunction), $(decayScale), $(windowSize))
    val indexSeed = java.lang.Long.hashCode($(seed))
    val signSeed = Murmur3_x86_32.hashLong($(seed), indexSeed)
    val partitions =
      if (isSet(numPartitions)) $(numPartitions)
      else math.max(input.getNumPartitions, 1)
    val normalizationMode = $(normalization)

    def toRows[K: ClassTag](pairs: RDD[(K, Array[Byte])]): RDD[Row] =
      Hash2Vec.sumContexts(pairs, size, partitions).map { case (word, sum) =>
        Row(word, Hash2Vec.normalize(sum, normalizationMode))
      }

    val rows = if (isString) {
      toRows(input.mapPartitions { iter =>
        val encoder = new Hash2Vec.ContextEncoder(size, weights)
        iter.flatMap(Hash2Vec.encodeStrings(_, encoder, indexSeed, signSeed))
      })
    } else {
      toRows(input.mapPartitions { iter =>
        val encoder = new Hash2Vec.ContextEncoder(size, weights)
        iter.flatMap(Hash2Vec.encodeLongs(_, encoder, indexSeed, signSeed))
      })
    }

    val schema = StructType(
      Array(
        StructField(Hash2Vec.WordCol, wordType, nullable = false),
        StructField(Hash2Vec.VectorCol, SQLDataTypes.VectorType, nullable = false)))
    val vectors = dataset.sparkSession
      .createDataFrame(rows, schema)
      .persist(StorageLevel.fromString($(finalStorageLevel)))
    // Fitting is eager, so invalid input fails here rather than on the first use of the model.
    instr.logNamedValue("numWords", vectors.count())

    copyValues(new Hash2VecModel(uid, vectors).setParent(this))
  }

  @Since("4.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("4.5.0")
  override def copy(extra: ParamMap): Hash2Vec = defaultCopy(extra)
}

@Since("4.5.0")
@Experimental
object Hash2Vec extends DefaultParamsReadable[Hash2Vec] {

  private[feature] val WordCol = "word"
  private[feature] val VectorCol = "vector"

  private val Constant = "constant"
  private val Gaussian = "gaussian"
  private val Exponential = "exponential"
  private val Linear = "linear"
  private val Harmonic = "harmonic"

  private[feature] val supportedDecayFunctions =
    Array(Constant, Gaussian, Exponential, Linear, Harmonic)

  private val NoNormalization = "none"
  private val L2 = "l2"
  private[feature] val L2WithMagnitude = "l2WithMagnitude"

  private[feature] val supportedNormalizations = Array(NoNormalization, L2, L2WithMagnitude)

  // A sparse context entry is packed as an Int index followed by a Double value.
  private val EntrySize = Integer.BYTES + java.lang.Double.BYTES

  @Since("4.5.0")
  override def load(path: String): Hash2Vec = super.load(path)

  /** Returns the weights indexed by distance. Index 0 is unused. */
  private[feature] def decayWeights(
      function: String,
      scale: Double,
      windowSize: Int): Array[Double] = {
    val weight: Int => Double = function match {
      case Constant => _ => 1.0
      case Gaussian => d => math.exp(-(d.toDouble * d) / (2 * scale * scale))
      case Exponential => d => math.exp(-d / scale)
      case Linear => d => (windowSize + 1 - d).toDouble / windowSize
      case Harmonic => d => 1.0 / d
    }
    Array.tabulate(windowSize + 1)(d => if (d == 0) 0.0 else weight(d))
  }

  /**
   * Builds the sparse context of each distinct token of one sequence. Hashes are computed once
   * per position, and positions of the same token are chained so that repeats merge without
   * boxing.
   */
  private[feature] final class ContextEncoder(vectorSize: Int, weights: Array[Double]) {
    private val windowSize = weights.length - 1
    private val sum = new Array[Double](vectorSize)
    private val isTouched = new Array[Boolean](vectorSize)
    private val touched = new Array[Int](vectorSize)

    private var length = 0
    private var indices = new Array[Int](64)
    private var signs = new Array[Double](64)
    private var nextPosition = new Array[Int](64)
    private var heads = new Array[Int](64)
    private var headCount = 0

    def reset(sequenceLength: Int): Unit = {
      length = sequenceLength
      headCount = 0
      if (sequenceLength > indices.length) {
        indices = new Array[Int](sequenceLength)
        signs = new Array[Double](sequenceLength)
        nextPosition = new Array[Int](sequenceLength)
        heads = new Array[Int](sequenceLength)
      }
    }

    /** Registers a position. `previous` is the last position of the same token, or -1. */
    def add(position: Int, indexHash: Int, signHash: Int, previous: Int): Unit = {
      indices(position) = Utils.nonNegativeMod(indexHash, vectorSize)
      signs(position) = if ((signHash & 1) == 1) 1.0 else -1.0
      nextPosition(position) = -1
      if (previous >= 0) {
        nextPosition(previous) = position
      } else {
        heads(headCount) = position
        headCount += 1
      }
    }

    def numHeads: Int = headCount

    def head(i: Int): Int = heads(i)

    /** Returns the packed context of the token whose first position is `first`. */
    def encode(first: Int): Array[Byte] = {
      var numTouched = 0
      var position = first
      while (position >= 0) {
        var context = math.max(0, position - windowSize)
        val end = math.min(length - 1, position + windowSize)
        while (context <= end) {
          if (context != position) {
            val index = indices(context)
            if (!isTouched(index)) {
              isTouched(index) = true
              touched(numTouched) = index
              numTouched += 1
            }
            sum(index) += signs(context) * weights(math.abs(context - position))
          }
          context += 1
        }
        position = nextPosition(position)
      }

      var numNonZeros = 0
      var i = 0
      while (i < numTouched) {
        if (sum(touched(i)) != 0.0) numNonZeros += 1
        i += 1
      }
      val buffer = ByteBuffer.allocate(numNonZeros * EntrySize)
      i = 0
      while (i < numTouched) {
        val index = touched(i)
        if (sum(index) != 0.0) {
          buffer.putInt(index)
          buffer.putDouble(sum(index))
        }
        sum(index) = 0.0
        isTouched(index) = false
        i += 1
      }
      buffer.array()
    }
  }

  private def nullSequenceError(): SparkException =
    new SparkException("Hash2Vec does not support null sequences.")

  private def nullElementError(): SparkException =
    new SparkException("Hash2Vec does not support null elements in sequences during fit.")

  private[feature] def encodeLongs(
      row: InternalRow,
      encoder: ContextEncoder,
      indexSeed: Int,
      signSeed: Int): Array[(Long, Array[Byte])] = {
    if (row.isNullAt(0)) throw nullSequenceError()
    val array = row.getArray(0)
    val length = array.numElements()
    val tokens = new Array[Long](length)
    val lastPositions = new mutable.LongMap[Int]()
    encoder.reset(length)
    var i = 0
    while (i < length) {
      if (array.isNullAt(i)) throw nullElementError()
      val token = array.getLong(i)
      tokens(i) = token
      encoder.add(
        i,
        Murmur3_x86_32.hashLong(token, indexSeed),
        Murmur3_x86_32.hashLong(token, signSeed),
        lastPositions.getOrElse(token, -1))
      lastPositions.update(token, i)
      i += 1
    }
    Array.tabulate(encoder.numHeads) { h =>
      val first = encoder.head(h)
      (tokens(first), encoder.encode(first))
    }
  }

  private[feature] def encodeStrings(
      row: InternalRow,
      encoder: ContextEncoder,
      indexSeed: Int,
      signSeed: Int): Array[(String, Array[Byte])] = {
    if (row.isNullAt(0)) throw nullSequenceError()
    val array = row.getArray(0)
    val length = array.numElements()
    val tokens = new Array[UTF8String](length)
    val lastPositions = new mutable.HashMap[UTF8String, Int]()
    encoder.reset(length)
    var i = 0
    while (i < length) {
      if (array.isNullAt(i)) throw nullElementError()
      val token = array.getUTF8String(i)
      tokens(i) = token
      encoder.add(
        i,
        hashString(token, indexSeed),
        hashString(token, signSeed),
        lastPositions.getOrElse(token, -1))
      lastPositions.update(token, i)
      i += 1
    }
    // The tokens point into the reused input row, so they are copied by toString here.
    Array.tabulate(encoder.numHeads) { h =>
      val first = encoder.head(h)
      (tokens(first).toString, encoder.encode(first))
    }
  }

  private def hashString(token: UTF8String, seed: Int): Int =
    Murmur3_x86_32.hashUnsafeBytes2(
      token.getBaseObject,
      token.getBaseOffset,
      token.numBytes(),
      seed)

  private def addPacked(target: Array[Double], packed: Array[Byte]): Array[Double] = {
    val buffer = ByteBuffer.wrap(packed)
    while (buffer.hasRemaining) {
      val index = buffer.getInt()
      target(index) += buffer.getDouble()
    }
    target
  }

  /**
   * Sums the packed contexts by token. Map-side combining is disabled on purpose: it would turn
   * small sparse contexts into dense vectors before the shuffle.
   */
  private[feature] def sumContexts[K: ClassTag](
      pairs: RDD[(K, Array[Byte])],
      vectorSize: Int,
      numPartitions: Int): RDD[(K, Array[Double])] = {
    pairs.combineByKey[Array[Double]](
      (packed: Array[Byte]) => addPacked(new Array[Double](vectorSize), packed),
      (sum: Array[Double], packed: Array[Byte]) => addPacked(sum, packed),
      (left: Array[Double], right: Array[Double]) => {
        BLAS.javaBLAS.daxpy(left.length, 1.0, right, 1, left, 1)
        left
      },
      new HashPartitioner(numPartitions),
      mapSideCombine = false)
  }

  private[feature] def normalize(sum: Array[Double], normalization: String): Vector = {
    if (normalization == NoNormalization) {
      Vectors.dense(sum)
    } else {
      val norm = BLAS.javaBLAS.dnrm2(sum.length, sum, 1)
      if (norm > 0.0) BLAS.javaBLAS.dscal(sum.length, 1.0 / norm, sum, 1)
      if (normalization == L2WithMagnitude) {
        Vectors.dense(sum :+ (math.log(norm + 1.0) / math.sqrt(sum.length.toDouble)))
      } else {
        Vectors.dense(sum)
      }
    }
  }
}

/**
 * :: Experimental :: Model fitted by [[Hash2Vec]]. It holds one vector per token as a distributed
 * DataFrame, see `getVectors`. The vectors are never collected to the driver or broadcast, so the
 * model scales with the vocabulary size.
 */
@Since("4.5.0")
@Experimental
class Hash2VecModel private[ml] (
    @Since("4.5.0") override val uid: String,
    @transient private val vectors: DataFrame)
    extends Model[Hash2VecModel]
    with Hash2VecBase
    with MLWritable {

  // For ml connect only
  private[ml] def this() = this("", null)

  /**
   * Returns the token vectors. After fitting, this DataFrame is persisted with
   * `finalStorageLevel` and has `numPartitions` partitions; call `unpersist()` on it to release
   * the storage. A loaded model reads the vectors from the saved files instead.
   *
   * @return
   *   a DataFrame with the columns "word" (string, or long for integral tokens) and "vector".
   */
  @Since("4.5.0")
  def getVectors: DataFrame = vectors

  /** @group setParam */
  @Since("4.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("4.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  private[spark] override def estimatedSize: Long = {
    var size = estimateMatadataSize
    if (vectors != null) {
      val wordSize = if (vectors.schema(Hash2Vec.WordCol).dataType == StringType) 15L else 8L
      size += vectors.count() * (wordSize + outputVectorSize * 8L)
    }
    size
  }

  /**
   * Transforms each sequence into the average of its token vectors, as Word2VecModel does.
   * Unknown and null tokens count towards the length of the sequence but add nothing to the sum.
   * An empty sequence becomes a zero vector. Null sequences are not supported.
   *
   * This method explodes the sequences, joins the tokens with the fitted vectors and aggregates
   * the result, so it is expensive and does not preserve the order of the input rows. To get the
   * vectors themselves, use `getVectors`.
   */
  @Since("4.5.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val idCol = s"${uid}_id"
    val rowCol = s"${uid}_row"
    val tokensCol = s"${uid}_tokens"
    val lengthCol = s"${uid}_length"
    val positionCol = s"${uid}_position"
    val tokenCol = s"${uid}_token"
    val wordCol = s"${uid}_word"
    val vectorCol = s"${uid}_vector"

    val wordType = vectors.schema(Hash2Vec.WordCol).dataType
    val tokens = when(
      col($(inputCol)).isNull,
      raise_error(lit("Hash2VecModel does not support null sequences.")))
      .otherwise(col($(inputCol)).cast(ArrayType(wordType)))

    // The input columns are kept on the first exploded row only, to avoid copying them per token.
    val exploded = dataset
      .toDF()
      .select(
        struct(col("*")).as(rowCol),
        monotonically_increasing_id().as(idCol),
        tokens.as(tokensCol))
      .select(
        col(idCol),
        col(rowCol),
        size(col(tokensCol)).as(lengthCol),
        posexplode_outer(col(tokensCol)).as(Seq(positionCol, tokenCol)))
      .select(
        col(idCol),
        when(col(positionCol).isNull || col(positionCol) === 0, col(rowCol)).as(rowCol),
        col(lengthCol),
        col(tokenCol))
    val words =
      vectors.select(col(Hash2Vec.WordCol).as(wordCol), col(Hash2Vec.VectorCol).as(vectorCol))

    val outputSize = outputVectorSize
    // As in Word2Vec, unknown and null tokens count towards the length of the sequence.
    val average = udf { (tokenVectors: Seq[Vector], length: Int) =>
      if (length == 0) {
        Vectors.sparse(outputSize, Array.emptyIntArray, Array.emptyDoubleArray)
      } else {
        val sum = Array.ofDim[Double](outputSize)
        tokenVectors.foreach(_.foreachNonZero((i, value) => sum(i) += value))
        BLAS.javaBLAS.dscal(outputSize, 1.0 / length, sum, 1)
        Vectors.dense(sum)
      }
    }

    exploded
      .join(words, col(tokenCol) === col(wordCol), "left")
      .groupBy(col(idCol))
      .agg(
        first(col(rowCol), ignoreNulls = true).as(rowCol),
        first(col(lengthCol)).as(lengthCol),
        collect_list(col(vectorCol)).as(vectorCol))
      .select(
        col(s"$rowCol.*"),
        average(col(vectorCol), col(lengthCol))
          .as($(outputCol), outputSchema($(outputCol)).metadata))
  }

  @Since("4.5.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if (vectors != null) {
      val isStringInput = SchemaUtils.getSchemaFieldType(schema, $(inputCol)) match {
        case ArrayType(StringType, _) => true
        case _ => false
      }
      val isStringModel = vectors.schema(Hash2Vec.WordCol).dataType == StringType
      require(
        isStringInput == isStringModel,
        s"Input column ${$(inputCol)} must have the same kind of tokens as the fitted model: " +
          s"${if (isStringModel) "string" else "integral"}.")
    }
    if ($(outputCol).nonEmpty) {
      outputSchema =
        SchemaUtils.updateAttributeGroupSize(outputSchema, $(outputCol), outputVectorSize)
    }
    outputSchema
  }

  @Since("4.5.0")
  override def copy(extra: ParamMap): Hash2VecModel = {
    val copied = new Hash2VecModel(uid, vectors)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("4.5.0")
  override def write: MLWriter = new Hash2VecModel.Hash2VecModelWriter(this)

  @Since("4.5.0")
  override def toString: String = {
    s"Hash2VecModel: uid=$uid, vectorSize=${$(vectorSize)}"
  }
}

@Since("4.5.0")
@Experimental
object Hash2VecModel extends MLReadable[Hash2VecModel] {

  private[Hash2VecModel] class Hash2VecModelWriter(instance: Hash2VecModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val dataPath = new Path(path, "data").toString
      // The Arrow files of the local saving mode do not keep the vector UDT.
      val data = if (ReadWriteUtils.localSavingModeState.get()) {
        instance.getVectors.withColumn(
          Hash2Vec.VectorCol,
          vector_to_array(col(Hash2Vec.VectorCol)))
      } else {
        instance.getVectors
      }
      ReadWriteUtils.saveDataFrame(dataPath, data)
    }
  }

  private class Hash2VecModelReader extends MLReader[Hash2VecModel] {

    private val className = classOf[Hash2VecModel].getName

    override def load(path: String): Hash2VecModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString
      val data = ReadWriteUtils.loadDataFrame(dataPath, sparkSession)
      val vectors = if (ReadWriteUtils.localSavingModeState.get()) {
        data.withColumn(Hash2Vec.VectorCol, array_to_vector(col(Hash2Vec.VectorCol)))
      } else {
        data
      }
      val model = new Hash2VecModel(metadata.uid, vectors)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("4.5.0")
  override def read: MLReader[Hash2VecModel] = new Hash2VecModelReader

  @Since("4.5.0")
  override def load(path: String): Hash2VecModel = super.load(path)
}
