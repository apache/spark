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

package org.apache.spark.graphframes.embeddings

import dev.ludovic.netlib.blas.BLAS
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.hash
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.pmod
import org.apache.spark.sql.functions.transform
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.hash.Murmur3_x86_32._
import org.apache.spark.graphframes.GraphFramesUnsupportedVertexTypeException
import org.apache.spark.graphframes.rw.RandomWalkBase

import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

/**
 * Implementation of Hash2Vec, an efficient word embedding technique using feature hashing. Based
 * on: Argerich, Luis, Joaquín Torré Zaffaroni, and Matías J. Cano. "Hash2vec, feature hashing for
 * word embeddings." arXiv preprint arXiv:1608.08940 (2016).
 *
 * Produces embeddings for elements in sequences using a hash-based approach to avoid storing a
 * vocabulary. Uses MurmurHash3 for hashing elements to embedding indices and signs.
 *
 * Output DataFrame has columns "id" (element identifier, same type as sequence elements) and
 * "vector" (dense vector of doubles, summed across all occurrences).
 *
 * Tradeoffs: Higher numPartitions reduces local state and memory per partition but increases
 * aggregation and merging overhead across partitions. Larger embeddingsDim provides richer
 * representations but consumes more memory. Seeds control hashing for reproducibility.
 */
class Hash2Vec extends Serializable {
  private def decayGaussian(d: Int, sigma: Double): Double = {
    math.exp(-(d * d) / (sigma * sigma))
  }
  private val possibleDecayFunctions: Seq[String] = Seq("gaussian", "constant")

  private var contextSize: Int = 5
  private var numPartitions: Int = 5
  private var embeddingsDim: Int = 512
  private var sequenceCol: String = RandomWalkBase.rwColName
  private var decayFunction: String = "gaussian"
  private var gaussianSigma: Double = 1.0
  private var hashingSeed: Int = 42
  private var signHashingSeed: Int = 18
  private var doL2Norm: Boolean = true
  private var safeL2NormAsChannel: Boolean = true
  private var maxVectorsPerPartition: Int = 100000

  /**
   * Sets whether final vectors are L2‑normalized after aggregation across partitions. When
   * normalization is enabled, each vector is scaled to unit length (L2 norm = 1).
   *
   * When `safeNorm` is true (default), the method adds an extra channel to the vector equal to
   * `log(L2‑norm + 1) / sqrt(dim)`. This preserves some information about the original magnitude
   * while still making vectors comparable via cosine similarity.
   *
   * When `safeNorm` is false, normalizes without adding an extra channel, discarding magnitude
   * entirely.
   *
   * This setting applies globally to all output vectors.
   *
   * @param doNorm
   *   If true, output vectors are normalized.
   * @param safeNorm
   *   If true (and doNorm is true), retains magnitude information in an extra dimension. If
   *   false, performs standard L2‑normalization.
   * @return
   *   This Hash2Vec instance for method chaining.
   */
  def setDoNormalization(doNorm: Boolean, safeNorm: Boolean): this.type = {
    doL2Norm = doNorm
    safeL2NormAsChannel = safeNorm
    this
  }

  /**
   * Limits the maximum number of distinct element vectors that can be processed inside a single
   * partition before flushing intermediate results to the iterator.
   *
   * Partition processing uses a paged matrix to store vectors. When the number of allocated
   * vectors reaches this limit within a partition, the current batch of vectors is returned (as
   * part of the iterator) and a new empty batch is started for the remaining elements.
   *
   * This prevents a single partition from consuming unbounded memory while processing very large
   * vocabularies, at the cost of producing multiple iterator batches per partition.
   *
   * The default value is 100000.
   *
   * @param value
   *   Upper bound on distinct vectors processed per batch inside a partition.
   * @return
   *   This Hash2Vec instance for method chaining.
   */
  def setMaxVectorsPerPartition(value: Int): this.type = {
    maxVectorsPerPartition = value
    this
  }

  /**
   * Convenience overload for `setDoNormalization(doNorm, safeNorm)` that uses safe‑mode (extra
   * channel) by default. Equivalent to `setDoNormalization(value, true)`.
   *
   * @param value
   *   If true, output vectors are L2‑normalized with safe (extra‑channel) semantics.
   * @return
   *   This Hash2Vec instance for method chaining.
   */
  def setDoNormalization(value: Boolean): this.type = {
    setDoNormalization(value, true)
    this
  }

  /**
   * Sets the context window size around each element to consider during training. Larger values
   * incorporate more distant elements but increase computation time. Default: 5.
   */
  def setContextSize(value: Int): this.type = {
    contextSize = value
    this
  }

  /**
   * Sets the number of partitions for RDDs to parallelize computation. More partitions distribute
   * workload and reduce memory per partition but complicate merging across partitions. Default:
   * 5.
   */
  def setNumPartitions(value: Int): this.type = {
    numPartitions = value
    this
  }

  /**
   * Sets the dimensionality of the dense embedding vectors. Larger dimensions allow richer
   * representations but require more memory. Corresponds to the hash table size. Default: 512.
   */
  def setEmbeddingsDim(value: Int): this.type = {
    embeddingsDim = value
    this
  }

  /**
   * Sets the column name containing sequences of elements (as arrays). Default: "random_walk".
   */
  def setSequenceCol(value: String): this.type = {
    sequenceCol = value
    this
  }

  /**
   * Sets the decay function used to weight context elements by distance. Supported values:
   * "gaussian", "constant". Default: "gaussian".
   */
  def setDecayFunction(value: String): this.type = {
    val sep = ", "
    require(
      possibleDecayFunctions.contains(value),
      s"supported functions: ${possibleDecayFunctions.mkString(sep)}")
    decayFunction = value
    this
  }

  /**
   * Sets the sigma parameter for Gaussian decay weighting. Smaller values decay weights faster
   * with distance. Default: 1.0.
   */
  def setGaussianSigma(value: Double): this.type = {
    gaussianSigma = value
    this
  }

  /**
   * Sets the seed for hashing elements to embedding indices. Used for reproducibility of
   * embeddings. Default: 42.
   */
  def setHashingSeed(value: Int): this.type = {
    hashingSeed = value
    this
  }

  /**
   * Sets the seed for hashing elements to determine the sign of contributions. Used for
   * reproducibility of embeddings. Default: 18.
   */
  def setSignHashSeed(value: Int): this.type = {
    signHashingSeed = value
    this
  }

  private def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  private var weightFunction: (Int) => Double = _

  private def seededStringHashFunc(s: String, seed: Int, dim: Int): Int = {
    var h = seed
    var i = 0
    val len = s.length
    while (i < len) {
      h = MurmurHash3.mix(h, s.charAt(i).toInt)
      i += 1
    }
    nonNegativeMod(MurmurHash3.finalizeHash(h, len), dim)
  }

  private def seededLongHashFunc(l: Long, seed: Int, dim: Int): Int =
    nonNegativeMod(hashLong(l, seed), dim)

  private def normalize(vector: linalg.Vector, addChannel: Boolean): linalg.Vector = {
    val blas = BLAS.getInstance()
    val arr = vector.toArray
    val norm = blas.dnrm2(arr.size, arr, 0, 1)
    blas.dscal(arr.size, 1 / (norm + 1e-6), arr, 1)
    if (addChannel) {
      val scaledL2 = math.log(norm + 1)
      val newChannel = scaledL2 / math.sqrt(vector.size.toDouble)
      new linalg.DenseVector(arr :+ newChannel)
    } else {
      new linalg.DenseVector(arr)
    }
  }

  /**
   * Runs the Hash2Vec algorithm on the input DataFrame containing sequences. The specified
   * sequenceCol must contain arrays of elements (string or numeric). Produces a DataFrame with
   * "id" (element ID, same type as elements) and "vector" (embedding vector, VectorType).
   * Embeddings are summed across all partitions and occurrences.
   */
  def run(rawData: DataFrame): DataFrame = {
    val spark = rawData.sparkSession
    require(
      rawData.schema(sequenceCol).dataType.isInstanceOf[ArrayType],
      "sequence should be array")
    val elDataType = rawData.schema(sequenceCol).dataType.asInstanceOf[ArrayType].elementType

    val data =
      if (elDataType.isInstanceOf[ByteType] || elDataType.isInstanceOf[ShortType] || elDataType
          .isInstanceOf[IntegerType]) {
        rawData.withColumn(
          sequenceCol,
          transform(col(sequenceCol), (f: Column) => f.cast(LongType)))
      } else {
        rawData
      }

    weightFunction = decayFunction match {
      case "gaussian" => (d: Int) => decayGaussian(d, gaussianSigma)
      case "constant" => (_: Int) => 1.0
      case _ => throw new RuntimeException(s"unsupported decay functions $decayFunction")
    }

    val (rowRDD, schema) = elDataType match {
      case _: StringType =>
        (
          runTyped[String](data).map(f => Row(f._1, Vectors.dense(f._2))),
          StructType(Seq(StructField("id", StringType), StructField("vector", VectorType))))
      case _: LongType =>
        (
          runTyped[Long](data).map(f => Row(f._1, Vectors.dense(f._2))),
          StructType(Seq(StructField("id", LongType), StructField("vector", VectorType))))
      case _ =>
        throw new GraphFramesUnsupportedVertexTypeException(
          s"Hash2vec supports only string or numeric types of elements but got ${elDataType.toString()}")
    }

    val embeddings = spark
      .createDataFrame(rowRDD, schema)
      .groupBy("id")
      .agg(Summarizer.sum(col("vector")).alias("vector"))

    val normalizer = (x: linalg.Vector) => normalize(x, safeL2NormAsChannel)

    if (doL2Norm) {
      embeddings.withColumn("vector", udf(normalizer).apply(col("vector")))
    } else {
      embeddings
    }
  }

  private def runTyped[T: ClassTag](data: DataFrame): RDD[(T, Array[Double])] = {
    // we should put sequences starts from the same vertex
    // to the same partition when possible
    data
      .withColumn("hash_id", pmod(hash(col(sequenceCol).getItem(0)), lit(numPartitions)))
      .repartition(numPartitions, col("hash_id"))
      .sortWithinPartitions(col("hash_id"))
      .select(col(sequenceCol))
      .rdd
      .map(_.getSeq[T](0))
      .mapPartitions { iter =>
        val elemType = implicitly[ClassTag[T]].runtimeClass
        elemType match {
          case clazz if clazz == classOf[String] =>
            processStringPartition(iter.asInstanceOf[Iterator[Seq[String]]])
              .asInstanceOf[Iterator[(T, Array[Double])]]
          case clazz if clazz == classOf[Long] =>
            processLongPartition(iter.asInstanceOf[Iterator[Seq[Long]]])
              .asInstanceOf[Iterator[(T, Array[Double])]]
          case _ =>
            throw new GraphFramesUnsupportedVertexTypeException(
              s"Hash2vec does not support type ${elemType.getCanonicalName}")
        }
      }
  }

  private def processStringPartition(
      iter: Iterator[Seq[String]]): Iterator[(String, Array[Double])] = {

    val localHashSeed = hashingSeed
    val localSignHashSeed = signHashingSeed
    val localEmbeddingsDim = embeddingsDim
    val localContextSize = contextSize
    val localMaxVecrtors = maxVectorsPerPartition

    val weightCache = new Array[Double](localContextSize + 1)
    for (d <- 1 to localContextSize) weightCache(d) = weightFunction(d)
    val signs = Array[Double](-1.0, 1.0)

    new Iterator[(String, Array[Double])] {

      var currentBatchResult: Iterator[(String, Array[Double])] = Iterator.empty

      var vocabIndex: collection.mutable.HashMap[String, Int] = _
      var matrix: Hash2Vec.PagedMatrixDouble = _

      override def hasNext: Boolean = {
        if (currentBatchResult.hasNext) return true
        if (!iter.hasNext) return false

        fetchNextBatch()
        currentBatchResult.hasNext
      }

      override def next(): (String, Array[Double]) = {
        currentBatchResult.next()
      }

      private def fetchNextBatch(): Unit = {
        vocabIndex = new collection.mutable.HashMap[String, Int]()
        vocabIndex.sizeHint(math.min(localMaxVecrtors, 50000))

        matrix = new Hash2Vec.PagedMatrixDouble(localEmbeddingsDim)

        var currentBatchSize = 0

        while (iter.hasNext && currentBatchSize < localMaxVecrtors) {
          val seq = iter.next()
          val currentSeqSize = seq.length
          var idx = 0

          while (idx < currentSeqSize) {
            val currentWord = seq(idx)

            var vectorId = vocabIndex.getOrElse(currentWord, -1)

            if (vectorId == -1) {
              vectorId = matrix.allocateVector()
              vocabIndex.put(currentWord, vectorId)
              currentBatchSize += 1
            }

            val start = math.max(0, idx - localContextSize)
            val end = math.min(currentSeqSize - 1, idx + localContextSize)
            var cIdx = start

            while (cIdx <= end) {
              if (cIdx != idx) {
                val word = seq(cIdx)
                val embeddingIdx = seededStringHashFunc(word, localHashSeed, localEmbeddingsDim)

                val rawSignHash = seededStringHashFunc(word, localSignHashSeed, 65536)
                val sign = signs(rawSignHash & 1)

                val weight = weightCache(math.abs(cIdx - idx))

                matrix.add(vectorId, embeddingIdx, sign * weight)
              }
              cIdx += 1
            }
            idx += 1
          }
        }

        currentBatchResult = vocabIndex.iterator.map { case (word, id) =>
          (word, matrix.getVector(id))
        }
      }
    }
  }

  // Specialized partition processing for Long
  private def processLongPartition(iter: Iterator[Seq[Long]]): Iterator[(Long, Array[Double])] = {

    val localHashSeed = hashingSeed
    val localSignHashSeed = signHashingSeed
    val localEmbeddingsDim = embeddingsDim
    val localContextSize = contextSize
    val localMaxVectors = maxVectorsPerPartition

    val weightCache = new Array[Double](localContextSize + 1)
    for (d <- 1 to localContextSize) weightCache(d) = weightFunction(d)
    val signs = Array[Double](-1.0, 1.0)

    new Iterator[(Long, Array[Double])] {

      var currentBatchResult: Iterator[(Long, Array[Double])] = Iterator.empty

      var vocabIndex: collection.mutable.LongMap[Int] = _
      var matrix: Hash2Vec.PagedMatrixDouble = _

      override def hasNext: Boolean = {
        if (currentBatchResult.hasNext) return true
        if (!iter.hasNext) return false

        fetchNextBatch()
        currentBatchResult.hasNext
      }

      override def next(): (Long, Array[Double]) = {
        currentBatchResult.next()
      }

      private def fetchNextBatch(): Unit = {
        vocabIndex = new collection.mutable.LongMap[Int]()
        vocabIndex.sizeHint(math.min(localMaxVectors, 100000))

        matrix = new Hash2Vec.PagedMatrixDouble(localEmbeddingsDim)

        var currentBatchSize = 0

        while (iter.hasNext && currentBatchSize < localMaxVectors) {
          val seq = iter.next()
          val currentSeqSize = seq.length
          var idx = 0

          while (idx < currentSeqSize) {
            val currentWord = seq(idx)

            var vectorId = vocabIndex.getOrElse(currentWord, -1)

            if (vectorId == -1) {
              vectorId = matrix.allocateVector()
              vocabIndex.put(currentWord, vectorId)
              currentBatchSize += 1
            }

            val start = math.max(0, idx - localContextSize)
            val end = math.min(currentSeqSize - 1, idx + localContextSize)
            var cIdx = start

            while (cIdx <= end) {
              if (cIdx != idx) {
                val word = seq(cIdx)
                val embeddingIdx = seededLongHashFunc(word, localHashSeed, localEmbeddingsDim)
                val sign = signs(seededLongHashFunc(word, localSignHashSeed, 2))
                val weight = weightCache(math.abs(cIdx - idx))

                matrix.add(vectorId, embeddingIdx, sign * weight)
              }
              cIdx += 1
            }
            idx += 1
          }
        }

        currentBatchResult = vocabIndex.iterator.map { case (word, id) =>
          (word, matrix.getVector(id))
        }
      }
    }
  }
}

object Hash2Vec {

  /**
   * A paged matrix of double-precision vectors that stores vectors contiguously in large
   * fixed‑sized pages, each holding PAGE_SIZE (4096) vectors of dimension `dim`.
   *
   * This layout replaces a HashMap[T, Array[Double]] with two separate structures:
   *   1. A mapping from element identifier (T) to a vector ID (Int), maintained by the caller.
   *   2. The actual vector data stored in a few large arrays (pages) instead of many small
   *      per‑element arrays.
   *
   * Advantages over a HashMap-of-arrays:
   *   1. Eliminates per‑vector Array object overhead (object
   * header, reference, GC metadata).
   *   2. Reduces GC pressure because the backing store is a small number of large long‑lived
   *      arrays, not many short‑lived small arrays that become garbage as the map is updated.
   *   3. Better memory locality: vectors of the same dimension are stored consecutively,
   *      improving cache line utilisation during sequential access (e.g., inside a page).
   *   4. Predictable memory growth: pages are allocated only when the current page is full,
   *      avoiding repeated resizing of a hash‑map and associated re‑hashing / copying.
   *
   * The cost is an extra indirection to compute the page index and offset, which is cheap (bit
   * shifts and masks) compared to the GC and memory overhead it saves.
   *
   * Implementation notes:
   *   1. PAGE_BITS = 12, PAGE_SIZE = 4096 (2^12). This keeps pageIdx =
   * vectorId >>> PAGE_BITS and localRow = vectorId & PAGE_MASK cheap, while limiting page memory
   * to PAGE_SIZE * dim doubles.
   *   2. The first page is pre‑allocated in the constructor; subsequent
   * pages are added on‑demand when allocateVector() crosses a page boundary.
   *   3. allocateVector()
   * returns a monotonically increasing integer ID, which is the index of the vector across all
   * pages. The caller stores this ID in a HashMap[T, Int] instead of storing the whole array.
   *   4. add() and getVector() compute the flat index inside the page as localRow * dim + offset.
   *   5. Thread safety: not required; each partition processes its own local PagedMatrixDouble
   *      instance.
   */
  private[graphframes] class PagedMatrixDouble(val dim: Int) {
    private final val PAGE_BITS = 12
    private final val PAGE_SIZE = 1 << PAGE_BITS // 4096 -> 2^12
    private final val PAGE_MASK = PAGE_SIZE - 1 // 0xFFF

    private val pages = new collection.mutable.ArrayBuffer[Array[Double]]()
    private var vectorCount = 0

    addPage()

    private def addPage(): Unit = {
      val size = PAGE_SIZE.toLong * dim
      if (size > Int.MaxValue) {
        throw new RuntimeException(s"Dimension $dim is too large for current Page Size.")
      }
      pages += new Array[Double](size.toInt)
    }

    /** Allocate a new zero‑initialized vector and return its unique integer ID. */
    def allocateVector(): Int = {
      val id = vectorCount
      val localIdx = id & PAGE_MASK // ~id % 4096

      if (localIdx == 0 && id > 0) {
        addPage()
      }

      vectorCount += 1
      id
    }

    /** Accumulate `value` into the component `offset` (0‑based) of vector `vectorId`. */
    @inline
    def add(vectorId: Int, offset: Int, value: Double): Unit = {
      // vectorId / PAGE_SIZE using unsigned shift (page index)
      val pageIdx = vectorId >>> PAGE_BITS
      // vectorId % PAGE_SIZE (row inside the page)
      val localRow = vectorId & PAGE_MASK

      val idx = (localRow * dim) + offset
      pages(pageIdx)(idx) += value
    }

    /** Return a fresh copy of the vector identified by `vectorId`. */
    def getVector(vectorId: Int): Array[Double] = {
      val pageIdx = vectorId >>> PAGE_BITS
      val localRow = vectorId & PAGE_MASK
      val page = pages(pageIdx)

      val res = new Array[Double](dim)
      val startPos = localRow * dim
      System.arraycopy(page, startPos, res, 0, dim)
      res
    }
  }
}
