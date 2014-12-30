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

package org.apache.spark.ml.recommendation

import java.{util => javaUtil}

import scala.collection.mutable

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW

import org.apache.spark.{Logging, HashPartitioner, Partitioner}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, StructType}
import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.types.{DoubleType, FloatType, IntegerType, StructField}
import org.apache.spark.util.collection.{OpenHashMap, OpenHashSet, SortDataFormat, Sorter}
import org.apache.spark.util.random.XORShiftRandom

/**
 * Common params for ALS.
 */
private[recommendation] trait ALSParams extends Params with HasMaxIter with HasRegParam
  with HasPredictionCol {

  /** Param for rank of the matrix factorization. */
  val rank = new IntParam(this, "rank", "rank of the factorization", Some(10))
  def getRank: Int = get(rank)

  /** Param for number of user blocks. */
  val numUserBlocks = new IntParam(this, "numUserBlocks", "number of user blocks", Some(10))
  def getNumUserBlocks: Int = get(numUserBlocks)

  /** Param for number of item blocks. */
  val numItemBlocks =
    new IntParam(this, "numItemBlocks", "number of item blocks", Some(10))
  def getNumItemBlocks: Int = get(numItemBlocks)

  /** Param to decide whether to use implicit preference. */
  val implicitPrefs =
    new BooleanParam(this, "implicitPrefs", "whether to use implicit preference", Some(false))
  def getImplicitPrefs: Boolean = get(implicitPrefs)

  /** Param for the alpha parameter in the implicit preference formulation. */
  val alpha = new DoubleParam(this, "alpha", "alpha for implicit preference", Some(1.0))
  def getAlpha: Double = get(alpha)

  /** Param for the column name for user ids. */
  val userCol = new Param[String](this, "userCol", "column name for user ids", Some("user"))
  def getUserCol: String = get(userCol)

  /** Param for the column name for item ids. */
  val itemCol =
    new Param[String](this, "itemCol", "column name for item ids", Some("item"))
  def getItemCol: String = get(itemCol)

  /** Param for the column name for ratings. */
  val ratingCol = new Param[String](this, "ratingCol", "column name for ratings", Some("rating"))
  def getRatingCol: String = get(ratingCol)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @param paramMap extra params
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = this.paramMap ++ paramMap
    assert(schema(map(userCol)).dataType == IntegerType)
    assert(schema(map(itemCol)).dataType== IntegerType)
    val ratingType = schema(map(ratingCol)).dataType
    assert(ratingType == FloatType || ratingType == DoubleType)
    val predictionColName = map(predictionCol)
    assert(!schema.fieldNames.contains(predictionColName),
      s"Prediction column $predictionColName already exists.")
    val newFields = schema.fields :+ StructField(map(predictionCol), FloatType, nullable = false)
    StructType(newFields)
  }
}

/**
 * Model fitted by ALS.
 */
class ALSModel private[ml] (
    override val parent: ALS,
    override val fittingParamMap: ParamMap,
    k: Int,
    userFactors: RDD[(Int, Array[Float])],
    itemFactors: RDD[(Int, Array[Float])])
  extends Model[ALSModel] with ALSParams {

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    import dataset.sqlContext._
    import org.apache.spark.ml.recommendation.ALSModel.Factor
    val map = this.paramMap ++ paramMap
    // TODO: Add DSL to simplify the code here.
    val instanceTable = s"instance_$uid"
    val userTable = s"user_$uid"
    val itemTable = s"item_$uid"
    val instances = dataset.as(Symbol(instanceTable))
    val users = userFactors.map { case (id, features) =>
      Factor(id, features)
    }.as(Symbol(userTable))
    val items = itemFactors.map { case (id, features) =>
      Factor(id, features)
    }.as(Symbol(itemTable))
    val predict: (Seq[Float], Seq[Float]) => Float = (userFeatures, itemFeatures) => {
      if (userFeatures != null && itemFeatures != null) {
        blas.sdot(k, userFeatures.toArray, 1, itemFeatures.toArray, 1)
      } else {
        Float.NaN
      }
    }
    val inputColumns = dataset.schema.fieldNames
    val prediction =
      predict.call(s"$userTable.features".attr, s"$itemTable.features".attr) as map(predictionCol)
    val outputColumns = inputColumns.map(f => s"$instanceTable.$f".attr as f) :+ prediction
    instances
      .join(users, LeftOuter, Some(map(userCol).attr === s"$userTable.id".attr))
      .join(items, LeftOuter, Some(map(itemCol).attr === s"$itemTable.id".attr))
      .select(outputColumns: _*)
  }

  override private[ml] def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap)
  }
}

private object ALSModel {
  /** Case class to convert factors to SchemaRDDs */
  private case class Factor(id: Int, features: Seq[Float])
}

/**
 * Alternating least squares (ALS).
 */
class ALS extends Estimator[ALSModel] with ALSParams {

  import ALS.Rating

  def setRank(value: Int): this.type = set(rank, value)
  def setNumUserBlocks(value: Int): this.type = set(numUserBlocks, value)
  def setNumItemBlocks(value: Int): this.type = set(numItemBlocks, value)
  def setImplicitPrefs(value: Boolean): this.type = set(implicitPrefs, value)
  def setAlpha(value: Double): this.type = set(alpha, value)
  def setUserCol(value: String): this.type = set(userCol, value)
  def setItemCol(value: String): this.type = set(itemCol, value)
  def setRatingCol(value: String): this.type = set(ratingCol, value)
  def setPredictionCol(value: String): this.type = set(predictionCol, value)
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** Sets both numUserBlocks and numItemBlocks to the specific value. */
  def setNumBlocks(value: Int): this.type = {
    setNumUserBlocks(value)
    setNumItemBlocks(value)
    this
  }

  setMaxIter(20)
  setRegParam(1.0)

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): ALSModel = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    val ratings =
      dataset.select(map(userCol).attr, map(itemCol).attr, Cast(map(ratingCol).attr, FloatType))
        .map { row =>
          new Rating(row.getInt(0), row.getInt(1), row.getFloat(2))
        }
    val (userFactors, itemFactors) = ALS.train(ratings, rank = map(rank),
      numUserBlocks = map(numUserBlocks), numItemBlocks = map(numItemBlocks),
      maxIter = map(maxIter), regParam = map(regParam), implicitPrefs = map(implicitPrefs),
      alpha = map(alpha))
    val model = new ALSModel(this, map, map(rank), userFactors, itemFactors)
    Params.inheritValues(map, this, model)
    model
  }

  override private[ml] def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap)
  }
}

private object ALS extends Logging {

  /** Rating class for better code readability. */
  private case class Rating(user: Int, item: Int, rating: Float)

  /** Cholesky solver for least square problems. */
  private class CholeskySolver(val k: Int) {

    val upper = "U"
    val info = new intW(0)

    def solve(ne: NormalEquation, lambda: Double): Array[Float] = {
      val scaledlambda = lambda * ne.n
      var i = 0
      var j = 2
      while (i < ne.triK) {
        ne.ata(i) += scaledlambda
        i += j
        j += 1
      }
      lapack.dppsv(upper, k, 1, ne.ata, ne.atb, k, info)
      val code = info.`val`
      assert(code == 0, s"lapack.sppsv returned $code.")
      val x = new Array[Float](k)
      i = 0
      while (i < k) {
        x(i) = ne.atb(i).toFloat
        i += 1
      }
      x
    }
  }

  /** Representing a normal equation (ALS' subproblem). */
  private[recommendation] class NormalEquation(val k: Int) extends Serializable {

    val triK = k * (k + 1) / 2
    val ata = new Array[Double](triK)
    val atb = new Array[Double](k)
    var n = 0

    private val da = new Array[Double](k)
    private val upper = "U"

    private def copyToDouble(a: Array[Float]): Unit = {
      var i = 0
      while (i < k) {
        da(i) = a(i)
        i += 1
      }
    }

    def add(a: Array[Float], b: Float): this.type = {
      require(a.size == k)
      copyToDouble(a)
      blas.dspr(upper, k, 1.0, da, 1, ata)
      blas.daxpy(k, b.toDouble, da, 1, atb, 1)
      n += 1
      this
    }

    def addImplicit(a: Array[Float], b: Float, alpha: Double): this.type = {
      require(a.size == k)
      val confidence = 1.0 + alpha * math.abs(b)
      copyToDouble(a)
      blas.dspr(upper, k, confidence - 1.0, da, 1, ata)
      if (b > 0) {
        blas.daxpy(k, confidence, da, 1, atb, 1)
        n += 1
      }
      this
    }

    def merge(other: NormalEquation): this.type = {
      require(other.k == k)
      blas.daxpy(ata.size, 1.0, other.ata, 1, ata, 1)
      blas.daxpy(atb.size, 1.0, other.atb, 1, atb, 1)
      n += other.n
      this
    }

    def reset(): Unit = {
      javaUtil.Arrays.fill(ata, 0.0)
      javaUtil.Arrays.fill(atb, 0.0)
      n = 0
    }
  }

  /**
   * Implementation of the ALS algorithm.
   */
  private def train(
      ratings: RDD[Rating],
      rank: Int = 10,
      numUserBlocks: Int = 10,
      numItemBlocks: Int = 10,
      maxIter: Int = 10,
      regParam: Double = 1.0,
      implicitPrefs: Boolean = false,
      alpha: Double = 1.0): (RDD[(Int, Array[Float])], RDD[(Int, Array[Float])]) = {
    val userPart = new HashPartitioner(numUserBlocks)
    val itemPart = new HashPartitioner(numItemBlocks)
    val userLocalIndexEncoder = new LocalIndexEncoder(userPart.numPartitions)
    val itemLocalIndexEncoder = new LocalIndexEncoder(itemPart.numPartitions)
    val blockRatings = blockifyRatings(ratings, userPart, itemPart).cache()
    val (userInBlocks, userOutBlocks) = makeBlocks("user", blockRatings, userPart, itemPart)
    // materialize blockRatings and user blocks
    userOutBlocks.count()
    val swappedBlockRatings = blockRatings.map {
      case ((userBlockId, itemBlockId), RatingBlock(userIds, itemIds, localRatings)) =>
        ((itemBlockId, userBlockId), RatingBlock(itemIds, userIds, localRatings))
    }
    val (itemInBlocks, itemOutBlocks) = makeBlocks("item", swappedBlockRatings, itemPart, userPart)
    // materialize item blocks
    itemOutBlocks.count()
    var userFactors = initialize(userInBlocks, rank)
    var itemFactors = initialize(itemInBlocks, rank)
    if (implicitPrefs) {
      for (iter <- 1 to maxIter) {
        userFactors.setName(s"userFactors-$iter").persist()
        val YtY = Some(computeYtY(userFactors, rank))
        val previousItemFactors = itemFactors
        itemFactors = computeFactors(userFactors, userOutBlocks, itemInBlocks, rank, regParam,
          userLocalIndexEncoder, implicitPrefs, alpha, YtY)
        previousItemFactors.unpersist()
        itemFactors.setName(s"itemFactors-$iter").persist()
        val XtX = Some(computeYtY(itemFactors, rank))
        val previousUserFactors = userFactors
        userFactors = computeFactors(itemFactors, itemOutBlocks, userInBlocks, rank, regParam,
          itemLocalIndexEncoder, implicitPrefs, alpha, XtX)
        previousUserFactors.unpersist()
      }
    } else {
      for (iter <- 0 until maxIter) {
        itemFactors = computeFactors(userFactors, userOutBlocks, itemInBlocks, rank, regParam,
          userLocalIndexEncoder)
        userFactors = computeFactors(itemFactors, itemOutBlocks, userInBlocks, rank, regParam,
          itemLocalIndexEncoder)
      }
    }
    val userIdAndFactors = userInBlocks
      .mapValues(_.srcIds)
      .join(userFactors)
      .values
      .setName("userFactors")
      .cache()
    userIdAndFactors.count()
    itemFactors.unpersist()
    val itemIdAndFactors = itemInBlocks
      .mapValues(_.srcIds)
      .join(itemFactors)
      .values
      .setName("itemFactors")
      .cache()
    itemIdAndFactors.count()
    userInBlocks.unpersist()
    userOutBlocks.unpersist()
    itemInBlocks.unpersist()
    itemOutBlocks.unpersist()
    blockRatings.unpersist()
    val userOutput = userIdAndFactors.flatMap { case (ids, factors) =>
      ids.view.zip(factors)
    }
    val itemOutput = itemIdAndFactors.flatMap { case (ids, factors) =>
      ids.view.zip(factors)
    }
    (userOutput, itemOutput)
  }

  /**
   * Factor block that stores factors (Array[Float]) in an Array.
   */
  private type FactorBlock = Array[Array[Float]]

  /**
   * Out block that stores, for each dst block, which src factors to send.
   * For example, outBlock(0) contains the indices of the src factors to send to dst block 0.
   */
  private type OutBlock = Array[Array[Int]]

  /**
   * In block for computing src factors.
   *
   * For each src id, it stores its associated dst block ids and local indices, and ratings.
   * So given the dst factors, it is easy to compute src factors one by one.
   * We use compressed sparse column (CSC) format.
   *
   * @param srcIds src ids (ordered)
   * @param dstPtrs dst pointers. Elements in range [dstPtrs(i), dstPtrs(i+1)) of dst indices and
   *                ratings are associated with srcIds(i).
   * @param dstEncodedIndices encoded dst indices
   * @param ratings ratings
   */
  private case class InBlock(
      srcIds: Array[Int],
      dstPtrs: Array[Int],
      dstEncodedIndices: Array[Int],
      ratings: Array[Float])

  /**
   * Initializes factors randomly given the in blocks.
   *
   * @param inBlocks in blocks
   * @param k rank
   * @return initialized factor blocks
   */
  private def initialize(inBlocks: RDD[(Int, InBlock)], k: Int): RDD[(Int, FactorBlock)] = {
    inBlocks.map { case (srcBlockId, inBlock) =>
      val random = new XORShiftRandom(srcBlockId)
      val factors = Array.fill(inBlock.srcIds.size) {
        val factor = Array.fill(k)(random.nextGaussian().toFloat)
        val nrm = blas.snrm2(k, factor, 1)
        blas.sscal(k, 1.0f / nrm, factor, 1)
        factor
      }
      (srcBlockId, factors)
    }
  }

  /**
   * A rating block that contains src ids, dst ids, and ratings.
   *
   * @param srcIds src ids
   * @param dstIds dst ids
   * @param ratings ratings
   */
  private case class RatingBlock(srcIds: Array[Int], dstIds: Array[Int], ratings: Array[Float])

  /**
   * Builder for [[RatingBlock]].
   */
  private class RatingBlockBuilder extends Serializable {

    private val srcIds = mutable.ArrayBuilder.make[Int]
    private val dstIds = mutable.ArrayBuilder.make[Int]
    private val ratings = mutable.ArrayBuilder.make[Float]
    var size = 0

    def add(r: Rating): this.type = {
      size += 1
      srcIds += r.user
      dstIds += r.item
      ratings += r.rating
      this
    }

    def merge(other: RatingBlock): this.type = {
      size += other.srcIds.size
      srcIds ++= other.srcIds
      dstIds ++= other.dstIds
      ratings ++= other.ratings
      this
    }

    def toRatingBlock: RatingBlock = {
      RatingBlock(srcIds.result(), dstIds.result(), ratings.result())
    }
  }

  /**
   * Blockifies raw ratings.
   */
  private def blockifyRatings(
      ratings: RDD[Rating],
      srcPart: Partitioner,
      dstPart: Partitioner): RDD[((Int, Int), RatingBlock)] = {
    val numPartitions = srcPart.numPartitions * dstPart.numPartitions
    ratings.mapPartitions { iter =>
      val blocks = Array.fill(numPartitions)(new RatingBlockBuilder)
      iter.flatMap { r =>
        val srcBlockId = srcPart.getPartition(r.user)
        val dstBlockId = dstPart.getPartition(r.item)
        val idx = srcBlockId + srcPart.numPartitions * dstBlockId
        val block = blocks(idx)
        block.add(r)
        if (block.size >= 2048) { // 2048 * (3 * 4) = 24k
          blocks(idx) = new RatingBlockBuilder
          Iterator.single(((srcBlockId, dstBlockId), block.toRatingBlock))
        } else {
          Iterator.empty
        }
      } ++ {
        blocks.view.zipWithIndex.filter(_._1.size > 0).map { case (block, idx) =>
          val srcBlockId = idx % srcPart.numPartitions
          val dstBlockId = idx / srcPart.numPartitions
          ((srcBlockId, dstBlockId), block.toRatingBlock)
        }
      }
    }.groupByKey().mapValues { iter =>
      val buffered = new RatingBlockBuilder
      iter.foreach(buffered.merge)
      buffered.toRatingBlock
    }.setName("blockRatings")
  }

  /**
   * Builder for blocks of (srcId, dstEncodedIndex, rating) tuples.
   * @param encoder
   */
  private class UncompressedBlockBuilder(encoder: LocalIndexEncoder) {

    val srcIds = mutable.ArrayBuilder.make[Int]
    val dstEncodedIndices = mutable.ArrayBuilder.make[Int]
    val ratings = mutable.ArrayBuilder.make[Float]

    def add(
        theDstBlockId: Int,
        theSrcIds: Array[Int],
        theDstLocalIndices: Array[Int],
        theRatings: Array[Float]): this.type = {
      val sz = theSrcIds.size
      require(theDstLocalIndices.size == sz)
      require(theRatings.size == sz)
      srcIds ++= theSrcIds
      ratings ++= theRatings
      var j = 0
      while (j < sz) {
        dstEncodedIndices += encoder.encode(theDstBlockId, theDstLocalIndices(j))
        j += 1
      }
      this
    }

    def build(): UncompressedBlock = {
      new UncompressedBlock(srcIds.result(), dstEncodedIndices.result(), ratings.result())
    }
  }

  /**
   * A block of (srcId, dstEncodedIndex, rating) tuples.
   */
  private class UncompressedBlock(
      val srcIds: Array[Int],
      val dstEncodedIndices: Array[Int],
      val ratings: Array[Float]) {

    def size: Int = srcIds.size

    def compress(): InBlock = {
      val sz = size
      assert(sz > 0)
      sort()
      val uniqueSrcIdsBuilder = mutable.ArrayBuilder.make[Int]
      val dstCountsBuilder = mutable.ArrayBuilder.make[Int]
      var preSrcId = srcIds(0)
      uniqueSrcIdsBuilder += preSrcId
      var curCount = 1
      var i = 1
      var j = 0
      while (i < sz) {
        val srcId = srcIds(i)
        if (srcId != preSrcId) {
          uniqueSrcIdsBuilder += srcId
          dstCountsBuilder += curCount
          preSrcId = srcId
          j += 1
          curCount = 0
        }
        curCount += 1
        i += 1
      }
      dstCountsBuilder += curCount
      val uniqueSrcIds = uniqueSrcIdsBuilder.result()
      val numUniqueSrdIds = uniqueSrcIds.size
      val dstCounts = dstCountsBuilder.result()
      val dstPtrs = new Array[Int](numUniqueSrdIds + 1)
      var sum = 0
      i = 0
      while (i < numUniqueSrdIds) {
        sum += dstCounts(i)
        i += 1
        dstPtrs(i) = sum
      }
      InBlock(uniqueSrcIds, dstPtrs, dstEncodedIndices, ratings)
    }

    private def sort(): Unit = {
      val sz = size
      logDebug(s"Sorting uncompressed block of size $sz.")
      val start = System.nanoTime()
      val sorter = new Sorter(new UncompressedBlockSort)
      sorter.sort(this, 0, size, Ordering[IntWrapper])
      logDebug("Sorting took " + (System.nanoTime() - start) / 1e9 + " seconds.")
    }
  }

  private class IntWrapper(var key: Int = 0) extends Ordered[IntWrapper] {
    override def compare(that: IntWrapper): Int = {
      key.compare(that.key)
    }
  }

  private class UncompressedBlockSort extends SortDataFormat[IntWrapper, UncompressedBlock] {

    override def newKey(): IntWrapper = new IntWrapper()

    override def getKey(
        data: UncompressedBlock,
        pos: Int,
        reuse: IntWrapper): IntWrapper = {
      if (reuse == null) {
        new IntWrapper(data.srcIds(pos))
      } else {
        reuse.key = data.srcIds(pos)
        reuse
      }
    }

    override def getKey(
        data: UncompressedBlock,
        pos: Int): IntWrapper = {
      getKey(data, pos, null)
    }

    private def swapElements[@specialized(Int, Float) T](
        data: Array[T],
        pos0: Int,
        pos1: Int): Unit = {
      val tmp = data(pos0)
      data(pos0) = data(pos1)
      data(pos1) = tmp
    }

    override def swap(data: UncompressedBlock, pos0: Int, pos1: Int): Unit = {
      swapElements(data.srcIds, pos0, pos1)
      swapElements(data.dstEncodedIndices, pos0, pos1)
      swapElements(data.ratings, pos0, pos1)
    }

    override def copyRange(
        src: UncompressedBlock,
        srcPos: Int,
        dst: UncompressedBlock,
        dstPos: Int,
        length: Int): Unit = {
      System.arraycopy(src.srcIds, srcPos, dst.srcIds, dstPos, length)
      System.arraycopy(src.dstEncodedIndices, srcPos, dst.dstEncodedIndices, dstPos, length)
      System.arraycopy(src.ratings, srcPos, dst.ratings, dstPos, length)
    }

    override def allocate(length: Int): UncompressedBlock = {
      new UncompressedBlock(
        new Array[Int](length), new Array[Int](length), new Array[Float](length))
    }

    override def copyElement(
        src: UncompressedBlock,
        srcPos: Int,
        dst: UncompressedBlock,
        dstPos: Int): Unit = {
      dst.srcIds(dstPos) = src.srcIds(srcPos)
      dst.dstEncodedIndices(dstPos) = src.dstEncodedIndices(srcPos)
      dst.ratings(dstPos) = src.ratings(srcPos)
    }
  }

  private def makeBlocks(
      prefix: String,
      ratingBlocks: RDD[((Int, Int), RatingBlock)],
      srcPart: Partitioner,
      dstPart: Partitioner): (RDD[(Int, InBlock)], RDD[(Int, OutBlock)]) = {
    val inBlocks = ratingBlocks.map {
      case ((srcBlockId, dstBlockId), RatingBlock(srcIds, dstIds, ratings)) =>
        // faster version of
        // val dstIdToLocalIndex = dstIds.toSet.toSeq.sorted.zipWithIndex.toMap
        val start = System.nanoTime()
        val dstIdSet = new OpenHashSet[Int](1 << 20)
        dstIds.foreach(dstIdSet.add)
        val sortedDstIds = new Array[Int](dstIdSet.size)
        var i = 0
        var pos = dstIdSet.nextPos(0)
        while (pos != -1) {
          sortedDstIds(i) = dstIdSet.getValue(pos)
          pos = dstIdSet.nextPos(pos + 1)
          i += 1
        }
        assert(i == dstIdSet.size)
        javaUtil.Arrays.sort(sortedDstIds)
        val dstIdToLocalIndex = new OpenHashMap[Int, Int](sortedDstIds.size)
        i = 0
        while (i < sortedDstIds.size) {
          dstIdToLocalIndex.update(sortedDstIds(i), i)
          i += 1
        }
        logDebug(
          "Converting to local indices took " + (System.nanoTime() - start) / 1e9 + "seconds.")
        val dstLocalIndices = dstIds.map(dstIdToLocalIndex.apply)
        (srcBlockId, (dstBlockId, srcIds, dstLocalIndices, ratings))
    }.groupByKey(new HashPartitioner(srcPart.numPartitions))
        .mapValues { iter =>
      val uncompressedBlockBuilder =
        new UncompressedBlockBuilder(new LocalIndexEncoder(dstPart.numPartitions))
      iter.foreach { case (dstBlockId, srcIds, dstLocalIndices, ratings) =>
        uncompressedBlockBuilder.add(dstBlockId, srcIds, dstLocalIndices, ratings)
      }
      uncompressedBlockBuilder.build().compress()
    }.setName(prefix + "InBlocks").cache()
    val outBlocks = inBlocks.mapValues { case InBlock(srcIds, dstPtrs, dstEncodedIndices, _) =>
      val encoder = new LocalIndexEncoder(dstPart.numPartitions)
      val activeIds = Array.fill(dstPart.numPartitions)(mutable.ArrayBuilder.make[Int])
      var i = 0
      val seen = new Array[Boolean](dstPart.numPartitions)
      while (i < srcIds.size) {
        var j = dstPtrs(i)
        javaUtil.Arrays.fill(seen, false)
        while (j < dstPtrs(i + 1)) {
          val dstBlockId = encoder.blockId(dstEncodedIndices(j))
          if (!seen(dstBlockId)) {
            activeIds(dstBlockId) += i // add the local index
            seen(dstBlockId) = true
          }
          j += 1
        }
        i += 1
      }
      activeIds.map { x =>
        x.result()
      }
    }.setName(prefix + "OutBlocks").cache()
    (inBlocks, outBlocks)
  }

  private def computeFactors(
      srcFactorBlocks: RDD[(Int, FactorBlock)],
      srcOutBlocks: RDD[(Int, OutBlock)],
      dstInBlocks: RDD[(Int, InBlock)],
      k: Int,
      lambda: Double,
      srcEncoder: LocalIndexEncoder,
      implicitPrefs: Boolean = false,
      alpha: Double = 1.0,
      YtY: Option[NormalEquation] = None): RDD[(Int, FactorBlock)] = {
    val srcOut = srcOutBlocks.join(srcFactorBlocks).flatMap {
      case (srcBlockId, (srcOutBlock, srcFactors)) =>
        srcOutBlock.view.zipWithIndex.map { case (activeIndices, dstBlockId) =>
          (dstBlockId, (srcBlockId, activeIndices.map(idx => srcFactors(idx))))
        }
    }
    val merged = srcOut.groupByKey(new HashPartitioner(dstInBlocks.partitions.size))
    dstInBlocks.join(merged).mapValues {
      case (InBlock(dstIds, srcPtrs, srcEncodedIndices, ratings), srcFactors) =>
        val sortedSrcFactors = srcFactors.toSeq.sortBy(_._1).map(_._2).toArray
        val dstFactors = new Array[Array[Float]](dstIds.size)
        var j = 0
        val ls = new NormalEquation(k)
        val solver = new CholeskySolver(k)
        while (j < dstIds.size) {
          ls.reset()
          if (implicitPrefs) {
            ls.merge(YtY.get)
          }
          var i = srcPtrs(j)
          while (i < srcPtrs(j + 1)) {
            val encoded = srcEncodedIndices(i)
            val blockId = srcEncoder.blockId(encoded)
            val localIndex = srcEncoder.localIndex(encoded)
            val srcFactor = sortedSrcFactors(blockId)(localIndex)
            val rating = ratings(i)
            if (implicitPrefs) {
              ls.addImplicit(srcFactor, rating, alpha)
            } else {
              ls.add(srcFactor, rating)
            }
            i += 1
          }
          dstFactors(j) = solver.solve(ls, lambda)
          j += 1
        }
        dstFactors
    }
  }

  private def computeYtY(factorBlocks: RDD[(Int, FactorBlock)], k: Int): NormalEquation = {
    factorBlocks.values.aggregate(new NormalEquation(k))(
      seqOp = (ne, factors) => {
        factors.foreach(ne.add(_, 0.0f))
        ne
      },
      combOp = (ne1, ne2) => ne1.merge(ne2))
  }

  /**
   * Encoder for storing (blockId, localIndex) into a single integer.
   *
   * We use the leading bits (including the sign bit) to store the block id and the rest to store
   * the local index. This is based on the assumption that users/items are approximately evenly
   * partitioned. With this assumption, we should be able to encode two billion distinct values.
   *
   * @param numBlocks number of blocks
   */
  private[recommendation] class LocalIndexEncoder(numBlocks: Int) extends Serializable {

    require(numBlocks > 0, s"numBlocks must be positive but found $numBlocks.")

    private[this] final val numLocalIndexBits =
      math.min(java.lang.Integer.numberOfLeadingZeros(numBlocks - 1), 31)
    private[this] final val localIndexMask = (1 << numLocalIndexBits) - 1

    def encode(blockId: Int, localIndex: Int): Int = {
      require(blockId < numBlocks)
      require((localIndex & ~localIndexMask) == 0)
      (blockId << numLocalIndexBits) | localIndex
    }

    @inline
    def blockId(encoded: Int): Int = {
      encoded >>> numLocalIndexBits
    }

    @inline
    def localIndex(encoded: Int): Int = {
      encoded & localIndexMask
    }
  }
}
