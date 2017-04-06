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

import java.util.Arrays

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag
import scala.util.Sorting

import org.apache.spark.Partitioner
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{OpenHashMap, OpenHashSet}

@DeveloperApi
private[recommendation] final case class RatingBlocks[ID](
  userIn: RDD[(Int, InBlock[ID])],
  userOut: RDD[(Int, OutBlock)],
  itemIn: RDD[(Int, InBlock[ID])],
  itemOut: RDD[(Int, OutBlock)]
)

@DeveloperApi
private[recommendation] object RatingBlocks extends RatingBlockMixin with Logging {

  @DeveloperApi
  def create[ID: ClassTag: Ordering](
      ratings: RDD[Rating[ID]],
      numUserBlocks: Int,
      numItemBlocks: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): RatingBlocks[ID] = {

    val userPartitioner = new ALSPartitioner(numUserBlocks)
    val itemPartitioner = new ALSPartitioner(numItemBlocks)

    val ratingsGrouped =
      groupRatingsByPartitionPair(ratings, userPartitioner, itemPartitioner)
        .persist(storageLevel)

    val (userInBlocks, userOutBlocks) =
      makeBlocks("user", ratingsGrouped, userPartitioner, itemPartitioner, storageLevel)

    // PLEASE_ADVISE(danielyli): Why is the following line needed?
    userOutBlocks.count()   // materialize `ratingsGrouped` and user blocks

    // Achieve the same result as calling
    //
    //     `groupRatingsByPartitionPair(ratings, itemPartitioner, userPartitioner)`
    //
    // by manually swapping the users and items in `ratingsGrouped`, since
    // `groupRatingsByPartitionPair` is an expensive operation.
    val ratingsGroupedSwapped = ratingsGrouped.map {
      case ((u, i), RatingBlock(uIds, iIds, r)) =>
        ((i, u), RatingBlock(iIds, uIds, r))
    }

    val (itemInBlocks, itemOutBlocks) =
      makeBlocks("item", ratingsGroupedSwapped, itemPartitioner, userPartitioner, storageLevel)

    // PLEASE_ADVISE(danielyli): Why is the following line needed?
    itemOutBlocks.count()   // materialize item blocks

    ratingsGrouped.unpersist()

    new RatingBlocks(userInBlocks, userOutBlocks, itemInBlocks, itemOutBlocks)
  }

  /**
   * Partitions raw ratings into blocks.
   *
   * @param ratings raw ratings
   * @param srcPart partitioner for src IDs
   * @param dstPart partitioner for dst IDs
   * @return an RDD of rating blocks in the form of ((srcBlockId, dstBlockId), ratingBlock)
   */
  private[this] def groupRatingsByPartitionPair[ID: ClassTag](
      ratings: RDD[Rating[ID]],
      userPartitioner: Partitioner,
      itemPartitioner: Partitioner
    ): RDD[((Int, Int), RatingBlock[ID])] = {

    /* The implementation produces the same result as the following but generates less objects.

    ratings.map { r =>
      ((userPartitioner.getPartition(r.user), itemPartitioner.getPartition(r.item)), r)
    }.aggregateByKey(new RatingBlock.Builder)(
        seqOp = (b, r) => b.add(r),
        combOp = (b0, b1) => b0.merge(b1.build()))
      .mapValues(_.build())
    */

    val numPartitions = userPartitioner.numPartitions * itemPartitioner.numPartitions
    ratings.mapPartitions { iter =>
      val builders = Array.fill(numPartitions)(new RatingBlock.Builder[ID])
      iter.flatMap { r =>
        val srcBlockId = userPartitioner.getPartition(r.user)
        val dstBlockId = itemPartitioner.getPartition(r.item)
        val idx = srcBlockId + userPartitioner.numPartitions * dstBlockId
        val builder = builders(idx)
        builder.add(r)
        if (builder.size >= 2048) { // 2048 * (3 * 4) = 24k
          builders(idx) = new RatingBlock.Builder
          Iterator.single(((srcBlockId, dstBlockId), builder.build()))
        } else {
          Iterator.empty
        }
      } ++ {
        builders.view.zipWithIndex.filter(_._1.size > 0).map { case (block, idx) =>
          val srcBlockId = idx % userPartitioner.numPartitions
          val dstBlockId = idx / userPartitioner.numPartitions
          ((srcBlockId, dstBlockId), block.build())
        }
      }
    }.groupByKey().mapValues { blocks =>
      val builder = new RatingBlock.Builder[ID]
      blocks.foreach(builder.merge)
      builder.build()
    }.setName("ratingBlocks")
  }

  /**
   * Creates in-blocks and out-blocks from rating blocks.
   *
   * @param prefix prefix for in/out-block names
   * @param ratingBlocks rating blocks
   * @param srcPartitioner partitioner for `src IDs`
   * @param dstPartitioner partitioner for `dst IDs`
   * @return (in-blocks, out-blocks)
   */
  private[this] def makeBlocks[ID: ClassTag](
      prefix: String,
      ratingBlocks: RDD[((Int, Int), RatingBlock[ID])],
      srcPart: Partitioner,
      dstPart: Partitioner,
      storageLevel: StorageLevel)(
      implicit srcOrd: Ordering[ID]): (RDD[(Int, InBlock[ID])], RDD[(Int, OutBlock)]) = {
    val inBlocks = ratingBlocks.map {
      case ((srcBlockId, dstBlockId), RatingBlock(srcIds, dstIds, ratings)) =>
        // The implementation is a faster version of
        // `val dstIdToLocalIndex = dstIds.toSet.toSeq.sorted.zipWithIndex.toMap`
        val start = System.nanoTime()
        val dstIdSet = new OpenHashSet[ID](1 << 20)
        dstIds.foreach(dstIdSet.add)
        val sortedDstIds = new Array[ID](dstIdSet.size)
        var i = 0
        var pos = dstIdSet.nextPos(0)
        while (pos != -1) {
          sortedDstIds(i) = dstIdSet.getValue(pos)
          pos = dstIdSet.nextPos(pos + 1)
          i += 1
        }
        assert(i == dstIdSet.size)
        Sorting.quickSort(sortedDstIds)
        val dstIdToLocalIndex = new OpenHashMap[ID, Int](sortedDstIds.length)
        i = 0
        while (i < sortedDstIds.length) {
          dstIdToLocalIndex.update(sortedDstIds(i), i)
          i += 1
        }
        logDebug(
          "Converting to local indices took " + (System.nanoTime() - start) / 1e9 + " seconds.")
        val dstLocalIndices = dstIds.map(dstIdToLocalIndex.apply)
        (srcBlockId, (dstBlockId, srcIds, dstLocalIndices, ratings))
    }.groupByKey(new ALSPartitioner(srcPart.numPartitions))
      .mapValues { iter =>
        val builder =
          new UncompressedInBlock.Builder[ID](new LocalIndexEncoder(dstPart.numPartitions))
        iter.foreach { case (dstBlockId, srcIds, dstLocalIndices, ratings) =>
          builder.add(dstBlockId, srcIds, dstLocalIndices, ratings)
        }
        builder.build().compress()
      }.setName(prefix + "InBlocks")
      .persist(storageLevel)
    val outBlocks = inBlocks.mapValues { case InBlock(srcIds, dstPtrs, dstEncodedIndices, _) =>
      val encoder = new LocalIndexEncoder(dstPart.numPartitions)
      val activeIds = Array.fill(dstPart.numPartitions)(ArrayBuilder.make[Int])
      var i = 0
      val seen = new Array[Boolean](dstPart.numPartitions)
      while (i < srcIds.length) {
        var j = dstPtrs(i)
        Arrays.fill(seen, false)
        while (j < dstPtrs(i + 1)) {
          val dstBlockId = encoder.blockId(dstEncodedIndices(j))
          if (!seen(dstBlockId)) {
            activeIds(dstBlockId) += i // add the local index in this out-block
            seen(dstBlockId) = true
          }
          j += 1
        }
        i += 1
      }
      activeIds.map { x =>
        x.result()
      }
    }.setName(prefix + "OutBlocks")
      .persist(storageLevel)
    (inBlocks, outBlocks)
  }

}
