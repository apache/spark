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

/** :: DeveloperApi ::
 * Case class to hold the precomputed rating dependencies of each partition.
 *
 * This algorithm is based on the paper by Yunhong Zhou, et al., "Large-Scale Parallel
 * Collaborative Filtering for the Netflix Prize" (doi:10.1007/978-3-540-68880-8_32).
 * Notations used below are borrowed from the paper and provided for easy cross-reference.
 *
 * =Preshuffled Ratings (`userIn`, `itemIn`)=
 *
 * The ALS algorithm partitions the columns of the users factor matrix $U$ evenly
 * among Spark workers.  Since each column $u_i$ is calculated using the known
 * ratings $R(i, I^U_i)$ of the corresponding user $i$, and since the ratings
 * don't change across iterations, the ALS algorithm preshuffles the ratings to
 * the appropriate partitions, storing them in the RDD `userIn`.
 *
 * The ratings shuffled by item ID are computed similarly and stored in the RDD
 * `itemIn`.  Note that this means every rating is stored twice, once in `userIn`
 * and once in `itemIn`; this is a necessary tradeoff, because in general a rating
 * will not be on the same worker when partitioned by user as by item.
 *
 * See the Scaladoc for [[RatingBlocks.makeBlocks]] for specific implementation
 * details on how the shuffling is performed and how the resulting ratings are
 * encoded.
 *
 * =Precomputed Factor Matrix Dependencies (`userOut`, `itemOut`)=
 *
 * Also precomputed and stored in this class are each user's (and item's)
 * dependencies on the columns of the items (and users) factor matrix.
 *
 * Specifically, when calculating the users factor matrix $U$, since only the
 * columns of the items factor matrix that correspond to the items that each user
 * $i$ has rated (denoted by the indices $I^U_i$) are needed to calculate $u_i$,
 * we can avoid having to repeatedly copy the entire items factor matrix to each
 * worker later in the algorithm by precomputing these dependencies, storing them
 * in the RDD `userOut`.  The items' dependencies on the columns of the users
 * factor matrix is computed similarly and stored in the RDD `itemOut`.
 *
 * See the Scaladoc for [[RatingBlocks.makeBlocks]] for specific implementation
 * details on how the dependencies are calculated.
 *
 * @param userIn the known ratings, each preshuffled to the partition on which it
 * will be used when calculating the corresponding column of the user factor matrix
 * @param userOut the dependencies each user has on the items factor matrix
 * @param itemIn the known ratings, each preshuffled to the partition on which it
 * will be used when calculating the corresponding column of the item factor matrix
 * @param itemOut the dependencies each item has on the users factor matrix
 *
 * @see [[ALS.train]]
 * @see [[ALS.computeFactors]]
 */
@DeveloperApi
private[recommendation] final case class RatingBlocks[ID] private (
  userIn: RDD[(Int, InBlock[ID])],
  userOut: RDD[(Int, OutBlock)],
  itemIn: RDD[(Int, InBlock[ID])],
  itemOut: RDD[(Int, OutBlock)]
)

@DeveloperApi
private[recommendation] object RatingBlocks extends RatingBlockMixin with Logging {

  /** :: DeveloperApi ::
   * Calculates the precomputed rating dependencies of each partition.  See the Scaladoc
   * for the [[RatingBlocks]] class for details.
   *
   * @param ratings the known ratings users have given
   * @param numUserBlocks the number of Spark partitions that will be used to store the
   * users factor matrix.  Must not exceed the total number of partitions used by the
   * application.
   * @param numUserBlocks the number of Spark partitions that will be used to store the
   * items factor matrix.  Must not exceed the total number of partitions used by the
   * application.
   * @param storageLevel the storage level to use to persist intermediate RDDs generated
   * during execution of this method.  It's best to leave this value at its default.
   * All intermediate RDDs are unpersisted before this method returns.
   * @return a `RatingBlocks` object to be used by the ALS algorithm
   */
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
   * Groups an RDD of `Rating`s by the user partition and item partition to which
   * each `Rating` maps according to the given partitioners.  The returned pair RDD
   * holds the ratings, encoded in a memory-efficient format but otherwise unchanged,
   * keyed by the (user partition ID, item partition ID) pair.
   *
   * Performance note: This is an expensive operation that performs an RDD shuffle.
   *
   * Implementation note: This implementation produces the same result as the
   * following but generates fewer intermediate objects:
   *
   * {{{
   *     ratings.map { r =>
   *       ((userPartitioner.getPartition(r.user), itemPartitioner.getPartition(r.item)), r)
   *     }.aggregateByKey(new RatingBlock.Builder)(
   *         seqOp = (b, r) => b.add(r),
   *         combOp = (b0, b1) => b0.merge(b1.build())
   *     ).mapValues(_.build())
   * }}}
   *
   * @param ratings the ratings to shuffle
   * @param userPartitioner the partitioner for users
   * @param itemPartitioner the partitioner for items
   * @return an RDD of the ratings, encoded as `RatingBlock`s and keyed by the (user partition ID,
   * item partition ID) pair to which the user and item partitioners map each rating
   */
  private[this] def groupRatingsByPartitionPair[ID: ClassTag](
      ratings: RDD[Rating[ID]],
      userPartitioner: Partitioner,
      itemPartitioner: Partitioner
    ): RDD[((Int, Int), RatingBlock[ID])] = {

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
      srcPartitioner: Partitioner,
      dstPartitioner: Partitioner,
      storageLevel: StorageLevel)(
      implicit srcOrd: Ordering[ID]): (RDD[(Int, InBlock[ID])], RDD[(Int, OutBlock)]) = {

    val encoder = new LocalIndexEncoder(dstPartitioner.numPartitions)

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
        logDebug("Converting to local indices took " +
          (System.nanoTime() - start) / 1e9 + " seconds.")
        val dstLocalIndices = dstIds.map(dstIdToLocalIndex.apply)
        (srcBlockId, (dstBlockId, srcIds, dstLocalIndices, ratings))
    }.groupByKey(
      srcPartitioner
    ).mapValues { iter =>
      val builder = new UncompressedInBlock.Builder[ID](encoder)
      iter.foreach((builder.add _).tupled)
      builder.build().compress()
    }.setName(
      prefix + "InBlocks"
    ).persist(storageLevel)

    val outBlocks = inBlocks.mapValues {
      case InBlock(srcIds, dstPtrs, dstEncodedIndices, _) =>
        val activeIds = Array.fill(dstPartitioner.numPartitions)(ArrayBuilder.make[Int])
        val seen = new Array[Boolean](dstPartitioner.numPartitions)

        for (i <- 0 until srcIds.length) {
          Arrays.fill(seen, false)

          for (j <- dstPtrs(i) until dstPtrs(i + 1)) {
            val b = encoder.blockId(dstEncodedIndices(j))

            if (!seen(b)) {
              activeIds(b) += i
              seen(b) = true
            }
          }
        }

        activeIds.map(_.result)
    }.setName(
      prefix + "OutBlocks"
    ).persist(storageLevel)

    (inBlocks, outBlocks)
  }

}
