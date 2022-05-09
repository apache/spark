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

package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{MapOutputStatistics, MapOutputTrackerMaster, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, EmptyPartitionSpec, PartialReducerPartitionSpec, ShufflePartitionSpec}

object ShufflePartitionsUtil extends Logging {
  final val SMALL_PARTITION_FACTOR = 0.2
  final val MERGED_PARTITION_FACTOR = 1.2

  /**
   * Coalesce the partitions from multiple shuffles, either in their original states, or applied
   * with skew handling partition specs. If called on partitions containing skew partition specs,
   * this method will keep the skew partition specs intact and only coalesce the partitions outside
   * the skew sections.
   *
   * This method will return an empty result if the shuffles have been coalesced already, or if
   * they do not have the same number of partitions, or if the coalesced result is the same as the
   * input partition layout.
   *
   * @return A sequence of sequence of [[ShufflePartitionSpec]]s, which each inner sequence as the
   *         new partition specs for its corresponding shuffle after coalescing. If Nil is returned,
   *         then no coalescing is applied.
   */
  def coalescePartitions(
      mapOutputStatistics: Seq[Option[MapOutputStatistics]],
      inputPartitionSpecs: Seq[Option[Seq[ShufflePartitionSpec]]],
      advisoryTargetSize: Long,
      minNumPartitions: Int,
      minPartitionSize: Long): Seq[Seq[ShufflePartitionSpec]] = {
    assert(mapOutputStatistics.length == inputPartitionSpecs.length)

    if (mapOutputStatistics.isEmpty) {
      return Seq.empty
    }

    // If `minNumPartitions` is very large, it is possible that we need to use a value less than
    // `advisoryTargetSize` as the target size of a coalesced task.
    val totalPostShuffleInputSize = mapOutputStatistics.flatMap(_.map(_.bytesByPartitionId.sum)).sum
    val maxTargetSize = math.ceil(totalPostShuffleInputSize / minNumPartitions.toDouble).toLong
    // It's meaningless to make target size smaller than minPartitionSize.
    val targetSize = maxTargetSize.min(advisoryTargetSize).max(minPartitionSize)

    val shuffleIds = mapOutputStatistics.flatMap(_.map(_.shuffleId)).mkString(", ")
    logInfo(s"For shuffle($shuffleIds), advisory target size: $advisoryTargetSize, " +
      s"actual target size $targetSize, minimum partition size: $minPartitionSize")

    // If `inputPartitionSpecs` are all empty, it means skew join optimization or
    // empty partition propagation is not applied.
    if (inputPartitionSpecs.forall(_.isEmpty)) {
      coalescePartitionsWithoutSkew(
        mapOutputStatistics, targetSize, minPartitionSize)
    } else {
      coalescePartitionsWithEmpty(
        mapOutputStatistics, inputPartitionSpecs, targetSize, minPartitionSize)
    }
  }

  private def coalescePartitionsWithoutSkew(
      mapOutputStatistics: Seq[Option[MapOutputStatistics]],
      targetSize: Long,
      minPartitionSize: Long): Seq[Seq[ShufflePartitionSpec]] = {
    // `ShuffleQueryStageExec#mapStats` returns None when the input RDD has 0 partitions,
    // we should skip it when calculating the `partitionStartIndices`.
    val validMetrics = mapOutputStatistics.flatten
    val numShuffles = mapOutputStatistics.length
    // If all input RDDs have 0 partition, we create an empty partition for every shuffle read.
    if (validMetrics.isEmpty) {
      return Seq.fill(numShuffles)(Seq(CoalescedPartitionSpec(0, 0, 0)))
    }

    // We may have different pre-shuffle partition numbers, don't reduce shuffle partition number
    // in that case. For example when we union fully aggregated data (data is arranged to a single
    // partition) and a result of a SortMergeJoin (multiple partitions).
    if (validMetrics.map(_.bytesByPartitionId.length).distinct.length > 1) {
      return Seq.empty
    }

    val numPartitions = validMetrics.head.bytesByPartitionId.length
    val newPartitionSpecs = coalescePartitions(
      0, numPartitions, validMetrics, targetSize, minPartitionSize)
    if (newPartitionSpecs.length < numPartitions) {
      attachDataSize(mapOutputStatistics, newPartitionSpecs)
    } else {
      Seq.empty
    }
  }

  private def coalescePartitionsWithEmpty(
      mapOutputStatistics: Seq[Option[MapOutputStatistics]],
      inputPartitionSpecs: Seq[Option[Seq[ShufflePartitionSpec]]],
      targetSize: Long,
      minPartitionSize: Long): Seq[Seq[ShufflePartitionSpec]] = {
    // Do not coalesce if any of the map output stats are missing or if not all shuffles have
    // partition specs, which should not happen in practice.
    if (!mapOutputStatistics.forall(_.isDefined) || !inputPartitionSpecs.forall(_.isDefined)) {
      logWarning("Could not apply partition coalescing because of missing MapOutputStatistics " +
        "or shuffle partition specs.")
      return Seq.empty
    }

    val emptyIndexSet = collection.mutable.Set.empty[Int]
    inputPartitionSpecs.foreach(_.get.iterator.zipWithIndex.foreach {
      case (EmptyPartitionSpec, i) => emptyIndexSet.add(i)
      case _ =>
    })

    if (emptyIndexSet.isEmpty) {
      return coalescePartitionsWithSkew(mapOutputStatistics, inputPartitionSpecs,
        targetSize, minPartitionSize, true)
    }

    // when all partitions are empty, return single EmptyPartitionSpec here to satisfy
    // SPARK-32083 (AQE coalesce should at least return one partition).
    if (inputPartitionSpecs.flatten.flatten.forall(_ == EmptyPartitionSpec)) {
      return inputPartitionSpecs.map(_ => Seq(EmptyPartitionSpec))
    }

    // ShufflePartitionSpecs at these emptyIndices can NOT be coalesced
    // split inputPartitionSpecs into sub-sequences by the empty indices, and
    // call coalescePartitionsWithSkew to optimize each sub-sequence.
    // let inputPartitionSpecs are:
    //   [A0(empty), A1, A2, A3(empty), A4(empty), A5, A6, A7, A8, A9, A10]
    //   [B0, B1, B2, B3, B4(empty), B5, B6, B7, B8(empty), B9, B10]
    // then:
    // 1, specs at index (0, 3, 8) are kept: (A0(empty)-B0), (A3(empty)-B3), (A8-B8(empty))
    // 2, specs at index 4 are discarded, since they are all empty: (A4(empty)-B4(empty))
    // 3, sub-sequences [A1-B1, A2-B2], [A5-B5, A6-B6, A7-B7], [A9-B9, A10-B10] are optimized

    val emptyIndices = emptyIndexSet.toArray.sorted
    val newSpecsSeq = Seq.fill(mapOutputStatistics.length)(ArrayBuffer.empty[ShufflePartitionSpec])
    var coalesced = false

    val firstEmptyIdx = emptyIndices(0)
    if (firstEmptyIdx > 0) {
      // coalesce specs before the first empty spec
      val firstSpecs = inputPartitionSpecs.map(_.map(_.take(firstEmptyIdx)))
      val partiallyCoalesced = coalescePartitionsWithSkew(
        mapOutputStatistics, firstSpecs, targetSize, minPartitionSize, true)
      if (partiallyCoalesced.nonEmpty) {
        newSpecsSeq.zip(partiallyCoalesced).foreach(t => t._1 ++= t._2)
        coalesced = true
      } else {
        newSpecsSeq.zip(firstSpecs).foreach(t => t._1 ++= t._2.get)
      }
    }
    val specsAtEmptyIdx = inputPartitionSpecs.map(_.get(firstEmptyIdx))
    if (specsAtEmptyIdx.forall(_ == EmptyPartitionSpec)) {
      // if all specs at an empty index are EmptyPartitionSpec, discard them
      coalesced = true
    } else {
      newSpecsSeq.zip(specsAtEmptyIdx).foreach(t => t._1 += t._2)
    }

    var i = 1
    while (i < emptyIndices.length) {
      val prevEmptyIdx = emptyIndices(i - 1)
      val currEmptyIdx = emptyIndices(i)
      if (prevEmptyIdx < currEmptyIdx - 1) {
        val slicedSpecs = inputPartitionSpecs.map(_.map(_.slice(prevEmptyIdx + 1, currEmptyIdx)))
        val partiallyCoalesced = coalescePartitionsWithSkew(
          mapOutputStatistics, slicedSpecs, targetSize, minPartitionSize, false)
        if (partiallyCoalesced.nonEmpty) {
          newSpecsSeq.zip(partiallyCoalesced).foreach(t => t._1 ++= t._2)
          coalesced = true
        } else {
          newSpecsSeq.zip(slicedSpecs).foreach(t => t._1 ++= t._2.get)
        }
      }
      val specsAtEmptyIdx = inputPartitionSpecs.map(_.get(currEmptyIdx))
      if (specsAtEmptyIdx.forall(_ == EmptyPartitionSpec)) {
        // if all specs at an empty index are EmptyPartitionSpec, discard them
        coalesced = true
      } else {
        newSpecsSeq.zip(specsAtEmptyIdx).foreach(t => t._1 += t._2)
      }
      i += 1
    }

    val lastEmptyIdx = emptyIndices.last
    val numSpecs = inputPartitionSpecs.head.get.size
    if (lastEmptyIdx < numSpecs - 1) {
      // coalesce specs after the last empty spec
      val lastSpecs = inputPartitionSpecs.map(_.map(_.slice(lastEmptyIdx + 1, numSpecs)))
      val partiallyCoalesced = coalescePartitionsWithSkew(
        mapOutputStatistics, lastSpecs, targetSize, minPartitionSize, false)
      if (partiallyCoalesced.nonEmpty) {
        newSpecsSeq.zip(partiallyCoalesced).foreach(t => t._1 ++= t._2)
        coalesced = true
      } else {
        newSpecsSeq.zip(lastSpecs).foreach(t => t._1 ++= t._2.get)
      }
    }

    if (coalesced) {
      newSpecsSeq.map(_.toSeq)
    } else {
      Seq.empty
    }
  }

  private def coalescePartitionsWithSkew(
      mapOutputStatistics: Seq[Option[MapOutputStatistics]],
      inputPartitionSpecs: Seq[Option[Seq[ShufflePartitionSpec]]],
      targetSize: Long,
      minPartitionSize: Long,
      checkFirstIndex: Boolean): Seq[Seq[ShufflePartitionSpec]] = {
    val validMetrics = mapOutputStatistics.map(_.get)
    // Extract the start indices of each partition spec. Give invalid index -1 to unexpected
    // partition specs. When we reach here, it means skew join optimization has been applied.
    val partitionIndicesSeq = inputPartitionSpecs.map(_.get.map {
      case CoalescedPartitionSpec(start, end, _) if start + 1 == end => start
      case PartialReducerPartitionSpec(reducerId, _, _, _) => reducerId
      case _ => -1 // invalid
    })

    // There should be no unexpected partition specs and the start indices should be identical
    // across all different shuffles.
    assert(partitionIndicesSeq.distinct.length == 1 && partitionIndicesSeq.head.forall(_ >= 0),
      s"Invalid shuffle partition specs: $inputPartitionSpecs")

    // The indices may look like [0, 1, 2, 2, 2, 3, 4, 4, 5], and the repeated `2` and `4` mean
    // skewed partitions.
    val partitionIndices = partitionIndicesSeq.head
    if (checkFirstIndex) {
      assert(partitionIndices.head == 0)
    }
    val newPartitionSpecsSeq = Seq.fill(mapOutputStatistics.length)(
      ArrayBuffer.empty[ShufflePartitionSpec])
    val numPartitions = partitionIndices.length
    var i = 1
    var start = 0
    while (i < numPartitions) {
      if (partitionIndices(i - 1) == partitionIndices(i)) {
        // a skew section detected, starting from partition(i - 1).
        val repeatValue = partitionIndices(i)
        // coalesce any partitions before partition(i - 1) and after the end of latest skew section.
        if (i - 1 > start) {
          val partitionSpecs = coalescePartitions(
            partitionIndices(start),
            repeatValue,
            validMetrics,
            targetSize,
            minPartitionSize,
            allowReturnEmpty = true)
          newPartitionSpecsSeq.zip(attachDataSize(mapOutputStatistics, partitionSpecs))
            .foreach(spec => spec._1 ++= spec._2)
        }
        // find the end of this skew section, skipping partition(i - 1) and partition(i).
        var repeatIndex = i + 1
        while (repeatIndex < numPartitions && partitionIndices(repeatIndex) == repeatValue) {
          repeatIndex += 1
        }
        // copy the partition specs in the skew section to the new partition specs.
        newPartitionSpecsSeq.zip(inputPartitionSpecs).foreach { case (newSpecs, oldSpecs) =>
          newSpecs ++= oldSpecs.get.slice(i - 1, repeatIndex)
        }
        // start from after the skew section
        start = repeatIndex
        i = repeatIndex
      } else {
        // Indices outside of the skew section should be larger than the previous one by 1.
        assert(partitionIndices(i - 1) + 1 == partitionIndices(i))
        // no skew section detected, advance to the next index.
        i += 1
      }
    }
    // coalesce any partitions after the end of last skew section.
    if (numPartitions > start) {
      val partitionSpecs = coalescePartitions(
        partitionIndices(start),
        partitionIndices.last + 1,
        validMetrics,
        targetSize,
        minPartitionSize,
        allowReturnEmpty = true)
      newPartitionSpecsSeq.zip(attachDataSize(mapOutputStatistics, partitionSpecs))
        .foreach(spec => spec._1 ++= spec._2)
    }
    // only return coalesced result if any coalescing has happened.
    if (newPartitionSpecsSeq.head.length < numPartitions) {
      newPartitionSpecsSeq.map(_.toSeq)
    } else {
      Seq.empty
    }
  }

  /**
   * Coalesce the partitions of [start, end) from multiple shuffles. This method assumes that all
   * the shuffles have the same number of partitions, and the partitions of same index will be read
   * together by one task.
   *
   * The strategy used to determine the number of coalesced partitions is described as follows.
   * To determine the number of coalesced partitions, we have a target size for a coalesced
   * partition. Once we have size statistics of all shuffle partitions, we will do
   * a pass of those statistics and pack shuffle partitions with continuous indices to a single
   * coalesced partition until adding another shuffle partition would cause the size of a
   * coalesced partition to be greater than the target size.
   *
   * For example, we have two shuffles with the following partition size statistics:
   *  - shuffle 1 (5 partitions): [100 MiB, 20 MiB, 100 MiB, 10MiB, 30 MiB]
   *  - shuffle 2 (5 partitions): [10 MiB,  10 MiB, 70 MiB,  5 MiB, 5 MiB]
   * Assuming the target size is 128 MiB, we will have 4 coalesced partitions, which are:
   *  - coalesced partition 0: shuffle partition 0 (size 110 MiB)
   *  - coalesced partition 1: shuffle partition 1 (size 30 MiB)
   *  - coalesced partition 2: shuffle partition 2 (size 170 MiB)
   *  - coalesced partition 3: shuffle partition 3 and 4 (size 50 MiB)
   *
   *  @return A sequence of [[CoalescedPartitionSpec]]s. For example, if partitions [0, 1, 2, 3, 4]
   *          split at indices [0, 2, 3], the returned partition specs will be:
   *          CoalescedPartitionSpec(0, 2), CoalescedPartitionSpec(2, 3) and
   *          CoalescedPartitionSpec(3, 5).
   */
  private def coalescePartitions(
      start: Int,
      end: Int,
      mapOutputStatistics: Seq[MapOutputStatistics],
      targetSize: Long,
      minPartitionSize: Long,
      allowReturnEmpty: Boolean = false): Seq[CoalescedPartitionSpec] = {
    // `minPartitionSize` is useful for cases like [64MB, 0.5MB, 64MB]: we can't do coalesce,
    // because merging 0.5MB to either the left or right partition will exceed the target size.
    // If 0.5MB is smaller than `minPartitionSize`, we will force-merge it to the left/right side.
    val partitionSpecs = ArrayBuffer.empty[CoalescedPartitionSpec]
    var coalescedSize = 0L
    var i = start
    var latestSplitPoint = i
    var latestPartitionSize = 0L

    def createPartitionSpec(forceCreate: Boolean = false): Unit = {
      // Skip empty inputs, as it is a waste to launch an empty task.
      if (coalescedSize > 0 || forceCreate) {
        partitionSpecs += CoalescedPartitionSpec(latestSplitPoint, i)
      }
    }

    while (i < end) {
      // We calculate the total size of i-th shuffle partitions from all shuffles.
      var totalSizeOfCurrentPartition = 0L
      var j = 0
      while (j < mapOutputStatistics.length) {
        totalSizeOfCurrentPartition += mapOutputStatistics(j).bytesByPartitionId(i)
        j += 1
      }

      // If including the `totalSizeOfCurrentPartition` would exceed the target size and the
      // current size has reached the `minPartitionSize`, then start a new coalesced partition.
      if (i > latestSplitPoint && coalescedSize + totalSizeOfCurrentPartition > targetSize) {
        if (coalescedSize < minPartitionSize) {
          // the current partition size is below `minPartitionSize`.
          // pack it with the smaller one between the two adjacent partitions (before and after).
          if (latestPartitionSize > 0 && latestPartitionSize < totalSizeOfCurrentPartition) {
            // pack with the before partition.
            partitionSpecs(partitionSpecs.length - 1) =
              CoalescedPartitionSpec(partitionSpecs.last.startReducerIndex, i)
            latestSplitPoint = i
            latestPartitionSize += coalescedSize
            // reset postShuffleInputSize.
            coalescedSize = totalSizeOfCurrentPartition
          } else {
            // pack with the after partition.
            coalescedSize += totalSizeOfCurrentPartition
          }
        } else {
          createPartitionSpec()
          latestSplitPoint = i
          latestPartitionSize = coalescedSize
          // reset postShuffleInputSize.
          coalescedSize = totalSizeOfCurrentPartition
        }
      } else {
        coalescedSize += totalSizeOfCurrentPartition
      }
      i += 1
    }

    if (coalescedSize < minPartitionSize && latestPartitionSize > 0) {
      // pack with the last partition.
      partitionSpecs(partitionSpecs.length - 1) =
        CoalescedPartitionSpec(partitionSpecs.last.startReducerIndex, end)
    } else {
      // If do not allowReturnEmpty, create at least one partition if all partitions are empty.
      createPartitionSpec(!allowReturnEmpty && partitionSpecs.isEmpty)
    }
    partitionSpecs.toSeq
  }

  private def attachDataSize(
      mapOutputStatistics: Seq[Option[MapOutputStatistics]],
      partitionSpecs: Seq[CoalescedPartitionSpec]): Seq[Seq[CoalescedPartitionSpec]] = {
    mapOutputStatistics.map {
      case Some(mapStats) =>
        partitionSpecs.map { spec =>
          val dataSize = spec.startReducerIndex.until(spec.endReducerIndex)
            .map(mapStats.bytesByPartitionId).sum
          spec.copy(dataSize = Some(dataSize))
        }.toSeq
      case None => partitionSpecs.map(_.copy(dataSize = Some(0))).toSeq
    }.toSeq
  }

  /**
   * Given a list of size, return an array of indices to split the list into multiple partitions,
   * so that the size sum of each partition is close to the target size. Each index indicates the
   * start of a partition.
   */
  // Visible for testing
  private[sql] def splitSizeListByTargetSize(
      sizes: Array[Long],
      targetSize: Long,
      smallPartitionFactor: Double): Array[Int] = {
    val partitionStartIndices = ArrayBuffer[Int]()
    partitionStartIndices += 0
    var i = 0
    var currentPartitionSize = 0L
    var lastPartitionSize = -1L

    def tryMergePartitions() = {
      // When we are going to start a new partition, it's possible that the current partition or
      // the previous partition is very small and it's better to merge the current partition into
      // the previous partition.
      val shouldMergePartitions = lastPartitionSize > -1 &&
        ((currentPartitionSize + lastPartitionSize) < targetSize * MERGED_PARTITION_FACTOR ||
        (currentPartitionSize < targetSize * smallPartitionFactor ||
          lastPartitionSize < targetSize * smallPartitionFactor))
      if (shouldMergePartitions) {
        // We decide to merge the current partition into the previous one, so the start index of
        // the current partition should be removed.
        partitionStartIndices.remove(partitionStartIndices.length - 1)
        lastPartitionSize += currentPartitionSize
      } else {
        lastPartitionSize = currentPartitionSize
      }
    }

    while (i < sizes.length) {
      // If including the next size in the current partition exceeds the target size, package the
      // current partition and start a new partition.
      if (i > 0 && currentPartitionSize + sizes(i) > targetSize) {
        tryMergePartitions()
        partitionStartIndices += i
        currentPartitionSize = sizes(i)
      } else {
        currentPartitionSize += sizes(i)
      }
      i += 1
    }
    tryMergePartitions()
    partitionStartIndices.toArray
  }

  /**
   * Get the map size of the specific shuffle and reduce ID. Note that, some map outputs can be
   * missing due to issues like executor lost. The size will be -1 for missing map outputs and the
   * caller side should take care of it.
   */
  private def getMapSizesForReduceId(shuffleId: Int, partitionId: Int): Array[Long] = {
    val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTracker.shuffleStatuses(shuffleId).withMapStatuses(_.map { stat =>
      if (stat == null) -1 else stat.getSizeForBlock(partitionId)
    })
  }

  /**
   * Splits the skewed partition based on the map size and the target partition size
   * after split, and create a list of `PartialReducerPartitionSpec`. Returns None if can't split.
   */
  def createSkewPartitionSpecs(
      shuffleId: Int,
      reducerId: Int,
      targetSize: Long,
      smallPartitionFactor: Double = SMALL_PARTITION_FACTOR)
  : Option[Seq[PartialReducerPartitionSpec]] = {
    val mapPartitionSizes = getMapSizesForReduceId(shuffleId, reducerId)
    if (mapPartitionSizes.exists(_ < 0)) return None
    val mapStartIndices = splitSizeListByTargetSize(
      mapPartitionSizes, targetSize, smallPartitionFactor)
    if (mapStartIndices.length > 1) {
      Some(mapStartIndices.indices.map { i =>
        val startMapIndex = mapStartIndices(i)
        val endMapIndex = if (i == mapStartIndices.length - 1) {
          mapPartitionSizes.length
        } else {
          mapStartIndices(i + 1)
        }
        var dataSize = 0L
        var mapIndex = startMapIndex
        while (mapIndex < endMapIndex) {
          dataSize += mapPartitionSizes(mapIndex)
          mapIndex += 1
        }
        PartialReducerPartitionSpec(reducerId, startMapIndex, endMapIndex, dataSize)
      })
    } else {
      None
    }
  }
}
