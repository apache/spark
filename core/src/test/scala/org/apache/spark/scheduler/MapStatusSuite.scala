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

package org.apache.spark.scheduler

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.util.Random

import org.mockito.Mockito.mock
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.LocalSparkContext._
import org.apache.spark.internal.config
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.Utils.createArray

class MapStatusSuite extends SparkFunSuite {
  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  test("compressSize") {
    assert(MapStatus.compressSize(0L) === 0)
    assert(MapStatus.compressSize(1L) === 1)
    assert(MapStatus.compressSize(2L) === 8)
    assert(MapStatus.compressSize(10L) === 25)
    assert((MapStatus.compressSize(1000000L) & 0xFF) === 145)
    assert((MapStatus.compressSize(1000000000L) & 0xFF) === 218)
    // This last size is bigger than we can encode in a byte, so check that we just return 255
    assert((MapStatus.compressSize(1000000000000000000L) & 0xFF) === 255)
  }

  test("decompressSize") {
    assert(MapStatus.decompressSize(0) === 0)
    for (size <- Seq(2L, 10L, 100L, 50000L, 1000000L, 1000000000L)) {
      val size2 = MapStatus.decompressSize(MapStatus.compressSize(size))
      assert(size2 >= 0.99 * size && size2 <= 1.11 * size,
        "size " + size + " decompressed to " + size2 + ", which is out of range")
    }
  }

  test("MapStatus should never report non-empty blocks' sizes as 0") {
    import Math._
    for (
      numSizes <- Seq(1, 10, 100, 1000, 10000);
      mean <- Seq(0L, 100L, 10000L, Int.MaxValue.toLong);
      stddev <- Seq(0.0, 0.01, 0.5, 1.0)
    ) {
      val sizes = Array.fill[Long](numSizes)(abs(round(Random.nextGaussian() * stddev)) + mean)
      val status = MapStatus(BlockManagerId("a", "b", 10), sizes, -1)
      val status1 = compressAndDecompressMapStatus(status)
      for (i <- 0 until numSizes) {
        if (sizes(i) != 0) {
          val failureMessage = s"Failed with $numSizes sizes with mean=$mean, stddev=$stddev"
          assert(status.getSizeForBlock(i) !== 0, failureMessage)
          assert(status1.getSizeForBlock(i) !== 0, failureMessage)
        }
      }
    }
  }

  test("large tasks should use " + classOf[HighlyCompressedMapStatus].getName) {
    val sizes = createArray(2001, 150L)
    val status = MapStatus(null, sizes, -1)
    assert(status.isInstanceOf[HighlyCompressedMapStatus])
    assert(status.getSizeForBlock(10) === 150L)
    assert(status.getSizeForBlock(50) === 150L)
    assert(status.getSizeForBlock(99) === 150L)
    assert(status.getSizeForBlock(2000) === 150L)
  }

  test("HighlyCompressedMapStatus: estimated size should be the average non-empty block size") {
    val sizes = Array.tabulate[Long](3000) { i => i.toLong }
    val avg = sizes.sum / sizes.count(_ != 0)
    val loc = BlockManagerId("a", "b", 10)
    val mapTaskAttemptId = 5
    val status = MapStatus(loc, sizes, mapTaskAttemptId)
    val status1 = compressAndDecompressMapStatus(status)
    assert(status1.isInstanceOf[HighlyCompressedMapStatus])
    assert(status1.location == loc)
    assert(status1.mapId == mapTaskAttemptId)
    for (i <- 0 until 3000) {
      val estimate = status1.getSizeForBlock(i)
      if (sizes(i) > 0) {
        assert(estimate === avg)
      }
    }
  }

  test("SPARK-22540: ensure HighlyCompressedMapStatus calculates correct avgSize") {
    val threshold = 1000
    val conf = new SparkConf().set(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD.key, threshold.toString)
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)
    val sizes = (0L to 3000L).toArray
    val smallBlockSizes = sizes.filter(n => n > 0 && n < threshold)
    val avg = smallBlockSizes.sum / smallBlockSizes.length
    val loc = BlockManagerId("a", "b", 10)
    val status = MapStatus(loc, sizes, 5)
    val status1 = compressAndDecompressMapStatus(status)
    assert(status1.isInstanceOf[HighlyCompressedMapStatus])
    assert(status1.location == loc)
    for (i <- 0 until threshold) {
      val estimate = status1.getSizeForBlock(i)
      if (sizes(i) > 0) {
        assert(estimate === avg)
      }
    }
  }

  def compressAndDecompressMapStatus(status: MapStatus): MapStatus = {
    val ser = new JavaSerializer(new SparkConf)
    val buf = ser.newInstance().serialize(status)
    ser.newInstance().deserialize[MapStatus](buf)
  }

  test("RoaringBitmap: runOptimize succeeded") {
    val r = new RoaringBitmap
    (1 to 200000).foreach(i =>
      if (i % 200 != 0) {
        r.add(i)
      }
    )
    val size1 = r.getSizeInBytes
    val success = r.runOptimize()
    r.trim()
    val size2 = r.getSizeInBytes
    assert(size1 > size2)
    assert(success)
  }

  test("RoaringBitmap: runOptimize failed") {
    val r = new RoaringBitmap
    (1 to 200000).foreach(i =>
      if (i % 200 == 0) {
        r.add(i)
      }
    )
    val size1 = r.getSizeInBytes
    val success = r.runOptimize()
    r.trim()
    val size2 = r.getSizeInBytes
    assert(size1 === size2)
    assert(!success)
  }

  test("Blocks which are bigger than SHUFFLE_ACCURATE_BLOCK_THRESHOLD should not be " +
    "underestimated.") {
    val conf = new SparkConf().set(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD.key, "1000")
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)
    // Value of element in sizes is equal to the corresponding index.
    val sizes = (0L to 2000L).toArray
    val status1 = MapStatus(BlockManagerId("exec-0", "host-0", 100), sizes, 5)
    val arrayStream = new ByteArrayOutputStream(102400)
    val objectOutputStream = new ObjectOutputStream(arrayStream)
    assert(status1.isInstanceOf[HighlyCompressedMapStatus])
    objectOutputStream.writeObject(status1)
    objectOutputStream.flush()
    val array = arrayStream.toByteArray
    val objectInput = new ObjectInputStream(new ByteArrayInputStream(array))
    val status2 = objectInput.readObject().asInstanceOf[HighlyCompressedMapStatus]
    (1001 to 2000).foreach {
      case part => assert(status2.getSizeForBlock(part) >= sizes(part))
    }
  }

  test("SPARK-21133 HighlyCompressedMapStatus#writeExternal throws NPE") {
    val conf = new SparkConf()
      .set(config.SERIALIZER, classOf[KryoSerializer].getName)
      .setMaster("local")
      .setAppName("SPARK-21133")
    withSpark(new SparkContext(conf)) { sc =>
      val count = sc.parallelize(0 until 3000, 10).repartition(2001).collect().length
      assert(count === 3000)
    }
  }

  def compressAndDecompressSize(size: Long): Long = {
    MapStatus.decompressSize(MapStatus.compressSize(size))
  }

  test("SPARK-36967: HighlyCompressedMapStatus should record accurately the size " +
    "of skewed shuffle blocks") {
    val emptyBlocksLength = 3
    val smallAndUntrackedBlocksLength = 2889
    val trackedSkewedBlocksLength = 20

    val conf = new SparkConf().set(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR.key, "5")
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)

    val emptyBlocks = createArray(emptyBlocksLength, 0L)
    val smallAndUntrackedBlocks = Array.tabulate[Long](smallAndUntrackedBlocksLength)(i => i)
    val trackedSkewedBlocks =
      Array.tabulate[Long](trackedSkewedBlocksLength)(i => i + 350 * 1024)
    val allBlocks = emptyBlocks ++: smallAndUntrackedBlocks ++: trackedSkewedBlocks
    val avg = smallAndUntrackedBlocks.sum / smallAndUntrackedBlocks.length
    val loc = BlockManagerId("a", "b", 10)
    val mapTaskAttemptId = 5
    val status = MapStatus(loc, allBlocks, mapTaskAttemptId)
    val status1 = compressAndDecompressMapStatus(status)
    assert(status1.isInstanceOf[HighlyCompressedMapStatus])
    assert(status1.location == loc)
    assert(status1.mapId == mapTaskAttemptId)
    assert(status1.getSizeForBlock(0) == 0)
    for (i <- 1 until emptyBlocksLength) {
      assert(status1.getSizeForBlock(i) === 0L)
    }
    for (i <- 1 until smallAndUntrackedBlocksLength) {
      assert(status1.getSizeForBlock(emptyBlocksLength + i) === avg)
    }
    for (i <- 0 until trackedSkewedBlocksLength) {
      assert(status1.getSizeForBlock(emptyBlocksLength + smallAndUntrackedBlocksLength + i) ===
        compressAndDecompressSize(trackedSkewedBlocks(i)),
        "Only tracked skewed block size is accurate")
    }
  }

  test("SPARK-36967: Limit accurate skewed block number if too many blocks are skewed") {
    val accurateBlockSkewedFactor = 5
    val emptyBlocksLength = 3
    val smallBlocksLength = 2500
    val untrackedSkewedBlocksLength = 500
    val trackedSkewedBlocksLength = 20

    val conf =
      new SparkConf()
        .set(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR.key, accurateBlockSkewedFactor.toString)
        .set(
          config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER.key,
          trackedSkewedBlocksLength.toString)
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)

    val emptyBlocks = createArray(emptyBlocksLength, 0L)
    val smallBlockSizes = Array.tabulate[Long](smallBlocksLength)(i => i + 1)
    val untrackedSkewedBlocksSizes =
      Array.tabulate[Long](untrackedSkewedBlocksLength)(i => i + 3500 * 1024)
    val trackedSkewedBlocksSizes =
      Array.tabulate[Long](trackedSkewedBlocksLength)(i => i + 4500 * 1024)
    val nonEmptyBlocks =
      smallBlockSizes ++: untrackedSkewedBlocksSizes ++: trackedSkewedBlocksSizes
    val allBlocks = emptyBlocks ++: nonEmptyBlocks

    val skewThreshold = Utils.median(allBlocks, false) * accurateBlockSkewedFactor
    assert(nonEmptyBlocks.count(_ > skewThreshold) ==
      untrackedSkewedBlocksLength + trackedSkewedBlocksLength,
      "number of skewed block sizes")

    val smallAndUntrackedBlocks =
      nonEmptyBlocks.slice(0, nonEmptyBlocks.length - trackedSkewedBlocksLength)
    val avg = smallAndUntrackedBlocks.sum / smallAndUntrackedBlocks.length

    val loc = BlockManagerId("a", "b", 10)
    val mapTaskAttemptId = 5
    val status = MapStatus(loc, allBlocks, mapTaskAttemptId)
    val status1 = compressAndDecompressMapStatus(status)
    assert(status1.isInstanceOf[HighlyCompressedMapStatus])
    assert(status1.location == loc)
    assert(status1.mapId == mapTaskAttemptId)
    assert(status1.getSizeForBlock(0) == 0)
    for (i <- emptyBlocksLength until allBlocks.length - trackedSkewedBlocksLength) {
      assert(status1.getSizeForBlock(i) === avg)
    }
    for (i <- 0 until trackedSkewedBlocksLength) {
      assert(status1.getSizeForBlock(allBlocks.length - trackedSkewedBlocksLength + i) ===
        compressAndDecompressSize(trackedSkewedBlocksSizes(i)),
        "Only tracked skewed block size is accurate")
    }
  }

  test("SPARK-48290: skewed blocks are recorded accurately with the default configuration") {
    val emptyBlocksLength = 3
    val smallBlocksLength = 3000
    val skewedBlocksLength = 5
    // Well above the median block size, but below SHUFFLE_ACCURATE_BLOCK_THRESHOLD, which is the
    // case for a skewed partition whose rows are spread over a large number of map tasks.
    val skewedBlockSize = 50 * 1024 * 1024L

    // No skew related config is set: this asserts the out of the box behavior.
    val conf = new SparkConf()
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)

    val emptyBlocks = createArray(emptyBlocksLength, 0L)
    val smallBlocks = Array.tabulate[Long](smallBlocksLength)(i => i + 1)
    val skewedBlocks = createArray(skewedBlocksLength, skewedBlockSize)
    val allBlocks = emptyBlocks ++: smallBlocks ++: skewedBlocks
    assert(skewedBlockSize < conf.get(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD),
      "the skewed blocks must not be tracked as huge blocks")
    val avg = smallBlocks.sum / smallBlocks.length

    val loc = BlockManagerId("a", "b", 10)
    val mapTaskAttemptId = 5
    val status = compressAndDecompressMapStatus(MapStatus(loc, allBlocks, mapTaskAttemptId))
    assert(status.isInstanceOf[HighlyCompressedMapStatus])
    for (i <- 0 until emptyBlocksLength) {
      assert(status.getSizeForBlock(i) === 0L)
    }
    for (i <- 0 until smallBlocksLength) {
      assert(status.getSizeForBlock(emptyBlocksLength + i) === avg,
        "the average size must not be inflated by the skewed blocks")
    }
    for (i <- 0 until skewedBlocksLength) {
      assert(status.getSizeForBlock(emptyBlocksLength + smallBlocksLength + i) ===
        compressAndDecompressSize(skewedBlockSize),
        "skewed block sizes must be accurate so that AQE can detect the skew")
    }
  }

  /**
   * Asserts that every block tied at the cutoff size is recorded, and that recording them costs no
   * more than the block id bitmap this status has always carried.
   *
   * All of the tied blocks share one size, so recording them costs a bitmap and that single size,
   * rather than an id and a size per block, and the number of them does not have to be capped.
   * What a bitmap costs depends on where the ids fall, not on how many there are, so the caller
   * chooses the layout: a contiguous run of tied reducers compresses into a single run container,
   * while alternating ids are the densest thing a bitmap can be asked to hold and degenerate into
   * a fixed size container. Neither may cost more than a bit per reduce partition, which is what
   * `emptyBlocks` has always cost for the same layout, so both are asserted against a status that
   * holds those very ids as empty blocks instead.
   */
  private def assertTiedBlocksAreRecordedCompactly(interleaveTiedBlocks: Boolean): Unit = {
    // The small blocks are the majority, so the median block size is smallBlockSize.
    val smallBlocksLength = 25001
    val tiedBlocksLength = 24999
    val numBlocks = smallBlocksLength + tiedBlocksLength
    val smallBlockSize = 1024L
    // Far more blocks share this size than the accurate block number would allow to be recorded
    // individually, and it is the cutoff size itself: it is above the median times the skew
    // factor, so the skew threshold is exactly this size.
    val tiedBlockSize = 8 * 1024L
    // A RoaringBitmap over ids below 65536 holds one container, and a container denser than 4096
    // ids becomes a fixed 8 KiB bitmap, so no layout of these ids costs more than that. The rest
    // of the status is a location, two short huge block arrays and a handful of scalars.
    val maxRetainedBytes = 8 * 1024 + 1024
    // Recording the ties may not cost materially more than holding the same ids as empty blocks.
    val maxRetainedBytesOverEmptyBlocks = 512

    // No skew related config is set: this asserts the out of the box behavior.
    val conf = new SparkConf()
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)

    val allBlocks = new Array[Long](numBlocks)
    var numTiedBlocksPlaced = 0
    for (i <- 0 until numBlocks) {
      val isTied = if (interleaveTiedBlocks) {
        i % 2 == 1 && numTiedBlocksPlaced < tiedBlocksLength
      } else {
        i >= smallBlocksLength
      }
      if (isTied) {
        allBlocks(i) = tiedBlockSize
        numTiedBlocksPlaced += 1
      } else {
        allBlocks(i) = smallBlockSize
      }
    }
    assert(numTiedBlocksPlaced === tiedBlocksLength)
    assert(tiedBlockSize < conf.get(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD),
      "the tied blocks must not be recorded as huge blocks")
    assert(Utils.median(allBlocks, false) *
      conf.get(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR) < tiedBlockSize,
      "the cutoff size, not the median times the skew factor, must set the skew threshold")

    val loc = BlockManagerId("a", "b", 10)
    val serializer = new JavaSerializer(new SparkConf).newInstance()
    val serialized = serializer.serialize(MapStatus(loc, allBlocks, 5L))
    val serializedBytes = serialized.remaining()
    val status = serializer.deserialize[MapStatus](serialized)
    assert(status.isInstanceOf[HighlyCompressedMapStatus])
    for (i <- 0 until numBlocks) {
      if (allBlocks(i) == tiedBlockSize) {
        assert(status.getSizeForBlock(i) === compressAndDecompressSize(tiedBlockSize),
          "every tied block must be recorded so that AQE sees the skew")
      } else {
        assert(status.getSizeForBlock(i) === smallBlockSize,
          "the average size must not be inflated by the tied blocks")
      }
    }

    // The same reduce ids, held as empty blocks rather than as ties. This status records no skew
    // at all, so what it retains is the cost of one bitmap over this layout and nothing else.
    val emptyBlocks = allBlocks.map(size => if (size == tiedBlockSize) 0L else size)
    val statusWithEmptyBlocks =
      compressAndDecompressMapStatus(MapStatus(loc, emptyBlocks, 5L))
    val retainedByEmptyBlocks = SizeEstimator.estimate(statusWithEmptyBlocks.asInstanceOf[AnyRef])

    val retained = SizeEstimator.estimate(status.asInstanceOf[AnyRef])
    assert(retained < maxRetainedBytes,
      s"recording $tiedBlocksLength tied blocks retained $retained bytes")
    assert(serializedBytes < maxRetainedBytes,
      s"recording $tiedBlocksLength tied blocks serialized to $serializedBytes bytes")
    assert(retained - retainedByEmptyBlocks < maxRetainedBytesOverEmptyBlocks,
      s"recording $tiedBlocksLength tied blocks retained $retained bytes, against " +
        s"$retainedByEmptyBlocks bytes for a status holding the same ids as empty blocks")
  }

  test("SPARK-48290: every block tied at the cutoff size is recorded, at a bounded cost") {
    assertTiedBlocksAreRecordedCompactly(interleaveTiedBlocks = false)
    assertTiedBlocksAreRecordedCompactly(interleaveTiedBlocks = true)
  }

  /**
   * Asserts that a stage whose skewed reducers are all the same size stays visible to AQE.
   *
   * The sizes a reducer is reported to have are what `MapOutputTracker.getStatistics` sums out of
   * the map statuses, and AQE splits a partition when that sum exceeds
   * `max(spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes,
   * spark.sql.adaptive.skewJoin.skewedPartitionFactor * medianPartitionSize)`. Both sides of that
   * comparison come out of the map statuses, so asserting on the recorded sizes alone can pass
   * while the skew stays invisible: a skewed block that is not recorded falls back to `avgSize`,
   * which raises the median, which raises the threshold.
   */
  private def assertSkewIsVisibleToAqe(
      numSmallBlocks: Int,
      smallBlockSize: Long,
      numSkewedBlocks: Int,
      skewedBlockSize: Long,
      numMapTasks: Int): Unit = {
    // Defaults of the two AQE configs named above. This suite is in core and cannot read them.
    val aqeSkewedPartitionThreshold = 256 * 1024 * 1024L
    val aqeSkewedPartitionFactor = 5

    // No skew related config is set: this asserts the out of the box behavior.
    val conf = new SparkConf()
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)

    val numBlocks = numSmallBlocks + numSkewedBlocks
    val allBlocks = createArray(numSmallBlocks, smallBlockSize) ++:
      createArray(numSkewedBlocks, skewedBlockSize)
    assert(skewedBlockSize < conf.get(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD),
      "the skewed blocks must not be recorded as huge blocks")
    val loc = BlockManagerId("a", "b", 10)
    val statuses = (0 until numMapTasks).map { mapTaskId =>
      compressAndDecompressMapStatus(MapStatus(loc, allBlocks, mapTaskId))
    }
    assert(statuses.head.isInstanceOf[HighlyCompressedMapStatus],
      s"this distribution must exercise HighlyCompressedMapStatus, got " +
        statuses.head.getClass.getSimpleName)

    val reportedSizes = (0 until numBlocks).map(i => statuses.map(_.getSizeForBlock(i)).sum)
    val medianReportedSize = reportedSizes.sorted.apply(numBlocks / 2)
    val skewThreshold = Math.max(aqeSkewedPartitionThreshold,
      aqeSkewedPartitionFactor * medianReportedSize)
    val realSize = numMapTasks * skewedBlockSize
    assert(realSize > skewThreshold,
      "the test itself is wrong if the skewed reducers do not exceed the threshold in reality")
    for (i <- numSmallBlocks until numBlocks) {
      assert(reportedSizes(i) > skewThreshold,
        s"reducer $i holds $realSize bytes but is reported as ${reportedSizes(i)}, below the " +
          s"AQE skew threshold of $skewThreshold, so it would never be split")
    }
  }

  test("SPARK-48290: skewed reducers stay visible to AQE when they outnumber the accurate " +
    "block limit") {
    val maxAccurateSkewedBlockNumber =
      config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER.defaultValue.get

    // Every map task sees the same block size distribution, so any rule that records only some of
    // the blocks tied at the cutoff hides the same reducers in every map status. The cases below
    // cover the two ways that shows up: too few map tasks for a rotation to reach every tied
    // reducer, and enough map tasks to reach all of them but never all at once.
    assertSkewIsVisibleToAqe(
      numSmallBlocks = 1881,
      smallBlockSize = 5 * 1024 * 1024L,
      numSkewedBlocks = maxAccurateSkewedBlockNumber + 20,
      skewedBlockSize = 50 * 1024 * 1024L,
      numMapTasks = 10)
    assertSkewIsVisibleToAqe(
      numSmallBlocks = 1801,
      smallBlockSize = 10 * 1024L,
      numSkewedBlocks = 2 * maxAccurateSkewedBlockNumber,
      skewedBlockSize = 100 * 1024L,
      numMapTasks = 10000)
    // One more tied reducer than may be recorded individually, and far more map tasks than tied
    // reducers, which is the case a rotation covers most easily.
    assertSkewIsVisibleToAqe(
      numSmallBlocks = 1900,
      smallBlockSize = 10 * 1024L,
      numSkewedBlocks = maxAccurateSkewedBlockNumber + 1,
      skewedBlockSize = 100 * 1024L,
      numMapTasks = 5000)
  }

  test("SPARK-48290: recorded skewed block sizes are held in compact storage") {
    // The driver retains one map status per map task for the lifetime of the shuffle, so the
    // accurately recorded sizes must not cost more than a few bytes each. A mutable.Map[Int, Byte]
    // costs an order of magnitude more, through its nodes, bucket array and boxed reduce ids.
    // MapStatus.apply only selects HighlyCompressedMapStatus above
    // spark.shuffle.minNumPartitionsToHighlyCompress, so a status built out of exactly that many
    // blocks would be a CompressedMapStatus, which ignores the skew factor entirely and would let
    // this test pass without ever exercising the storage it is about.
    val smallBlocksLength = 1901
    val skewedBlocksLength = 100
    val smallBlockSize = 10 * 1024L
    val skewedBlockSize = 100 * 1024L
    val maxRetainedBytesPerBlock = 16

    val allBlocks = createArray(smallBlocksLength, smallBlockSize) ++:
      createArray(skewedBlocksLength, skewedBlockSize)
    assert(allBlocks.length >
      new SparkConf().get(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS),
      "the status under test must be a HighlyCompressedMapStatus")
    val loc = BlockManagerId("a", "b", 10)

    def retainedBytes(accurateBlockSkewedFactor: Double): Long = {
      val conf = new SparkConf()
        .set(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR.key, accurateBlockSkewedFactor.toString)
      val env = mock(classOf[SparkEnv])
      doReturn(conf).when(env).conf
      SparkEnv.set(env)
      val status = compressAndDecompressMapStatus(MapStatus(loc, allBlocks, 5))
      assert(status.isInstanceOf[HighlyCompressedMapStatus],
        s"expected a HighlyCompressedMapStatus, got ${status.getClass.getSimpleName}")
      val numAccurate = (smallBlocksLength until allBlocks.length)
        .count(status.getSizeForBlock(_) === compressAndDecompressSize(skewedBlockSize))
      assert(numAccurate === (if (accurateBlockSkewedFactor > 0) skewedBlocksLength else 0),
        "the skewed sizes must actually be recorded, or this measures nothing")
      SizeEstimator.estimate(status.asInstanceOf[AnyRef])
    }

    val withoutSkewedSizes = retainedBytes(-1.0)
    val withSkewedSizes = retainedBytes(5.0)
    val perBlock = (withSkewedSizes - withoutSkewedSizes).toDouble / skewedBlocksLength
    assert(perBlock < maxRetainedBytesPerBlock,
      s"recording $skewedBlocksLength skewed block sizes retained $perBlock bytes each")
  }
}
