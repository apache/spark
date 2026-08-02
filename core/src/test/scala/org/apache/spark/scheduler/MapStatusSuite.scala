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

  test("SPARK-48290: blocks tied at the cutoff size do not bypass the accurate skewed block " +
    "limit") {
    // The small blocks are the majority, so the median block size is smallBlockSize. Without the
    // limit, all of the tied blocks would be recorded, which for a stage of 10000 map tasks would
    // add more than a gigabyte of map status entries.
    val smallBlocksLength = 25001
    val tiedBlocksLength = 24999
    val smallBlockSize = 1024L
    // Far more blocks share this size than may be recorded, and it is the cutoff size itself:
    // it is above the median times the skew factor, so the skew threshold is exactly this size.
    val tiedBlockSize = 8 * 1024L
    val maxAccurateSkewedBlockNumber =
      config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER.defaultValue.get

    // No skew related config is set: this asserts the out of the box behavior.
    val conf = new SparkConf()
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)

    val allBlocks = createArray(smallBlocksLength, smallBlockSize) ++:
      createArray(tiedBlocksLength, tiedBlockSize)
    assert(tiedBlockSize < conf.get(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD),
      "the tied blocks must not be recorded as huge blocks")
    assert(Utils.median(allBlocks, false) *
      conf.get(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR) < tiedBlockSize,
      "the cutoff size, not the median times the skew factor, must set the skew threshold")
    val numSmallBlocks = allBlocks.length - maxAccurateSkewedBlockNumber
    val avg =
      (smallBlockSize * smallBlocksLength +
        tiedBlockSize * (tiedBlocksLength - maxAccurateSkewedBlockNumber)) / numSmallBlocks

    val loc = BlockManagerId("a", "b", 10)
    val mapTaskId = 5L
    val status = compressAndDecompressMapStatus(MapStatus(loc, allBlocks, mapTaskId))
    assert(status.isInstanceOf[HighlyCompressedMapStatus])
    for (i <- 0 until smallBlocksLength) {
      assert(status.getSizeForBlock(i) === avg)
    }
    // The recorded ties are a window of maxAccurateSkewedBlockNumber tied blocks, in block index
    // order, starting at the offset the map task id rotates to.
    val firstAccurateTie = (mapTaskId % tiedBlocksLength).toInt
    for (i <- 0 until tiedBlocksLength) {
      val isAccurate =
        i >= firstAccurateTie && i < firstAccurateTie + maxAccurateSkewedBlockNumber
      val expected = if (isAccurate) compressAndDecompressSize(tiedBlockSize) else avg
      assert(status.getSizeForBlock(smallBlocksLength + i) === expected,
        "no more than maxAccurateSkewedBlockNumber blocks may be recorded accurately")
    }
  }

  test("SPARK-48290: blocks tied at the cutoff size rotate across map tasks so that no reducer " +
    "is hidden") {
    // One more reducer is tied at the cutoff than may be recorded per map task. Every map task
    // sees the same distribution, so a fixed tie break would hide the same reducer in every map
    // status, and MapOutputTracker.getStatistics would report it as the average block size.
    val smallBlocksLength = 1900
    val tiedBlocksLength = 101
    val smallBlockSize = 10 * 1024L
    val tiedBlockSize = 100 * 1024L
    val numMapTasks = 2000
    val maxAccurateSkewedBlockNumber =
      config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER.defaultValue.get
    assert(tiedBlocksLength > maxAccurateSkewedBlockNumber)

    // No skew related config is set: this asserts the out of the box behavior.
    val conf = new SparkConf()
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)

    val allBlocks = createArray(smallBlocksLength, smallBlockSize) ++:
      createArray(tiedBlocksLength, tiedBlockSize)
    val loc = BlockManagerId("a", "b", 10)
    val statuses = (0 until numMapTasks).map { mapTaskId =>
      compressAndDecompressMapStatus(MapStatus(loc, allBlocks, mapTaskId))
    }

    val accurateSize = compressAndDecompressSize(tiedBlockSize)
    statuses.foreach { status =>
      val numAccurate =
        (smallBlocksLength until allBlocks.length).count(status.getSizeForBlock(_) == accurateSize)
      assert(numAccurate === maxAccurateSkewedBlockNumber,
        "the accurate skewed block limit must still hold for every map task")
    }

    // Summing a reducer over the map statuses is what AQE sees. Each tied reducer loses the tie on
    // one map task out of tiedBlocksLength, so its total is off by that fraction at worst, instead
    // of collapsing to the average block size.
    val realTotal = numMapTasks * accurateSize
    for (i <- smallBlocksLength until allBlocks.length) {
      val total = statuses.map(_.getSizeForBlock(i)).sum
      assert(total > realTotal * 0.98,
        s"reducer $i must not be hidden behind the average block size")
      assert(total <= realTotal)
    }
  }

  test("SPARK-48290: recorded skewed block sizes are held in compact storage") {
    // The driver retains one map status per map task for the lifetime of the shuffle, so the
    // accurately recorded sizes must not cost more than a few bytes each. A mutable.Map[Int, Byte]
    // costs an order of magnitude more, through its nodes, bucket array and boxed reduce ids.
    val smallBlocksLength = 1900
    val skewedBlocksLength = 100
    val smallBlockSize = 10 * 1024L
    val skewedBlockSize = 100 * 1024L
    val maxRetainedBytesPerBlock = 16

    val allBlocks = createArray(smallBlocksLength, smallBlockSize) ++:
      createArray(skewedBlocksLength, skewedBlockSize)
    val loc = BlockManagerId("a", "b", 10)

    def retainedBytes(accurateBlockSkewedFactor: Double): Long = {
      val conf = new SparkConf()
        .set(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR.key, accurateBlockSkewedFactor.toString)
      val env = mock(classOf[SparkEnv])
      doReturn(conf).when(env).conf
      SparkEnv.set(env)
      SizeEstimator.estimate(compressAndDecompressMapStatus(MapStatus(loc, allBlocks, 5)))
    }

    val withoutSkewedSizes = retainedBytes(-1.0)
    val withSkewedSizes = retainedBytes(5.0)
    val perBlock = (withSkewedSizes - withoutSkewedSizes).toDouble / skewedBlocksLength
    assert(perBlock < maxRetainedBytesPerBlock,
      s"recording $skewedBlocksLength skewed block sizes retained $perBlock bytes each")
  }
}
