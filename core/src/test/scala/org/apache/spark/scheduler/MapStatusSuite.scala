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

import scala.util.Random

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.BlockManagerId

class MapStatusSuite extends SparkFunSuite {

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
      val status = MapStatus(BlockManagerId("a", "b", 10), sizes)
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
    val sizes = Array.fill[Long](2001)(150L)
    val status = MapStatus(null, sizes)
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
    val status = MapStatus(loc, sizes)
    val status1 = compressAndDecompressMapStatus(status)
    assert(status1.isInstanceOf[HighlyCompressedMapStatus])
    assert(status1.location == loc)
    for (i <- 0 until 3000) {
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
}
