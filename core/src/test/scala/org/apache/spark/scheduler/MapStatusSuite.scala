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

import org.apache.spark.storage.BlockManagerId
import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer


class MapStatusSuite extends FunSuite {

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

  test("large tasks should use " + classOf[HighlyCompressedMapStatus].getName) {
    val sizes = Array.fill[Long](2001)(150L)
    val status = MapStatus(null, sizes)
    assert(status.isInstanceOf[HighlyCompressedMapStatus])
    assert(status.getSizeForBlock(10) === 150L)
    assert(status.getSizeForBlock(50) === 150L)
    assert(status.getSizeForBlock(99) === 150L)
    assert(status.getSizeForBlock(2000) === 150L)
  }

  test(classOf[HighlyCompressedMapStatus].getName + ": estimated size is within 10%") {
    val sizes = Array.tabulate[Long](50) { i => i.toLong }
    val loc = BlockManagerId("a", "b", 10)
    val status = MapStatus(loc, sizes)
    val ser = new JavaSerializer(new SparkConf)
    val buf = ser.newInstance().serialize(status)
    val status1 = ser.newInstance().deserialize[MapStatus](buf)
    assert(status1.location == loc)
    for (i <- 0 until sizes.length) {
      // make sure the estimated size is within 10% of the input; note that we skip the very small
      // sizes because the compression is very lossy there.
      val estimate = status1.getSizeForBlock(i)
      if (estimate > 100) {
        assert(math.abs(estimate - sizes(i)) * 10 <= sizes(i),
          s"incorrect estimated size $estimate, original was ${sizes(i)}")
      }
    }
  }

  test(classOf[HighlyCompressedMapStatus].getName + ": estimated size should be the average size") {
    val sizes = Array.tabulate[Long](3000) { i => i.toLong }
    val avg = sizes.sum / sizes.length
    val loc = BlockManagerId("a", "b", 10)
    val status = MapStatus(loc, sizes)
    val ser = new JavaSerializer(new SparkConf)
    val buf = ser.newInstance().serialize(status)
    val status1 = ser.newInstance().deserialize[MapStatus](buf)
    assert(status1.location == loc)
    for (i <- 0 until 3000) {
      val estimate = status1.getSizeForBlock(i)
      assert(estimate === avg)
    }
  }
}
