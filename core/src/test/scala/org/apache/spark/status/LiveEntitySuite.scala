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

package org.apache.spark.status

import java.util.Arrays

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{AccumulatorMetadata, CollectionAccumulator}

class LiveEntitySuite extends SparkFunSuite {

  test("partition seq") {
    val seq = new RDDPartitionSeq()
    val items = (1 to 10).map { i =>
      val part = newPartition(i)
      seq.addPartition(part)
      part
    }.toList

    checkSize(seq, 10)

    val added = newPartition(11)
    seq.addPartition(added)
    checkSize(seq, 11)
    assert(seq.last.blockName === added.blockName)

    seq.removePartition(items(0))
    assert(seq.head.blockName === items(1).blockName)
    assert(!seq.exists(_.blockName == items(0).blockName))
    checkSize(seq, 10)

    seq.removePartition(added)
    assert(seq.last.blockName === items.last.blockName)
    assert(!seq.exists(_.blockName == added.blockName))
    checkSize(seq, 9)

    seq.removePartition(items(5))
    checkSize(seq, 8)
    assert(!seq.exists(_.blockName == items(5).blockName))
  }

  test("Only show few elements of CollectionAccumulator when converting to v1.AccumulableInfo") {
    val acc = new CollectionAccumulator[Int]()
    val value = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    acc.setValue(value)
    acc.metadata = AccumulatorMetadata(0L, None, false)
    val accuInfo = LiveEntityHelpers
      .newAccumulatorInfos(Seq(acc.toInfo(Some(acc.value), Some(acc.value))))(0)
    assert(accuInfo.update.get == "[1,2,3,4,5,... 5 more items]")
    assert(accuInfo.value == "[1,2,3,4,5,... 5 more items]")
  }

  private def checkSize(seq: Seq[_], expected: Int): Unit = {
    assert(seq.length === expected)
    var count = 0
    seq.iterator.foreach { _ => count += 1 }
    assert(count === expected)
  }

  private def newPartition(i: Int): LiveRDDPartition = {
    val part = new LiveRDDPartition(i.toString, StorageLevel.MEMORY_AND_DISK)
    part.update(Seq(i.toString), i, i)
    part
  }

}
