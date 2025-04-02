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

package org.apache.spark.storage

import java.util.UUID

import org.apache.spark.SparkFunSuite

class BlockIdSuite extends SparkFunSuite {
  def assertSame(id1: BlockId, id2: BlockId): Unit = {
    assert(id1.name === id2.name)
    assert(id1.hashCode === id2.hashCode)
    assert(id1 === id2)
  }

  def assertDifferent(id1: BlockId, id2: BlockId): Unit = {
    assert(id1.name != id2.name)
    assert(id1.hashCode != id2.hashCode)
    assert(id1 != id2)
  }

  test("test-bad-deserialization") {
    intercept[UnrecognizedBlockId] {
      BlockId("myblock")
    }
  }

  test("rdd") {
    val id = RDDBlockId(1, 2)
    assertSame(id, RDDBlockId(1, 2))
    assertDifferent(id, RDDBlockId(1, 1))
    assert(id.name === "rdd_1_2")
    assert(id.asRDDId.get.rddId === 1)
    assert(id.asRDDId.get.splitIndex === 2)
    assert(id.isRDD)
    assertSame(id, BlockId(id.toString))
  }

  test("shuffle") {
    val id = ShuffleBlockId(1, 2, 3)
    assertSame(id, ShuffleBlockId(1, 2, 3))
    assertDifferent(id, ShuffleBlockId(3, 2, 3))
    assert(id.name === "shuffle_1_2_3")
    assert(id.asRDDId === None)
    assert(id.shuffleId === 1)
    assert(id.mapId === 2)
    assert(id.reduceId === 3)
    assert(id.isShuffle)
    assertSame(id, BlockId(id.toString))
  }

  test("shuffle batch") {
    val id = ShuffleBlockBatchId(1, 2, 3, 4)
    assertSame(id, ShuffleBlockBatchId(1, 2, 3, 4))
    assertDifferent(id, ShuffleBlockBatchId(2, 2, 3, 4))
    assert(id.name === "shuffle_1_2_3_4")
    assert(id.asRDDId === None)
    assert(id.shuffleId === 1)
    assert(id.mapId === 2)
    assert(id.startReduceId === 3)
    assert(id.endReduceId === 4)
    assert(id.isShuffle)
    assertSame(id, BlockId(id.toString))
  }

  test("shuffle data") {
    val id = ShuffleDataBlockId(4, 5, 6)
    assertSame(id, ShuffleDataBlockId(4, 5, 6))
    assertDifferent(id, ShuffleDataBlockId(6, 5, 6))
    assert(id.name === "shuffle_4_5_6.data")
    assert(id.asRDDId === None)
    assert(id.shuffleId === 4)
    assert(id.mapId === 5)
    assert(id.reduceId === 6)
    assert(id.isShuffle)
    assertSame(id, BlockId(id.toString))
  }

  test("shuffle index") {
    val id = ShuffleIndexBlockId(7, 8, 9)
    assertSame(id, ShuffleIndexBlockId(7, 8, 9))
    assertDifferent(id, ShuffleIndexBlockId(9, 8, 9))
    assert(id.name === "shuffle_7_8_9.index")
    assert(id.asRDDId === None)
    assert(id.shuffleId === 7)
    assert(id.mapId === 8)
    assert(id.reduceId === 9)
    assert(id.isShuffle)
    assertSame(id, BlockId(id.toString))
  }

  test("shuffle merged data") {
    val id = ShuffleMergedDataBlockId("app_000", 8, 0, 9)
    assertSame(id, ShuffleMergedDataBlockId("app_000", 8, 0, 9))
    assertDifferent(id, ShuffleMergedDataBlockId("app_000", 9, 0, 9))
    assert(id.name === "shuffleMerged_app_000_8_0_9.data")
    assert(id.asRDDId === None)
    assert(id.appId === "app_000")
    assert(id.shuffleMergeId == 0)
    assert(id.shuffleId=== 8)
    assert(id.reduceId === 9)
    assertSame(id, BlockId(id.toString))
  }

  test("shuffle merged index") {
    val id = ShuffleMergedIndexBlockId("app_000", 8, 0, 9)
    assertSame(id, ShuffleMergedIndexBlockId("app_000", 8, 0, 9))
    assertDifferent(id, ShuffleMergedIndexBlockId("app_000", 9, 0, 9))
    assert(id.name === "shuffleMerged_app_000_8_0_9.index")
    assert(id.asRDDId === None)
    assert(id.appId === "app_000")
    assert(id.shuffleId=== 8)
    assert(id.shuffleMergeId == 0)
    assert(id.reduceId === 9)
    assertSame(id, BlockId(id.toString))
  }

  test("shuffle merged meta") {
    val id = ShuffleMergedMetaBlockId("app_000", 8, 0, 9)
    assertSame(id, ShuffleMergedMetaBlockId("app_000", 8, 0, 9))
    assertDifferent(id, ShuffleMergedMetaBlockId("app_000", 9, 0, 9))
    assert(id.name === "shuffleMerged_app_000_8_0_9.meta")
    assert(id.asRDDId === None)
    assert(id.appId === "app_000")
    assert(id.shuffleId=== 8)
    assert(id.shuffleMergeId == 0)
    assert(id.reduceId === 9)
    assertSame(id, BlockId(id.toString))
  }

  test("shuffle merged block") {
    val id = ShuffleMergedBlockId(8, 0, 9)
    assertSame(id, ShuffleMergedBlockId(8, 0, 9))
    assertDifferent(id, ShuffleMergedBlockId(8, 1, 9))
    assert(id.name === "shuffleMerged_8_0_9")
    assert(id.asRDDId === None)
    assert(id.shuffleId=== 8)
    assert(id.shuffleMergeId == 0)
    assert(id.reduceId === 9)
    assertSame(id, BlockId(id.toString))
  }

  test("broadcast") {
    val id = BroadcastBlockId(42)
    assertSame(id, BroadcastBlockId(42))
    assertDifferent(id, BroadcastBlockId(123))
    assert(id.name === "broadcast_42")
    assert(id.asRDDId === None)
    assert(id.broadcastId === 42)
    assert(id.isBroadcast)
    assertSame(id, BlockId(id.toString))
  }

  test("taskresult") {
    val id = TaskResultBlockId(60)
    assertSame(id, TaskResultBlockId(60))
    assertDifferent(id, TaskResultBlockId(61))
    assert(id.name === "taskresult_60")
    assert(id.asRDDId === None)
    assert(id.taskId === 60)
    assert(!id.isRDD)
    assertSame(id, BlockId(id.toString))
  }

  test("stream") {
    val id = StreamBlockId(1, 100)
    assertSame(id, StreamBlockId(1, 100))
    assertDifferent(id, StreamBlockId(2, 101))
    assert(id.name === "input-1-100")
    assert(id.asRDDId === None)
    assert(id.streamId === 1)
    assert(id.uniqueId === 100)
    assert(!id.isBroadcast)
    assertSame(id, BlockId(id.toString))
  }

  test("temp local") {
    val id = TempLocalBlockId(new UUID(5, 2))
    assertSame(id, TempLocalBlockId(new UUID(5, 2)))
    assertDifferent(id, TempLocalBlockId(new UUID(5, 3)))
    assert(id.name === "temp_local_00000000-0000-0005-0000-000000000002")
    assert(id.asRDDId === None)
    assert(id.isBroadcast === false)
    assert(id.id.getMostSignificantBits() === 5)
    assert(id.id.getLeastSignificantBits() === 2)
    assert(!id.isShuffle)
    assertSame(id, BlockId(id.toString))
  }

  test("temp shuffle") {
    val id = TempShuffleBlockId(new UUID(1, 2))
    assertSame(id, TempShuffleBlockId(new UUID(1, 2)))
    assertDifferent(id, TempShuffleBlockId(new UUID(1, 3)))
    assert(id.name === "temp_shuffle_00000000-0000-0001-0000-000000000002")
    assert(id.asRDDId === None)
    assert(id.isBroadcast === false)
    assert(id.id.getMostSignificantBits() === 1)
    assert(id.id.getLeastSignificantBits() === 2)
    assert(!id.isShuffle)
    assertSame(id, BlockId(id.toString))
  }

  test("test") {
    val id = TestBlockId("abc")
    assertSame(id, TestBlockId("abc"))
    assertDifferent(id, TestBlockId("ab"))
    assert(id.name === "test_abc")
    assert(id.asRDDId === None)
    assert(id.id === "abc")
    assert(!id.isShuffle)
    assertSame(id, BlockId(id.toString))
  }

  test("merged shuffle id") {
    val id = ShuffleMergedBlockId(1, 2, 0)
    assertSame(id, ShuffleMergedBlockId(1, 2, 0))
    assertDifferent(id, ShuffleMergedBlockId(1, 3, 1))
    assert(id.name === "shuffleMerged_1_2_0")
    assert(id.asRDDId === None)
    assert(id.shuffleId === 1)
    assert(id.shuffleMergeId === 2)
    assert(id.reduceId === 0)
    assertSame(id, BlockId(id.toString))
  }

  test("shuffle chunk") {
    val id = ShuffleBlockChunkId(1, 0, 1, 0)
    assertSame(id, ShuffleBlockChunkId(1, 0, 1, 0))
    assertDifferent(id, ShuffleBlockChunkId(1, 0, 1, 1))
    assert(id.name === "shuffleChunk_1_0_1_0")
    assert(id.asRDDId === None)
    assert(id.shuffleId === 1)
    assert(id.reduceId === 1)
    assert(id.chunkId === 0)
    assertSame(id, BlockId(id.toString))
  }

}
