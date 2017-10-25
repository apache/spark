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

import org.apache.spark.SparkFunSuite

class BlockIdSuite extends SparkFunSuite {
  def assertSame(id1: BlockId, id2: BlockId) {
    assert(id1.name === id2.name)
    assert(id1.hashCode === id2.hashCode)
    assert(id1 === id2)
  }

  def assertDifferent(id1: BlockId, id2: BlockId) {
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
}
