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

package org.apache.spark.rdd

import scala.collection.immutable.LongMap

import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.serializer.KryoSerializer

class IndexedRDDPartitionSuite extends FunSuite {

  test("isDefined, filter") {
    val vp = IndexedRDDPartition(Iterator((0L, 1), (1L, 1))).filter { (vid, attr) => vid == 0 }
    assert(vp.isDefined(0))
    assert(!vp.isDefined(1))
    assert(!vp.isDefined(2))
    assert(!vp.isDefined(-1))
  }

  test("multiget") {
    val vp = IndexedRDDPartition(Iterator((0L, 1), (1L, 1)))
    assert(vp.multiget(Array(-1L, 0L, 1L, 2L)) === LongMap(0L -> 1, 1L -> 1))
    val vpFiltered = vp.filter { (vid, attr) => vid == 0 }
    assert(vpFiltered.multiget(Array(-1L, 0L, 1L, 2L)) === LongMap(0L -> 1))
  }

  test("multiput") {
    val vp = IndexedRDDPartition(Iterator((0L, 0), (1L, 1), (2L, 2)))
    def sum(id: IndexedRDD.Id, a: Int, b: Int) = a + b
    assert(vp.multiput(Seq(0L -> 1, 1L -> 1), sum).iterator.toSet ===
      Set(0L -> 1, 1L -> 2, 2L -> 2))
    assert(vp.multiput(Seq(0L -> 1, 100L -> 1), sum).iterator.toSet ===
      Set(0L -> 1, 1L -> 1, 2L -> 2, 100L -> 1))
    assert(vp.multiput(Seq(100L -> 1), (id, a, b) => fail()).iterator.toSet ===
      Set(0L -> 0, 1L -> 1, 2L -> 2, 100L -> 1))
  }

  test("delete") {
    val vp = IndexedRDDPartition(Iterator((0L, 0), (1L, 1), (2L, 2)))
    assert(vp.delete(Array(0L)).iterator.toSet === Set(1L -> 1, 2L -> 2))
    assert(vp.delete(Array(3L)).iterator.toSet === Set(0L -> 0, 1L -> 1, 2L -> 2))
  }

  test("mapValues") {
    val vp = IndexedRDDPartition(Iterator((0L, 1), (1L, 1))).mapValues { (vid, attr) => 2 }
    assert(vp(0) === 2)
  }

  test("diff") {
    val vp = IndexedRDDPartition(Iterator((0L, 1), (1L, 1), (2L, 1)))
    val vp2 = vp.filter { (vid, attr) => vid <= 1 }
    val vp3a = vp.mapValues { (vid, attr) => 2 }
    val vp3b = IndexedRDDPartition(vp3a.iterator)
    // diff with same index
    val diff1 = vp3a.diff(vp2)
    assert(diff1(0) === 2)
    assert(diff1(1) === 2)
    assert(diff1(2) === 2)
    assert(!diff1.isDefined(2))
    // diff with different indexes
    val diff2 = vp3b.diff(vp2)
    assert(diff2(0) === 2)
    assert(diff2(1) === 2)
    assert(diff2(2) === 2)
    assert(!diff2.isDefined(2))
  }

  test("leftJoin") {
    val vp = IndexedRDDPartition(Iterator((0L, 1), (1L, 1), (2L, 1)))
    val vp2a = vp.filter { (vid, attr) => vid <= 1 }.mapValues { (vid, attr) => 2 }
    val vp2b = IndexedRDDPartition(vp2a.iterator)
    // leftJoin with same index
    val join1 = vp.leftJoin(vp2a) { (vid, a, bOpt) => bOpt.getOrElse(a) }
    assert(join1.iterator.toSet === Set((0L, 2), (1L, 2), (2L, 1)))
    // leftJoin with different indexes
    val join2 = vp.leftJoin(vp2b) { (vid, a, bOpt) => bOpt.getOrElse(a) }
    assert(join2.iterator.toSet === Set((0L, 2), (1L, 2), (2L, 1)))
    // leftJoin an iterator
    val join3 = vp.leftJoin(vp2a.iterator) { (vid, a, bOpt) => bOpt.getOrElse(a) }
    assert(join3.iterator.toSet === Set((0L, 2), (1L, 2), (2L, 1)))
  }

  test("join") {
    val vp = IndexedRDDPartition(Iterator((0L, 1), (1L, 1), (2L, 1)))
    val vp2a = vp.filter { (vid, attr) => vid <= 1 }.mapValues { (vid, attr) => 2 }
    val vp2b = IndexedRDDPartition(vp2a.iterator)
    // join with same index
    val join1 = vp.join(vp2a) { (vid, a, b) => b }
    assert(join1.iterator.toSet === Set((0L, 2), (1L, 2), (2L, 1)))
    // join with different indexes
    val join2 = vp.join(vp2b) { (vid, a, b) => b }
    assert(join2.iterator.toSet === Set((0L, 2), (1L, 2), (2L, 1)))
    // join an iterator
    val join3 = vp.join(vp2a.iterator) { (vid, a, b) => b }
    assert(join3.iterator.toSet === Set((0L, 2), (1L, 2), (2L, 1)))
  }

  test("innerJoin") {
    val vp = IndexedRDDPartition(Iterator((0L, 1), (1L, 1), (2L, 1)))
    val vp2a = vp.filter { (vid, attr) => vid <= 1 }.mapValues { (vid, attr) => 2 }
    val vp2b = IndexedRDDPartition(vp2a.iterator)
    // innerJoin with same index
    val join1 = vp.innerJoin(vp2a) { (vid, a, b) => b }
    assert(join1.iterator.toSet === Set((0L, 2), (1L, 2)))
    // innerJoin with different indexes
    val join2 = vp.innerJoin(vp2b) { (vid, a, b) => b }
    assert(join2.iterator.toSet === Set((0L, 2), (1L, 2)))
    // innerJoin an iterator
    val join3 = vp.innerJoin(vp2a.iterator) { (vid, a, b) => b }
    assert(join3.iterator.toSet === Set((0L, 2), (1L, 2)))
  }

  test("createUsingIndex") {
    val vp = IndexedRDDPartition(Iterator((0L, 1), (1L, 1), (2L, 1)))

    // No new elements
    val elems1 = List((0L, 2), (2L, 2))
    val vp1 = vp.createUsingIndex(elems1.iterator)
    assert(vp1.iterator.toSet === elems1.toSet)
    assert(vp.index === vp1.index)

    // New elements
    val elems2 = List((0L, 2), (2L, 2), (3L, 2))
    val vp2 = vp.createUsingIndex(elems2.iterator)
    assert(vp2.iterator.toSet === elems2.toSet)
    assert(vp.index != vp2.index)
}

  test("innerJoinKeepLeft") {
    val vp = IndexedRDDPartition(Iterator((0L, 1), (1L, 1), (2L, 1)))
    val elems = List((0L, 2), (2L, 2), (3L, 2))
    val vp2 = vp.innerJoinKeepLeft(elems.iterator)
    assert(vp2.iterator.toSet === Set((0L, 2), (2L, 2)))
    assert(vp2(1) === 1)
  }

  test("aggregateUsingIndex") {
    val vp = IndexedRDDPartition(Iterator((0L, 1), (1L, 1), (2L, 1)))

    // No new elements
    val messages1 = List((0L, "a"), (2L, "b"), (0L, "c"))
    val vp1 = vp.aggregateUsingIndex[String](messages1.iterator, _ + _)
    assert(vp1.iterator.toSet === Set((0L, "ac"), (2L, "b")))
    assert(vp.index === vp1.index)

    // No new elements
    val messages2 = List((0L, "a"), (2L, "b"), (0L, "c"), (3L, "d"))
    val vp2 = vp.aggregateUsingIndex[String](messages2.iterator, _ + _)
    assert(vp2.iterator.toSet === Set((0L, "ac"), (2L, "b"), (3L, "d")))
    assert(vp.index != vp2.index)
}

  test("reindex") {
    val vp = IndexedRDDPartition(Iterator((0L, 1), (1L, 1), (2L, 1)))
    val vp2 = vp.filter { (vid, attr) => vid <= 1 }
    val vp3 = vp2.reindex()
    assert(vp2.iterator.toSet === vp3.iterator.toSet)
    assert(vp2(2) === 1)
    assert(vp3.index.getPos(2) === -1)
  }

  test("serialization") {
    val elems = Set((0L, 1), (1L, 1), (2L, 1))
    val vp = IndexedRDDPartition(elems.iterator)
    val javaSer = new JavaSerializer(new SparkConf())
    val kryoSer = new KryoSerializer(new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))

    for (ser <- List(javaSer, kryoSer); s = ser.newInstance()) {
      val vpSer: IndexedRDDPartition[Int] = s.deserialize(s.serialize(vp))
      assert(vpSer.iterator.toSet === elems)
    }
  }
}
