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

package org.apache.spark.graphx.impl

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.serializer.KryoSerializer

import org.apache.spark.graphx._

class EdgePartitionSuite extends SparkFunSuite {

  def makeEdgePartition[A: ClassTag](xs: Iterable[(Int, Int, A)]): EdgePartition[A, Int] = {
    val builder = new EdgePartitionBuilder[A, Int]
    for ((src, dst, attr) <- xs) { builder.add(src: VertexId, dst: VertexId, attr) }
    builder.toEdgePartition
  }

  test("reverse") {
    val edges = List(Edge(0, 1, 0), Edge(1, 2, 0), Edge(2, 0, 0))
    val reversedEdges = List(Edge(0, 2, 0), Edge(1, 0, 0), Edge(2, 1, 0))
    val builder = new EdgePartitionBuilder[Int, Nothing]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.reverse.iterator.map(_.copy()).toList === reversedEdges)
    assert(edgePartition.reverse.reverse.iterator.map(_.copy()).toList === edges)
  }

  test("map") {
    val edges = List(Edge(0, 1, 0), Edge(1, 2, 0), Edge(2, 0, 0))
    val builder = new EdgePartitionBuilder[Int, Nothing]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.map(e => e.srcId + e.dstId).iterator.map(_.copy()).toList ===
      edges.map(e => e.copy(attr = e.srcId + e.dstId)))
  }

  test("filter") {
    val edges = List(Edge(0, 1, 0), Edge(0, 2, 0), Edge(2, 0, 0))
    val builder = new EdgePartitionBuilder[Int, Int]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    val filtered = edgePartition.filter(et => et.srcId == 0, (vid, attr) => vid == 0 || vid == 1)
    assert(filtered.tripletIterator().toList.map(et => (et.srcId, et.dstId)) === List((0L, 1L)))
  }

  test("groupEdges") {
    val edges = List(
      Edge(0, 1, 1), Edge(1, 2, 2), Edge(2, 0, 4), Edge(0, 1, 8), Edge(1, 2, 16), Edge(2, 0, 32))
    val groupedEdges = List(Edge(0, 1, 9), Edge(1, 2, 18), Edge(2, 0, 36))
    val builder = new EdgePartitionBuilder[Int, Nothing]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.groupEdges(_ + _).iterator.map(_.copy()).toList === groupedEdges)
  }

  test("innerJoin") {
    val aList = List((0, 1, 0), (1, 0, 0), (1, 2, 0), (5, 4, 0), (5, 5, 0))
    val bList = List((0, 1, 0), (1, 0, 0), (1, 1, 0), (3, 4, 0), (5, 5, 0))
    val a = makeEdgePartition(aList)
    val b = makeEdgePartition(bList)

    assert(a.innerJoin(b) { (src, dst, a, b) => a }.iterator.map(_.copy()).toList ===
      List(Edge(0, 1, 0), Edge(1, 0, 0), Edge(5, 5, 0)))
  }

  test("isActive, numActives, replaceActives") {
    val ep = new EdgePartitionBuilder[Nothing, Nothing].toEdgePartition
      .withActiveSet(Iterator(0L, 2L, 0L))
    assert(ep.isActive(0))
    assert(!ep.isActive(1))
    assert(ep.isActive(2))
    assert(!ep.isActive(-1))
    assert(ep.numActives == Some(2))
  }

  test("tripletIterator") {
    val builder = new EdgePartitionBuilder[Int, Int]
    builder.add(1, 2, 0)
    builder.add(1, 3, 0)
    builder.add(1, 4, 0)
    val ep = builder.toEdgePartition
    val result = ep.tripletIterator().toList.map(et => (et.srcId, et.dstId))
    assert(result === Seq((1, 2), (1, 3), (1, 4)))
  }

  test("serialization") {
    val aList = List((0, 1, 1), (1, 0, 2), (1, 2, 3), (5, 4, 4), (5, 5, 5))
    val a: EdgePartition[Int, Int] = makeEdgePartition(aList)
    val javaSer = new JavaSerializer(new SparkConf())
    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)
    val kryoSer = new KryoSerializer(conf)

    for (ser <- List(javaSer, kryoSer); s = ser.newInstance()) {
      val aSer: EdgePartition[Int, Int] = s.deserialize(s.serialize(a))
      assert(aSer.tripletIterator().toList === a.tripletIterator().toList)
    }
  }
}
