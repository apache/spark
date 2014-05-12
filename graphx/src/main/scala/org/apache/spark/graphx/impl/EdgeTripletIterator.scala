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

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

/**
 * The Iterator type returned when constructing edge triplets. This could be an anonymous class in
 * EdgePartition.tripletIterator, but we name it here explicitly so it is easier to debug / profile.
 */
private[impl]
class EdgeTripletIterator[VD: ClassTag, ED: ClassTag](
    val edgePartition: EdgePartition[ED, VD],
    val includeSrc: Boolean,
    val includeDst: Boolean)
  extends Iterator[EdgeTriplet[VD, ED]] {

  // Current position in the array.
  private var pos = 0

  override def hasNext: Boolean = pos < edgePartition.size

  override def next() = {
    val triplet = new EdgeTriplet[VD, ED]
    triplet.srcId = edgePartition.srcIds(pos)
    if (includeSrc) {
      triplet.srcAttr = edgePartition.vertices(triplet.srcId)
    }
    triplet.dstId = edgePartition.dstIds(pos)
    if (includeDst) {
      triplet.dstAttr = edgePartition.vertices(triplet.dstId)
    }
    triplet.attr = edgePartition.data(pos)
    pos += 1
    triplet
  }
}

/**
 * An Iterator type for internal use that reuses EdgeTriplet objects. This could be an anonymous
 * class in EdgePartition.upgradeIterator, but we name it here explicitly so it is easier to debug /
 * profile.
 */
private[impl]
class ReusingEdgeTripletIterator[VD: ClassTag, ED: ClassTag](
    val edgeIter: Iterator[Edge[ED]],
    val edgePartition: EdgePartition[ED, VD],
    val includeSrc: Boolean,
    val includeDst: Boolean)
  extends Iterator[EdgeTriplet[VD, ED]] {

  private val triplet = new EdgeTriplet[VD, ED]

  override def hasNext = edgeIter.hasNext

  override def next() = {
    triplet.set(edgeIter.next())
    if (includeSrc) {
      triplet.srcAttr = edgePartition.vertices(triplet.srcId)
    }
    if (includeDst) {
      triplet.dstAttr = edgePartition.vertices(triplet.dstId)
    }
    triplet
  }
}
