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

import org.apache.spark.util.collection.BitSet

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

private[graphx] object VertexPartition {
  /** Construct a `VertexPartition` from the given vertices. */
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)])
    : VertexPartition[VD] = {
    val (index, values, mask) = VertexPartitionBase.initFrom(iter)
    new VertexPartition(index, values, mask)
  }

  import scala.language.implicitConversions

  /**
   * Implicit conversion to allow invoking `VertexPartitionBase` operations directly on a
   * `VertexPartition`.
   */
  implicit def partitionToOps[VD: ClassTag](partition: VertexPartition[VD]) =
    new VertexPartitionOps(partition)

  /**
   * Implicit evidence that `VertexPartition` is a member of the `VertexPartitionBaseOpsConstructor`
   * typeclass. This enables invoking `VertexPartitionBase` operations on a `VertexPartition` via an
   * evidence parameter, as in [[VertexPartitionBaseOps]].
   */
  implicit object VertexPartitionOpsConstructor
    extends VertexPartitionBaseOpsConstructor[VertexPartition] {
    def toOps[VD: ClassTag](partition: VertexPartition[VD])
      : VertexPartitionBaseOps[VD, VertexPartition] = partitionToOps(partition)
  }
}

/** A map from vertex id to vertex attribute. */
private[graphx] class VertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet)
  extends VertexPartitionBase[VD]

private[graphx] class VertexPartitionOps[VD: ClassTag](self: VertexPartition[VD])
  extends VertexPartitionBaseOps[VD, VertexPartition](self) {

  def withIndex(index: VertexIdToIndexMap): VertexPartition[VD] = {
    new VertexPartition(index, self.values, self.mask)
  }

  def withValues[VD2: ClassTag](values: Array[VD2]): VertexPartition[VD2] = {
    new VertexPartition(self.index, values, self.mask)
  }

  def withMask(mask: BitSet): VertexPartition[VD] = {
    new VertexPartition(self.index, self.values, mask)
  }
}
