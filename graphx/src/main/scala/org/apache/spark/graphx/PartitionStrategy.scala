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

package org.apache.spark.graphx

/**
 * Represents the way edges are assigned to edge partitions based on their source and destination
 * vertex IDs.
 */
trait PartitionStrategy extends Serializable {
  /** Returns the partition number for a given edge. */
  def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID
}

/**
 * Collection of built-in [[PartitionStrategy]] implementations.
 */
object PartitionStrategy {
  /**
   * Assigns edges to partitions using a 2D partitioning of the sparse edge adjacency matrix,
   * guaranteeing a `2 * sqrt(numParts)` bound on vertex replication.
   *
   * Suppose we have a graph with 12 vertices that we want to partition
   * over 9 machines.  We can use the following sparse matrix representation:
   *
   * <pre>
   *       __________________________________
   *  v0   | P0 *     | P1       | P2    *  |
   *  v1   |  ****    |  *       |          |
   *  v2   |  ******* |      **  |  ****    |
   *  v3   |  *****   |  *  *    |       *  |
   *       ----------------------------------
   *  v4   | P3 *     | P4 ***   | P5 **  * |
   *  v5   |  *  *    |  *       |          |
   *  v6   |       *  |      **  |  ****    |
   *  v7   |  * * *   |  *  *    |       *  |
   *       ----------------------------------
   *  v8   | P6   *   | P7    *  | P8  *   *|
   *  v9   |     *    |  *    *  |          |
   *  v10  |       *  |      **  |  *  *    |
   *  v11  | * <-E    |  ***     |       ** |
   *       ----------------------------------
   * </pre>
   *
   * The edge denoted by `E` connects `v11` with `v1` and is assigned to processor `P6`. To get the
   * processor number we divide the matrix into `sqrt(numParts)` by `sqrt(numParts)` blocks. Notice
   * that edges adjacent to `v11` can only be in the first column of blocks `(P0, P3,
   * P6)` or the last
   * row of blocks `(P6, P7, P8)`.  As a consequence we can guarantee that `v11` will need to be
   * replicated to at most `2 * sqrt(numParts)` machines.
   *
   * Notice that `P0` has many edges and as a consequence this partitioning would lead to poor work
   * balance.  To improve balance we first multiply each vertex id by a large prime to shuffle the
   * vertex locations.
   *
   * When the number of partitions requested is not a perfect square we use a slightly different
   * method where the last column can have a different number of rows than the others while still
   * maintaining the same size per block.
   */
  case object EdgePartition2D extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(numParts)).toInt
      val mixingPrime: VertexId = 1125899906842597L
      if (numParts == ceilSqrtNumParts * ceilSqrtNumParts) {
        // Use old method for perfect squared to ensure we get same results
        val col: PartitionID = (math.abs(src * mixingPrime) % ceilSqrtNumParts).toInt
        val row: PartitionID = (math.abs(dst * mixingPrime) % ceilSqrtNumParts).toInt
        (col * ceilSqrtNumParts + row) % numParts

      } else {
        // Otherwise use new method
        val cols = ceilSqrtNumParts
        val rows = (numParts + cols - 1) / cols
        val lastColRows = numParts - rows * (cols - 1)
        val col = (math.abs(src * mixingPrime) % numParts / rows).toInt
        val row = (math.abs(dst * mixingPrime) % (if (col < cols - 1) rows else lastColRows)).toInt
        col * rows + row

      }
    }
  }


  /**
   * Assigns edges to partitions using a triangle partitioning of the sparse edge adjacency matrix,
   * guaranteeing a `sqrt(2 * numParts)` bound on vertex replication.
   *
   * This strategy is mostly inspired by the attempting to combine the partition strategy
   * EdgePartition2D with the partition strategy CanonicalRandomVertexCut. The basic idea is to fold
   * the partitions obtained by EdgePartition2D diagonally to colocate all edges between two
   * vertices regardless of direction.
   *
   * Edges in diagonal partitions are relocated by more complex strategy to achieve better work
   * balance, and when the number of partitions requested is not a triangle number, we use a
   * slightly different method while still maintaining the almost same size per block.
   */
  case object EdgePartitionTriangle extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val mixingPrime: VertexId = 1125899906842597L
      val numRowTriParts = ((math.sqrt(1 + 8 * numParts) - 1) / 2).toInt
      val numTriParts = numRowTriParts * (numRowTriParts + 1) / 2
      val segmentFactor = 100
      val numSegments = (segmentFactor * math.sqrt(4 * numParts * numTriParts)).toInt
      val segRow = (math.abs(src * mixingPrime) % numSegments).toInt
      val segCol = (math.abs(dst * mixingPrime) % numSegments).toInt
      var row = segRow / (segmentFactor * numRowTriParts)
      var col = segCol / (segmentFactor * numRowTriParts)
      if (math.max(segRow, segCol) >= 2 * segmentFactor * numTriParts) {
        // non triangle parts
        row = numRowTriParts + 1
        col = math.min(segRow, segCol) % (numParts - numTriParts)
      }
      else if (row == col) {
        // diagonal parts
        val ind = math.min(segRow % numRowTriParts, segCol % numRowTriParts)
        col = (math.min(2 * numRowTriParts - ind - 1, ind) + row + 1) % (numRowTriParts + 1)
      }
      if (row > col) row * (row - 1) / 2 + col else col * (col - 1) / 2 + row
    }
  }


  /**
   * Assigns edges to partitions using only the source vertex ID, colocating edges with the same
   * source.
   */
  case object EdgePartition1D extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val mixingPrime: VertexId = 1125899906842597L
      (math.abs(src * mixingPrime) % numParts).toInt
    }
  }


  /**
   * Assigns edges to partitions by hashing the source and destination vertex IDs, resulting in a
   * random vertex cut that colocates all same-direction edges between two vertices.
   */
  case object RandomVertexCut extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      math.abs((src, dst).hashCode()) % numParts
    }
  }


  /**
   * Assigns edges to partitions by hashing the source and destination vertex IDs in a canonical
   * direction, resulting in a random vertex cut that colocates all edges between two vertices,
   * regardless of direction.
   */
  case object CanonicalRandomVertexCut extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      if (src < dst) {
        math.abs((src, dst).hashCode()) % numParts
      } else {
        math.abs((dst, src).hashCode()) % numParts
      }
    }
  }

  /** Returns the PartitionStrategy with the specified name. */
  def fromString(s: String): PartitionStrategy = s match {
    case "RandomVertexCut" => RandomVertexCut
    case "EdgePartition1D" => EdgePartition1D
    case "EdgePartition2D" => EdgePartition2D
    case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
    case "EdgePartitionTriangle" => EdgePartitionTriangle
    case _ => throw new IllegalArgumentException("Invalid PartitionStrategy: " + s)
  }
}
