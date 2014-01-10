package org.apache.spark.graphx

/**
 * Represents the way edges are assigned to edge partitions based on their source and destination
 * vertex IDs.
 */
sealed trait PartitionStrategy extends Serializable {
  def getPartition(src: VertexID, dst: VertexID, numParts: PartitionID): PartitionID
}


/**
 * Assigns edges to partitions using a 2D partitioning of the sparse edge adjacency matrix,
 * guaranteeing a `2 * sqrt(numParts)` bound on vertex replication.
 *
 * Suppose we have a graph with 11 vertices that we want to partition
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
 * processor number we divide the matrix into `sqrt(numParts)` by `sqrt(numParts)` blocks.  Notice
 * that edges adjacent to `v11` can only be in the first column of blocks `(P0, P3, P6)` or the last
 * row of blocks `(P6, P7, P8)`.  As a consequence we can guarantee that `v11` will need to be
 * replicated to at most `2 * sqrt(numParts)` machines.
 *
 * Notice that `P0` has many edges and as a consequence this partitioning would lead to poor work
 * balance.  To improve balance we first multiply each vertex id by a large prime to shuffle the
 * vertex locations.
 *
 * One of the limitations of this approach is that the number of machines must either be a perfect
 * square. We partially address this limitation by computing the machine assignment to the next
 * largest perfect square and then mapping back down to the actual number of machines.
 * Unfortunately, this can also lead to work imbalance and so it is suggested that a perfect square
 * is used.
 */
case object EdgePartition2D extends PartitionStrategy {
  override def getPartition(src: VertexID, dst: VertexID, numParts: PartitionID): PartitionID = {
    val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(numParts)).toInt
    val mixingPrime: VertexID = 1125899906842597L
    val col: PartitionID = ((math.abs(src) * mixingPrime) % ceilSqrtNumParts).toInt
    val row: PartitionID = ((math.abs(dst) * mixingPrime) % ceilSqrtNumParts).toInt
    (col * ceilSqrtNumParts + row) % numParts
  }
}

/**
 * Assigns edges to partitions using only the source vertex ID, colocating edges with the same
 * source.
 */
case object EdgePartition1D extends PartitionStrategy {
  override def getPartition(src: VertexID, dst: VertexID, numParts: PartitionID): PartitionID = {
    val mixingPrime: VertexID = 1125899906842597L
    (math.abs(src) * mixingPrime).toInt % numParts
  }
}


/**
 * Assigns edges to partitions by hashing the source and destination vertex IDs, resulting in a
 * random vertex cut that colocates all same-direction edges between two vertices.
 */
case object RandomVertexCut extends PartitionStrategy {
  override def getPartition(src: VertexID, dst: VertexID, numParts: PartitionID): PartitionID = {
    math.abs((src, dst).hashCode()) % numParts
  }
}


/**
 * Assigns edges to partitions by hashing the source and destination vertex IDs in a canonical
 * direction, resulting in a random vertex cut that colocates all edges between two vertices,
 * regardless of direction.
 */
case object CanonicalRandomVertexCut extends PartitionStrategy {
  override def getPartition(src: VertexID, dst: VertexID, numParts: PartitionID): PartitionID = {
    val lower = math.min(src, dst)
    val higher = math.max(src, dst)
    math.abs((lower, higher).hashCode()) % numParts
  }
}
