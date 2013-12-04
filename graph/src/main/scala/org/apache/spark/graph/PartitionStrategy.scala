package org.apache.spark.graph


sealed trait PartitionStrategy extends Serializable {
  def getPartition(src: Vid, dst: Vid, numParts: Pid): Pid
}


/**
 * This function implements a classic 2D-Partitioning of a sparse matrix.
 * Suppose we have a graph with 11 vertices that we want to partition
 * over 9 machines.  We can use the following sparse matrix representation:
 *
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
 *
 * The edge denoted by E connects v11 with v1 and is assigned to
 * processor P6.  To get the processor number we divide the matrix
 * into sqrt(numProc) by sqrt(numProc) blocks.  Notice that edges
 * adjacent to v11 can only be in the first colum of
 * blocks (P0, P3, P6) or the last row of blocks (P6, P7, P8).
 * As a consequence we can guarantee that v11 will need to be
 * replicated to at most 2 * sqrt(numProc) machines.
 *
 * Notice that P0 has many edges and as a consequence this
 * partitioning would lead to poor work balance.  To improve
 * balance we first multiply each vertex id by a large prime
 * to effectively shuffle the vertex locations.
 *
 * One of the limitations of this approach is that the number of
 * machines must either be a perfect square.  We partially address
 * this limitation by computing the machine assignment to the next
 * largest perfect square and then mapping back down to the actual
 * number of machines.  Unfortunately, this can also lead to work
 * imbalance and so it is suggested that a perfect square is used.
 *
 *
 */
case object EdgePartition2D extends PartitionStrategy {
  override def getPartition(src: Vid, dst: Vid, numParts: Pid): Pid = {
    val ceilSqrtNumParts: Pid = math.ceil(math.sqrt(numParts)).toInt
    val mixingPrime: Vid = 1125899906842597L
    val col: Pid = ((math.abs(src) * mixingPrime) % ceilSqrtNumParts).toInt
    val row: Pid = ((math.abs(dst) * mixingPrime) % ceilSqrtNumParts).toInt
    (col * ceilSqrtNumParts + row) % numParts
  }
}


case object EdgePartition1D extends PartitionStrategy {
  override def getPartition(src: Vid, dst: Vid, numParts: Pid): Pid = {
    val mixingPrime: Vid = 1125899906842597L
    (math.abs(src) * mixingPrime).toInt % numParts
  }
}


/**
 * Assign edges to an aribtrary machine corresponding to a
 * random vertex cut.
 */
case object RandomVertexCut extends PartitionStrategy {
  override def getPartition(src: Vid, dst: Vid, numParts: Pid): Pid = {
    math.abs((src, dst).hashCode()) % numParts
  }
}


/**
 * Assign edges to an arbitrary machine corresponding to a random vertex cut. This
 * function ensures that edges of opposite direction between the same two vertices
 * will end up on the same partition.
 */
case object CanonicalRandomVertexCut extends PartitionStrategy {
  override def getPartition(src: Vid, dst: Vid, numParts: Pid): Pid = {
    val lower = math.min(src, dst)
    val higher = math.max(src, dst)
    math.abs((lower, higher).hashCode()) % numParts
  }
}
