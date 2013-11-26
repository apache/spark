package org.apache.spark.graph.impl

import org.apache.spark.util.collection.{BitSet, PrimitiveKeyOpenHashMap}

import org.apache.spark.graph._

class VertexPartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD: ClassManifest](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet) {

  /**
   * Pass each vertex attribute along with the vertex id through a map
   * function and retain the original RDD's partitioning and index.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each vertex id and vertex
   * attribute in the RDD
   *
   * @return a new VertexPartition with values obtained by applying `f` to
   * each of the entries in the original VertexSet.  The resulting
   * VertexPartition retains the same index.
   */
  def map[VD2: ClassManifest](f: (Vid, VD) => VD2): VertexPartition[VD2] = {
    // Construct a view of the map transformation
    val newValues = new Array[VD2](index.capacity)
    mask.iterator.foreach { ind =>
      newValues(ind) = f(index.getValueSafe(ind), values(ind))
    }
    new VertexPartition[VD2](index, newValues, mask)
  }

  /**
   * Restrict the vertex set to the set of vertices satisfying the
   * given predicate.
   *
   * @param pred the user defined predicate
   *
   * @note The vertex set preserves the original index structure
   * which means that the returned RDD can be easily joined with
   * the original vertex-set.  Furthermore, the filter only
   * modifies the bitmap index and so no new values are allocated.
   */
  def filter(pred: (Vid, VD) => Boolean): VertexPartition[VD] = {
    // Allocate the array to store the results into
    val newMask = new BitSet(index.capacity)
    // Iterate over the active bits in the old bitset and
    // evaluate the predicate
    var ind = mask.nextSetBit(0)
    while (ind >= 0) {
      val k = index.getValueSafe(ind)
      if (pred(k, values(ind))) {
        newMask.set(ind)
      }
      ind = mask.nextSetBit(ind + 1)
    }
    new VertexPartition(index, values, newMask)
  }

  /**
   * Construct a new VertexPartition whose index contains only the vertices in
   * the mask.
   */
  def reindex(): VertexPartition[VD] = {
    val hashMap = new PrimitiveKeyOpenHashMap[Vid, VD]
    val arbitraryMerge = (a: VD, b: VD) => a
    for ((k, v) <- this.iterator) {
      hashMap.setMerge(k, v, arbitraryMerge)
    }
    new VertexPartition(hashMap.keySet, hashMap._values, hashMap.keySet.getBitSet)
  }

  def iterator = mask.iterator.map(ind => (index.getValueSafe(ind), values(ind)))
}
