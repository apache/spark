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

package org.apache.spark.graph

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveKeyOpenHashMap}

import org.apache.spark.graph.impl.VertexPartition


/**
 * Maintains the per-partition mapping from vertex id to the corresponding
 * location in the per-partition values array. This class is meant to be an
 * opaque type.
 */
private[graph]
class VertexSetIndex(private[spark] val rdd: RDD[VertexIdToIndexMap]) {
  /**
   * The persist function behaves like the standard RDD persist
   */
  def persist(newLevel: StorageLevel): VertexSetIndex = {
    rdd.persist(newLevel)
    this
  }

  /**
   * Returns the partitioner object of the underlying RDD.  This is
   * used by the VertexSetRDD to partition the values RDD.
   */
  def partitioner: Partitioner = rdd.partitioner.get
} // end of VertexSetIndex

/**
 * A `VertexSetRDD[VD]` extends the `RDD[(Vid, VD)]` by ensuring that there is
 * only one entry for each vertex and by pre-indexing the entries for fast,
 * efficient joins.
 *
 * @tparam VD the vertex attribute associated with each vertex in the set.
 *
 * To construct a `VertexSetRDD` use the singleton object:
 *
 * @example Construct a `VertexSetRDD` from a plain RDD
 * {{{
 * // Construct an intial vertex set
 * val someData: RDD[(Vid, SomeType)] = loadData(someFile)
 * val vset = VertexSetRDD(someData)
 * // If there were redundant values in someData we would use a reduceFunc
 * val vset2 = VertexSetRDD(someData, reduceFunc)
 * // Finally we can use the VertexSetRDD to index another dataset
 * val otherData: RDD[(Vid, OtherType)] = loadData(otherFile)
 * val vset3 = VertexSetRDD(otherData, vset.index)
 * // Now we can construct very fast joins between the two sets
 * val vset4: VertexSetRDD[(SomeType, OtherType)] = vset.leftJoin(vset3)
 * }}}
 *
 */
class VertexSetRDD[@specialized VD: ClassManifest](
    @transient val partitionsRDD: RDD[VertexPartition[VD]])
  extends RDD[(Vid, VD)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  /**
   * The `VertexSetIndex` representing the layout of this `VertexSetRDD`.
   */
  // TOOD: Consider removing the exposure of index to outside, and implement methods in this
  // class to handle any operations that would require indexing.
  def index = new VertexSetIndex(partitionsRDD.mapPartitions(_.map(_.index),
    preservesPartitioning = true))

  /**
   * Construct a new VertexSetRDD that is indexed by only the keys in the RDD.
   * The resulting VertexSet will be based on a different index and can
   * no longer be quickly joined with this RDD.
   */
   def reindex(): VertexSetRDD[VD] = VertexSetRDD(this)

  /**
   * An internal representation which joins the block indices with the values
   * This is used by the compute function to emulate `RDD[(Vid, VD)]`
   */
  protected[spark] val tuples = partitionsRDD.flatMap(_.iterator)

  /**
   * The partitioner is defined by the index.
   */
  override val partitioner = partitionsRDD.partitioner

  /**
   * The actual partitions are defined by the tuples.
   */
  override def getPartitions: Array[Partition] = tuples.partitions

  /**
   * The preferred locations are computed based on the preferred
   * locations of the tuples.
   */
  override def getPreferredLocations(s: Partition): Seq[String] =
    tuples.preferredLocations(s)

  /**
   * Caching a VertexSetRDD causes the index and values to be cached separately.
   */
  override def persist(newLevel: StorageLevel): VertexSetRDD[VD] = {
    partitionsRDD.persist(newLevel)
    this
  }

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def persist(): VertexSetRDD[VD] = persist(StorageLevel.MEMORY_ONLY)

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def cache(): VertexSetRDD[VD] = persist()

  /**
   * Provide the `RDD[(Vid, VD)]` equivalent output.
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(Vid, VD)] =
    tuples.compute(part, context)

  /**
   * Return a new VertexSetRDD by applying a function to each VertexPartition of this RDD.
   */
  def mapVertexPartitions[VD2: ClassManifest](f: VertexPartition[VD] => VertexPartition[VD2])
    : VertexSetRDD[VD2] = {
    val cleanF = sparkContext.clean(f)
    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(cleanF), preservesPartitioning = true)
    new VertexSetRDD(newPartitionsRDD)
  }

  /**
   * Return a new VertexSetRDD by applying a function to corresponding
   * VertexPartitions of this VertexSetRDD and another one.
   */
  def zipVertexPartitions[VD2: ClassManifest, VD3: ClassManifest]
    (other: VertexSetRDD[VD2])
    (f: (VertexPartition[VD], VertexPartition[VD2]) => VertexPartition[VD3]): VertexSetRDD[VD3] = {
    val cleanF = sparkContext.clean(f)
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) {
      (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(cleanF(thisPart, otherPart))
    }
    new VertexSetRDD(newPartitionsRDD)
  }

  /**
   * Restrict the vertex set to the set of vertices satisfying the
   * given predicate.
   *
   * @param pred the user defined predicate, which takes a tuple to conform to
   * the RDD[(Vid, VD)] interface
   *
   * @note The vertex set preserves the original index structure
   * which means that the returned RDD can be easily joined with
   * the original vertex-set.  Furthermore, the filter only
   * modifies the bitmap index and so no new values are allocated.
   */
  // TODO: Should we consider making pred taking two arguments, instead of a tuple?
  override def filter(pred: Tuple2[Vid, VD] => Boolean): VertexSetRDD[VD] =
    this.mapVertexPartitions(_.filter(Function.untupled(pred)))

  /**
   * Pass each vertex attribute through a map function and retain the
   * original RDD's partitioning and index.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each value in the RDD
   * @return a new VertexSetRDD with values obtained by applying `f` to
   * each of the entries in the original VertexSet.  The resulting
   * VertexSetRDD retains the same index.
   */
  def mapValues[VD2: ClassManifest](f: VD => VD2): VertexSetRDD[VD2] =
    this.mapVertexPartitions(_.map((vid, attr) => f(attr)))

  /**
   * Pass each vertex attribute through a map function and retain the
   * original RDD's partitioning and index.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each value in the RDD
   * @return a new VertexSetRDD with values obtained by applying `f` to
   * each of the entries in the original VertexSet.  The resulting
   * VertexSetRDD retains the same index.
   */
  def mapValues[VD2: ClassManifest](f: (Vid, VD) => VD2): VertexSetRDD[VD2] =
    this.mapVertexPartitions(_.map(f))

  /**
   * Fill in missing values for all vertices in the index.
   *
   * @param missingValue the value to use for vertices that don't currently have values.
   * @return A VertexSetRDD with a value for all vertices.
   */
  def fillMissing(missingValue: VD): VertexSetRDD[VD] = {
    this.mapVertexPartitions { part =>
      // Allocate a new values array with missing value as the default
      val newValues = Array.fill(part.values.size)(missingValue)
      // Copy over the old values
      part.mask.iterator.foreach { ind =>
        newValues(ind) = part.values(ind)
      }
      // Create a new mask with all vertices in the index
      val newMask = part.index.getBitSet
      new VertexPartition(part.index, newValues, newMask)
    }
  }

  /**
   * Inner join this VertexSet with another VertexSet which has the
   * same Index.  This function will fail if both VertexSets do not
   * share the same index.  The resulting vertex set will only contain
   * vertices that are in both this and the other vertex set.
   *
   * @tparam VD2 the attribute type of the other VertexSet
   * @tparam VD3 the attribute type of the resulting VertexSet
   *
   * @param other the other VertexSet with which to join.
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a new vertex attribute.
   * @return a VertexSetRDD containing only the vertices in both this
   * and the other VertexSet and with tuple attributes.
   *
   */
  def zipJoin[VD2: ClassManifest, VD3: ClassManifest]
    (other: VertexSetRDD[VD2])(f: (Vid, VD, VD2) => VD3): VertexSetRDD[VD3] = {
    this.zipVertexPartitions(other) {
      (thisPart, otherPart) =>
      if (thisPart.index != otherPart.index) {
        throw new SparkException("can't zip join VertexSetRDDs with different indexes")
      }
      val newValues = new Array[VD3](thisPart.index.capacity)
      val newMask = thisPart.mask & otherPart.mask
      newMask.iterator.foreach { ind =>
        newValues(ind) =
          f(thisPart.index.getValueSafe(ind), thisPart.values(ind), otherPart.values(ind))
      }
      new VertexPartition(thisPart.index, newValues, newMask)
    }
  }

  /**
   * Left join this VertexSet with another VertexSet which has the
   * same Index.  This function will fail if both VertexSets do not
   * share the same index.  The resulting vertex set contains an entry
   * for each vertex in this set.  If the other VertexSet is missing
   * any vertex in this VertexSet then a `None` attribute is generated
   *
   * @tparam VD2 the attribute type of the other VertexSet
   * @tparam VD3 the attribute type of the resulting VertexSet
   *
   * @param other the other VertexSet with which to join.
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a new vertex attribute.
   * @return a VertexSetRDD containing all the vertices in this
   * VertexSet with `None` attributes used for Vertices missing in the
   * other VertexSet.
   *
   */
  def leftZipJoin[VD2: ClassManifest, VD3: ClassManifest]
    (other: VertexSetRDD[VD2])(f: (Vid, VD, Option[VD2]) => VD3): VertexSetRDD[VD3] = {
    this.zipVertexPartitions(other) {
      (thisPart, otherPart) =>
      if (thisPart.index != otherPart.index) {
        throw new SparkException("can't zip join VertexSetRDDs with different indexes")
      }
      val newValues = new Array[VD3](thisPart.index.capacity)
      thisPart.mask.iterator.foreach { ind =>
        val otherV = if (otherPart.mask.get(ind)) Option(otherPart.values(ind)) else None
        newValues(ind) = f(
          thisPart.index.getValueSafe(ind), thisPart.values(ind), otherV)
      }
      new VertexPartition(thisPart.index, newValues, thisPart.mask)
    }
  } // end of leftZipJoin

  /**
   * Left join this VertexSet with an RDD containing vertex attribute
   * pairs.  If the other RDD is backed by a VertexSet with the same
   * index than the efficient leftZipJoin implementation is used.  The
   * resulting vertex set contains an entry for each vertex in this
   * set.  If the other VertexSet is missing any vertex in this
   * VertexSet then a `None` attribute is generated
   *
   * @tparam VD2 the attribute type of the other VertexSet
   * @tparam VD3 the attribute type of the resulting VertexSet
   *
   * @param other the other VertexSet with which to join.
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a new vertex attribute.
   * @param merge the function used combine duplicate vertex
   * attributes
   * @return a VertexSetRDD containing all the vertices in this
   * VertexSet with the attribute emitted by f.
   *
   */
  def leftJoin[VD2: ClassManifest, VD3: ClassManifest]
    (other: RDD[(Vid, VD2)])
    (f: (Vid, VD, Option[VD2]) => VD3, merge: (VD2, VD2) => VD2 = (a: VD2, b: VD2) => a)
    : VertexSetRDD[VD3] = {
    // Test if the other vertex is a VertexSetRDD to choose the optimal join strategy.
    // If the other set is a VertexSetRDD then we use the much more efficient leftZipJoin
    other match {
      case other: VertexSetRDD[VD2] =>
        leftZipJoin(other)(f)
      case _ =>
        val indexedOther: VertexSetRDD[VD2] = VertexSetRDD(other, this.index, merge)
        leftZipJoin(indexedOther)(f)
    }
  } // end of leftJoin

} // end of VertexSetRDD


/**
 * The VertexSetRDD singleton is used to construct VertexSets
 */
object VertexSetRDD {

  /**
   * Construct a vertex set from an RDD of vertex-attribute pairs.
   * Duplicate entries are removed arbitrarily.
   *
   * @tparam VD the vertex attribute type
   *
   * @param rdd the collection of vertex-attribute pairs
   */
  def apply[VD: ClassManifest](rdd: RDD[(Vid, VD)]): VertexSetRDD[VD] =
    apply(rdd, (a: VD, b: VD) => a)

  /**
   * Construct a vertex set from an RDD of vertex-attribute pairs
   * where duplicate entries are merged using the reduceFunc
   *
   * @tparam VD the vertex attribute type
   *
   * @param rdd the collection of vertex-attribute pairs
   * @param reduceFunc the function used to merge attributes of
   * duplicate vertices.
   */
  def apply[VD: ClassManifest](
      rdd: RDD[(Vid, VD)], reduceFunc: (VD, VD) => VD): VertexSetRDD[VD] = {
    val cReduceFunc = rdd.context.clean(reduceFunc)
    // Preaggregate and shuffle if necessary
    val preAgg = rdd.partitioner match {
      case Some(p) => rdd
      case None =>
        val partitioner = new HashPartitioner(rdd.partitions.size)
        // Preaggregation.
        val aggregator = new Aggregator[Vid, VD, VD](v => v, cReduceFunc, cReduceFunc)
        rdd.mapPartitions(aggregator.combineValuesByKey, true).partitionBy(partitioner)
    }

    val partitionsRDD = preAgg.mapPartitions(iter => {
      val hashMap = new PrimitiveKeyOpenHashMap[Vid, VD]
      for ((k, v) <- iter) {
        hashMap.setMerge(k, v, cReduceFunc)
      }
      val part = new VertexPartition(hashMap.keySet, hashMap._values, hashMap.keySet.getBitSet)
      Iterator(part)
    }, preservesPartitioning = true).cache
    new VertexSetRDD(partitionsRDD)
  } // end of apply

  /**
   * Construct a vertex set from an RDD using an existing index.
   *
   * @note duplicate vertices are discarded arbitrarily
   *
   * @tparam VD the vertex attribute type
   * @param rdd the rdd containing vertices
   * @param index a VertexSetRDD whose indexes will be reused. The
   * indexes must be a superset of the vertices in rdd
   * in RDD
   */
  def apply[VD: ClassManifest](rdd: RDD[(Vid, VD)], index: VertexSetIndex): VertexSetRDD[VD] =
    apply(rdd, index, (a: VD, b: VD) => a)

  /**
   * Construct a vertex set from an RDD using an existing index and a
   * user defined `combiner` to merge duplicate vertices.
   *
   * @tparam VD the vertex attribute type
   * @param rdd the rdd containing vertices
   * @param index a VertexSetRDD whose indexes will be reused. The
   *              indexes must be a superset of the vertices in rdd
   * @param reduceFunc the user defined reduce function used to merge
   * duplicate vertex attributes.
   */
  def apply[VD: ClassManifest](
      rdd: RDD[(Vid, VD)],
      index: VertexSetIndex,
      reduceFunc: (VD, VD) => VD): VertexSetRDD[VD] =
    // TODO: Considering removing the following apply.
    apply(rdd, index, (v: VD) => v, reduceFunc, reduceFunc)

  /**
   * Construct a vertex set from an RDD of Product2[Vid, VD]
   *
   * @tparam VD the vertex attribute type
   * @param rdd the rdd containing vertices
   * @param index a VertexSetRDD whose indexes will be reused. The
   * indexes must be a superset of the vertices in rdd
   * @param reduceFunc the user defined reduce function used to merge
   * duplicate vertex attributes.
   */
  private[spark] def aggregate[VD: ClassManifest, VidVDPair <: Product2[Vid, VD] : ClassManifest](
      rdd: RDD[VidVDPair],
      index: VertexSetIndex,
      reduceFunc: (VD, VD) => VD): VertexSetRDD[VD] = {

    val cReduceFunc = rdd.context.clean(reduceFunc)
    assert(rdd.partitioner == Some(index.partitioner))
    // Use the index to build the new values table
    val partitionsRDD = index.rdd.zipPartitions(rdd, preservesPartitioning = true) {
      (indexIter, tblIter) =>
      // There is only one map
      val index = indexIter.next()
      val mask = new BitSet(index.capacity)
      val values = new Array[VD](index.capacity)
      for (vertexPair <- tblIter) {
        // Get the location of the key in the index
        val pos = index.getPos(vertexPair._1)
        if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) {
          throw new SparkException("Error: Trying to bind an external index " +
            "to an RDD which contains keys that are not in the index.")
        } else {
          // Get the actual index
          val ind = pos & OpenHashSet.POSITION_MASK
          // If this value has already been seen then merge
          if (mask.get(ind)) {
            values(ind) = cReduceFunc(values(ind), vertexPair._2)
          } else { // otherwise just store the new value
            mask.set(ind)
            values(ind) = vertexPair._2
          }
        }
      }
      Iterator(new VertexPartition(index, values, mask))
    }

    new VertexSetRDD(partitionsRDD)
  }

  /**
   * Construct a vertex set from an RDD using an existing index and a
   * user defined `combiner` to merge duplicate vertices.
   *
   * @tparam VD the vertex attribute type
   * @param rdd the rdd containing vertices
   * @param index the index which must be a superset of the vertices
   * in RDD
   * @param createCombiner a user defined function to create a combiner
   * from a vertex attribute
   * @param mergeValue a user defined function to merge a vertex
   * attribute into an existing combiner
   * @param mergeCombiners a user defined function to merge combiners
   *
   */
  def apply[VD: ClassManifest, C: ClassManifest](
      rdd: RDD[(Vid, VD)],
      index: VertexSetIndex,
      createCombiner: VD => C,
      mergeValue: (C, VD) => C,
      mergeCombiners: (C, C) => C): VertexSetRDD[C] = {
    val cCreateCombiner = rdd.context.clean(createCombiner)
    val cMergeValue = rdd.context.clean(mergeValue)
    val cMergeCombiners = rdd.context.clean(mergeCombiners)
    val partitioner = index.partitioner
    // Preaggregate and shuffle if necessary
    val partitioned =
      if (rdd.partitioner != Some(partitioner)) {
        // Preaggregation.
        val aggregator = new Aggregator[Vid, VD, C](cCreateCombiner, cMergeValue, cMergeCombiners)
        rdd.mapPartitions(aggregator.combineValuesByKey).partitionBy(partitioner)
      } else {
        rdd.mapValues(x => createCombiner(x))
      }

    aggregate(partitioned, index, mergeCombiners)
  } // end of apply

  /**
   * Construct an index of the unique vertices.  The resulting index
   * can be used to build VertexSets over subsets of the vertices in
   * the input.
   */
  def makeIndex(
      keys: RDD[Vid], partitionerOpt: Option[Partitioner] = None): VertexSetIndex = {
    val partitioner = partitionerOpt match {
      case Some(p) => p
      case None => Partitioner.defaultPartitioner(keys)
    }

    val preAgg: RDD[(Vid, Unit)] = keys.mapPartitions(iter => {
      val keys = new VertexIdToIndexMap
      while (iter.hasNext) { keys.add(iter.next) }
      keys.iterator.map(k => (k, ()))
    }, preservesPartitioning = true).partitionBy(partitioner)

    val index = preAgg.mapPartitions(iter => {
      val index = new VertexIdToIndexMap
      while (iter.hasNext) { index.add(iter.next._1) }
      Iterator(index)
    }, preservesPartitioning = true).cache

    new VertexSetIndex(index)
  }

  /**
   * Create a VertexSetRDD with all vertices initialized to the default value.
   *
   * @param index an index over the set of vertices
   * @param defaultValue the default value to use when initializing the vertices
   * @tparam VD the type of the vertex attribute
   * @return
   */
  def apply[VD: ClassManifest](index: VertexSetIndex, defaultValue: VD): VertexSetRDD[VD] = {
    // Use the index to build the new values tables
    val partitionsRDD = index.rdd.mapPartitions(_.map { index =>
      val values = Array.fill(index.capacity)(defaultValue)
      val mask = index.getBitSet
      new VertexPartition(index, values, mask)
    }, preservesPartitioning = true)
    new VertexSetRDD(partitionsRDD)
  } // end of apply
} // end of object VertexSetRDD
